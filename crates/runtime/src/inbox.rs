//! The inbox step: a transaction per delivery, with a receipt that makes a redelivery a no-op.

use std::future;
use std::ops::DerefMut;
use std::sync::Arc;

use sisa_messaging_core::{Envelope, InboxStore, OutboxStore, StoreError};

use crate::endpoint::{BoxFuture, Failure, Handlers, Pipeline, Plain, call, find};
use crate::{HandlerContext, NoOutbox};

/// The type of an endpoint without an inbox: its handlers take a [`HandlerContext`] without a
/// transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct NoInbox;

/// What an endpoint's inbox hands its handlers: no transaction without an inbox, the store's
/// transaction with one. Implemented for [`NoInbox`] and every [`InboxStore`]; not for use on its
/// own.
#[doc(hidden)]
pub trait EndpointInbox: Send + Sync + 'static {
    type Transaction: Send + 'static;

    const TRANSACTIONAL: bool;
}

// No transaction.
impl EndpointInbox for NoInbox {
    type Transaction = ();

    const TRANSACTIONAL: bool = false;
}

// The store's transaction.
impl<Store> EndpointInbox for Store
where
    Store: InboxStore,
    Store::Transaction: 'static,
{
    type Transaction = Store::Transaction;

    const TRANSACTIONAL: bool = true;
}

/// What a bus's outbox does with the publishes of a handler on an endpoint with an inbox.
/// Implemented for [`NoOutbox`] and every [`OutboxStore`] that writes on the inbox's
/// transaction; not for use on its own.
#[doc(hidden)]
pub trait HandlerOutbox<Transaction>: Send + Sync + 'static {
    fn write(
        &self,
        transaction: &mut Transaction,
        held: &mut Vec<Envelope>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

// Leaves them held, so the endpoint sends them after the commit.
impl<Transaction> HandlerOutbox<Transaction> for NoOutbox {
    fn write(
        &self,
        _transaction: &mut Transaction,
        _held: &mut Vec<Envelope>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        future::ready(Ok(()))
    }
}

// Writes them to the outbox in the handler's transaction, so they go out if and only if it
// commits.
impl<Store, Transaction> HandlerOutbox<Transaction> for Store
where
    Store: OutboxStore,
    Transaction: DerefMut<Target = Store::Connection> + Send,
{
    async fn write(
        &self,
        transaction: &mut Transaction,
        held: &mut Vec<Envelope>,
    ) -> Result<(), StoreError> {
        if !held.is_empty() {
            self.insert(&mut **transaction, held).await?;
            held.clear();
        }
        Ok(())
    }
}

/// Builds an endpoint's pipeline on a bus whose outbox is `Outbox`. Implemented for [`NoInbox`]
/// with any outbox, and for an [`InboxStore`] whose transaction the outbox writes on, so an
/// outbox on another database doesn't compile; not for use on its own.
#[doc(hidden)]
pub trait InboxPipeline<Outbox>: EndpointInbox {
    fn pipeline(
        self,
        endpoint: String,
        handlers: Handlers<Self::Transaction>,
        outbox: &Arc<Outbox>,
    ) -> Box<dyn Pipeline>;
}

// The handler alone.
impl<Outbox> InboxPipeline<Outbox> for NoInbox {
    fn pipeline(
        self,
        endpoint: String,
        handlers: Handlers<()>,
        _outbox: &Arc<Outbox>,
    ) -> Box<dyn Pipeline> {
        Box::new(Plain { endpoint, handlers })
    }
}

// The handler in a transaction with its receipt.
impl<Store, Outbox> InboxPipeline<Outbox> for Store
where
    Store: InboxStore,
    Store::Transaction: 'static,
    Outbox: HandlerOutbox<Store::Transaction>,
{
    fn pipeline(
        self,
        endpoint: String,
        handlers: Handlers<Store::Transaction>,
        outbox: &Arc<Outbox>,
    ) -> Box<dyn Pipeline> {
        Box::new(Transactional {
            endpoint,
            handlers,
            inbox: self,
            outbox: Arc::clone(outbox),
        })
    }
}

// An endpoint with an inbox.
struct Transactional<Store: InboxStore, Outbox> {
    endpoint: String,
    handlers: Handlers<Store::Transaction>,
    inbox: Store,
    outbox: Arc<Outbox>,
}

// Begins a transaction, records the message, runs the handler, writes its publishes to the outbox
// and commits. A message recorded before is committed at once, without running the handler. On a
// failure the transaction drops, which rolls the receipt back with the handler's work.
impl<Store, Outbox> Pipeline for Transactional<Store, Outbox>
where
    Store: InboxStore,
    Store::Transaction: 'static,
    Outbox: HandlerOutbox<Store::Transaction>,
{
    fn run(&self, context: HandlerContext) -> BoxFuture<'_, Result<Vec<Envelope>, Failure>> {
        Box::pin(async move {
            let handler = find(&self.handlers, &self.endpoint, context.envelope())?;
            let mut transaction = self.inbox.begin().await?;
            let recorded = self
                .inbox
                .record(&mut transaction, &self.endpoint, context.envelope())
                .await?;
            if !recorded {
                tracing::debug!(
                    endpoint = %self.endpoint,
                    message_id = %context.envelope().id,
                    "handled before; acking without running the handler",
                );
                self.inbox.commit(transaction).await?;
                return Ok(Vec::new());
            }

            let (context, ()) = context.with_transaction(transaction);
            let mut handled = call(handler, context).await?;
            self.outbox
                .write(&mut handled.transaction, &mut handled.held)
                .await?;
            self.inbox.commit(handled.transaction).await?;
            Ok(handled.held)
        })
    }
}
