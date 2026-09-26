//! Application business logic invoked inside a framework-owned transaction.

use std::error::Error;
use std::future::Future;

use sisa_messaging::{Envelope, ErrorClassifier};

/// Handles one typed envelope inside the transaction that also completes its inbox receipt.
///
/// The handler receives the transaction by mutable reference and must not commit, roll back, or
/// retain it; the consumer commits only after the handler and inbox completion succeed. The
/// complete envelope is available so metadata can be propagated to business writes or outbound
/// messages.
///
/// A returned error is classified through [`ErrorClassifier`] and recorded after rollback with
/// [`ErrorSummary::from_safe_error`](sisa_messaging::ErrorSummary::from_safe_error). Its
/// `Display` output and complete `source` chain must therefore be safe to persist: they must not
/// contain payload bytes, header values, credentials, or connection details. The consumer never
/// emits handler error text to telemetry.
///
/// The consumer does not time out or catch panics from handler code. A panic drops the
/// transaction, leaves the delivery unsettled, and stops the consumer with
/// [`ConsumerErrorKind::HandlerPanicked`](crate::ConsumerErrorKind::HandlerPanicked).
pub trait ConsumerHandler<M, Tx>: Send + Sync {
    /// Application error with an explicit retry classification.
    type Error: Error + Send + Sync + 'static + ErrorClassifier;

    /// Applies business effects for one envelope inside `tx`.
    fn handle(
        &self,
        tx: &mut Tx,
        envelope: &Envelope<M>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
