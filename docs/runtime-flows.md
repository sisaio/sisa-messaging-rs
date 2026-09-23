# Runtime flows

## 1. Durable outbox enqueue

```mermaid
sequenceDiagram
    autonumber
    actor App as Application
    participant Store as PostgresOutboxStore
    participant DB as PostgreSQL transaction

    App->>DB: BEGIN
    App->>DB: Write business state
    App->>Store: enqueue(tx, envelope)
    Store->>Store: Serialize envelope before I/O
    Store->>DB: INSERT outbox_messages
    alt message_id is new
        DB-->>Store: Inserted
        Store-->>App: Success
        App->>DB: COMMIT
        DB-->>App: Business state and message visible together
    else message_id already exists
        DB-->>Store: Unique violation
        Store-->>App: Enqueue error
        App->>DB: ROLLBACK
    end
```

The application owns the transaction. Enqueue performs serialization before awaiting the insert,
executes one insert, and never commits, rolls back, retries, sleeps, or publishes.

A reused `message_id` is an error. With PostgreSQL it aborts the caller's transaction, preventing a
business write from committing without the corresponding new message.

Calling a transport `Publisher` directly is a different operation: it sends immediately, does not
join the database transaction, and receives none of the outbox guarantees.

## 2. Outbox dispatch

```mermaid
sequenceDiagram
    autonumber
    participant Dispatcher
    participant DB as PostgreSQL
    participant Publisher
    participant Broker

    Dispatcher->>DB: Claim only available capacity
    Note over Dispatcher,DB: CTE + FOR UPDATE SKIP LOCKED<br/>short database transaction
    DB-->>Dispatcher: Rows + fresh claim tokens

    loop Each record, bounded by max_in_flight
        Dispatcher->>Publisher: publish(record)
        Publisher->>Broker: Publish once
        alt Broker acknowledges
            Broker-->>Publisher: Acknowledgement
            Publisher-->>Dispatcher: Published
            Dispatcher->>DB: Complete WHERE id + claim_token
        else Transient, permanent, or timeout
            Broker-->>Publisher: Failure or no acknowledgement
            Publisher-->>Dispatcher: Classified failure
            Dispatcher->>DB: Retry or dead WHERE id + claim_token
        end
        DB-->>Dispatcher: Confirmed affected-row count
        opt No row matched
            Note over Dispatcher,DB: Claim was superseded, treat as fencing outcome
        end
    end
```

### Claim

The dispatcher claims only enough rows to fill available `max_in_flight` capacity. A claim:

1. selects active, due, unexpired rows;
2. applies the per-key head condition when `Message::order_by` resolved an `ordering_key`; the
   stored UUIDv7 row ID only breaks ties between rows with that same key;
3. locks candidates with `FOR UPDATE SKIP LOCKED`;
4. stamps each row with `locked_by`, a fresh database-generated `claim_token`, and
   `claimable_at = now() + lease`;
5. returns the rows after the statement commits.

Publication never occurs while a row lock or database transaction is held.

```mermaid
flowchart TD
    Candidate[Active, due, unexpired candidate] --> Ordered{ordering_key present?}
    Ordered -- No --> Lock[FOR UPDATE SKIP LOCKED]
    Ordered -- Yes --> Head{Lowest durable row ID among<br/>eligible rows for this key?}
    Head -- No --> Blocked[Not claimable in this pass]
    Head -- Yes --> Lock
    Lock --> Stamp[Set locked_by, fresh claim_token,<br/>and lease deadline]
    Stamp --> Return[Commit and return claimed row]
```

### Publish and outcome

Each claimed message is published with bounded concurrency and an explicit timeout. The publisher
returns only after the broker acknowledges the operation or a classified failure occurs. It does
not retry internally; the outbox retry policy owns retry scheduling.

- Acknowledged publish: `complete` increments `attempts`, sets `published_at`, and clears the
  claim.
- Transient failure with retry budget: `fail` increments `attempts`, records a safe error summary,
  moves `claimable_at` by the retry delay, and clears the claim.
- Permanent failure or exhausted attempts: `fail` increments `attempts`, sets `dead_at` and
  `dead_reason`, records a safe error summary, and clears the claim.
- Lost claim: an outcome statement affects no row. This is normal fencing, not a store error.

```mermaid
flowchart TD
    Result[Publish result] --> Ack{Acknowledged?}
    Ack -- Yes --> Complete[Complete and clear claim]
    Ack -- No --> Retryable{Transient and retry budget remains?}
    Retryable -- Yes --> Retry[Record safe error,<br/>schedule backoff, clear claim]
    Retryable -- No --> Dead[Record safe error and death reason,<br/>mark dead, clear claim]
    Complete --> Fence{Outcome row matched<br/>id + claim_token?}
    Retry --> Fence
    Dead --> Fence
    Fence -- Yes --> Observe[Emit transition log and metric]
    Fence -- No --> Lost[Benign lost-claim outcome]
```

Metrics and state-transition logs are emitted only after the database confirms the affected rows.
Database backlog gauges remain authoritative because a committed write can succeed while its
client acknowledgement is lost.

### Lease renewal

While a publish is unresolved, renewal runs before the lease can expire. Renewal uses the original
claim token and moves only `claimable_at`. A shortfall means another claim already superseded this
one.

`store_timeout` must remain below half the lease. Every dispatcher loop turn performs at most one
bounded external store call before polling lease readiness again.

```mermaid
sequenceDiagram
    participant Dispatcher
    participant Publisher
    participant DB as PostgreSQL

    par Publish remains unresolved
        Dispatcher->>Publisher: Await bounded publish
        Publisher-->>Dispatcher: Acknowledged or failed
    and Renew before lease threshold
        Dispatcher->>DB: Extend WHERE id + original claim_token
        alt Claim still owned
            DB-->>Dispatcher: Lease deadline moved, token unchanged
        else Claim superseded
            DB-->>Dispatcher: Zero rows affected
        end
    end
```

### Poison rows

Failure to decode one database row does not fail the batch. The provider returns healthy records,
reports the unreadable row as poisoned, and attempts to mark it dead with `undecodable`, fenced by
the claim token. If that follow-up fails after the claim committed, the claim result is still
returned and the row is retried after its lease lapses.

## 3. Duplicate windows

At-least-once publication necessarily permits duplicates:

1. **Publish/complete crash:** the broker accepts the message, then the worker dies before the
   database records completion.
2. **Lease expiry:** publication outlives the lease and another worker reclaims the row.
3. **Shutdown drain:** a publish remains unresolved at the drain deadline and the row is released
   or later reclaimed.
4. **Outcome acknowledgement loss:** PostgreSQL commits completion, but the client does not receive
   confirmation and cannot safely infer the result.

```mermaid
flowchart LR
    Crash[Broker accepted;<br/>worker crashes before complete] --> Duplicate[Duplicate publication possible]
    Lease[Publish outlives lease;<br/>another worker reclaims] --> Duplicate
    Drain[Drain deadline leaves<br/>publish unresolved] --> Duplicate
    AckLoss[Database commits outcome;<br/>client loses confirmation] --> Duplicate
    Duplicate --> Fence[Fencing protects database state]
    Duplicate --> Idempotent[Consumer must be idempotent]
```

Fencing prevents stale workers from corrupting newer state; it cannot retract a message already
accepted by the broker. Consumers must be idempotent.

## 4. Graceful dispatcher shutdown

On cancellation the dispatcher:

1. stops claiming new rows;
2. releases claimed rows whose publication has not started;
3. waits up to `drain_timeout` for in-flight publishes;
4. persists the outcomes that resolved;
5. releases unresolved claims;
6. returns.

```mermaid
sequenceDiagram
    autonumber
    actor Supervisor
    participant Dispatcher
    participant Tasks as Publish tasks
    participant DB as PostgreSQL

    Supervisor->>Dispatcher: Cancel
    Dispatcher->>Dispatcher: Stop claiming
    Dispatcher->>DB: Release claims not yet publishing
    Dispatcher->>Tasks: Drain up to drain_timeout
    alt Publish resolves before deadline
        Tasks-->>Dispatcher: Publish outcome
        Dispatcher->>DB: Persist complete, retry, or dead
    else Drain deadline expires
        Dispatcher->>Tasks: Stop waiting
        Dispatcher->>DB: Release unresolved claims
    end
    Dispatcher-->>Supervisor: Return shutdown result
```

Store calls already executing are allowed to reach their explicit `store_timeout`; they are not
dropped merely because a different `select!` branch became ready.

Worker-loop rule: a `tokio::select!` branch future may only wait for cancel-safe readiness—a
cancellation token, timer, channel receive with defined cancellation behavior, or task join.
Externally visible database and broker I/O happens in the selected arm body.

## 5. Inbox processing

```mermaid
sequenceDiagram
    autonumber
    participant Source as Delivery source
    participant Consumer
    participant Tx as PostgreSQL transaction
    participant Inbox as PostgresInboxStore
    participant Handler
    participant Broker

    Source-->>Consumer: Delivery
    Consumer->>Tx: BEGIN
    Consumer->>Inbox: claim(tx, scope, message_id)
    Inbox->>Tx: Advisory transaction lock + receipt state
    Tx-->>Inbox: Claimed
    Inbox-->>Consumer: Claimed
    Consumer->>Handler: handle(envelope, tx)
    Handler-->>Consumer: Success
    Consumer->>Inbox: complete(tx, receipt)
    Inbox->>Tx: Mark completed
    Consumer->>Tx: COMMIT
    Tx-->>Consumer: Commit confirmed
    Consumer->>Broker: ACK
```

### Claim outcomes

| Outcome | Transaction action | Broker action |
|---|---|---|
| `Claimed` | Run handler; complete; commit | Ack after commit |
| `AlreadyCompleted` | Roll back/drop transaction | Ack |
| `InProgress` | Roll back/drop transaction | NAK with delay |
| `Dead` | Roll back/drop transaction | Terminate; do not redeliver |
| Store error | Roll back | NAK |

The claim uses a transaction-scoped advisory lock for `(scope, message_id)`, then inserts the
receipt or reads its existing state. This prevents two live transactions from executing the same
handler concurrently while allowing completed redeliveries to resolve quickly.

### Handler failure

If the handler fails:

1. roll back the application transaction;
2. call `fail` with the error's `FailureKind` through the store's pool, outside the rolled-back
   transaction;
3. atomically increment `attempts` and transition to dead immediately for a permanent failure or
   when a transient failure reaches `max_attempts`;
4. NAK when recorded for retry, terminate when dead, or ack if another transaction already
   completed the message.

```mermaid
sequenceDiagram
    autonumber
    participant Consumer
    participant Tx as PostgreSQL transaction
    participant Handler
    participant Inbox as PostgresInboxStore pool path
    participant Broker

    Consumer->>Handler: handle(envelope, tx)
    Handler-->>Consumer: Error + FailureKind
    Consumer->>Tx: ROLLBACK
    Tx-->>Consumer: Rollback confirmed
    Consumer->>Inbox: fail(scope, message_id, FailureKind)
    alt Recorded for retry
        Inbox-->>Consumer: Retry outcome
        Consumer->>Broker: Delayed NAK
    else Permanent or attempts exhausted
        Inbox-->>Consumer: Dead outcome
        Consumer->>Broker: TERMINATE
    else Another transaction completed first
        Inbox-->>Consumer: Already completed
        Consumer->>Broker: ACK
    else Failure recording fails
        Inbox-->>Consumer: Store error
        Consumer->>Broker: Leave unresolved or delayed NAK
    end
```

Failure recording is best effort. A crash between rollback and failure recording produces an
uncounted attempt and a later redelivery, never false completion. Ack-before-commit is forbidden
because it converts a commit failure into message loss.

## 6. Consumer framework

For individual delivery (including NATS), the recommended integration delegates the section 5
protocol to `sisa-messaging-consumer`:

```mermaid
flowchart LR
    Receive[Receive delivery] --> Map[Map transport wire data]
    Map --> Decode[Decode typed envelope]
    Decode --> Begin[Begin transaction]
    Begin --> Claim[Claim inbox receipt]
    Claim --> Handle[Run typed handler]
    Handle --> Complete[Complete inbox receipt]
    Complete --> Commit[Commit transaction]
    Commit --> Plan[Create settlement plan]
    Plan --> Settle[Ack, delayed NAK, TERMINATE,<br/>or leave unresolved]
```

The source stops polling at `max_in_flight`. Each received delivery retains its broker settlement
handle while an owned workflow task performs the database and handler work. A coordinator can send
heartbeat acknowledgements while waiting without cancelling or repolling the workflow future.

The individual-delivery workflow returns a private settlement plan:

| Result | Settlement |
|---|---|
| Commit confirmed | Ack |
| Already completed | Ack |
| Another transaction in progress | Delayed NAK |
| Transient handler/store failure | Delayed NAK |
| Permanent or exhausted failure | Terminate |
| Decode poison without safe identity | Terminate |
| Commit result ambiguous | Delayed NAK; never record handler failure |
| Permanent provider/settlement failure | Leave unresolved; stop receiving and return fatal error |

```mermaid
flowchart TD
    Result[Private workflow result] --> Kind{Result kind}
    Kind -- Commit confirmed<br/>or already completed --> Ack[ACK]
    Kind -- In progress<br/>or transient failure --> Nak[Delayed NAK]
    Kind -- Permanent, exhausted,<br/>or decode poison --> Terminate[TERMINATE]
    Kind -- Commit ambiguous --> Ambiguous[Delayed NAK;<br/>do not record handler failure]
    Kind -- Permanent provider<br/>or settlement failure --> Fatal[Leave unresolved;<br/>stop receiving and return fatal]
```

On cancellation it stops receiving, drains already received work for a bounded duration, settles
completed work, then drops remaining transactions and leaves their deliveries unacknowledged for
redelivery. See [Consumer framework](consumer-framework.md) for the API and complete state machine.

Partitioned-log sources use the same inbox transaction outcomes but not the individual-delivery
ACK/NAK/TERMINATE operations above. After a confirmed commit or durable terminal disposition, the
partition workflow may `Advance` its ordered cursor; otherwise it must `LeaveUnresolved`. It never
advances past an earlier unresolved record. Ownership loss is a distinct receive outcome, not a
clean source close: it fences the old settlement generation and blocks that partition until its
cursor and ownership are reconciled. An indeterminate advance similarly pauses only the affected
partition until reconciliation proves whether the cursor moved; other partitions may continue.

## 7. NATS publish

```mermaid
sequenceDiagram
    autonumber
    participant Dispatcher
    participant Publisher as NatsPublisher
    participant Resolver as Subject resolver
    participant NATS as JetStream

    Dispatcher->>Publisher: publish(serialized envelope)
    Publisher->>Resolver: Resolve subject
    Resolver-->>Publisher: Subject
    Publisher->>Publisher: Project metadata and custom headers
    Publisher->>Publisher: Set broker deduplication ID
    Publisher->>Publisher: Check negotiated payload limit
    Publisher->>NATS: Publish once under publish_timeout
    alt JetStream acknowledges
        NATS-->>Publisher: Publish acknowledgement
        Publisher-->>Dispatcher: Success
    else Rejected, disconnected, or timed out
        NATS-->>Publisher: Failure or no acknowledgement
        Publisher-->>Dispatcher: Classified failure
    end
```

For one `SerializedEnvelope`, the NATS publisher resolves a subject, projects framework metadata
and custom headers, applies the broker deduplication ID, checks the current negotiated payload
limit, publishes once, and awaits the JetStream acknowledgement under `publish_timeout`.

The application owns the NATS client and JetStream context. The library neither connects nor
creates streams or consumers.

## 8. Maintenance

Maintenance is explicit and independently supervised.

```mermaid
flowchart TD
    Supervisor[Application-owned scheduler or task] --> Kind{Maintenance capability}
    Kind -- Outbox --> Outbox[Expire eligible rows and purge<br/>old published or dead rows]
    Kind -- Inbox --> Inbox[Purge old completed or dead receipts]
    Kind -- Observe --> Stats[Read database-authoritative levels]
    Outbox --> Limit{Pass reached configured limit?}
    Inbox --> Limit
    Limit -- Yes --> Yield[Yield and check cancellation]
    Yield --> Supervisor
    Limit -- No --> Done[Pass complete]
    Stats --> Gauge[Record OTel gauges,<br/>including zero values]
```

- An outbox purge deletes old published/dead rows and transitions expired, non-terminal,
  non-actively-leased rows to dead.
- An inbox purge deletes old completed/dead receipts and never automatically deletes
  pending/retrying receipts.
- Each call is a bounded pass. The caller repeats while progress reaches the limit, yielding and
  honoring cancellation between passes.
- One application-owned observer per database/schema reads database-authoritative outbox levels
  and records OTel gauges. Statistics are not polled by every dispatcher.
