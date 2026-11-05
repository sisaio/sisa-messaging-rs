# Flows

Diagrams of each step that [Architecture](01-architecture.md) describes.

## Publishing without an outbox

```mermaid
sequenceDiagram
    participant App
    participant Bus
    participant Broker
    App->>Bus: publish(message)
    Bus->>Bus: encode, build the envelope
    Bus->>Broker: publish(envelope)
    Broker-->>Bus: acknowledged
    Bus-->>App: Ok
```

If the application's database transaction then fails, the message is out anyway. That is what the
outbox prevents.

## Publishing through the outbox

```mermaid
sequenceDiagram
    participant App
    participant DB as Database
    participant Dispatcher
    participant Broker
    App->>DB: BEGIN, business writes
    App->>DB: bus.outbox(&mut tx).publish: INSERT INTO outbox_messages
    App->>DB: COMMIT
    loop every poll interval
        Dispatcher->>DB: claim due rows (FOR UPDATE SKIP LOCKED), move their due time by the lease, sort by id
        Dispatcher->>Broker: publish each envelope, in id order
        Broker-->>Dispatcher: acknowledged
        Dispatcher->>DB: mark the rows dispatched
    end
```

A rollback removes the outbox row with the business writes, so nothing is published. A dispatcher
that crashes after publishing and before marking leaves rows that come back when the lease runs
out; they are published again with the same message id.

## Receiving on an endpoint with an inbox

```mermaid
sequenceDiagram
    participant Broker
    participant Endpoint
    participant DB as Database
    participant Handler
    Broker->>Endpoint: delivery
    Endpoint->>Endpoint: decode the data
    Endpoint->>DB: BEGIN
    Endpoint->>DB: INSERT INTO inbox_receipts ON CONFLICT DO NOTHING
    alt recorded before
        Endpoint->>DB: COMMIT
        Endpoint->>Broker: ack
    else first time
        Endpoint->>Handler: handle(message, context with tx)
        Handler->>DB: business writes
        Handler-->>Endpoint: Ok, with publishes
        Endpoint->>DB: INSERT INTO outbox_messages (the publishes)
        Endpoint->>DB: COMMIT
        Endpoint->>Broker: ack
    end
```

## Handler errors and retries

```mermaid
flowchart TD
    delivery([Delivery]) --> decode{Data decodes?}
    decode -->|no| error[Publish to the error destination]
    error -->|published| reject([Reject])
    error -->|failed| retry
    decode -->|yes| handle[Run the handler]
    handle -->|Ok| commit[Commit, then ack]
    handle -->|Err| rollback[Roll back]
    rollback --> attempts{Attempts left?}
    attempts -->|yes| retry([Retry after the policy's delay])
    attempts -->|no| error
```

## Partition maintenance

```mermaid
flowchart TD
    tick([Every hour]) --> lock{Advisory lock taken?}
    lock -->|no, another instance runs it| done([Wait for the next tick])
    lock -->|yes| ahead[Create the partitions for the next 7 intervals]
    ahead --> retention{Retention set?}
    retention -->|no| done
    retention -->|yes| each[Each partition older than the retention]
    each --> pending{Outbox partition with pending or failed rows?}
    pending -->|yes| warn[Skip it and warn] --> done
    pending -->|no| expire[Detach it, or drop it with on_expiry = drop] --> done
```
