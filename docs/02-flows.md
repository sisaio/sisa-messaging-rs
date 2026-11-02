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
    App->>DB: publish_in(tx): INSERT INTO outbox
    App->>DB: COMMIT
    loop every poll interval
        Dispatcher->>DB: claim due rows (FOR UPDATE SKIP LOCKED), move their due time by the lease
        Dispatcher->>Broker: publish each envelope, in sequence order
        Broker-->>Dispatcher: acknowledged
        Dispatcher->>DB: DELETE the published rows
    end
```

A rollback removes the outbox row with the business writes, so nothing is published. A dispatcher
that crashes after publishing and before deleting leaves rows that come back when the lease runs
out; they are published again with the same message id.

## Receiving on a transactional endpoint with the inbox

```mermaid
sequenceDiagram
    participant Broker
    participant Endpoint
    participant DB as Database
    participant Handler
    Broker->>Endpoint: delivery
    Endpoint->>Endpoint: decode the body
    Endpoint->>DB: BEGIN
    Endpoint->>DB: INSERT INTO inbox ON CONFLICT DO NOTHING
    alt recorded before
        Endpoint->>DB: COMMIT
        Endpoint->>Broker: ack
    else first time
        Endpoint->>Handler: handle(message, context with tx)
        Handler->>DB: business writes
        Handler-->>Endpoint: Ok, with publishes
        Endpoint->>DB: INSERT INTO outbox (the publishes)
        Endpoint->>DB: COMMIT
        Endpoint->>Broker: ack
    end
```

## Handler errors and retries

```mermaid
flowchart TD
    delivery([Delivery]) --> decode{Body decodes?}
    decode -->|no| error[Publish to the error destination] --> reject([Reject])
    decode -->|yes| handle[Run the handler]
    handle -->|Ok| commit[Commit, then ack]
    handle -->|Err| rollback[Roll back]
    rollback --> attempts{Attempts left?}
    attempts -->|yes| retry([Retry after the policy's delay])
    attempts -->|no| error
```
