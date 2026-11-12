-- The example's own tables, in the application's schema. The messaging tables come from the
-- library's migrations/postgresql, in a schema of their own.

CREATE TABLE orders (
    id          bigint GENERATED ALWAYS AS IDENTITY,
    customer    text   NOT NULL,
    total_cents bigint NOT NULL,
    CONSTRAINT pk_orders PRIMARY KEY (id)
);

CREATE TABLE invoices (
    order_id    bigint      NOT NULL,
    total_cents bigint      NOT NULL,
    sent_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_invoices PRIMARY KEY (order_id),
    CONSTRAINT fk_invoices_order_id FOREIGN KEY (order_id) REFERENCES orders (id)
);
