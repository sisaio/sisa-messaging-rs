#![forbid(unsafe_code)]

#[path = "postgres/inbox.rs"]
mod inbox;
#[path = "postgres/outbox.rs"]
mod outbox;
#[path = "postgres/plans.rs"]
mod plans;
#[path = "postgres/support.rs"]
mod support;
