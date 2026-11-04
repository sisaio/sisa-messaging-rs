//! What both servers share: the image an environment variable names, a host port that survives a
//! restart, and waiting for a server to answer.

use std::time::{Duration, Instant};

use testcontainers::GenericImage;

/// The image `variable` names, such as `nats:2.14-alpine`, or `default` when it is unset.
pub(crate) fn image(variable: &str, default: &str) -> GenericImage {
    let image = std::env::var(variable).unwrap_or_else(|_| default.into());
    let (name, tag) = image.rsplit_once(':').unwrap_or((&image, "latest"));

    GenericImage::new(name, tag)
}

/// A free host port. Docker gives a restarted container a new host port unless it was told one,
/// so a restartable server is mapped to this. Another process can take the port before Docker
/// binds it, so only tests that restart a server use it.
pub(crate) fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Waits up to 60 s for `check`: a server's first start can take that long on a busy machine.
pub(crate) async fn wait_for(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);

    while !check().await {
        assert!(Instant::now() < deadline, "timed out after 60 s: {what}");

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
