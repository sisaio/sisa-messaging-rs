//! One NATS server with JetStream, which a test can stop and restart.

use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt, core::IntoContainerPort,
    runners::AsyncRunner,
};

use crate::server::{free_port, image, wait_for};

const PORT: u16 = 4222;

fn request() -> ContainerRequest<GenericImage> {
    image("NATS_IMAGE", "nats:2.15-alpine")
        .with_exposed_port(PORT.tcp())
        .with_cmd(["--jetstream"])
}

/// A NATS server of its own for one test, in Docker, with JetStream on. It is removed when this
/// value drops.
///
/// `NATS_IMAGE` picks the image, such as `nats:2.14-alpine`; the default is `nats:2.15-alpine`.
pub struct Nats {
    container: ContainerAsync<GenericImage>,
    url: String,
    restartable: bool,
}

impl std::fmt::Debug for Nats {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Nats")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl Nats {
    /// A server on a host port Docker picks.
    pub async fn start() -> Self {
        let container = request()
            .start()
            .await
            .expect("Docker must be running to start NATS for the tests");
        let port = container.get_host_port_ipv4(PORT).await.unwrap();

        Self::ready(container, port, false).await
    }

    /// A server that keeps its address and its streams across [`stop`](Self::stop) and
    /// [`restart`](Self::restart).
    pub async fn start_restartable() -> Self {
        let port = free_port();
        let container = request()
            .with_mapped_port(port, PORT.tcp())
            .start()
            .await
            .expect("Docker must be running to start NATS for the tests");

        Self::ready(container, port, true).await
    }

    async fn ready(container: ContainerAsync<GenericImage>, port: u16, restartable: bool) -> Self {
        let nats = Self {
            container,
            url: format!("nats://127.0.0.1:{port}"),
            restartable,
        };

        nats.wait_until_ready().await;
        nats
    }

    // The server answers before JetStream has loaded its streams, so wait for JetStream itself.
    async fn wait_until_ready(&self) {
        wait_for("JetStream answers", async || {
            match async_nats::connect(self.url.as_str()).await {
                Ok(client) => async_nats::jetstream::new(client)
                    .query_account()
                    .await
                    .is_ok(),
                Err(_) => false,
            }
        })
        .await;
    }

    /// Kills the server, as a crash would: connections drop and new ones are refused.
    pub async fn stop(&self) {
        self.container.stop_with_timeout(Some(0)).await.unwrap();
    }

    /// Starts a stopped server again at the same address, with the streams it had, and waits until
    /// JetStream answers.
    pub async fn restart(&self) {
        assert!(
            self.restartable,
            "only a server from `Nats::start_restartable` keeps its address"
        );
        self.container.start().await.unwrap();
        self.wait_until_ready().await;
    }

    /// The server's URL, such as `nats://127.0.0.1:32768`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// A new client connected to the server.
    pub async fn client(&self) -> async_nats::Client {
        async_nats::connect(self.url.as_str()).await.unwrap()
    }

    /// A JetStream context on a new client.
    pub async fn jetstream(&self) -> async_nats::jetstream::Context {
        async_nats::jetstream::new(self.client().await)
    }
}
