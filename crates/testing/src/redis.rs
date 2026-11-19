//! One Redis server, or a server that speaks its protocol: Valkey or Dragonfly.

use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt, core::IntoContainerPort,
    runners::AsyncRunner,
};

use crate::server::{free_port, image, wait_for};

const PORT: u16 = 6379;

/// A Redis server of its own for one test, in Docker, which a test can stop and restart. It is
/// removed when this value drops.
///
/// `REDIS_IMAGE` picks the image, such as `valkey/valkey:9.1-alpine` or
/// `docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2`; the default is `redis:8.10-alpine`.
/// Every one of them listens on 6379 with no password.
pub struct Redis {
    container: ContainerAsync<GenericImage>,
    url: String,
    restartable: bool,
}

impl std::fmt::Debug for Redis {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Redis")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

fn request() -> ContainerRequest<GenericImage> {
    image("REDIS_IMAGE", "redis:8.10-alpine")
        .with_exposed_port(PORT.tcp())
        .into()
}

impl Redis {
    /// A server on a host port Docker picks.
    pub async fn start() -> Self {
        let container = request()
            .start()
            .await
            .expect("Docker must be running to start Redis for the tests");
        let port = container.get_host_port_ipv4(PORT).await.unwrap();

        Self::ready(container, port, false).await
    }

    /// A server that keeps its address across [`stop`](Self::stop) and
    /// [`restart`](Self::restart), and its data as each server saves it on shutdown.
    pub async fn start_restartable() -> Self {
        let port = free_port();
        let container = request()
            .with_mapped_port(port, PORT.tcp())
            .start()
            .await
            .expect("Docker must be running to start Redis for the tests");

        Self::ready(container, port, true).await
    }

    async fn ready(container: ContainerAsync<GenericImage>, port: u16, restartable: bool) -> Self {
        let redis = Self {
            container,
            url: format!("redis://127.0.0.1:{port}"),
            restartable,
        };

        redis.wait_until_ready().await;
        redis
    }

    async fn wait_until_ready(&self) {
        wait_for("Redis answers", async || {
            match self.try_connection().await {
                Some(mut connection) => redis::cmd("PING")
                    .query_async::<()>(&mut connection)
                    .await
                    .is_ok(),
                None => false,
            }
        })
        .await;
    }

    /// Stops the server as an orchestrator would, so it saves its data before it exits.
    /// Connections drop and new ones are refused.
    pub async fn stop(&self) {
        self.container.stop_with_timeout(Some(10)).await.unwrap();
    }

    /// Starts a stopped server again at the same address, with the data it saved, and waits until
    /// it answers.
    pub async fn restart(&self) {
        assert!(
            self.restartable,
            "only a server from `Redis::start_restartable` keeps its address"
        );
        self.container.start().await.unwrap();
        self.wait_until_ready().await;
    }

    async fn try_connection(&self) -> Option<redis::aio::MultiplexedConnection> {
        redis::Client::open(self.url.as_str())
            .ok()?
            .get_multiplexed_async_connection()
            .await
            .ok()
    }

    /// The server's URL, such as `redis://127.0.0.1:32768`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// A new connection to the server.
    pub async fn connection(&self) -> redis::aio::MultiplexedConnection {
        self.try_connection().await.unwrap()
    }
}
