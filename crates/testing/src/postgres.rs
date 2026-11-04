//! One PostgreSQL server with an empty database, which a test can stop and restart.

use sqlx::{Connection, PgConnection, PgPool};
use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt, core::IntoContainerPort,
    runners::AsyncRunner,
};

use crate::server::{free_port, image, wait_for};

const PORT: u16 = 5432;

fn request() -> ContainerRequest<GenericImage> {
    image("POSTGRES_IMAGE", "postgres:18-alpine")
        .with_exposed_port(PORT.tcp())
        .with_env_var("POSTGRES_PASSWORD", "postgres")
}

/// A PostgreSQL server of its own for one test, in Docker, so its database starts empty. It is
/// removed when this value drops.
///
/// `POSTGRES_IMAGE` picks the image, such as `postgres:18.1-alpine`; the default is
/// `postgres:18-alpine`.
pub struct Postgres {
    container: ContainerAsync<GenericImage>,
    url: String,
    restartable: bool,
}

impl std::fmt::Debug for Postgres {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Postgres")
            .field("port", &self.url.rsplit_once(':').map(|(_, port)| port))
            .finish_non_exhaustive()
    }
}

impl Postgres {
    /// A server on a host port Docker picks.
    pub async fn start() -> Self {
        let container = request()
            .start()
            .await
            .expect("Docker must be running to start PostgreSQL for the tests");
        let port = container.get_host_port_ipv4(PORT).await.unwrap();

        Self::ready(container, port, false).await
    }

    /// A server that keeps its address and its data across [`stop`](Self::stop) and
    /// [`restart`](Self::restart).
    pub async fn start_restartable() -> Self {
        let port = free_port();
        let container = request()
            .with_mapped_port(port, PORT.tcp())
            .start()
            .await
            .expect("Docker must be running to start PostgreSQL for the tests");

        Self::ready(container, port, true).await
    }

    async fn ready(container: ContainerAsync<GenericImage>, port: u16, restartable: bool) -> Self {
        let postgres = Self {
            container,
            url: format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
            restartable,
        };

        postgres.wait_until_ready().await;
        postgres
    }

    // The image's first start runs a server for its init scripts that listens on no TCP port, then
    // restarts it, so a query over TCP only succeeds once the real server is up.
    async fn wait_until_ready(&self) {
        wait_for(
            "PostgreSQL answers",
            async || match PgConnection::connect(&self.url).await {
                Ok(mut connection) => sqlx::query("SELECT 1")
                    .execute(&mut connection)
                    .await
                    .is_ok(),
                Err(_) => false,
            },
        )
        .await;
    }

    /// Kills the server, as a crash would: connections drop and new ones are refused.
    pub async fn stop(&self) {
        self.container.stop_with_timeout(Some(0)).await.unwrap();
    }

    /// Starts a stopped server again at the same address, with the data it had, and waits until it
    /// answers.
    pub async fn restart(&self) {
        assert!(
            self.restartable,
            "only a server from `Postgres::start_restartable` keeps its address"
        );
        self.container.start().await.unwrap();
        self.wait_until_ready().await;
    }

    /// The database's URL, with its user and password.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// A new pool on the database.
    pub async fn pool(&self) -> PgPool {
        PgPool::connect(&self.url).await.unwrap()
    }
}
