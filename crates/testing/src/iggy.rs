//! One Apache Iggy server, which a test can stop and restart.

use iggy::prelude::{Client, IggyClient, SystemClient};
use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt, core::IntoContainerPort,
    runners::AsyncRunner,
};

use crate::server::{free_port, image, wait_for};

const PORT: u16 = 8090;

// The server runs on io_uring with a thread per core, so it needs the seccomp profile off and
// the right to set its threads' priority. It binds every interface in the container and tells
// clients its address, which 0.10 requires for a wildcard bind.
fn request() -> ContainerRequest<GenericImage> {
    image("IGGY_IMAGE", "apache/iggy:0.10.0")
        .with_exposed_port(PORT.tcp())
        .with_cap_add("SYS_NICE")
        .with_security_opt("seccomp=unconfined")
        .with_env_var("IGGY_ROOT_USERNAME", "iggy")
        .with_env_var("IGGY_ROOT_PASSWORD", "iggy")
        .with_env_var("IGGY_TCP_ADDRESS", format!("0.0.0.0:{PORT}"))
        .with_env_var("IGGY_NODE_ADVERTISED_ADDRESS", "127.0.0.1")
}

/// An Iggy server of its own for one test, in Docker, with the root user `iggy` and password
/// `iggy`. It is removed when this value drops.
///
/// `IGGY_IMAGE` picks the image; the default is `apache/iggy:0.10.0`.
pub struct Iggy {
    container: ContainerAsync<GenericImage>,
    url: String,
    restartable: bool,
}

impl std::fmt::Debug for Iggy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Iggy")
            // The address only: the URL holds the password.
            .field(
                "address",
                &self.url.split_once('@').map_or("", |(_, address)| address),
            )
            .finish_non_exhaustive()
    }
}

impl Iggy {
    /// A server on a host port Docker picks.
    pub async fn start() -> Self {
        let container = request()
            .start()
            .await
            .expect("Docker must be running to start Iggy for the tests");
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
            .expect("Docker must be running to start Iggy for the tests");

        Self::ready(container, port, true).await
    }

    async fn ready(container: ContainerAsync<GenericImage>, port: u16, restartable: bool) -> Self {
        let iggy = Self {
            container,
            url: format!("iggy://iggy:iggy@127.0.0.1:{port}"),
            restartable,
        };

        iggy.wait_until_ready().await;
        iggy
    }

    async fn wait_until_ready(&self) {
        wait_for("Iggy answers", async || {
            let Ok(client) = IggyClient::from_connection_string(self.url.as_str()) else {
                return false;
            };
            let answered = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                client.connect().await.is_ok() && client.ping().await.is_ok()
            })
            .await
            .unwrap_or(false);
            let _ = client.shutdown().await;
            answered
        })
        .await;
    }

    /// Kills the server, as a crash would: connections drop and new ones are refused.
    pub async fn stop(&self) {
        self.container.stop_with_timeout(Some(0)).await.unwrap();
    }

    /// Starts a stopped server again at the same address, with the streams it had, and waits until
    /// it answers.
    pub async fn restart(&self) {
        assert!(
            self.restartable,
            "only a server from `Iggy::start_restartable` keeps its address"
        );
        self.container.start().await.unwrap();
        self.wait_until_ready().await;
    }

    /// The connection string of the root user, such as `iggy://iggy:iggy@127.0.0.1:32768`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// A new client, connected and logged in as the root user.
    pub async fn client(&self) -> IggyClient {
        let client = IggyClient::from_connection_string(self.url.as_str()).unwrap();
        client.connect().await.unwrap();
        client
    }
}
