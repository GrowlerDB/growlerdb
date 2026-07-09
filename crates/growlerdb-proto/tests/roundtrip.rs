//! Proves the gRPC toolchain end to end: a tonic `System` server + client
//! round-trip over a local TCP socket.

use growlerdb_proto::v1::health_response::Status as HealthStatus;
use growlerdb_proto::v1::system_client::SystemClient;
use growlerdb_proto::v1::{HealthRequest, VersionRequest};
use growlerdb_proto::{SystemServer, SystemService};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

#[tokio::test]
async fn system_service_round_trips_over_grpc() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(SystemServer::new(SystemService::new("9.9.9")))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = connect(&format!("http://{addr}")).await;

    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(health.status, HealthStatus::Serving as i32);

    let version = client
        .version(VersionRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(version.version, "9.9.9");
}

/// Connect, retrying briefly until the spawned server is accepting.
async fn connect(url: &str) -> SystemClient<Channel> {
    for _ in 0..40 {
        if let Ok(client) = SystemClient::connect(url.to_string()).await {
            return client;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("gRPC server did not come up");
}
