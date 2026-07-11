use std::{net::SocketAddr, sync::Arc, time::Duration};

use tempfile::tempdir;
use tokio::{net::TcpListener, sync::oneshot};
use tonic::Code;
use zoocooker_client::Client;
use zoocooker_consensus::PersistentSingleNodeConsensus;
use zoocooker_protocol::proto::WatchKind;
use zoocooker_server::{
    config::ServerConfig, serve_incoming_with_shutdown, service::CoordinationService,
};

/// Binds a real listener and hands it straight to the server instead of
/// learning a free port, dropping the listener, and rebinding by address —
/// that gap lets another concurrently-running test grab the same port.
async fn bind() -> (SocketAddr, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, listener)
}

async fn connect(addr: SocketAddr) -> Client {
    let endpoint = format!("http://{addr}");
    let mut last_error = None;
    for _ in 0..50 {
        match Client::connect(&endpoint).await {
            Ok(client) => return client,
            Err(err) => {
                last_error = Some(err);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    panic!("client failed to connect: {last_error:?}");
}

#[tokio::test]
async fn tcp_client_runs_crud_and_watch_flow() {
    let (addr, listener) = bind().await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = CoordinationService::new(Arc::new(
        zoocooker_consensus::SingleNodeConsensus::new(zoocooker_storage::store::TreeStore::new()),
    ));
    let server = tokio::spawn(async move {
        serve_incoming_with_shutdown(listener, service, async {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut client = connect(addr).await;
    assert!(!client.exists("/app").await.unwrap().exists);
    assert_eq!(
        client.create("/app", b"one".to_vec()).await.unwrap().path,
        "/app"
    );
    assert_eq!(client.get("/app").await.unwrap().data, b"one");

    let mut watch = client.watch("/app", WatchKind::Data).await.unwrap();
    assert_eq!(
        client
            .set("/app", b"two".to_vec(), Some(0))
            .await
            .unwrap()
            .version,
        1
    );
    let event = watch.message().await.unwrap().unwrap();
    assert_eq!(event.path, "/app");

    client.delete("/app", Some(1)).await.unwrap();
    assert!(!client.exists("/app").await.unwrap().exists);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_ephemeral_node_is_deleted_after_session_expiry() {
    let (addr, listener) = bind().await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = CoordinationService::with_config(
        Arc::new(zoocooker_consensus::SingleNodeConsensus::new(
            zoocooker_storage::store::TreeStore::new(),
        )),
        ServerConfig {
            session_ttl: Duration::from_millis(50),
            ..ServerConfig::default()
        },
    );
    let server = tokio::spawn(async move {
        serve_incoming_with_shutdown(listener, service, async {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut client = connect(addr).await;
    let session = client.heartbeat(None).await.unwrap().session_id;
    client
        .create_with_options("/e", Vec::new(), true, false, Some(session))
        .await
        .unwrap();
    assert!(client.exists("/e").await.unwrap().exists);

    tokio::time::sleep(Duration::from_millis(160)).await;
    assert!(!client.exists("/e").await.unwrap().exists);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_persistent_server_recovers_data_after_restart() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("commands.wal");
    let snapshot_path = dir.path().join("snapshot.json");

    let (first_addr, first_listener) = bind().await;
    let (first_shutdown_tx, first_shutdown_rx) = oneshot::channel();
    let first_consensus =
        PersistentSingleNodeConsensus::new_with_snapshot(&wal_path, &snapshot_path).unwrap();
    let first_service = CoordinationService::new(Arc::new(first_consensus));
    let first_server = tokio::spawn(async move {
        serve_incoming_with_shutdown(first_listener, first_service, async {
            let _ = first_shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut first_client = connect(first_addr).await;
    first_client
        .create("/app", b"persisted".to_vec())
        .await
        .unwrap();
    let _ = first_shutdown_tx.send(());
    first_server.await.unwrap();

    let (second_addr, second_listener) = bind().await;
    let (second_shutdown_tx, second_shutdown_rx) = oneshot::channel();
    let second_consensus =
        PersistentSingleNodeConsensus::new_with_snapshot(&wal_path, &snapshot_path).unwrap();
    let second_service = CoordinationService::new(Arc::new(second_consensus));
    let second_server = tokio::spawn(async move {
        serve_incoming_with_shutdown(second_listener, second_service, async {
            let _ = second_shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    let mut second_client = connect(second_addr).await;
    assert_eq!(second_client.get("/app").await.unwrap().data, b"persisted");

    let duplicate = second_client.create("/app", Vec::new()).await.unwrap_err();
    assert_eq!(duplicate.code(), Code::AlreadyExists);

    let _ = second_shutdown_tx.send(());
    second_server.await.unwrap();
}
