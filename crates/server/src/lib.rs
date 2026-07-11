use std::{future::Future, net::SocketAddr, path::PathBuf, sync::Arc};

use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use zoocooker_consensus::{PersistentSingleNodeConsensus, SingleNodeConsensus};
use zoocooker_protocol::proto::coordination_server::CoordinationServer;
use zoocooker_storage::store::TreeStore;

pub mod config;
pub mod metrics;
pub mod service;
pub mod session;
pub mod watch;

pub async fn serve_single_node(addr: SocketAddr) -> Result<(), tonic::transport::Error> {
    serve_single_node_with_config(addr, config::ServerConfig::default()).await
}

pub async fn serve_single_node_with_config(
    addr: SocketAddr,
    config: config::ServerConfig,
) -> Result<(), tonic::transport::Error> {
    let consensus = Arc::new(SingleNodeConsensus::new(TreeStore::new()));
    let service = service::CoordinationService::with_config(consensus, config);
    serve_service(addr, service).await
}

pub async fn serve_persistent_single_node(
    addr: SocketAddr,
    wal_path: impl Into<PathBuf>,
    snapshot_path: Option<impl Into<PathBuf>>,
    config: config::ServerConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let consensus = match snapshot_path {
        Some(snapshot_path) => {
            PersistentSingleNodeConsensus::new_with_snapshot(wal_path.into(), snapshot_path.into())?
        }
        None => PersistentSingleNodeConsensus::new(wal_path.into())?,
    };
    // Ephemeral nodes restored from the WAL/snapshot own a session_id that
    // no session in a freshly-constructed SessionManager knows about; seed
    // it so those nodes remain subject to expiry instead of becoming
    // permanent for the rest of this process's lifetime.
    let restored_sessions = {
        let store = consensus.store();
        let store = store.lock().await;
        store.ephemeral_session_ids()
    };
    let service = service::CoordinationService::with_config(Arc::new(consensus), config);
    service.seed_restored_sessions(restored_sessions).await;
    serve_service(addr, service).await?;
    Ok(())
}

fn spawn_cleanup<C>(service: &service::CoordinationService<C>) -> JoinHandle<()>
where
    C: zoocooker_consensus::Consensus + 'static,
{
    let service_for_cleanup = Arc::new(service.clone());
    service::CoordinationService::spawn_session_expiration_task(
        service_for_cleanup,
        service.config().session_ttl / 2,
    )
}

pub async fn serve_service<C>(
    addr: SocketAddr,
    service: service::CoordinationService<C>,
) -> Result<(), tonic::transport::Error>
where
    C: zoocooker_consensus::Consensus + 'static,
{
    let cleanup_task = spawn_cleanup(&service);
    let result = Server::builder()
        .add_service(CoordinationServer::new(service))
        .serve(addr)
        .await;
    cleanup_task.abort();
    result
}

pub async fn serve_service_with_shutdown<C, S>(
    addr: SocketAddr,
    service: service::CoordinationService<C>,
    shutdown: S,
) -> Result<(), tonic::transport::Error>
where
    C: zoocooker_consensus::Consensus + 'static,
    S: Future<Output = ()> + Send + 'static,
{
    let cleanup_task = spawn_cleanup(&service);
    let result = Server::builder()
        .add_service(CoordinationServer::new(service))
        .serve_with_shutdown(addr, shutdown)
        .await;
    cleanup_task.abort();
    result
}

/// Same as [`serve_service_with_shutdown`] but takes an already-bound
/// listener instead of a [`SocketAddr`]. Lets callers (tests, mainly) learn
/// the bound address and start serving on it without an unbind/rebind gap
/// that another process could race into.
pub async fn serve_incoming_with_shutdown<C, S>(
    listener: TcpListener,
    service: service::CoordinationService<C>,
    shutdown: S,
) -> Result<(), tonic::transport::Error>
where
    C: zoocooker_consensus::Consensus + 'static,
    S: Future<Output = ()> + Send + 'static,
{
    let cleanup_task = spawn_cleanup(&service);
    let result = Server::builder()
        .add_service(CoordinationServer::new(service))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await;
    cleanup_task.abort();
    result
}
