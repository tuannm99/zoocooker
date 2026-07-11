use std::{pin::Pin, sync::Arc, time::Duration};

use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use zoocooker_consensus::Consensus;
use zoocooker_protocol::{
    command::{ApplyResult, Command},
    error::ZkError,
    proto::{
        CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, ExistsRequest,
        ExistsResponse, GetRequest, GetResponse, HeartbeatRequest, HeartbeatResponse, SetRequest,
        SetResponse, WatchEvent as ProtoWatchEvent, WatchEventKind as ProtoWatchEventKind,
        WatchRequest, coordination_server::Coordination,
    },
    types::{SessionId, WatchEventKind, WatchKind},
};

use crate::{
    config::ServerConfig,
    metrics::{Metrics, MetricsSnapshot},
    session::SessionManager,
    watch::WatchRegistry,
};

type WatchStream = Pin<Box<dyn Stream<Item = Result<ProtoWatchEvent, Status>> + Send>>;

pub struct CoordinationService<C> {
    consensus: Arc<C>,
    sessions: Arc<Mutex<SessionManager>>,
    watches: Arc<Mutex<WatchRegistry>>,
    config: ServerConfig,
    metrics: Metrics,
}

impl<C> Clone for CoordinationService<C> {
    fn clone(&self) -> Self {
        Self {
            consensus: Arc::clone(&self.consensus),
            sessions: Arc::clone(&self.sessions),
            watches: Arc::clone(&self.watches),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<C> CoordinationService<C> {
    pub fn new(consensus: Arc<C>) -> Self {
        Self::with_config(consensus, ServerConfig::default())
    }

    pub fn with_config(consensus: Arc<C>, config: ServerConfig) -> Self {
        Self {
            consensus,
            sessions: Arc::new(Mutex::new(SessionManager::new(config.session_ttl))),
            watches: Arc::new(Mutex::new(WatchRegistry::new(config.max_watches))),
            config,
            metrics: Metrics::default(),
        }
    }

    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Deletes ephemeral nodes for sessions whose heartbeat has expired.
    ///
    /// Ephemeral ownership is looked up fresh from the consensus store for
    /// each attempt rather than taken destructively out of SessionManager,
    /// so a submit error (transient persistence failure, leadership change)
    /// just leaves the session queued for the next tick instead of silently
    /// losing track of its remaining ephemeral paths.
    pub async fn cleanup_expired_sessions(&self) -> Vec<SessionId>
    where
        C: Consensus,
    {
        let expired_sessions = self.sessions.lock().await.collect_expired();
        let mut cleaned = Vec::new();
        for session_id in &expired_sessions {
            let paths = self
                .consensus
                .ephemeral_paths_for_session(*session_id)
                .await;
            let mut fully_cleaned = true;
            for path in paths {
                let command = Command::Delete {
                    path,
                    expected_version: None,
                };
                match self.consensus.submit(command).await {
                    Ok(applied) => {
                        let to_send = self
                            .watches
                            .lock()
                            .await
                            .take_matching(applied.watch_events);
                        for (watcher, event) in to_send {
                            let _ = watcher.send(event).await;
                        }
                    }
                    Err(ZkError::NoNode(_)) => {}
                    Err(_) => {
                        fully_cleaned = false;
                        break;
                    }
                }
            }
            if fully_cleaned {
                self.sessions.lock().await.remove_session(*session_id);
                cleaned.push(*session_id);
            }
        }

        self.metrics.inc_session_cleanups(cleaned.len() as u64);
        cleaned
    }

    pub fn spawn_session_expiration_task(service: Arc<Self>, interval: Duration) -> JoinHandle<()>
    where
        C: Consensus + 'static,
    {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                service.cleanup_expired_sessions().await;
            }
        })
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Seeds SessionManager with sessions restored from a persisted store
    /// (WAL/snapshot replay) so their ephemeral nodes remain subject to
    /// expiry instead of becoming permanent after a restart. Each restored
    /// session gets one fresh TTL window starting now, since its real last
    /// heartbeat time isn't persisted.
    pub async fn seed_restored_sessions(&self, session_ids: impl IntoIterator<Item = SessionId>) {
        let mut sessions = self.sessions.lock().await;
        for session_id in session_ids {
            sessions.heartbeat(Some(session_id));
        }
    }
}

#[tonic::async_trait]
impl<C> Coordination for CoordinationService<C>
where
    C: Consensus + 'static,
{
    type WatchStream = WatchStream;

    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let req = request.into_inner();
        let session_id = req
            .session_id
            .as_deref()
            .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
            .map(SessionId);
        if req.ephemeral {
            let session_id = session_id
                .ok_or_else(|| Status::invalid_argument("ephemeral node requires session_id"))?;
            if !self.sessions.lock().await.contains(session_id) {
                return Err(Status::invalid_argument("unknown session_id"));
            }
        }
        let command = Command::Create {
            path: req.path,
            data: req.data,
            ephemeral: req.ephemeral,
            sequential: req.sequential,
            session_id,
        };

        let applied = self
            .consensus
            .submit(command)
            .await
            .map_err(status_from_error)?;
        let path = match applied.result {
            ApplyResult::Created { path } => path,
            _ => return Err(Status::internal("create returned unexpected result")),
        };
        let to_send = self
            .watches
            .lock()
            .await
            .take_matching(applied.watch_events);
        for (watcher, event) in to_send {
            let _ = watcher.send(event).await;
        }
        self.metrics.inc_writes();

        Ok(Response::new(CreateResponse { path }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let (data, version) = self
            .consensus
            .get_data(&req.path)
            .await
            .map_err(status_from_error)?;
        self.metrics.inc_reads();

        Ok(Response::new(GetResponse { data, version }))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let command = Command::SetData {
            path: req.path,
            data: req.data,
            expected_version: req.expected_version,
        };

        let applied = self
            .consensus
            .submit(command)
            .await
            .map_err(status_from_error)?;
        let version = match applied.result {
            ApplyResult::Data { version, .. } => version,
            _ => return Err(Status::internal("set returned unexpected result")),
        };
        let to_send = self
            .watches
            .lock()
            .await
            .take_matching(applied.watch_events);
        for (watcher, event) in to_send {
            let _ = watcher.send(event).await;
        }
        self.metrics.inc_writes();

        Ok(Response::new(SetResponse { version }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let command = Command::Delete {
            path: req.path,
            expected_version: req.expected_version,
        };

        let applied = self
            .consensus
            .submit(command)
            .await
            .map_err(status_from_error)?;
        match applied.result {
            ApplyResult::Deleted => {}
            _ => return Err(Status::internal("delete returned unexpected result")),
        }
        let to_send = self
            .watches
            .lock()
            .await
            .take_matching(applied.watch_events);
        for (watcher, event) in to_send {
            let _ = watcher.send(event).await;
        }
        self.metrics.inc_writes();

        Ok(Response::new(DeleteResponse {}))
    }

    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<ExistsResponse>, Status> {
        let req = request.into_inner();
        let version = self
            .consensus
            .exists(&req.path)
            .await
            .map_err(status_from_error)?;
        self.metrics.inc_reads();

        Ok(Response::new(ExistsResponse {
            exists: version.is_some(),
            version,
        }))
    }

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let req = request.into_inner();
        let kind = match req.kind {
            1 => WatchKind::Data,
            2 => WatchKind::Children,
            _ => return Err(Status::invalid_argument("invalid watch kind")),
        };

        let (tx, rx) = mpsc::channel(self.config.watch_channel_capacity);
        match self.watches.lock().await.register(req.path, kind, tx) {
            Ok(()) => self.metrics.inc_watch_registrations(),
            Err(err) => {
                self.metrics.inc_watch_rejections();
                return Err(status_from_error(err));
            }
        }

        Ok(Response::new(Box::pin(ReceiverStream::new(rx).map(
            |event| {
                Ok(ProtoWatchEvent {
                    path: event.path,
                    kind: proto_watch_event_kind(event.kind) as i32,
                })
            },
        ))))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let session_id = req
            .session_id
            .and_then(|raw| uuid::Uuid::parse_str(&raw).ok())
            .map(SessionId);

        let mut sessions = self.sessions.lock().await;
        let session_id = sessions.heartbeat(session_id);

        Ok(Response::new(HeartbeatResponse {
            session_id: session_id.0.to_string(),
            ttl_ms: sessions.ttl().as_millis() as u64,
        }))
    }
}

fn status_from_error(err: ZkError) -> Status {
    match err {
        ZkError::InvalidPath(_) | ZkError::InvalidSession => {
            Status::invalid_argument(err.to_string())
        }
        ZkError::NodeExists(_) => Status::already_exists(err.to_string()),
        ZkError::NoNode(_) | ZkError::NoParent(_) => Status::not_found(err.to_string()),
        ZkError::BadVersion { .. } | ZkError::NotEmpty(_) | ZkError::EphemeralParent => {
            Status::failed_precondition(err.to_string())
        }
        ZkError::Persistence(_) => Status::internal(err.to_string()),
        ZkError::NotLeader { leader_id } => {
            let message = leader_id
                .map(|leader| format!("not leader; leader={leader}"))
                .unwrap_or_else(|| "not leader".to_string());
            Status::failed_precondition(message)
        }
        ZkError::ResourceExhausted(_) => Status::resource_exhausted(err.to_string()),
        ZkError::Unimplemented(_) => Status::unimplemented(err.to_string()),
    }
}

fn proto_watch_event_kind(kind: WatchEventKind) -> ProtoWatchEventKind {
    match kind {
        WatchEventKind::NodeCreated => ProtoWatchEventKind::NodeCreated,
        WatchEventKind::NodeDeleted => ProtoWatchEventKind::NodeDeleted,
        WatchEventKind::NodeDataChanged => ProtoWatchEventKind::NodeDataChanged,
        WatchEventKind::NodeChildrenChanged => ProtoWatchEventKind::NodeChildrenChanged,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio_stream::StreamExt;
    use tonic::Request;
    use zoocooker_consensus::{
        Consensus, PersistentSingleNodeConsensus, ReplicatedClusterConsensus, SingleNodeConsensus,
    };
    use zoocooker_protocol::proto::{
        CreateRequest, DeleteRequest, ExistsRequest, GetRequest, SetRequest, WatchEventKind,
        WatchKind, WatchRequest, coordination_server::Coordination,
    };
    use zoocooker_storage::store::TreeStore;

    use super::CoordinationService;
    use crate::config::ServerConfig;

    fn service() -> CoordinationService<SingleNodeConsensus> {
        CoordinationService::new(Arc::new(SingleNodeConsensus::new(TreeStore::new())))
    }

    #[tokio::test]
    async fn handles_crud_flow() {
        let service = service();

        let exists = service
            .exists(Request::new(ExistsRequest {
                path: "/app".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!exists.exists);
        assert_eq!(exists.version, None);

        let created = service
            .create(Request::new(CreateRequest {
                path: "/app".to_string(),
                data: b"one".to_vec(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(created.path, "/app");

        let got = service
            .get(Request::new(GetRequest {
                path: "/app".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(got.data, b"one");
        assert_eq!(got.version, 0);

        let set = service
            .set(Request::new(SetRequest {
                path: "/app".to_string(),
                data: b"two".to_vec(),
                expected_version: Some(0),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(set.version, 1);

        service
            .delete(Request::new(DeleteRequest {
                path: "/app".to_string(),
                expected_version: Some(1),
            }))
            .await
            .unwrap();

        let exists = service
            .exists(Request::new(ExistsRequest {
                path: "/app".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!exists.exists);
    }

    #[tokio::test]
    async fn cleanup_expired_session_deletes_ephemeral_node() {
        let service = service();
        let session_id = service
            .heartbeat(Request::new(zoocooker_protocol::proto::HeartbeatRequest {
                session_id: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .session_id;

        service
            .create(Request::new(CreateRequest {
                path: "/e".to_string(),
                data: Vec::new(),
                ephemeral: true,
                sequential: false,
                session_id: Some(session_id.clone()),
            }))
            .await
            .unwrap();

        {
            let mut sessions = service.sessions.lock().await;
            let id = uuid::Uuid::parse_str(&session_id)
                .map(zoocooker_protocol::types::SessionId)
                .unwrap();
            sessions.heartbeat_at(
                Some(id),
                std::time::Instant::now() - std::time::Duration::from_secs(11),
            );
        }

        let expired = service.cleanup_expired_sessions().await;
        assert_eq!(expired.len(), 1);

        let exists = service
            .exists(Request::new(ExistsRequest {
                path: "/e".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!exists.exists);
    }

    #[tokio::test]
    async fn restored_ephemeral_node_is_cleaned_up_after_seeded_session_expires() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("commands.wal");
        let session_id = zoocooker_protocol::types::SessionId::new();

        // First "process": create an ephemeral node tied to a session, then
        // "crash" (drop) without ever heartbeating again.
        let first_consensus = PersistentSingleNodeConsensus::new(&wal_path).unwrap();
        first_consensus
            .submit(zoocooker_protocol::command::Command::Create {
                path: "/lock".to_string(),
                data: Vec::new(),
                ephemeral: true,
                sequential: false,
                session_id: Some(session_id),
            })
            .await
            .unwrap();
        drop(first_consensus);

        // "Restart": a fresh process replays the WAL into a brand-new
        // consensus and would normally pair it with an empty SessionManager.
        let restarted_consensus = PersistentSingleNodeConsensus::new(&wal_path).unwrap();
        let restored_sessions = restarted_consensus
            .store()
            .lock()
            .await
            .ephemeral_session_ids();
        assert_eq!(restored_sessions, vec![session_id]);

        let service = CoordinationService::with_config(
            Arc::new(restarted_consensus),
            ServerConfig {
                session_ttl: std::time::Duration::from_millis(1),
                ..ServerConfig::default()
            },
        );
        // Without this seeding step, /lock would never expire: no session in
        // SessionManager ever heartbeated, so collect_expired() would never
        // report it.
        service.seed_restored_sessions(restored_sessions).await;

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let cleaned = service.cleanup_expired_sessions().await;
        assert_eq!(cleaned, vec![session_id]);

        let exists = service
            .exists(Request::new(ExistsRequest {
                path: "/lock".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!exists.exists);
    }

    #[tokio::test]
    async fn data_watch_fires_once_on_set() {
        let service = service();
        service
            .create(Request::new(CreateRequest {
                path: "/app".to_string(),
                data: b"one".to_vec(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            }))
            .await
            .unwrap();

        let mut stream = service
            .watch(Request::new(WatchRequest {
                path: "/app".to_string(),
                kind: WatchKind::Data as i32,
            }))
            .await
            .unwrap()
            .into_inner();

        service
            .set(Request::new(SetRequest {
                path: "/app".to_string(),
                data: b"two".to_vec(),
                expected_version: Some(0),
            }))
            .await
            .unwrap();
        service
            .set(Request::new(SetRequest {
                path: "/app".to_string(),
                data: b"three".to_vec(),
                expected_version: Some(1),
            }))
            .await
            .unwrap();

        let event = stream.next().await.unwrap().unwrap();
        assert_eq!(event.path, "/app");
        assert_eq!(event.kind, WatchEventKind::NodeDataChanged as i32);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn child_watch_fires_on_child_create() {
        let service = service();
        let mut stream = service
            .watch(Request::new(WatchRequest {
                path: "/".to_string(),
                kind: WatchKind::Children as i32,
            }))
            .await
            .unwrap()
            .into_inner();

        service
            .create(Request::new(CreateRequest {
                path: "/app".to_string(),
                data: Vec::new(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            }))
            .await
            .unwrap();

        let event = stream.next().await.unwrap().unwrap();
        assert_eq!(event.path, "/");
        assert_eq!(event.kind, WatchEventKind::NodeChildrenChanged as i32);
    }

    #[tokio::test]
    async fn rejects_watch_registrations_over_configured_limit() {
        let service = CoordinationService::with_config(
            Arc::new(SingleNodeConsensus::new(TreeStore::new())),
            ServerConfig {
                max_watches: 1,
                ..ServerConfig::default()
            },
        );

        // Keep the first stream alive: a dropped stream closes its channel,
        // and a closed watcher no longer occupies capacity (by design), so
        // dropping it here would make the second registration wrongly
        // succeed instead of exercising the limit.
        let _first_stream = service
            .watch(Request::new(WatchRequest {
                path: "/one".to_string(),
                kind: WatchKind::Data as i32,
            }))
            .await
            .unwrap();
        let result = service
            .watch(Request::new(WatchRequest {
                path: "/two".to_string(),
                kind: WatchKind::Data as i32,
            }))
            .await;
        let err = match result {
            Ok(_) => panic!("second watch registration should be rejected"),
            Err(err) => err,
        };

        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        let metrics = service.metrics_snapshot();
        assert_eq!(metrics.watch_registrations, 1);
        assert_eq!(metrics.watch_rejections, 1);
    }

    #[tokio::test]
    async fn follower_service_rejects_write_with_leader_hint() {
        let nodes = ReplicatedClusterConsensus::cluster(["n1", "n2", "n3"]);
        let service = CoordinationService::new(Arc::new(nodes[1].clone()));

        let err = service
            .create(Request::new(CreateRequest {
                path: "/app".to_string(),
                data: Vec::new(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("leader=n1"));
    }

    #[tokio::test]
    async fn metrics_count_reads_and_writes() {
        let service = service();
        service
            .create(Request::new(CreateRequest {
                path: "/app".to_string(),
                data: Vec::new(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            }))
            .await
            .unwrap();
        service
            .exists(Request::new(ExistsRequest {
                path: "/app".to_string(),
            }))
            .await
            .unwrap();

        let metrics = service.metrics_snapshot();
        assert_eq!(metrics.writes, 1);
        assert_eq!(metrics.reads, 1);
    }
}
