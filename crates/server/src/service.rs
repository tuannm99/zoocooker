use std::{pin::Pin, sync::Arc, time::Duration};

use tokio::sync::{mpsc, Mutex};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status};
use zoocooker_consensus::Consensus;
use zoocooker_protocol::{
    command::Command,
    proto::{
        coordination_server::Coordination, CreateRequest, CreateResponse, DeleteRequest,
        DeleteResponse, ExistsRequest, ExistsResponse, GetRequest, GetResponse, HeartbeatRequest,
        HeartbeatResponse, SetRequest, SetResponse, WatchEvent as ProtoWatchEvent, WatchRequest,
    },
    types::{SessionId, WatchKind},
};

use crate::{session::SessionManager, watch::WatchRegistry};

type WatchStream = Pin<Box<dyn Stream<Item = Result<ProtoWatchEvent, Status>> + Send>>;

pub struct CoordinationService<C> {
    consensus: Arc<C>,
    sessions: Arc<Mutex<SessionManager>>,
    watches: Arc<Mutex<WatchRegistry>>,
}

impl<C> CoordinationService<C> {
    pub fn new(consensus: Arc<C>) -> Self {
        Self {
            consensus,
            sessions: Arc::new(Mutex::new(SessionManager::new(Duration::from_secs(10)))),
            watches: Arc::new(Mutex::new(WatchRegistry::default())),
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
        let command = Command::Create {
            path: req.path,
            data: req.data,
            ephemeral: req.ephemeral,
            sequential: req.sequential,
            session_id: req
                .session_id
                .and_then(|raw| uuid::Uuid::parse_str(&raw).ok())
                .map(SessionId),
        };

        let _applied = self
            .consensus
            .submit(command)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(CreateResponse {
            path: "todo".to_string(),
        }))
    }

    async fn get(&self, _request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        Err(Status::unimplemented("get is not implemented yet"))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let command = Command::SetData {
            path: req.path,
            data: req.data,
            expected_version: req.expected_version,
        };

        let _applied = self
            .consensus
            .submit(command)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(SetResponse { version: 0 }))
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

        let _applied = self
            .consensus
            .submit(command)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(DeleteResponse {}))
    }

    async fn exists(
        &self,
        _request: Request<ExistsRequest>,
    ) -> Result<Response<ExistsResponse>, Status> {
        Err(Status::unimplemented("exists is not implemented yet"))
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

        let (tx, rx) = mpsc::channel(32);
        self.watches.lock().await.register(req.path, kind, tx);

        Ok(Response::new(Box::pin(ReceiverStream::new(rx).map(|event| {
            Ok(ProtoWatchEvent {
                path: event.path,
                kind: 0,
            })
        }))))
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
