use tonic::transport::{Channel, Endpoint};
use zoocooker_protocol::proto::{
    CreateRequest, CreateResponse, DeleteRequest, DeleteResponse, ExistsRequest, ExistsResponse,
    GetRequest, GetResponse, HeartbeatRequest, HeartbeatResponse, SetRequest, SetResponse,
    WatchEvent, WatchKind, WatchRequest, coordination_client::CoordinationClient,
};

pub struct Client {
    inner: CoordinationClient<Channel>,
}

impl Client {
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.as_ref().to_string())?
            .connect()
            .await?;

        Ok(Self {
            inner: CoordinationClient::new(channel),
        })
    }

    pub fn inner(&mut self) -> &mut CoordinationClient<Channel> {
        &mut self.inner
    }

    pub async fn create(
        &mut self,
        path: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> Result<CreateResponse, tonic::Status> {
        let response = self
            .inner
            .create(CreateRequest {
                path: path.into(),
                data: data.into(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            })
            .await?;
        Ok(response.into_inner())
    }

    pub async fn create_with_options(
        &mut self,
        path: impl Into<String>,
        data: impl Into<Vec<u8>>,
        ephemeral: bool,
        sequential: bool,
        session_id: Option<String>,
    ) -> Result<CreateResponse, tonic::Status> {
        let response = self
            .inner
            .create(CreateRequest {
                path: path.into(),
                data: data.into(),
                ephemeral,
                sequential,
                session_id,
            })
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get(&mut self, path: impl Into<String>) -> Result<GetResponse, tonic::Status> {
        let response = self.inner.get(GetRequest { path: path.into() }).await?;
        Ok(response.into_inner())
    }

    pub async fn set(
        &mut self,
        path: impl Into<String>,
        data: impl Into<Vec<u8>>,
        expected_version: Option<i32>,
    ) -> Result<SetResponse, tonic::Status> {
        let response = self
            .inner
            .set(SetRequest {
                path: path.into(),
                data: data.into(),
                expected_version,
            })
            .await?;
        Ok(response.into_inner())
    }

    pub async fn delete(
        &mut self,
        path: impl Into<String>,
        expected_version: Option<i32>,
    ) -> Result<DeleteResponse, tonic::Status> {
        let response = self
            .inner
            .delete(DeleteRequest {
                path: path.into(),
                expected_version,
            })
            .await?;
        Ok(response.into_inner())
    }

    pub async fn exists(
        &mut self,
        path: impl Into<String>,
    ) -> Result<ExistsResponse, tonic::Status> {
        let response = self
            .inner
            .exists(ExistsRequest { path: path.into() })
            .await?;
        Ok(response.into_inner())
    }

    pub async fn watch(
        &mut self,
        path: impl Into<String>,
        kind: WatchKind,
    ) -> Result<tonic::Streaming<WatchEvent>, tonic::Status> {
        let response = self
            .inner
            .watch(WatchRequest {
                path: path.into(),
                kind: kind as i32,
            })
            .await?;
        Ok(response.into_inner())
    }

    pub async fn heartbeat(
        &mut self,
        session_id: Option<String>,
    ) -> Result<HeartbeatResponse, tonic::Status> {
        let response = self
            .inner
            .heartbeat(HeartbeatRequest { session_id })
            .await?;
        Ok(response.into_inner())
    }
}
