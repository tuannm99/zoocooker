use tonic::transport::{Channel, Endpoint};
use zoocooker_protocol::proto::coordination_client::CoordinationClient;

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
}
