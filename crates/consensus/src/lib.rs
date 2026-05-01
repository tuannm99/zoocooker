use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use zoocooker_protocol::{
    command::{Applied, Command},
    error::ZkError,
};
use zoocooker_storage::store::TreeStore;

#[async_trait]
pub trait Consensus: Send + Sync {
    async fn submit(&self, command: Command) -> Result<Applied, ZkError>;
}

#[derive(Debug, Default)]
pub struct SingleNodeConsensus {
    store: Arc<Mutex<TreeStore>>,
}

impl SingleNodeConsensus {
    pub fn new(store: TreeStore) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }

    pub fn store(&self) -> Arc<Mutex<TreeStore>> {
        Arc::clone(&self.store)
    }
}

#[async_trait]
impl Consensus for SingleNodeConsensus {
    async fn submit(&self, command: Command) -> Result<Applied, ZkError> {
        let mut store = self.store.lock().await;
        store.apply(command)
    }
}
