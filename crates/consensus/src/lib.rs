use std::{collections::HashMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;
use zoocooker_protocol::{
    command::{Applied, Command},
    error::ZkError,
    types::SessionId,
};
use zoocooker_storage::{
    persistence::{Wal, load_snapshot, replay_into, save_snapshot},
    store::TreeStore,
};

#[async_trait]
pub trait Consensus: Send + Sync {
    async fn submit(&self, command: Command) -> Result<Applied, ZkError>;
    async fn get_data(&self, path: &str) -> Result<(Vec<u8>, i32), ZkError>;
    async fn exists(&self, path: &str) -> Result<Option<i32>, ZkError>;
    async fn leadership(&self) -> Leadership;
    async fn ephemeral_paths_for_session(&self, session_id: SessionId) -> Vec<String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leadership {
    pub node_id: String,
    pub leader_id: Option<String>,
    pub term: u64,
    pub role: Role,
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

#[derive(Debug)]
pub struct PersistentSingleNodeConsensus {
    store: Arc<Mutex<TreeStore>>,
    wal: Wal,
}

impl PersistentSingleNodeConsensus {
    pub fn new(wal_path: impl Into<PathBuf>) -> Result<Self, ZkError> {
        Self::from_store_and_wal(TreeStore::new(), Wal::new(wal_path))
    }

    pub fn new_with_snapshot(
        wal_path: impl Into<PathBuf>,
        snapshot_path: impl Into<PathBuf>,
    ) -> Result<Self, ZkError> {
        let store = load_snapshot(snapshot_path.into())
            .map_err(|err| ZkError::Persistence(err.to_string()))?
            .unwrap_or_default();
        Self::from_store_and_wal(store, Wal::new(wal_path))
    }

    fn from_store_and_wal(mut store: TreeStore, wal: Wal) -> Result<Self, ZkError> {
        let records = wal
            .replay()
            .map_err(|err| ZkError::Persistence(err.to_string()))?;
        replay_into(&mut store, records)?;

        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            wal,
        })
    }

    pub async fn save_snapshot(&self, snapshot_path: impl Into<PathBuf>) -> Result<(), ZkError> {
        let path = snapshot_path.into();
        // Hold the store lock across the whole snapshot+compact so no
        // concurrent submit() can append a WAL record while we're compacting
        // it away; spawn_blocking still keeps the actual disk I/O off the
        // async runtime thread.
        let store = self.store.lock().await;
        let store_snapshot = store.clone();
        let zxid = store_snapshot.current_zxid();
        let wal = self.wal.clone();
        tokio::task::spawn_blocking(move || {
            save_snapshot(path, &store_snapshot)?;
            wal.compact_below(zxid)
        })
        .await
        .map_err(|err| ZkError::Persistence(err.to_string()))?
        .map_err(|err| ZkError::Persistence(err.to_string()))?;
        Ok(())
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

    async fn get_data(&self, path: &str) -> Result<(Vec<u8>, i32), ZkError> {
        let store = self.store.lock().await;
        store.get_data(path)
    }

    async fn exists(&self, path: &str) -> Result<Option<i32>, ZkError> {
        let store = self.store.lock().await;
        store.exists(path)
    }

    async fn leadership(&self) -> Leadership {
        Leadership {
            node_id: "single-node".to_string(),
            leader_id: Some("single-node".to_string()),
            term: 1,
            role: Role::Leader,
        }
    }

    async fn ephemeral_paths_for_session(&self, session_id: SessionId) -> Vec<String> {
        self.store
            .lock()
            .await
            .ephemeral_paths_for_session(session_id)
    }
}

#[async_trait]
impl Consensus for PersistentSingleNodeConsensus {
    async fn submit(&self, command: Command) -> Result<Applied, ZkError> {
        // Validate by applying to the in-memory store first; only commands
        // that actually commit get logged. Logging a rejected command (e.g.
        // NodeExists, BadVersion) would make it un-replayable, aborting
        // startup on the next restart. The store lock stays held across the
        // WAL write below so concurrent submits' WAL records stay in the
        // same order as their store applies.
        let mut store = self.store.lock().await;
        let logged = command.clone();
        let applied = store.apply(command)?;
        let zxid = store.current_zxid();
        let wal = self.wal.clone();
        tokio::task::spawn_blocking(move || wal.append(zxid, &logged))
            .await
            .map_err(|err| ZkError::Persistence(err.to_string()))?
            .map_err(|err| ZkError::Persistence(err.to_string()))?;
        Ok(applied)
    }

    async fn get_data(&self, path: &str) -> Result<(Vec<u8>, i32), ZkError> {
        let store = self.store.lock().await;
        store.get_data(path)
    }

    async fn exists(&self, path: &str) -> Result<Option<i32>, ZkError> {
        let store = self.store.lock().await;
        store.exists(path)
    }

    async fn leadership(&self) -> Leadership {
        Leadership {
            node_id: "persistent-single-node".to_string(),
            leader_id: Some("persistent-single-node".to_string()),
            term: 1,
            role: Role::Leader,
        }
    }

    async fn ephemeral_paths_for_session(&self, session_id: SessionId) -> Vec<String> {
        self.store
            .lock()
            .await
            .ephemeral_paths_for_session(session_id)
    }
}

#[derive(Debug, Clone)]
pub struct ReplicatedClusterConsensus {
    node_id: String,
    state: Arc<Mutex<ReplicatedClusterState>>,
}

#[derive(Debug)]
struct ReplicatedClusterState {
    leader_id: String,
    term: u64,
    stores: HashMap<String, TreeStore>,
    log: Vec<Command>,
    committed: usize,
}

impl ReplicatedClusterConsensus {
    pub fn cluster(node_ids: impl IntoIterator<Item = impl Into<String>>) -> Vec<Self> {
        let node_ids = node_ids.into_iter().map(Into::into).collect::<Vec<_>>();
        assert!(!node_ids.is_empty(), "cluster requires at least one node");
        let leader_id = node_ids[0].clone();
        let stores = node_ids
            .iter()
            .map(|node_id| (node_id.clone(), TreeStore::new()))
            .collect::<HashMap<_, _>>();
        let state = Arc::new(Mutex::new(ReplicatedClusterState {
            leader_id,
            term: 1,
            stores,
            log: Vec::new(),
            committed: 0,
        }));

        node_ids
            .into_iter()
            .map(|node_id| Self {
                node_id,
                state: Arc::clone(&state),
            })
            .collect()
    }

    pub async fn set_leader(&self, leader_id: impl Into<String>) {
        let mut state = self.state.lock().await;
        state.leader_id = leader_id.into();
        state.term += 1;
    }

    pub async fn committed_len(&self) -> usize {
        self.state.lock().await.committed
    }

    fn ensure_leader(state: &ReplicatedClusterState, node_id: &str) -> Result<(), ZkError> {
        if state.leader_id == node_id {
            Ok(())
        } else {
            Err(ZkError::NotLeader {
                leader_id: Some(state.leader_id.clone()),
            })
        }
    }
}

#[async_trait]
impl Consensus for ReplicatedClusterConsensus {
    async fn submit(&self, command: Command) -> Result<Applied, ZkError> {
        let mut state = self.state.lock().await;
        Self::ensure_leader(&state, &self.node_id)?;

        state.log.push(command.clone());
        let mut leader_result = None;
        let leader_id = state.leader_id.clone();
        for (node_id, store) in &mut state.stores {
            let applied = store.apply(command.clone())?;
            if node_id == &leader_id {
                leader_result = Some(applied);
            }
        }
        state.committed += 1;
        leader_result.ok_or_else(|| ZkError::NoNode("leader store missing".to_string()))
    }

    async fn get_data(&self, path: &str) -> Result<(Vec<u8>, i32), ZkError> {
        let state = self.state.lock().await;
        Self::ensure_leader(&state, &self.node_id)?;
        state
            .stores
            .get(&self.node_id)
            .ok_or_else(|| ZkError::NoNode("node store missing".to_string()))?
            .get_data(path)
    }

    async fn exists(&self, path: &str) -> Result<Option<i32>, ZkError> {
        let state = self.state.lock().await;
        Self::ensure_leader(&state, &self.node_id)?;
        state
            .stores
            .get(&self.node_id)
            .ok_or_else(|| ZkError::NoNode("node store missing".to_string()))?
            .exists(path)
    }

    async fn leadership(&self) -> Leadership {
        let state = self.state.lock().await;
        let role = if state.leader_id == self.node_id {
            Role::Leader
        } else {
            Role::Follower
        };
        Leadership {
            node_id: self.node_id.clone(),
            leader_id: Some(state.leader_id.clone()),
            term: state.term,
            role,
        }
    }

    async fn ephemeral_paths_for_session(&self, session_id: SessionId) -> Vec<String> {
        let state = self.state.lock().await;
        state
            .stores
            .get(&self.node_id)
            .map(|store| store.ephemeral_paths_for_session(session_id))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use zoocooker_protocol::command::{ApplyResult, Command};

    use super::{Consensus, PersistentSingleNodeConsensus, ReplicatedClusterConsensus, Role};

    #[tokio::test]
    async fn rejected_command_is_not_logged_and_restart_still_succeeds() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("commands.wal");

        let consensus = PersistentSingleNodeConsensus::new(&wal_path).unwrap();
        consensus
            .submit(Command::Create {
                path: "/app".to_string(),
                data: b"one".to_vec(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            })
            .await
            .unwrap();

        // A duplicate create is rejected; it must not end up in the WAL,
        // otherwise replaying it on restart would hit the same rejection
        // and abort startup.
        let err = consensus
            .submit(Command::Create {
                path: "/app".to_string(),
                data: b"two".to_vec(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            zoocooker_protocol::error::ZkError::NodeExists(_)
        ));

        let restarted = PersistentSingleNodeConsensus::new(&wal_path).unwrap();
        assert_eq!(
            restarted.get_data("/app").await.unwrap(),
            (b"one".to_vec(), 0)
        );
    }

    #[tokio::test]
    async fn persistent_consensus_replays_wal_on_restart() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("commands.wal");

        let consensus = PersistentSingleNodeConsensus::new(&wal_path).unwrap();
        consensus
            .submit(Command::Create {
                path: "/app".to_string(),
                data: b"one".to_vec(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            })
            .await
            .unwrap();
        let applied = consensus
            .submit(Command::SetData {
                path: "/app".to_string(),
                data: b"two".to_vec(),
                expected_version: Some(0),
            })
            .await
            .unwrap();
        assert!(matches!(
            applied.result,
            ApplyResult::Data { version: 1, .. }
        ));

        let restarted = PersistentSingleNodeConsensus::new(&wal_path).unwrap();
        assert_eq!(
            restarted.get_data("/app").await.unwrap(),
            (b"two".to_vec(), 1)
        );
    }

    #[tokio::test]
    async fn persistent_consensus_restores_snapshot_then_replays_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("commands.wal");
        let snapshot_path = dir.path().join("snapshot.json");

        let consensus =
            PersistentSingleNodeConsensus::new_with_snapshot(&wal_path, &snapshot_path).unwrap();
        consensus
            .submit(Command::Create {
                path: "/app".to_string(),
                data: b"one".to_vec(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            })
            .await
            .unwrap();
        consensus.save_snapshot(&snapshot_path).await.unwrap();
        consensus
            .submit(Command::SetData {
                path: "/app".to_string(),
                data: b"two".to_vec(),
                expected_version: Some(0),
            })
            .await
            .unwrap();

        let restarted =
            PersistentSingleNodeConsensus::new_with_snapshot(&wal_path, &snapshot_path).unwrap();
        assert_eq!(
            restarted.get_data("/app").await.unwrap(),
            (b"two".to_vec(), 1)
        );
    }

    #[tokio::test]
    async fn replicated_cluster_rejects_follower_writes() {
        let nodes = ReplicatedClusterConsensus::cluster(["n1", "n2", "n3"]);
        let err = nodes[1]
            .submit(Command::Create {
                path: "/app".to_string(),
                data: Vec::new(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            })
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            zoocooker_protocol::error::ZkError::NotLeader {
                leader_id: Some(ref leader)
            } if leader == "n1"
        ));
        assert_eq!(nodes[1].leadership().await.role, Role::Follower);
    }

    #[tokio::test]
    async fn replicated_cluster_applies_committed_write_to_all_nodes() {
        let nodes = ReplicatedClusterConsensus::cluster(["n1", "n2", "n3"]);
        nodes[0]
            .submit(Command::Create {
                path: "/app".to_string(),
                data: b"one".to_vec(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            })
            .await
            .unwrap();

        assert_eq!(nodes[0].committed_len().await, 1);
        for node in &nodes {
            let store = node.state.lock().await;
            assert_eq!(
                store
                    .stores
                    .get(&node.node_id)
                    .unwrap()
                    .get_data("/app")
                    .unwrap(),
                (b"one".to_vec(), 0)
            );
        }
    }

    #[tokio::test]
    async fn replicated_cluster_surfaces_leadership_changes() {
        let nodes = ReplicatedClusterConsensus::cluster(["n1", "n2", "n3"]);
        nodes[0].set_leader("n2").await;

        assert_eq!(nodes[0].leadership().await.role, Role::Follower);
        assert_eq!(nodes[1].leadership().await.role, Role::Leader);
        nodes[1]
            .submit(Command::Create {
                path: "/after".to_string(),
                data: Vec::new(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            })
            .await
            .unwrap();
    }
}
