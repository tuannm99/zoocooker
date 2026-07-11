use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use zoocooker_protocol::{
    command::{Applied, ApplyResult, Command},
    error::ZkError,
    path::{leaf_name, normalize, parent_of},
    types::{SessionId, WatchEvent, WatchEventKind, Zxid},
};

use crate::{stat::Stat, tree::ZNode};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeStore {
    nodes: HashMap<String, ZNode>,
    zxid: u64,
    sequence_counters: HashMap<String, u64>,
}

impl Default for TreeStore {
    fn default() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert("/".to_string(), ZNode::default());
        Self {
            nodes,
            zxid: 0,
            sequence_counters: HashMap::new(),
        }
    }
}

impl TreeStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, path: &str) -> Option<&ZNode> {
        self.nodes.get(path)
    }

    pub fn get_data(&self, path: &str) -> Result<(Vec<u8>, i32), ZkError> {
        let path = normalize(path)?;
        let node = self
            .nodes
            .get(&path)
            .ok_or_else(|| ZkError::NoNode(path.clone()))?;
        Ok((node.data.clone(), node.stat.version))
    }

    pub fn exists(&self, path: &str) -> Result<Option<i32>, ZkError> {
        let path = normalize(path)?;
        Ok(self.nodes.get(&path).map(|node| node.stat.version))
    }

    pub fn child_names(&self, path: &str) -> Result<Vec<String>, ZkError> {
        let path = normalize(path)?;
        let node = self
            .nodes
            .get(&path)
            .ok_or_else(|| ZkError::NoNode(path.clone()))?;
        Ok(node.child_names().map(str::to_string).collect())
    }

    pub fn ephemeral_owner(&self, path: &str) -> Result<Option<SessionId>, ZkError> {
        let path = normalize(path)?;
        let node = self
            .nodes
            .get(&path)
            .ok_or_else(|| ZkError::NoNode(path.clone()))?;
        Ok(node.ephemeral_owner)
    }

    pub fn ephemeral_paths_for_session(&self, session_id: SessionId) -> Vec<String> {
        let mut paths = self
            .nodes
            .iter()
            .filter_map(|(path, node)| {
                (node.ephemeral_owner == Some(session_id)).then(|| path.clone())
            })
            .collect::<Vec<_>>();
        sort_deepest_first(&mut paths);
        paths
    }

    /// Distinct session ids that currently own at least one ephemeral node.
    ///
    /// Used to reseed a fresh `SessionManager` after restoring a store from a
    /// snapshot/WAL, so previously-created ephemeral nodes remain subject to
    /// session expiry instead of becoming permanent.
    pub fn ephemeral_session_ids(&self) -> Vec<SessionId> {
        let mut seen = std::collections::HashSet::new();
        self.nodes
            .values()
            .filter_map(|node| node.ephemeral_owner)
            .filter(|session_id| seen.insert(*session_id))
            .collect()
    }

    pub fn current_zxid(&self) -> u64 {
        self.zxid
    }

    pub fn create(
        &mut self,
        path: &str,
        data: Vec<u8>,
        ephemeral: bool,
        sequential: bool,
        session_id: Option<SessionId>,
    ) -> Result<Applied, ZkError> {
        let requested_path = normalize(path)?;
        if requested_path == "/" {
            return Err(ZkError::NodeExists(requested_path));
        }

        let path = if sequential {
            let next = self
                .sequence_counters
                .entry(requested_path.clone())
                .or_default();
            let path = format!("{requested_path}{next:010}");
            *next += 1;
            path
        } else {
            requested_path
        };

        if self.nodes.contains_key(&path) {
            return Err(ZkError::NodeExists(path));
        }

        let parent_path = parent_of(&path).expect("non-root path has parent");
        let child_name = leaf_name(&path)
            .expect("non-root path has leaf")
            .to_string();
        let parent = self
            .nodes
            .get(&parent_path)
            .ok_or_else(|| ZkError::NoParent(parent_path.clone()))?;

        if parent.is_ephemeral() {
            return Err(ZkError::EphemeralParent);
        }

        let ephemeral_owner = if ephemeral {
            Some(session_id.ok_or(ZkError::InvalidSession)?)
        } else {
            None
        };

        let zxid = self.next_zxid();
        let parent = self
            .nodes
            .get_mut(&parent_path)
            .ok_or_else(|| ZkError::NoParent(parent_path.clone()))?;
        parent.children.insert(child_name);
        parent.stat.cversion += 1;
        parent.stat.modified_zxid = Some(zxid);

        let stat = Stat {
            version: 0,
            cversion: 0,
            created_zxid: Some(zxid),
            modified_zxid: Some(zxid),
        };
        self.nodes
            .insert(path.clone(), ZNode::new(data, stat, ephemeral_owner));

        Ok(Applied {
            result: ApplyResult::Created { path: path.clone() },
            watch_events: vec![
                WatchEvent {
                    path: path.clone(),
                    kind: WatchEventKind::NodeCreated,
                },
                WatchEvent {
                    path: parent_path,
                    kind: WatchEventKind::NodeChildrenChanged,
                },
            ],
        })
    }

    pub fn set_data(
        &mut self,
        path: &str,
        data: Vec<u8>,
        expected_version: Option<i32>,
    ) -> Result<Applied, ZkError> {
        let path = normalize(path)?;
        let current_version = self
            .nodes
            .get(&path)
            .ok_or_else(|| ZkError::NoNode(path.clone()))?
            .stat
            .version;
        check_version(expected_version, current_version)?;
        let zxid = self.next_zxid();
        let node = self
            .nodes
            .get_mut(&path)
            .ok_or_else(|| ZkError::NoNode(path.clone()))?;
        node.data = data;
        node.stat.version += 1;
        node.stat.modified_zxid = Some(zxid);
        let version = node.stat.version;

        Ok(Applied {
            result: ApplyResult::Data {
                data: node.data.clone(),
                version,
            },
            watch_events: vec![WatchEvent {
                path,
                kind: WatchEventKind::NodeDataChanged,
            }],
        })
    }

    pub fn delete(
        &mut self,
        path: &str,
        expected_version: Option<i32>,
    ) -> Result<Applied, ZkError> {
        let path = normalize(path)?;
        if path == "/" {
            return Err(ZkError::NotEmpty(path));
        }

        let node = self
            .nodes
            .get(&path)
            .ok_or_else(|| ZkError::NoNode(path.clone()))?;
        if !node.children.is_empty() {
            return Err(ZkError::NotEmpty(path));
        }
        check_version(expected_version, node.stat.version)?;

        let parent_path = parent_of(&path).expect("non-root path has parent");
        let child_name = leaf_name(&path)
            .expect("non-root path has leaf")
            .to_string();
        let zxid = self.next_zxid();
        self.nodes.remove(&path);

        if let Some(parent) = self.nodes.get_mut(&parent_path) {
            parent.children.remove(&child_name);
            parent.stat.cversion += 1;
            parent.stat.modified_zxid = Some(zxid);
        }

        Ok(Applied {
            result: ApplyResult::Deleted,
            watch_events: vec![
                WatchEvent {
                    path: path.clone(),
                    kind: WatchEventKind::NodeDeleted,
                },
                WatchEvent {
                    path: parent_path,
                    kind: WatchEventKind::NodeChildrenChanged,
                },
            ],
        })
    }

    pub fn apply(&mut self, command: Command) -> Result<Applied, ZkError> {
        match command {
            Command::Create {
                path,
                data,
                ephemeral,
                sequential,
                session_id,
            } => self.create(&path, data, ephemeral, sequential, session_id),
            Command::SetData {
                path,
                data,
                expected_version,
            } => self.set_data(&path, data, expected_version),
            Command::Delete {
                path,
                expected_version,
            } => self.delete(&path, expected_version),
        }
    }

    fn next_zxid(&mut self) -> Zxid {
        self.zxid += 1;
        Zxid(self.zxid)
    }
}

fn check_version(expected_version: Option<i32>, actual: i32) -> Result<(), ZkError> {
    match expected_version {
        Some(expected) if expected != actual => Err(ZkError::BadVersion { expected, actual }),
        _ => Ok(()),
    }
}

pub fn sort_deepest_first(paths: &mut [String]) {
    paths.sort_by(|left, right| {
        right
            .matches('/')
            .count()
            .cmp(&left.matches('/').count())
            .then_with(|| right.cmp(left))
    });
}

#[cfg(test)]
mod tests {
    use super::TreeStore;
    use zoocooker_protocol::{
        command::{ApplyResult, Command},
        error::ZkError,
        types::{SessionId, WatchEventKind},
    };

    #[test]
    fn store_starts_with_root() {
        let store = TreeStore::new();
        assert_eq!(store.exists("/").unwrap(), Some(0));
    }

    #[test]
    fn create_get_exists_and_child_metadata() {
        let mut store = TreeStore::new();

        let applied = store
            .create("/app", b"hello".to_vec(), false, false, None)
            .unwrap();

        assert!(matches!(
            applied.result,
            ApplyResult::Created { ref path } if path == "/app"
        ));
        assert_eq!(store.get_data("/app").unwrap(), (b"hello".to_vec(), 0));
        assert_eq!(store.exists("/app").unwrap(), Some(0));
        assert_eq!(store.child_names("/").unwrap(), vec!["app".to_string()]);
        assert_eq!(store.get("/").unwrap().cversion(), 1);
        assert_eq!(
            applied
                .watch_events
                .iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>(),
            vec![
                WatchEventKind::NodeCreated,
                WatchEventKind::NodeChildrenChanged
            ]
        );
    }

    #[test]
    fn create_rejects_missing_duplicate_and_ephemeral_parent() {
        let mut store = TreeStore::new();
        assert!(matches!(
            store.create("/missing/child", Vec::new(), false, false, None),
            Err(ZkError::NoParent(path)) if path == "/missing"
        ));

        store
            .create("/e", Vec::new(), true, false, Some(SessionId::new()))
            .unwrap();
        assert!(matches!(
            store.create("/e/child", Vec::new(), false, false, None),
            Err(ZkError::EphemeralParent)
        ));
        assert!(matches!(
            store.create("/e", Vec::new(), false, false, None),
            Err(ZkError::NodeExists(path)) if path == "/e"
        ));
    }

    #[test]
    fn ephemeral_create_requires_session() {
        let mut store = TreeStore::new();
        assert!(matches!(
            store.create("/e", Vec::new(), true, false, None),
            Err(ZkError::InvalidSession)
        ));
    }

    #[test]
    fn records_ephemeral_owner_and_lists_deepest_first() {
        let mut store = TreeStore::new();
        let session_id = SessionId::new();
        store.create("/a", Vec::new(), false, false, None).unwrap();
        store.create("/b", Vec::new(), false, false, None).unwrap();
        store
            .create("/a/e", Vec::new(), true, false, Some(session_id))
            .unwrap();
        store
            .create("/b/e", Vec::new(), true, false, Some(session_id))
            .unwrap();

        assert_eq!(store.ephemeral_owner("/a/e").unwrap(), Some(session_id));
        assert_eq!(
            store.ephemeral_paths_for_session(session_id),
            vec!["/b/e".to_string(), "/a/e".to_string()]
        );
    }

    #[test]
    fn set_checks_version_and_emits_data_event() {
        let mut store = TreeStore::new();
        store
            .create("/app", b"one".to_vec(), false, false, None)
            .unwrap();

        let applied = store.set_data("/app", b"two".to_vec(), Some(0)).unwrap();
        assert!(matches!(
            applied.result,
            ApplyResult::Data { ref data, version } if data == b"two" && version == 1
        ));
        assert_eq!(store.get_data("/app").unwrap(), (b"two".to_vec(), 1));
        assert_eq!(
            applied.watch_events[0].kind,
            WatchEventKind::NodeDataChanged
        );
        assert!(matches!(
            store.set_data("/app", Vec::new(), Some(0)),
            Err(ZkError::BadVersion {
                expected: 0,
                actual: 1
            })
        ));
    }

    #[test]
    fn delete_rejects_non_empty_and_checks_version() {
        let mut store = TreeStore::new();
        store
            .create("/app", Vec::new(), false, false, None)
            .unwrap();
        store
            .create("/app/child", Vec::new(), false, false, None)
            .unwrap();

        assert!(matches!(
            store.delete("/app", None),
            Err(ZkError::NotEmpty(path)) if path == "/app"
        ));
        assert!(matches!(
            store.delete("/app/child", Some(1)),
            Err(ZkError::BadVersion {
                expected: 1,
                actual: 0
            })
        ));

        let applied = store.delete("/app/child", Some(0)).unwrap();
        assert!(matches!(applied.result, ApplyResult::Deleted));
        assert_eq!(store.exists("/app/child").unwrap(), None);
        assert_eq!(store.get("/app").unwrap().cversion(), 2);
        assert_eq!(
            applied
                .watch_events
                .iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>(),
            vec![
                WatchEventKind::NodeDeleted,
                WatchEventKind::NodeChildrenChanged
            ]
        );
    }

    #[test]
    fn apply_dispatches_write_commands() {
        let mut store = TreeStore::new();

        store
            .apply(Command::Create {
                path: "/app".to_string(),
                data: b"one".to_vec(),
                ephemeral: false,
                sequential: false,
                session_id: None,
            })
            .unwrap();
        let updated = store
            .apply(Command::SetData {
                path: "/app".to_string(),
                data: b"two".to_vec(),
                expected_version: Some(0),
            })
            .unwrap();
        assert!(matches!(
            updated.result,
            ApplyResult::Data { version: 1, .. }
        ));
        store
            .apply(Command::Delete {
                path: "/app".to_string(),
                expected_version: Some(1),
            })
            .unwrap();
        assert_eq!(store.exists("/app").unwrap(), None);
    }

    #[test]
    fn sequential_create_appends_monotonic_suffix() {
        let mut store = TreeStore::new();
        let first = store
            .create("/worker-", Vec::new(), false, true, None)
            .unwrap();
        let second = store
            .create("/worker-", Vec::new(), false, true, None)
            .unwrap();

        assert!(matches!(
            first.result,
            ApplyResult::Created { ref path } if path == "/worker-0000000000"
        ));
        assert!(matches!(
            second.result,
            ApplyResult::Created { ref path } if path == "/worker-0000000001"
        ));
    }
}
