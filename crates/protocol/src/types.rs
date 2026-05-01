use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Zxid(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchKind {
    Data,
    Children,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEventKind {
    NodeCreated,
    NodeDeleted,
    NodeDataChanged,
    NodeChildrenChanged,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub path: String,
    pub kind: WatchEventKind,
}
