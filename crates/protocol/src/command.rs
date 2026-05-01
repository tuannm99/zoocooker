use crate::types::{SessionId, WatchEvent};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Create {
        path: String,
        data: Vec<u8>,
        ephemeral: bool,
        sequential: bool,
        session_id: Option<SessionId>,
    },
    SetData {
        path: String,
        data: Vec<u8>,
        expected_version: Option<i32>,
    },
    Delete {
        path: String,
        expected_version: Option<i32>,
    },
}

#[derive(Debug, Clone)]
pub enum ApplyResult {
    Created { path: String },
    Data { data: Vec<u8>, version: i32 },
    Deleted,
    Exists { exists: bool, version: Option<i32> },
}

#[derive(Debug, Clone)]
pub struct Applied {
    pub result: ApplyResult,
    pub watch_events: Vec<WatchEvent>,
}
