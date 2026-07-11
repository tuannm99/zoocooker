use thiserror::Error;

#[derive(Debug, Error)]
pub enum ZkError {
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("node already exists: {0}")]
    NodeExists(String),
    #[error("node not found: {0}")]
    NoNode(String),
    #[error("parent node not found: {0}")]
    NoParent(String),
    #[error("node has children: {0}")]
    NotEmpty(String),
    #[error("bad version: expected {expected}, actual {actual}")]
    BadVersion { expected: i32, actual: i32 },
    #[error("ephemeral nodes cannot have children")]
    EphemeralParent,
    #[error("ephemeral node requires a valid session")]
    InvalidSession,
    #[error("persistence error: {0}")]
    Persistence(String),
    #[error("not leader; leader is {leader_id:?}")]
    NotLeader { leader_id: Option<String> },
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    #[error("unimplemented: {0}")]
    Unimplemented(&'static str),
}
