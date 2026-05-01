use std::collections::HashMap;

use zoocooker_protocol::{
    command::{Applied, ApplyResult, Command},
    error::ZkError,
};

use crate::tree::ZNode;

#[derive(Debug)]
pub struct TreeStore {
    nodes: HashMap<String, ZNode>,
}

impl Default for TreeStore {
    fn default() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert("/".to_string(), ZNode::default());
        Self { nodes }
    }
}

impl TreeStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, path: &str) -> Option<&ZNode> {
        self.nodes.get(path)
    }

    pub fn exists(&self, path: &str) -> bool {
        self.nodes.contains_key(path)
    }

    pub fn apply(&mut self, _command: Command) -> Result<Applied, ZkError> {
        Err(ZkError::Unimplemented(
            "storage apply is not implemented yet; start with create/get/set/delete in TreeStore",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::TreeStore;

    #[test]
    fn store_starts_with_root() {
        let store = TreeStore::new();
        assert!(store.exists("/"));
    }
}
