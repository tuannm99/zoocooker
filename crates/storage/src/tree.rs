use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use zoocooker_protocol::types::SessionId;

use crate::stat::Stat;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ZNode {
    pub data: Vec<u8>,
    pub children: BTreeSet<String>,
    pub stat: Stat,
    pub ephemeral_owner: Option<SessionId>,
}

impl ZNode {
    pub fn new(data: Vec<u8>, stat: Stat, ephemeral_owner: Option<SessionId>) -> Self {
        Self {
            data,
            children: BTreeSet::new(),
            stat,
            ephemeral_owner,
        }
    }

    pub fn is_ephemeral(&self) -> bool {
        self.ephemeral_owner.is_some()
    }

    pub fn version(&self) -> i32 {
        self.stat.version
    }

    pub fn cversion(&self) -> i32 {
        self.stat.cversion
    }

    pub fn child_names(&self) -> impl Iterator<Item = &str> {
        self.children.iter().map(String::as_str)
    }
}
