use std::collections::BTreeSet;

use zoocooker_protocol::types::SessionId;

use crate::stat::Stat;

#[derive(Debug, Clone, Default)]
pub struct ZNode {
    pub data: Vec<u8>,
    pub children: BTreeSet<String>,
    pub stat: Stat,
    pub ephemeral_owner: Option<SessionId>,
}

impl ZNode {
    pub fn is_ephemeral(&self) -> bool {
        self.ephemeral_owner.is_some()
    }
}
