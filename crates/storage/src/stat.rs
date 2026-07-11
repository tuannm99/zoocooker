use serde::{Deserialize, Serialize};
use zoocooker_protocol::types::Zxid;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Stat {
    pub version: i32,
    pub cversion: i32,
    pub created_zxid: Option<Zxid>,
    pub modified_zxid: Option<Zxid>,
}
