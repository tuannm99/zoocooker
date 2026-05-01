use zoocooker_protocol::types::Zxid;

#[derive(Debug, Clone, Default)]
pub struct Stat {
    pub version: i32,
    pub cversion: i32,
    pub created_zxid: Option<Zxid>,
    pub modified_zxid: Option<Zxid>,
}
