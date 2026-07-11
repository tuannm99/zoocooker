use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub session_ttl: Duration,
    pub watch_channel_capacity: usize,
    pub max_watches: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            session_ttl: Duration::from_secs(10),
            watch_channel_capacity: 32,
            max_watches: 10_000,
        }
    }
}
