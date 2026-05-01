use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use zoocooker_protocol::types::SessionId;

#[derive(Debug)]
pub struct SessionState {
    pub last_heartbeat: Instant,
    pub ephemeral_paths: HashSet<String>,
}

#[derive(Debug)]
pub struct SessionManager {
    ttl: Duration,
    sessions: HashMap<SessionId, SessionState>,
}

impl SessionManager {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            sessions: HashMap::new(),
        }
    }

    pub fn heartbeat(&mut self, session_id: Option<SessionId>) -> SessionId {
        let session_id = session_id.unwrap_or_else(SessionId::new);
        self.sessions
            .entry(session_id)
            .and_modify(|state| state.last_heartbeat = Instant::now())
            .or_insert_with(|| SessionState {
                last_heartbeat: Instant::now(),
                ephemeral_paths: HashSet::new(),
            });
        session_id
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn collect_expired(&mut self) -> Vec<SessionId> {
        let now = Instant::now();
        let mut expired = Vec::new();
        self.sessions.retain(|session_id, state| {
            let alive = now.duration_since(state.last_heartbeat) <= self.ttl;
            if !alive {
                expired.push(*session_id);
            }
            alive
        });
        expired
    }
}
