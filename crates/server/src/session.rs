use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use zoocooker_protocol::types::SessionId;

#[derive(Debug)]
pub struct SessionState {
    pub last_heartbeat: Instant,
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
        self.heartbeat_at(session_id, Instant::now())
    }

    pub fn heartbeat_at(&mut self, session_id: Option<SessionId>, now: Instant) -> SessionId {
        let session_id = session_id.unwrap_or_else(SessionId::new);
        self.sessions
            .entry(session_id)
            .and_modify(|state| state.last_heartbeat = now)
            .or_insert_with(|| SessionState {
                last_heartbeat: now,
            });
        session_id
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn collect_expired(&mut self) -> Vec<SessionId> {
        self.collect_expired_at(Instant::now())
    }

    pub fn collect_expired_at(&mut self, now: Instant) -> Vec<SessionId> {
        self.sessions
            .iter()
            .filter_map(|(session_id, state)| {
                (now.duration_since(state.last_heartbeat) > self.ttl).then_some(*session_id)
            })
            .collect()
    }

    /// Drops session tracking. Ephemeral-node ownership itself lives in
    /// TreeStore, not here, so this only needs to happen once the session's
    /// ephemeral nodes have actually been deleted (or found already gone).
    pub fn remove_session(&mut self, session_id: SessionId) {
        self.sessions.remove(&session_id);
    }

    pub fn contains(&self, session_id: SessionId) -> bool {
        self.sessions.contains_key(&session_id)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::SessionManager;

    #[test]
    fn heartbeat_creates_and_refreshes_session() {
        let mut sessions = SessionManager::new(Duration::from_secs(5));
        let start = Instant::now();
        let session_id = sessions.heartbeat_at(None, start);
        assert!(sessions.contains(session_id));

        sessions.heartbeat_at(Some(session_id), start + Duration::from_secs(4));
        assert!(
            sessions
                .collect_expired_at(start + Duration::from_secs(8))
                .is_empty()
        );
        assert_eq!(
            sessions.collect_expired_at(start + Duration::from_secs(10)),
            vec![session_id]
        );
    }

    #[test]
    fn collect_expired_retains_non_expired_sessions() {
        let mut sessions = SessionManager::new(Duration::from_secs(5));
        let start = Instant::now();
        let session_id = sessions.heartbeat_at(None, start);

        assert!(
            sessions
                .collect_expired_at(start + Duration::from_secs(5))
                .is_empty()
        );
        assert!(sessions.contains(session_id));
    }
}
