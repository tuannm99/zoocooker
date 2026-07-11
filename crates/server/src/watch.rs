use std::collections::HashMap;

use tokio::sync::mpsc;
use zoocooker_protocol::{
    error::ZkError,
    types::{WatchEvent, WatchEventKind, WatchKind},
};

#[derive(Debug)]
pub struct WatchRegistry {
    data_watchers: HashMap<String, Vec<mpsc::Sender<WatchEvent>>>,
    child_watchers: HashMap<String, Vec<mpsc::Sender<WatchEvent>>>,
    max_watches: usize,
    active_watches: usize,
}

impl Default for WatchRegistry {
    fn default() -> Self {
        Self::new(10_000)
    }
}

impl WatchRegistry {
    pub fn new(max_watches: usize) -> Self {
        Self {
            data_watchers: HashMap::new(),
            child_watchers: HashMap::new(),
            max_watches,
            active_watches: 0,
        }
    }

    /// Drops senders whose receiver was already dropped (client disconnected
    /// without a matching event ever firing on its watched path) and
    /// reclaims their slot in `active_watches`. Without this, a client that
    /// registers and disconnects repeatedly leaks watch capacity forever.
    fn sweep_closed(&mut self) {
        let mut removed = 0usize;
        for watchers in self
            .data_watchers
            .values_mut()
            .chain(self.child_watchers.values_mut())
        {
            let before = watchers.len();
            watchers.retain(|watcher| !watcher.is_closed());
            removed += before - watchers.len();
        }
        self.data_watchers
            .retain(|_, watchers| !watchers.is_empty());
        self.child_watchers
            .retain(|_, watchers| !watchers.is_empty());
        self.active_watches = self.active_watches.saturating_sub(removed);
    }

    pub fn register(
        &mut self,
        path: String,
        kind: WatchKind,
        sender: mpsc::Sender<WatchEvent>,
    ) -> Result<(), ZkError> {
        self.sweep_closed();
        if self.active_watches >= self.max_watches {
            return Err(ZkError::ResourceExhausted(format!(
                "watch registration limit {} reached",
                self.max_watches
            )));
        }

        let target = match kind {
            WatchKind::Data => &mut self.data_watchers,
            WatchKind::Children => &mut self.child_watchers,
        };

        target.entry(path).or_default().push(sender);
        self.active_watches += 1;
        Ok(())
    }

    /// Removes matching watchers from the registry and hands their senders
    /// back to the caller instead of sending here. Callers should drop the
    /// registry lock before awaiting the sends — a slow/stalled watcher must
    /// not stall every other RPC that dispatches through the same registry.
    pub fn take_matching(
        &mut self,
        events: Vec<WatchEvent>,
    ) -> Vec<(mpsc::Sender<WatchEvent>, WatchEvent)> {
        let mut out = Vec::new();
        for event in events {
            let watchers = match event.kind {
                WatchEventKind::NodeCreated
                | WatchEventKind::NodeDeleted
                | WatchEventKind::NodeDataChanged => self.data_watchers.remove(&event.path),
                WatchEventKind::NodeChildrenChanged => self.child_watchers.remove(&event.path),
            };

            if let Some(watchers) = watchers {
                self.active_watches = self.active_watches.saturating_sub(watchers.len());
                for watcher in watchers {
                    out.push((watcher, event.clone()));
                }
            }
        }
        out
    }

    pub async fn dispatch(&mut self, events: Vec<WatchEvent>) {
        for (watcher, event) in self.take_matching(events) {
            let _ = watcher.send(event).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WatchRegistry;
    use zoocooker_protocol::error::ZkError;
    use zoocooker_protocol::types::{WatchEvent, WatchEventKind, WatchKind};

    #[tokio::test]
    async fn dispatches_matching_watch_once() {
        let mut registry = WatchRegistry::default();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        registry
            .register("/app".to_string(), WatchKind::Data, tx)
            .unwrap();

        registry
            .dispatch(vec![WatchEvent {
                path: "/app".to_string(),
                kind: WatchEventKind::NodeDataChanged,
            }])
            .await;
        registry
            .dispatch(vec![WatchEvent {
                path: "/app".to_string(),
                kind: WatchEventKind::NodeDataChanged,
            }])
            .await;

        let event = rx.recv().await.unwrap();
        assert_eq!(event.path, "/app");
        assert_eq!(event.kind, WatchEventKind::NodeDataChanged);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn separates_data_and_child_watches() {
        let mut registry = WatchRegistry::default();
        let (data_tx, mut data_rx) = tokio::sync::mpsc::channel(1);
        let (child_tx, mut child_rx) = tokio::sync::mpsc::channel(1);
        registry
            .register("/app".to_string(), WatchKind::Data, data_tx)
            .unwrap();
        registry
            .register("/app".to_string(), WatchKind::Children, child_tx)
            .unwrap();

        registry
            .dispatch(vec![WatchEvent {
                path: "/app".to_string(),
                kind: WatchEventKind::NodeChildrenChanged,
            }])
            .await;

        assert!(data_rx.try_recv().is_err());
        assert_eq!(
            child_rx.recv().await.unwrap().kind,
            WatchEventKind::NodeChildrenChanged
        );
    }

    #[tokio::test]
    async fn rejects_watch_registration_over_limit() {
        let mut registry = WatchRegistry::new(1);
        let (first_tx, _first_rx) = tokio::sync::mpsc::channel(1);
        let (second_tx, _second_rx) = tokio::sync::mpsc::channel(1);
        registry
            .register("/app".to_string(), WatchKind::Data, first_tx)
            .unwrap();

        assert!(matches!(
            registry.register("/other".to_string(), WatchKind::Data, second_tx),
            Err(ZkError::ResourceExhausted(_))
        ));
    }

    #[tokio::test]
    async fn reclaims_capacity_from_disconnected_watchers() {
        let mut registry = WatchRegistry::new(1);
        let (first_tx, first_rx) = tokio::sync::mpsc::channel(1);
        registry
            .register("/app".to_string(), WatchKind::Data, first_tx)
            .unwrap();
        drop(first_rx);

        let (second_tx, _second_rx) = tokio::sync::mpsc::channel(1);
        registry
            .register("/other".to_string(), WatchKind::Data, second_tx)
            .unwrap();
    }
}
