use std::collections::HashMap;

use tokio::sync::mpsc;
use zoocooker_protocol::types::{WatchEvent, WatchKind};

#[derive(Debug, Default)]
pub struct WatchRegistry {
    data_watchers: HashMap<String, Vec<mpsc::Sender<WatchEvent>>>,
    child_watchers: HashMap<String, Vec<mpsc::Sender<WatchEvent>>>,
}

impl WatchRegistry {
    pub fn register(&mut self, path: String, kind: WatchKind, sender: mpsc::Sender<WatchEvent>) {
        let target = match kind {
            WatchKind::Data => &mut self.data_watchers,
            WatchKind::Children => &mut self.child_watchers,
        };

        target.entry(path).or_default().push(sender);
    }

    pub async fn dispatch(&mut self, _events: Vec<WatchEvent>) {
        // One-shot watch dispatch belongs here.
        // After a matching event is sent, remove that watcher.
    }
}
