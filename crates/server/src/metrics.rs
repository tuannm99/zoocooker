use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Default, Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug, Default)]
struct MetricsInner {
    reads: AtomicU64,
    writes: AtomicU64,
    watch_registrations: AtomicU64,
    watch_rejections: AtomicU64,
    session_cleanups: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub watch_registrations: u64,
    pub watch_rejections: u64,
    pub session_cleanups: u64,
}

impl Metrics {
    pub fn inc_reads(&self) {
        self.inner.reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_writes(&self) {
        self.inner.writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_watch_registrations(&self) {
        self.inner
            .watch_registrations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_watch_rejections(&self) {
        self.inner.watch_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_session_cleanups(&self, count: u64) {
        self.inner
            .session_cleanups
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            reads: self.inner.reads.load(Ordering::Relaxed),
            writes: self.inner.writes.load(Ordering::Relaxed),
            watch_registrations: self.inner.watch_registrations.load(Ordering::Relaxed),
            watch_rejections: self.inner.watch_rejections.load(Ordering::Relaxed),
            session_cleanups: self.inner.session_cleanups.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Metrics, MetricsSnapshot};

    #[test]
    fn metrics_snapshot_reflects_counters() {
        let metrics = Metrics::default();
        metrics.inc_reads();
        metrics.inc_writes();
        metrics.inc_watch_registrations();
        metrics.inc_watch_rejections();
        metrics.inc_session_cleanups(2);

        assert_eq!(
            metrics.snapshot(),
            MetricsSnapshot {
                reads: 1,
                writes: 1,
                watch_registrations: 1,
                watch_rejections: 1,
                session_cleanups: 2,
            }
        );
    }
}
