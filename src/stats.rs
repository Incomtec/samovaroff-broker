use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Stats {
    pub ack: AtomicU64,
    pub nack: AtomicU64,
    pub connections: AtomicU64,
    pub next_id: AtomicU64,
}

impl Stats {
    pub fn new_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn inc_ack(&self) {
        self.ack.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_nack(&self) {
        self.nack.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_connections(&self) {
        self.connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.ack.load(Ordering::Relaxed),
            self.nack.load(Ordering::Relaxed),
            self.connections.load(Ordering::Relaxed),
        )
    }
}
