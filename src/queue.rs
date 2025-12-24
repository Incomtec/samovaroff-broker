use tokio::sync::{
    mpsc::{Sender, error::TrySendError},
    oneshot,
};

use crate::{stats::Stats, wal::WalRecord};

pub enum Request {
    Produce {
        id: u64,
        msg: String,
        committed: oneshot::Sender<()>,
    },
    Fetch {
        from: u64,
        limit: usize,
        reply: oneshot::Sender<Vec<WalRecord>>,
    },
}

pub enum EnqueueResult {
    Enqueued(u64),
    Full,
    Closed,
}

pub fn try_enqueue(
    tx: &Sender<Request>,
    stats: &Stats,
    msg: String,
    committed: oneshot::Sender<()>,
) -> EnqueueResult {
    let id = stats.new_id();
    let req = Request::Produce { id, msg, committed };

    match tx.try_send(req) {
        Ok(_) => EnqueueResult::Enqueued(id),
        Err(TrySendError::Full(_)) => EnqueueResult::Full,
        Err(TrySendError::Closed(_)) => EnqueueResult::Closed,
    }
}
