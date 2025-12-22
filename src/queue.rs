use tokio::sync::{
    mpsc::{Sender, error::TrySendError},
    oneshot,
};

use crate::stats::Stats;

pub struct Request {
    pub id: u64,
    pub msg: String,
    pub committed: oneshot::Sender<()>,
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
    let req = Request { id, msg, committed };

    match tx.try_send(req) {
        Ok(_) => {
            stats.inc_ack();
            EnqueueResult::Enqueued(id)
        }
        Err(TrySendError::Full(_)) => {
            stats.inc_nack();
            EnqueueResult::Full
        }
        Err(TrySendError::Closed(_)) => EnqueueResult::Closed,
    }
}
