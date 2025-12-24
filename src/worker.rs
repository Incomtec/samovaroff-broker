use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::queue::Request;

// const WORKER_CONCURRENCY: usize = 8;

pub fn spawn_worker(rx: Receiver<Request>, data_dir: String) -> JoinHandle<()> {
    // Worker is an internal persistence pipeline stub.
    // Message is considered durably accepted once appended to WAL + fsync.
    tokio::task::spawn_blocking(move || {
        use crate::wal::Wal;

        let wal_path = format!("{}/wal.log", data_dir);
        let mut wal = Wal::open(&wal_path).expect("wal open failed");

        let mut rx = rx;

        while let Some(req) = rx.blocking_recv() {
            match req {
                Request::Produce { id, msg, committed } => {
                    let _offset = wal.append_msg(id, &msg).expect("wal append failed");
                    let _ = committed.send(());
                    tracing::info!(id = id, "stored");
                }
                Request::Fetch { from, limit, reply } => {
                    let entries = wal.read_from(from, limit).unwrap_or_default();
                    let _ = reply.send(entries);
                    continue;
                }
            }
        }

        tracing::info!("worker stopped");
    })
}
