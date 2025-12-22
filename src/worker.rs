use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::queue::Request;

// const WORKER_CONCURRENCY: usize = 8;

pub fn spawn_worker(rx: Receiver<Request>, data_dir: String) -> JoinHandle<()> {
    // Worker is an internal persistence pipeline stub.
    // Message is considered durably accepted once appended to WAL + fsync.
    tokio::task::spawn_blocking(move || {
        use crate::wal::Wal;
        use base64::{Engine as _, engine::general_purpose::STANDARD};

        let wal_path = format!("{}/wal.log", data_dir);
        let mut wal = Wal::open(&wal_path).expect("wal open failed");

        let mut rx = rx;

        while let Some(req) = rx.blocking_recv() {
            let payload_b64 = STANDARD.encode(req.msg.as_bytes());
            let _offset = wal.append(req.id, &payload_b64).expect("wal append failed");
            let _ = req.committed.send(());
            tracing::info!(id = req.id, "stored");
        }

        tracing::info!("worker stopped");
    })
    /*
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(WORKER_CONCURRENCY));

        while let Some(req) = rx.recv().await {
            let sem = sem.clone();
            let permit = sem.acquire_owned().await.expect("semaphore closed");

            tokio::spawn(async move {
                // "обработка" сообщения
                tracing::info!(id = req.id, msg = %req.msg, "consumed");

                drop(permit);
            });
        }
        tracing::info!("worker stopped");
    })
    */
}
