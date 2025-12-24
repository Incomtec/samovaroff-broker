use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::queue::Request;

// const WORKER_CONCURRENCY: usize = 8;

pub fn spawn_worker(rx: Receiver<Request>, data_dir: String) -> JoinHandle<()> {
    // Worker is an internal persistence pipeline stub.
    // Message is considered durably accepted once appended to WAL + fsync.
    tokio::task::spawn_blocking(move || {
        use crate::wal::Wal;
        use std::collections::HashMap;

        let mut rx = rx;
        let mut wals: HashMap<String, Wal> = HashMap::new();

        while let Some(req) = rx.blocking_recv() {
            match req {
                Request::Produce {
                    topic,
                    id,
                    msg,
                    committed,
                } => {
                    let wal = wals.entry(topic.clone()).or_insert_with(|| {
                        let topic_dir = format!("{}/{}", data_dir, topic);
                        std::fs::create_dir_all(&topic_dir).expect("topic dir create failed");
                        let wal_path = format!("{}/wal.log", topic_dir);
                        Wal::open(&wal_path).expect("wal open failed")
                    });

                    let _offset = wal.append_msg(id, &msg).expect("wal append failed");
                    let _ = committed.send(());
                    tracing::info!(topic = %topic, id, "stored");
                }

                Request::Fetch {
                    topic,
                    from,
                    limit,
                    reply,
                } => {
                    let entries = match wals.get_mut(&topic) {
                        Some(wal) => wal.read_from(from, limit).unwrap_or_default(),
                        None => {
                            // топик не создаем на FETCH
                            let wal_path = format!("{}/{}/wal.log", data_dir, topic);

                            // если файла нет - считаем, что топика нет
                            if std::path::Path::new(&wal_path).exists() {
                                let wal = Wal::open(&wal_path).expect("wal open failed");
                                let res = wal.read_from(from, limit).unwrap_or_default();
                                wals.insert(topic.clone(), wal);
                                res
                            } else {
                                Vec::new()
                            }
                        }
                    };

                    let _ = reply.send(entries);
                }
            }
        }

        tracing::info!("worker stopped");
    })
}
