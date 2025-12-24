use std::sync::Arc;

use tokio::{
    net::TcpListener,
    sync::mpsc::{self},
};
use tracing::{debug, info};

use crate::stats::Stats;
use crate::{config::AppConfig, init::Shutdown, worker};
use crate::{ingress, protocol::Response};

use crate::queue::Request;

pub struct Service {
    pub bind_addr: String,
    pub max_connections: usize,
    pub data_dir: String,
}

impl Service {
    pub fn new(conf: &AppConfig) -> Self {
        Service {
            bind_addr: conf.bind_addr.clone(),
            max_connections: conf.max_connections,
            data_dir: conf.data_dir.clone(),
        }
    }

    pub async fn start(&self, shutdown: &Shutdown) -> Result<(), Box<dyn std::error::Error>> {
        info!(bind_addr = %self.bind_addr, "service started");

        let listener = TcpListener::bind(&self.bind_addr).await?;
        info!(bind_addr = %self.bind_addr, "listening");

        let (mq_sndr, mq_rcvr) = mpsc::channel::<Request>(100);

        let worker_task = worker::spawn_worker(mq_rcvr, self.data_dir.clone());

        let mut shutdown_rx = shutdown.clone();

        debug_assert!(self.max_connections > 0);
        debug_assert!(self.max_connections >= 1);

        let stats = Arc::new(Stats::default());

        let mut client_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let mut accept_count: u64 = 0;

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                res = listener.accept() => {
                    if let Ok((socket, peer)) = res {
                        if client_tasks.len() >= self.max_connections {
                            let (mut _r, mut w) = socket.into_split();
                            ingress::reply(&mut w, stats.as_ref(), Response::ErrBusy).await;
                            continue;
                        }
                        debug_assert!(client_tasks.len() <= self.max_connections);
                        let h = ingress::spawn_client(socket, peer, mq_sndr.clone(), shutdown.clone(), stats.clone());
                        client_tasks.push(h);

                        accept_count += 1;
                        if accept_count.is_multiple_of(64) {
                            client_tasks.retain(|h| !h.is_finished());
                        }

                    } else if let Err(e) = res {
                        debug!(err = %e, "accept failed");
                    }
                }
            }
        }
        drop(listener);

        for task in client_tasks {
            let _ = task.await;
        }

        drop(mq_sndr);

        let _ = worker_task.await;

        info!("service shutting down");

        let (ack, nack, err_wal, connections) = stats.snapshot();
        info!(ack, nack, err_wal, connections, "broker stats");

        Ok(())
    }
}
