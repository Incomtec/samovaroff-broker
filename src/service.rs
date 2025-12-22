use std::sync::Arc;

use tokio::{
    net::TcpListener,
    sync::mpsc::{self},
};
use tracing::{debug, info};

use crate::ingress;
use crate::stats::Stats;
use crate::{config::AppConfig, consumer, init::Shutdown};

use crate::queue::Request;

pub struct Service {
    pub bind_addr: String,
    pub max_connections: usize,
}

impl Service {
    pub fn new(conf: &AppConfig) -> Self {
        Service {
            bind_addr: conf.bind_addr.clone(),
            max_connections: conf.max_connections,
        }
    }

    pub async fn start(&self, shutdown: &Shutdown) -> Result<(), Box<dyn std::error::Error>> {
        info!(bind_addr = %self.bind_addr, "service started");

        let listener = TcpListener::bind(&self.bind_addr).await?;
        info!(bind_addr = %self.bind_addr, "listening");

        let (mq_sndr, mq_rcvr) = mpsc::channel::<Request>(100);

        let consumer_task = consumer::spawn_consumer(mq_rcvr);

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
                            use tokio::io::AsyncWriteExt;
                            let mut socket = socket;
                            let _ = socket.write_all(b"ERR BUSY\n").await;
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
        drop(mq_sndr);

        for task in client_tasks {
            let _ = task.await;
        }

        let _ = consumer_task.await;
        info!("service shutting down");

        let (ack, nack, connections) = stats.snapshot();
        info!(ack, nack, connections, "broker stats");

        Ok(())
    }
}
