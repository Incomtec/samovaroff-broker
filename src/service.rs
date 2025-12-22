use std::sync::Arc;

use tokio::{
    net::{TcpListener, tcp::OwnedWriteHalf},
    sync::mpsc::{self, Sender, error::TrySendError},
};
use tracing::{debug, info};

use crate::ingress;
use crate::stats::Stats;
use crate::{config::AppConfig, consumer, init::Shutdown};

pub struct Request {
    pub id: u64,
    pub msg: String,
}

pub struct Service {
    pub bind_addr: String,
}

pub(crate) async fn enqueue_and_reply(
    tx: &Sender<Request>,
    writer: &mut OwnedWriteHalf,
    stats: &Stats,
    msg: String,
) -> bool {
    let id = stats.new_id();
    let req = Request { id, msg };

    match tx.try_send(req) {
        Ok(_) => {
            stats.inc_ack();
            tracing::info!(id, "enqueued");
            ingress::send(writer, ingress::ACK).await;
            true
        }
        Err(TrySendError::Full(_)) => {
            stats.inc_nack();
            tracing::warn!(id, "queue full, nack sent");
            ingress::send(writer, ingress::NACK).await;
            true
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

impl Service {
    pub fn new(conf: &AppConfig) -> Self {
        Service {
            bind_addr: conf.bind_addr.clone(),
        }
    }

    pub async fn start(&self, shutdown: &Shutdown) -> Result<(), Box<dyn std::error::Error>> {
        info!(bind_addr = %self.bind_addr, "service started");

        let listener = TcpListener::bind(&self.bind_addr).await?;
        info!(bind_addr = %self.bind_addr, "listening");

        let (mq_sndr, mq_rcvr) = mpsc::channel::<Request>(100);

        // Consumer: читает очередь и логирует

        let consumer_task = consumer::spawn_consumer(mq_rcvr);

        let mut shutdown_rx = shutdown.clone();

        let stats = Arc::new(Stats::default());

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                res = listener.accept() => {
                    if let Ok((socket, peer)) = res {
                        ingress::spawn_client(socket, peer, mq_sndr.clone(), shutdown.clone(), stats.clone());
                    } else if let Err(e) = res {
                        debug!(err = %e, "accept failed");
                    }
                }
            }
        }
        drop(listener);
        drop(mq_sndr);
        let _ = consumer_task.await;
        info!("service shutting down");

        let (ack, nack, connections) = stats.snapshot();
        info!(ack, nack, connections, "broker stats");

        Ok(())
    }
}
