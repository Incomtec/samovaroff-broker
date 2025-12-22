use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc::{self, Sender, error::TrySendError},
};
use tracing::{debug, info};

use crate::protocol::Command;
use crate::{config::AppConfig, consumer, init::Shutdown};

const ACK: &[u8] = b"ACK\n";
const NACK: &[u8] = b"NACK\n";
const OK: &[u8] = b"OK\n";

async fn send(writer: &mut OwnedWriteHalf, msg: &[u8]) {
    let _ = writer.write_all(msg).await;
}

#[derive(Default)]
struct Stats {
    ack: AtomicU64,
    nack: AtomicU64,
    connections: AtomicU64,
}

pub struct Request {
    pub msg: String,
}

pub struct Service {
    pub bind_addr: String,
}

async fn enqueue_and_reply(
    tx: &Sender<Request>,
    writer: &mut OwnedWriteHalf,
    stats: &Stats,
    msg: String,
) -> bool {
    let req = Request { msg };

    match tx.try_send(req) {
        Ok(_) => {
            stats.ack.fetch_add(1, Ordering::Relaxed);
            send(writer, ACK).await;
            true
        }
        Err(TrySendError::Full(_)) => {
            stats.nack.fetch_add(1, Ordering::Relaxed);
            send(writer, NACK).await;
            true
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

async fn next_line(lines: &mut Lines<BufReader<OwnedReadHalf>>) -> Option<String> {
    match lines.next_line().await {
        Ok(Some(line)) => Some(line),
        _ => None,
    }
}

fn spawn_client(
    socket: TcpStream,
    peer: SocketAddr,
    tx: Sender<Request>,
    shutdown: Shutdown,
    stats: Arc<Stats>,
) {
    stats.connections.fetch_add(1, Ordering::Relaxed);
    tracing::info!(peer = %peer, "client connected");
    tokio::spawn(handle_client(socket, peer, tx, shutdown, stats));
}

async fn handle_client(
    socket: TcpStream,
    peer: SocketAddr,
    tx: Sender<Request>,
    shutdown: Shutdown,
    stats: Arc<Stats>,
) {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut shutdown = shutdown;

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            msg = next_line(&mut lines) => {
                let Some(msg) = msg else { break; };

                let payload = match Command::parse(&msg) {
                    Command::Ping => {
                        send(&mut writer, OK).await;
                        continue;
                    }
                    Command::Echo(text) => text,
                    Command::Unknown(_) => msg,
                };
                if !enqueue_and_reply(&tx, &mut writer, &stats, payload).await { break; }
            }
        }
    }
    tracing::info!(peer = %peer, "client disconnected");
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
                        spawn_client(socket, peer, mq_sndr.clone(), shutdown.clone(), stats.clone());
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

        info!(
            ack = stats.ack.load(Ordering::Relaxed),
            nack = stats.nack.load(Ordering::Relaxed),
            connections = stats.connections.load(Ordering::Relaxed),
            "broker stats"
        );

        Ok(())
    }
}
