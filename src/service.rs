use std::net::SocketAddr;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc::{self, Sender, error::TrySendError},
};
use tracing::{debug, info};

use crate::{config::AppConfig, consumer, init::Shutdown};

const ACK: &[u8] = b"ACK\n";
const NACK: &[u8] = b"NACK\n";

async fn send(writer: &mut OwnedWriteHalf, msg: &[u8]) {
    let _ = writer.write_all(msg).await;
}

pub struct Request {
    pub msg: String,
}

pub struct Service {
    pub bind_addr: String,
}

async fn enqueue_and_reply(tx: &Sender<Request>, writer: &mut OwnedWriteHalf, msg: String) -> bool {
    let req = Request { msg };

    match tx.try_send(req) {
        Ok(_) => {
            send(writer, ACK).await;
            true
        }
        Err(TrySendError::Full(_)) => {
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

fn spawn_client(socket: TcpStream, peer: SocketAddr, tx: Sender<Request>, shutdown: Shutdown) {
    tracing::info!(peer = %peer, "client connected");
    tokio::spawn(handle_client(socket, peer, tx, shutdown));
}

async fn handle_client(
    socket: TcpStream,
    peer: SocketAddr,
    tx: Sender<Request>,
    shutdown: Shutdown,
) {
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut shutdown = shutdown;

    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            msg = next_line(&mut lines) => {
                let Some(msg) = msg else { break; };
                if !enqueue_and_reply(&tx, &mut writer, msg).await { break; }
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

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                res = listener.accept() => {
                    if let Ok((socket, peer)) = res {
                        spawn_client(socket, peer, mq_sndr.clone(), shutdown.clone());
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
        Ok(())
    }
}
