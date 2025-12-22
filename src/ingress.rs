use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use crate::init::Shutdown;
use crate::protocol::Command;
use crate::queue::{EnqueueResult, Request, try_enqueue};
use crate::stats::Stats;

pub(crate) const ACK: &[u8] = b"ACK\n";
pub(crate) const NACK: &[u8] = b"NACK\n";
pub(crate) const TOO_LARGE: &[u8] = b"ERR TOO_LARGE\n";
pub(crate) const OK: &[u8] = b"OK\n";
pub(crate) const TIMEOUT: &[u8] = b"ERR TIMEOUT\n";

const MAX_MSG_BYTES: usize = 64 * 1024; // 64KB
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub(crate) async fn send(writer: &mut OwnedWriteHalf, msg: &[u8]) {
    let _ = writer.write_all(msg).await;
}

async fn next_line(lines: &mut Lines<BufReader<OwnedReadHalf>>) -> Option<String> {
    match lines.next_line().await {
        Ok(Some(line)) => Some(line),
        _ => None,
    }
}

pub fn spawn_client(
    socket: TcpStream,
    peer: SocketAddr,
    tx: Sender<Request>,
    shutdown: Shutdown,
    stats: Arc<Stats>,
) -> tokio::task::JoinHandle<()> {
    stats.inc_connections();
    tracing::info!(peer = %peer, "client connected");
    tokio::spawn(handle_client(socket, tx, shutdown, stats))
}

async fn handle_client(
    socket: TcpStream,
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
            timed = tokio::time::timeout(READ_TIMEOUT, next_line(&mut lines)) => {
                let line = match timed {
                    Ok(Some(v)) => v,
                    Ok(None) => break,
                    Err(_) => {
                        send(&mut writer, TIMEOUT).await;
                        break;
                    }
                };

                if line.len() > MAX_MSG_BYTES {
                    send(&mut writer, TOO_LARGE).await;
                    continue;
                }

                let payload = match Command::parse(&line) {
                    Command::Ping => {
                        send(&mut writer, OK).await;
                        continue;
                    }
                    Command::Echo(text) => text,
                    Command::Unknown(text) => text,
                };

                let (commit_tx, commit_rx) = oneshot::channel();

                match try_enqueue(&tx, &stats, payload, commit_tx) {
                    EnqueueResult::Enqueued(id) => {
                        // Ждем подтверждение от worker-а: WAL append + fsync
                        if commit_rx.await.is_ok() {
                            tracing::info!(id, "committed");
                            send(&mut writer, ACK).await;
                        } else {
                            // worker умер/закрылся - durability не подтверждена
                            send(&mut writer, b"ERR WAL\n").await;
                            break;
                        }
                    }
                    EnqueueResult::Full => {
                        send(&mut writer, NACK).await;
                    }
                    EnqueueResult::Closed => break,
                }
            }
        }
    }

    tracing::info!("client disconnected");
}
