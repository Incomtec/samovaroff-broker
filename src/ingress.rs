use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::Sender;

use crate::init::Shutdown;
use crate::protocol::Command;
use crate::service::{Request, enqueue_and_reply};
use crate::stats::Stats;

pub const ACK: &[u8] = b"ACK\n";
pub const NACK: &[u8] = b"NACK\n";
pub const TOO_LARGE: &[u8] = b"ERR TOO_LARGE\n";
const OK: &[u8] = b"OK\n";

const MAX_MSG_BYTES: usize = 64 * 1024; // 64KB
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub async fn send(writer: &mut OwnedWriteHalf, msg: &[u8]) {
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
) {
    stats.inc_connections();
    tracing::info!(peer = %peer, "client connected");
    tokio::spawn(handle_client(socket, tx, shutdown, stats));
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
                    Err(_) => break,
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

                // enqueue_and_reply сам отправит ACK/NACK
                if !enqueue_and_reply(&tx, &mut writer, &stats, payload).await {
                    break;
                }
            }
        }
    }

    tracing::info!("client disconnected");
}
