use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use crate::init::Shutdown;
use crate::protocol::{Command, Response};
use crate::queue::{EnqueueResult, Request, try_enqueue};
use crate::stats::Stats;

const MAX_MSG_BYTES: usize = 64 * 1024; // 64KB
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

async fn next_line(lines: &mut Lines<BufReader<OwnedReadHalf>>) -> Option<String> {
    match lines.next_line().await {
        Ok(Some(line)) => Some(line),
        _ => None,
    }
}

async fn read_line(
    lines: &mut Lines<BufReader<OwnedReadHalf>>,
    writer: &mut OwnedWriteHalf,
    stats: &Stats,
) -> Option<String> {
    let timed = tokio::time::timeout(READ_TIMEOUT, next_line(lines)).await;

    match timed {
        Ok(Some(v)) => Some(v),
        Ok(None) => None, // EOF
        Err(_) => {
            reply(writer, stats, Response::ErrTimeout).await;
            None
        }
    }
}

async fn process_line(
    tx: &Sender<Request>,
    writer: &mut OwnedWriteHalf,
    stats: &Stats,
    line: String,
) -> bool {
    if line.len() > MAX_MSG_BYTES {
        reply(writer, stats, Response::ErrTooLarge).await;
        return true;
    }

    match Command::parse(&line) {
        Command::Ping => {
            reply(writer, stats, Response::Ok).await;
            true
        }
        Command::Echo(text) => handle_produce(tx, stats, writer, text).await,
        Command::Fetch { offset, limit } => handle_fetch(tx, stats, writer, offset, limit).await,
        Command::Unknown(text) => {
            tracing::warn!(cmd = %text, "unknown command");
            reply(writer, stats, Response::Nack).await;
            true
        }
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

async fn handle_fetch(
    tx: &Sender<Request>,
    stats: &Stats,
    writer: &mut OwnedWriteHalf,
    from: u64,
    limit: usize,
) -> bool {
    let (reply_tx, reply_rx) = oneshot::channel();

    let req = Request::Fetch {
        from,
        limit,
        reply: reply_tx,
    };

    // отправили запрос в backend
    if tx.send(req).await.is_err() {
        reply(writer, stats, Response::ErrWal).await;
        return false;
    }

    // получили ответ от worker
    let entries = match reply_rx.await {
        Ok(v) => v,
        Err(_) => {
            reply(writer, stats, Response::ErrWal).await;
            return false;
        }
    };

    for e in entries {
        let line = format!("{}\t{}\t{}\n", e.offset, e.id, e.payload);
        let _ = writer.write_all(line.as_bytes()).await;
    }

    reply(writer, stats, Response::Ok).await;
    true
}

async fn handle_produce(
    tx: &Sender<Request>,
    stats: &Stats,
    writer: &mut OwnedWriteHalf,
    payload: String,
) -> bool {
    let (commit_tx, commit_rx) = oneshot::channel();

    match try_enqueue(tx, stats, payload, commit_tx) {
        EnqueueResult::Enqueued(id) => {
            if commit_rx.await.is_ok() {
                tracing::info!(id, "committed");
                reply(writer, stats, Response::Ack).await;
                true
            } else {
                tracing::error!(id, "commit failed");
                reply(writer, stats, Response::ErrWal).await;
                false
            }
        }
        EnqueueResult::Full => {
            tracing::error!("queue is full");
            reply(writer, stats, Response::Nack).await;
            true
        }
        EnqueueResult::Closed => false,
    }
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

            line = read_line(&mut lines, &mut writer, &stats) => {
                let Some(line) = line else { break; };
                if !process_line(&tx, &mut writer, &stats, line).await {
                    break;
                }
            }
        }
    }

    tracing::info!("client disconnected");
}

fn record(stats: &Stats, r: &Response) {
    match r {
        Response::Ack => stats.inc_ack(),
        Response::Nack => stats.inc_nack(),
        Response::ErrWal => stats.inc_err_wal(),
        _ => {}
    }
}

pub(crate) async fn reply(writer: &mut OwnedWriteHalf, stats: &Stats, r: Response) {
    record(stats, &r);
    let _ = writer.write_all(r.as_bytes()).await;
}
