use tokio::sync::mpsc::Receiver;

use crate::protocol::Command;
use crate::service::Request;

pub fn spawn_consumer(mut rx: Receiver<Request>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            match Command::parse(&req.msg) {
                Command::Ping => tracing::info!("PING"),
                Command::Echo(text) => tracing::info!(text = %text, "ECHO"),
                Command::Unknown(cmd) => tracing::info!(cmd = %cmd, "UNKNOWN"),
            }
        }
        tracing::info!("consumer stopped");
    })
}
