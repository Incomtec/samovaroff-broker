use tokio::sync::mpsc::Receiver;

use crate::service::Request;

enum Command {
    Ping,
    Echo(String),
    Unknown(String),
}

impl Command {
    pub fn parse(cmd: &str) -> Self {
        let cmd = cmd.trim();
        if cmd == "PING" {
            Command::Ping
        } else if let Some(rest) = cmd.strip_prefix("ECHO ") {
            Command::Echo(rest.to_string())
        } else {
            Command::Unknown(cmd.to_string())
        }
    }
}

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
