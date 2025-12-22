use tokio::sync::mpsc::Receiver;

use crate::service::Request;

pub fn spawn_consumer(mut rx: Receiver<Request>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            tracing::info!(msg = %req.msg, "consumed");
        }
        tracing::info!("consumer stopped");
    })
}
