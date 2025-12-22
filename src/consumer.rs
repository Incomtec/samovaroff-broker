use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::service::Request;

pub fn spawn_consumer(mut rx: Receiver<Request>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            tracing::info!(id = req.id, msg = %req.msg, "consumed");
        }
        tracing::info!("consumer stopped");
    })
}
