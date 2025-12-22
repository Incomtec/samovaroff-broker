mod config;
mod consumer;
mod init;
mod protocol;
mod service;

use service::Service;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conf, _shutdown) = init::all()?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Ждем Ctrl+C и шлем сигнал остановки
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown requested");

        let _ = shutdown_tx.send(true);
    });

    let service = Service::new(&conf);

    service.start(&shutdown_rx).await?;

    Ok(())
}
