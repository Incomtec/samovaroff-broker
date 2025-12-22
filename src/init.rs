use tokio::sync::watch;
use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::AppConfig;

pub type Shutdown = watch::Receiver<bool>;

pub fn all() -> Result<(AppConfig, tokio::sync::watch::Receiver<bool>), Box<dyn std::error::Error>>
{
    init_tracing()?;

    let conf = AppConfig::load();
    debug!(
        node_id = %conf.node_id,
        bind_addr = %conf.bind_addr,
        data_dir = %conf.data_dir,
        "config loaded"
    );

    debug!(pid = std::process::id(), "process info");

    let shutdown = init_shutdown();

    Ok((conf, shutdown))
}

pub fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_level(true)
        .compact()
        .init();

    info!("tracing starting");

    Ok(())
}

pub fn init_shutdown() -> watch::Receiver<bool> {
    let (_tx, rx) = watch::channel(false);
    rx
}
