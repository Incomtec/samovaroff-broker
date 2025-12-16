use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::AppConfig;

pub type Shutdown = Arc<AtomicBool>;

pub fn all() -> Result<(AppConfig, Shutdown), Box<dyn std::error::Error>> {
    init_tracing()?;

    let conf = AppConfig::load();
    debug!(
        node_id = %conf.node_id,
        bind_addr = %conf.bind_addr,
        data_dir = %conf.data_dir,
        "config loaded"
    );

    debug!(pid = std::process::id(), "process info");

    let shutdown = init_shutdown()?;
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

pub fn init_shutdown() -> Result<Arc<AtomicBool>, ctrlc::Error> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let sh_clone = shutdown.clone();
    ctrlc::set_handler(move || {
        sh_clone.store(true, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");
    Ok(shutdown)
}
