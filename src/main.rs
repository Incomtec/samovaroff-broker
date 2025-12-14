use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, fmt};

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_level(true)
        .compact()
        .init();
}

fn main() {
    init_tracing();

    info!("tracing starting");
    debug!(pid = std::process::id(), "process info");

    println!("Hello, world!");
}
