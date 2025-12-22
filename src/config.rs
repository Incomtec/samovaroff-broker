use tracing::{debug, error, info};

pub struct AppConfig {
    pub node_id: String,
    pub bind_addr: String,
    pub data_dir: String,
    pub max_connections: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            node_id: "node-1".to_string(),
            bind_addr: "[::]:7001".to_string(),
            data_dir: "./data".to_string(),
            max_connections: 256,
        }
    }
}

impl AppConfig {
    pub fn load() -> Self {
        let mut c = Self::default();
        c.bind_addr = std::env::var("BIND_ADDR").unwrap_or(c.bind_addr);
        c.data_dir = std::env::var("DATA_DIR").unwrap_or(c.data_dir);
        c.node_id = std::env::var("NODE_ID").unwrap_or(c.node_id);
        c.max_connections = std::env::var("MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(c.max_connections);

        if let Err(e) = Self::check_data_dir(&c) {
            error!(error = %e, "failed to create data dir");
            std::process::exit(1);
        }
        c
    }

    fn check_data_dir(config: &AppConfig) -> Result<(), std::io::Error> {
        let dir = std::path::Path::new(config.data_dir.as_str());
        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
            info!(data_dir = %config.data_dir, "data dir created");
        } else {
            debug!(data_dir = %config.data_dir, "data dir already exists");
        }
        Ok(())
    }
}
