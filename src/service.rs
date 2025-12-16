use tracing::{debug, info};

use std::{sync::atomic::Ordering, thread};

use crate::{config::AppConfig, init::Shutdown};

pub struct Service {
    pub bind_addr: String,
}

impl Service {
    pub fn new(conf: &AppConfig) -> Self {
        Service {
            bind_addr: conf.bind_addr.clone(),
        }
    }

    pub fn start(&self, shutdown: &Shutdown) {
        info!(bind_addr = %self.bind_addr, "service started");

        let shutdown_worker = shutdown.clone();
        let bind_addr = self.bind_addr.clone();

        let worker = thread::spawn(move || {
            info!(bind_addr = %bind_addr, "worker started");

            while !shutdown_worker.load(Ordering::SeqCst) {
                debug!("worker tick");
                thread::sleep(std::time::Duration::from_millis(500));
            }

            info!("worker stopping");
        });
        // while !shutdown.load(Ordering::SeqCst) {
        //     std::thread::sleep(std::time::Duration::from_millis(100));
        // }
        info!("shutdown requested");

        worker.join().expect("worker thread panicked");
        info!("service shutting down");
    }
}
