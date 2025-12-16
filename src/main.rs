mod service;
use service::Service;
mod config;
mod init;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conf, shutdown) = init::all()?;

    let service = Service::new(&conf);

    service.start(&shutdown);

    Ok(())
}
