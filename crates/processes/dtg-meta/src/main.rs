use std::error::Error;
use std::path::PathBuf;

use dtg_meta::{MetaConfig, MetaProcess};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("dtgproxy-meta: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config_path = config_path()?;
    let _ = tracing_subscriber::fmt().with_target(false).try_init();
    MetaProcess::open(MetaConfig::load(config_path)?)
        .await?
        .serve()
        .await?;
    Ok(())
}

fn config_path() -> Result<PathBuf, Box<dyn Error>> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("--config")) {
        return Err("usage: dtgproxy-meta --config PATH".into());
    }
    let path = arguments
        .next()
        .ok_or("usage: dtgproxy-meta --config PATH")?;
    if arguments.next().is_some() {
        return Err("usage: dtgproxy-meta --config PATH".into());
    }
    Ok(path.into())
}
