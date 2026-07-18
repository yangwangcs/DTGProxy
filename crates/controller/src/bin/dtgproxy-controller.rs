use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use controller::{
    BackendReconciler, CatalogApi, ControllerRuntimeConfig, Reconciler, RemoteCatalog,
    RemoteDataPlane,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("dtgproxy-controller: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config = ControllerRuntimeConfig::load(config_path_from_args()?)?;
    let catalog = RemoteCatalog::new(
        *config.cluster_id(),
        config.controller_id(),
        config.meta_seeds().to_vec(),
        config.request_timeout(),
    )?;
    let data = RemoteDataPlane::new(
        *config.cluster_id(),
        config.controller_id(),
        config.data_nodes().clone(),
        config.request_timeout(),
    )?;
    let initial_lease = catalog.acquire_lease().await?;
    println!(
        "DTGPROXY_CONTROLLER_READY node={} owner_term={} lease_expires={}",
        config.controller_id(),
        initial_lease.owner_term,
        initial_lease.expires_unix_ms
    );
    std::io::stdout().flush()?;

    let mut interval = tokio::time::interval(config.reconcile_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = interval.tick() => {
                let lease = match catalog.acquire_lease().await {
                    Ok(lease) => lease,
                    Err(error) => {
                        eprintln!("dtgproxy-controller lease: {error}");
                        continue;
                    }
                };
                let state = match catalog.load().await {
                    Ok(state) => state,
                    Err(error) => {
                        eprintln!("dtgproxy-controller catalog: {error}");
                        continue;
                    }
                };
                let migrations = state
                    .migrations()
                    .values()
                    .filter(|migration| !migration.state().is_terminal())
                    .map(|migration| migration.migration_id())
                    .collect::<Vec<_>>();
                let backend_migrations = state
                    .backend_migrations()
                    .values()
                    .filter(|migration| !migration.state().is_terminal())
                    .map(|migration| migration.migration_id())
                    .collect::<Vec<_>>();
                let reconciler = Reconciler::new(catalog.clone(), data.clone(), lease.owner_term)?;
                for migration_id in migrations {
                    if let Err(error) = reconciler.reconcile(migration_id, now_ms()?).await {
                        eprintln!("dtgproxy-controller migration={migration_id:032x}: {error}");
                    }
                }
                let backend_reconciler =
                    BackendReconciler::new(catalog.clone(), data.clone(), lease.owner_term)?;
                for migration_id in backend_migrations {
                    if let Err(error) = backend_reconciler.reconcile(migration_id, now_ms()?).await {
                        eprintln!(
                            "dtgproxy-controller backend-migration={migration_id:032x}: {error}"
                        );
                    }
                }
            }
        }
    }
}

fn config_path_from_args() -> Result<PathBuf, Box<dyn Error>> {
    let mut arguments = std::env::args_os();
    let _binary = arguments.next();
    let path = arguments
        .next()
        .ok_or("usage: dtgproxy-controller <config.json>")?;
    if arguments.next().is_some() {
        return Err("usage: dtgproxy-controller <config.json>".into());
    }
    Ok(PathBuf::from(path))
}

fn now_ms() -> Result<u64, Box<dyn Error>> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
