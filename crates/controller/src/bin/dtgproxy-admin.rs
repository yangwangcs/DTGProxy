use std::collections::BTreeMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use control_plane::BackendMigrationState;
use controller::{
    BackendTargetSpec, CatalogApi, ControllerRuntimeConfig, RemoteCatalog, abort_backend_migration,
    start_backend_migration,
};
use serde::Deserialize;

const MAX_TARGET_FILE_BYTES: usize = 1024 * 1024;
const USAGE: &str = "usage:\n  dtgproxy-admin backend-migrate <controller.json> <migration-id-hex> <graph-id> <target.json>\n  dtgproxy-admin backend-abort <controller.json> <migration-id-hex>\n  dtgproxy-admin backend-status <controller.json> <migration-id-hex>";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetFile {
    version: u32,
    provider: String,
    #[serde(default)]
    public_parameters: BTreeMap<String, String>,
    #[serde(default)]
    secret_references: BTreeMap<String, String>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("dtgproxy-admin: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args();
    let _binary = arguments.next();
    let operation = arguments.next().ok_or(USAGE)?;
    match operation.as_str() {
        "backend-migrate" => {
            let config_path = PathBuf::from(arguments.next().ok_or(USAGE)?);
            let migration_id = parse_migration_id(&arguments.next().ok_or(USAGE)?)?;
            let graph_id = arguments.next().ok_or(USAGE)?.parse::<u64>()?;
            let target_path = PathBuf::from(arguments.next().ok_or(USAGE)?);
            ensure_no_more(arguments)?;
            let config = ControllerRuntimeConfig::load(config_path)?;
            let catalog = remote_catalog(&config)?;
            let lease = catalog.acquire_lease().await?;
            let target = load_target(&target_path)?;
            let migration = start_backend_migration(
                &catalog,
                migration_id,
                graph_id,
                target,
                lease.owner_term,
                now_ms()?,
            )
            .await?;
            print_migration(&migration)?;
        }
        "backend-abort" => {
            let config_path = PathBuf::from(arguments.next().ok_or(USAGE)?);
            let migration_id = parse_migration_id(&arguments.next().ok_or(USAGE)?)?;
            ensure_no_more(arguments)?;
            let config = ControllerRuntimeConfig::load(config_path)?;
            let catalog = remote_catalog(&config)?;
            let lease = catalog.acquire_lease().await?;
            let migration =
                abort_backend_migration(&catalog, migration_id, lease.owner_term, now_ms()?)
                    .await?;
            print_migration(&migration)?;
        }
        "backend-status" => {
            let config_path = PathBuf::from(arguments.next().ok_or(USAGE)?);
            let migration_id = parse_migration_id(&arguments.next().ok_or(USAGE)?)?;
            ensure_no_more(arguments)?;
            let config = ControllerRuntimeConfig::load(config_path)?;
            let catalog = remote_catalog(&config)?;
            let state = catalog.load().await?;
            let migration = state
                .backend_migration(migration_id)
                .ok_or_else(|| format!("backend migration {migration_id:032x} does not exist"))?;
            print_migration(migration)?;
        }
        _ => return Err(USAGE.into()),
    }
    Ok(())
}

fn remote_catalog(config: &ControllerRuntimeConfig) -> Result<RemoteCatalog, Box<dyn Error>> {
    Ok(RemoteCatalog::new(
        *config.cluster_id(),
        config.controller_id(),
        config.meta_seeds().to_vec(),
        config.request_timeout(),
    )?)
}

fn load_target(path: &Path) -> Result<BackendTargetSpec, Box<dyn Error>> {
    let bytes = std::fs::read(path)?;
    if bytes.is_empty() || bytes.len() > MAX_TARGET_FILE_BYTES {
        return Err("invalid backend target file size".into());
    }
    let raw: TargetFile = serde_json::from_slice(&bytes)?;
    if raw.version != 1 {
        return Err(format!("unsupported backend target version {}", raw.version).into());
    }
    Ok(BackendTargetSpec::new(
        raw.provider,
        raw.public_parameters,
        raw.secret_references,
    )?)
}

fn parse_migration_id(value: &str) -> Result<u128, Box<dyn Error>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.is_empty() || value.len() > 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("migration ID must contain 1 to 32 hexadecimal digits".into());
    }
    let migration_id = u128::from_str_radix(value, 16)?;
    if migration_id == 0 {
        return Err("migration ID must be nonzero".into());
    }
    Ok(migration_id)
}

fn print_migration(
    migration: &control_plane::BackendMigrationRecord,
) -> Result<(), Box<dyn Error>> {
    let value = serde_json::json!({
        "migration_id": format!("{:032x}", migration.migration_id()),
        "graph_id": migration.graph_id(),
        "source_provider": migration.source().provider(),
        "source_generation": migration.source().generation(),
        "target_provider": migration.target().provider(),
        "target_generation": migration.target().generation(),
        "state": state_name(migration.state()),
        "state_revision": migration.state_revision(),
        "owner_term": migration.owner_term(),
        "receipt_count": migration.receipts().len(),
    });
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}

const fn state_name(state: BackendMigrationState) -> &'static str {
    match state {
        BackendMigrationState::Preparing => "preparing",
        BackendMigrationState::Restored => "restored",
        BackendMigrationState::DualApplying => "dual_applying",
        BackendMigrationState::Verified => "verified",
        BackendMigrationState::CutOver => "cut_over",
        BackendMigrationState::Published => "published",
        BackendMigrationState::SourceRetired => "source_retired",
        BackendMigrationState::Aborting => "aborting",
        BackendMigrationState::Aborted => "aborted",
    }
}

fn ensure_no_more(mut arguments: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    if arguments.next().is_some() {
        return Err(USAGE.into());
    }
    Ok(())
}

fn now_ms() -> Result<u64, Box<dyn Error>> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
