use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, Digest32, ProviderKind, ReplicaBinding,
    StorageError,
};
use tokio_postgres::{Client, Row};

use crate::config::{PostgresConfig, postgres_error};

pub(crate) const POSTGRES_CONTRACT_VERSION: u32 = 1;
pub(crate) const POSTGRES_LAYOUT_VERSION: u32 = 1;
pub(crate) const MIGRATION_SQL: &str = include_str!("../migrations/0001_replica_schema.sql");

pub(crate) fn postgres_capabilities() -> Result<CapabilityManifest, StorageError> {
    CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
}

pub(crate) fn validate_postgres_binding(binding: &ReplicaBinding) -> Result<(), StorageError> {
    let capabilities = postgres_capabilities()?;
    let class = BackendClass::new(
        ProviderKind::PostgreSql,
        POSTGRES_CONTRACT_VERSION,
        POSTGRES_LAYOUT_VERSION,
        capabilities.names().map(str::to_owned),
    )?;
    if binding.provider_kind() != &ProviderKind::PostgreSql
        || binding.contract_version() != POSTGRES_CONTRACT_VERSION
        || binding.layout_version() != POSTGRES_LAYOUT_VERSION
        || binding.capability_digest() != capabilities.digest()
        || binding.backend_class_digest() != class.digest()
    {
        return Err(StorageError::InvalidBinding(
            "binding does not match the native PostgreSQL backend class".into(),
        ));
    }
    Ok(())
}

pub(crate) async fn initialize_namespace(
    config: &PostgresConfig,
    schema_name: &str,
    binding: &ReplicaBinding,
) -> Result<(), StorageError> {
    validate_postgres_binding(binding)?;
    let client = config.connect_unscoped().await?;
    client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema_name}; SET search_path TO {schema_name}, pg_catalog; {MIGRATION_SQL}"
        ))
        .await
        .map_err(postgres_error)?;
    client
        .batch_execute(
            "BEGIN ISOLATION LEVEL SERIALIZABLE;
             SET LOCAL synchronous_commit = on",
        )
        .await
        .map_err(postgres_error)?;
    let result = async {
        insert_owner_if_absent(&client, binding).await?;
        let actual = load_owner(&client, true).await?;
        if &actual != binding {
            return Err(StorageError::NamespaceOwnerMismatch {
                expected: Box::new(binding.clone()),
                actual: Box::new(actual),
            });
        }
        client
            .execute(
                "INSERT INTO replica_meta (singleton, applied_index) VALUES (TRUE, $1) ON CONFLICT (singleton) DO NOTHING",
                &[&u64_bytes(0)],
            )
            .await
            .map_err(postgres_error)?;
        Ok(())
    }
    .await;
    finish_transaction(&client, result).await
}

async fn insert_owner_if_absent(
    client: &Client,
    binding: &ReplicaBinding,
) -> Result<(), StorageError> {
    let cluster_id = u64_bytes(binding.cluster_id().get());
    let graph_id = u64_bytes(binding.graph_id().get());
    let shard_id = u64_bytes(binding.shard_id().get());
    let placement_epoch = u64_bytes(binding.placement_epoch().get());
    let replica_id = u64_bytes(binding.replica_id().get());
    let backend_generation = u64_bytes(binding.backend_generation().get());
    let backend_class_digest = binding.backend_class_digest().get().to_vec();
    let capability_digest = binding.capability_digest().get().to_vec();
    let binding_digest = binding.identity_digest().get().to_vec();
    client
        .execute(
            "INSERT INTO replica_owner (
                singleton, cluster_id, graph_id, shard_id, placement_epoch, replica_id,
                backend_generation, backend_class_digest, provider_kind, contract_version,
                layout_version, capability_digest, namespace_id, endpoint_profile_ref,
                credential_ref, binding_role, binding_digest
             ) VALUES (
                TRUE, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
             ) ON CONFLICT (singleton) DO NOTHING",
            &[
                &cluster_id,
                &graph_id,
                &shard_id,
                &placement_epoch,
                &replica_id,
                &backend_generation,
                &backend_class_digest,
                &"postgresql",
                &i32::try_from(binding.contract_version()).map_err(|_| {
                    StorageError::InvalidBinding(
                        "contract version exceeds PostgreSQL INTEGER".into(),
                    )
                })?,
                &i32::try_from(binding.layout_version()).map_err(|_| {
                    StorageError::InvalidBinding("layout version exceeds PostgreSQL INTEGER".into())
                })?,
                &capability_digest,
                &binding.namespace_id().as_str(),
                &binding.endpoint_profile_ref(),
                &binding.credential_ref(),
                &role_tag(binding.role()),
                &binding_digest,
            ],
        )
        .await
        .map_err(postgres_error)?;
    Ok(())
}

pub(crate) async fn verify_owner(
    client: &Client,
    binding: &ReplicaBinding,
    lock: bool,
) -> Result<(), StorageError> {
    let actual = load_owner(client, lock).await?;
    if &actual == binding {
        Ok(())
    } else {
        Err(StorageError::NamespaceOwnerMismatch {
            expected: Box::new(binding.clone()),
            actual: Box::new(actual),
        })
    }
}

pub(crate) fn ensure_serving_binding(binding: &ReplicaBinding) -> Result<(), StorageError> {
    if binding.role() == BindingRole::Active {
        Ok(())
    } else {
        Err(StorageError::InvalidBinding(
            "PostgreSQL candidate and retiring replicas are non-serving".into(),
        ))
    }
}

pub(crate) async fn load_owner(
    client: &Client,
    lock: bool,
) -> Result<ReplicaBinding, StorageError> {
    let suffix = if lock { " FOR UPDATE" } else { "" };
    let row = client
        .query_opt(
            &format!(
                "SELECT cluster_id, graph_id, shard_id, placement_epoch, replica_id,
                        backend_generation, backend_class_digest, provider_kind,
                        contract_version, layout_version, capability_digest, namespace_id,
                        endpoint_profile_ref, credential_ref, binding_role, binding_digest
                 FROM replica_owner WHERE singleton = TRUE{suffix}"
            ),
            &[],
        )
        .await
        .map_err(postgres_error)?
        .ok_or_else(|| StorageError::Internal("PostgreSQL namespace has no owner row".into()))?;
    decode_binding(&row)
}

fn decode_binding(row: &Row) -> Result<ReplicaBinding, StorageError> {
    let provider: String = row.get(7);
    if provider != "postgresql" {
        return Err(StorageError::Internal(
            "PostgreSQL owner row contains a foreign provider kind".into(),
        ));
    }
    let contract_version: i32 = row.get(8);
    let layout_version: i32 = row.get(9);
    let binding = ReplicaBinding::builder()
        .cluster_id(decode_u64(row.get::<_, Vec<u8>>(0).as_slice())?)
        .graph_id(decode_u64(row.get::<_, Vec<u8>>(1).as_slice())?)
        .shard_id(decode_u64(row.get::<_, Vec<u8>>(2).as_slice())?)
        .placement_epoch(decode_u64(row.get::<_, Vec<u8>>(3).as_slice())?)
        .replica_id(decode_u64(row.get::<_, Vec<u8>>(4).as_slice())?)
        .backend_generation(decode_u64(row.get::<_, Vec<u8>>(5).as_slice())?)
        .backend_class_digest(decode_digest(row.get::<_, Vec<u8>>(6).as_slice())?)
        .provider_kind(ProviderKind::PostgreSql)
        .contract_version(
            u32::try_from(contract_version).map_err(|_| {
                StorageError::Internal("invalid PostgreSQL contract version".into())
            })?,
        )
        .layout_version(
            u32::try_from(layout_version)
                .map_err(|_| StorageError::Internal("invalid PostgreSQL layout version".into()))?,
        )
        .capability_digest(decode_digest(row.get::<_, Vec<u8>>(10).as_slice())?)
        .namespace_id(row.get::<_, String>(11))
        .endpoint_profile_ref(row.get::<_, String>(12))
        .credential_ref(row.get::<_, String>(13))
        .role(decode_role(row.get(14))?)
        .build()?;
    let stored_digest = decode_digest(row.get::<_, Vec<u8>>(15).as_slice())?;
    if binding.identity_digest() != stored_digest {
        return Err(StorageError::Internal(
            "PostgreSQL owner binding digest is corrupt".into(),
        ));
    }
    Ok(binding)
}

pub(crate) async fn read_applied_index(client: &Client) -> Result<u64, StorageError> {
    let row = client
        .query_one(
            "SELECT applied_index FROM replica_meta WHERE singleton = TRUE",
            &[],
        )
        .await
        .map_err(postgres_error)?;
    decode_u64(row.get::<_, Vec<u8>>(0).as_slice())
}

pub(crate) async fn finish_transaction<T>(
    client: &Client,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    match result {
        Ok(value) => {
            client
                .batch_execute("COMMIT")
                .await
                .map_err(postgres_error)?;
            Ok(value)
        }
        Err(error) => {
            let _ = client.batch_execute("ROLLBACK").await;
            Err(error)
        }
    }
}

pub(crate) fn u64_bytes(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

pub(crate) fn u128_bytes(value: u128) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

pub(crate) fn decode_u64(bytes: &[u8]) -> Result<u64, StorageError> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        StorageError::Internal("invalid PostgreSQL u64 bytes".into())
    })?))
}

pub(crate) fn decode_u128(bytes: &[u8]) -> Result<u128, StorageError> {
    Ok(u128::from_be_bytes(bytes.try_into().map_err(|_| {
        StorageError::Internal("invalid PostgreSQL u128 bytes".into())
    })?))
}

pub(crate) fn decode_digest(bytes: &[u8]) -> Result<Digest32, StorageError> {
    Ok(Digest32::new(bytes.try_into().map_err(|_| {
        StorageError::Internal("invalid PostgreSQL digest bytes".into())
    })?))
}

pub(crate) fn role_tag(role: BindingRole) -> i16 {
    match role {
        BindingRole::Candidate => 1,
        BindingRole::Active => 2,
        BindingRole::Retiring => 3,
    }
}

fn decode_role(tag: i16) -> Result<BindingRole, StorageError> {
    match tag {
        1 => Ok(BindingRole::Candidate),
        2 => Ok(BindingRole::Active),
        3 => Ok(BindingRole::Retiring),
        _ => Err(StorageError::Internal(
            "invalid PostgreSQL binding role".into(),
        )),
    }
}
