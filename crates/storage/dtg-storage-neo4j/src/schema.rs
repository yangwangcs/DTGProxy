use base64::{Engine as _, engine::general_purpose::STANDARD};
use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, Digest32, ProviderKind, ReplicaBinding,
    StorageError,
};
use serde_json::{Map, Value, json};

use crate::{
    NativeModel,
    config::{QueryApiClient, QueryApiTransaction, QueryRows},
};

pub(crate) const NEO4J_CONTRACT_VERSION: u32 = 1;
pub(crate) const NEO4J_LAYOUT_VERSION: u32 = 1;

pub(crate) fn neo4j_capabilities() -> Result<CapabilityManifest, StorageError> {
    CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
}

pub(crate) fn validate_neo4j_binding(binding: &ReplicaBinding) -> Result<(), StorageError> {
    let capabilities = neo4j_capabilities()?;
    let class = BackendClass::new(
        ProviderKind::Neo4j,
        NEO4J_CONTRACT_VERSION,
        NEO4J_LAYOUT_VERSION,
        capabilities.names().map(str::to_owned),
    )?;
    if binding.provider_kind() != &ProviderKind::Neo4j
        || binding.contract_version() != NEO4J_CONTRACT_VERSION
        || binding.layout_version() != NEO4J_LAYOUT_VERSION
        || binding.capability_digest() != capabilities.digest()
        || binding.backend_class_digest() != class.digest()
    {
        return Err(StorageError::InvalidBinding(
            "binding does not match the native Neo4j backend class".into(),
        ));
    }
    Ok(())
}

pub(crate) async fn initialize_namespace(
    client: &QueryApiClient,
    binding: &ReplicaBinding,
) -> Result<(), StorageError> {
    validate_neo4j_binding(binding)?;
    for statement in NativeModel::v1()
        .constraints()
        .iter()
        .chain(NativeModel::v1().indexes())
    {
        client.execute(*statement, json!({})).await?;
    }
    let rows = client
        .execute(
            "MERGE (owner:DtgOwner {namespace_id: $namespace_id})
             ON CREATE SET owner.backend_generation = $backend_generation,
               owner.cluster_id = $cluster_id, owner.graph_id = $graph_id,
               owner.shard_id = $shard_id, owner.placement_epoch = $placement_epoch,
               owner.replica_id = $replica_id,
               owner.backend_class_digest = $backend_class_digest,
               owner.provider_kind = $provider_kind,
               owner.contract_version = $contract_version,
               owner.layout_version = $layout_version,
               owner.capability_digest = $capability_digest,
               owner.endpoint_profile_ref = $endpoint_profile_ref,
               owner.credential_ref = $credential_ref,
               owner.binding_role = $binding_role,
               owner.binding_digest = $binding_digest,
               owner.applied_index = $zero_index,
               owner.lock_nonce = 0
             RETURN owner.cluster_id, owner.graph_id, owner.shard_id,
               owner.placement_epoch, owner.replica_id, owner.backend_generation,
               owner.backend_class_digest, owner.provider_kind,
               owner.contract_version, owner.layout_version,
               owner.capability_digest, owner.namespace_id,
               owner.endpoint_profile_ref, owner.credential_ref,
               owner.binding_role, owner.binding_digest",
            owner_parameters(binding),
        )
        .await?;
    let actual = decode_single_owner(&rows)?;
    if &actual != binding {
        return Err(StorageError::NamespaceOwnerMismatch {
            expected: Box::new(binding.clone()),
            actual: Box::new(actual),
        });
    }
    Ok(())
}

pub(crate) async fn begin_fenced_transaction(
    client: &QueryApiClient,
    binding: &ReplicaBinding,
    lock: bool,
) -> Result<(QueryApiTransaction, u64), StorageError> {
    let statement = if lock {
        format!(
            "{} SET owner.lock_nonce = owner.lock_nonce + 1 RETURN owner.applied_index",
            exact_owner_match()
        )
    } else {
        format!("{} RETURN owner.applied_index", exact_owner_match())
    };
    let (transaction, rows) = client
        .begin_transaction(statement, owner_parameters(binding))
        .await?;
    if let Some(applied) = rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_str)
    {
        return Ok((transaction, decode_u64_hex(applied)?));
    }
    transaction.rollback().await?;
    Err(owner_mismatch(client, binding).await?)
}

pub(crate) async fn read_applied_index(
    client: &QueryApiClient,
    binding: &ReplicaBinding,
) -> Result<u64, StorageError> {
    let rows = client
        .execute(
            format!("{} RETURN owner.applied_index", exact_owner_match()),
            owner_parameters(binding),
        )
        .await?;
    if let Some(value) = rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_str)
    {
        decode_u64_hex(value)
    } else {
        Err(owner_mismatch(client, binding).await?)
    }
}

async fn owner_mismatch(
    client: &QueryApiClient,
    binding: &ReplicaBinding,
) -> Result<StorageError, StorageError> {
    let rows = client
        .execute(
            "MATCH (owner:DtgOwner {namespace_id: $namespace_id})
             RETURN owner.cluster_id, owner.graph_id, owner.shard_id,
               owner.placement_epoch, owner.replica_id, owner.backend_generation,
               owner.backend_class_digest, owner.provider_kind,
               owner.contract_version, owner.layout_version,
               owner.capability_digest, owner.namespace_id,
               owner.endpoint_profile_ref, owner.credential_ref,
               owner.binding_role, owner.binding_digest",
            json!({"namespace_id": binding.namespace_id().as_str()}),
        )
        .await?;
    let actual = decode_single_owner(&rows)?;
    Ok(StorageError::NamespaceOwnerMismatch {
        expected: Box::new(binding.clone()),
        actual: Box::new(actual),
    })
}

pub(crate) fn owner_parameters(binding: &ReplicaBinding) -> Value {
    json!({
        "namespace_id": binding.namespace_id().as_str(),
        "backend_generation": u64_hex(binding.backend_generation().get()),
        "cluster_id": u64_hex(binding.cluster_id().get()),
        "graph_id": u64_hex(binding.graph_id().get()),
        "shard_id": u64_hex(binding.shard_id().get()),
        "placement_epoch": u64_hex(binding.placement_epoch().get()),
        "replica_id": u64_hex(binding.replica_id().get()),
        "backend_class_digest": digest_text(binding.backend_class_digest()),
        "provider_kind": "neo4j",
        "contract_version": binding.contract_version(),
        "layout_version": binding.layout_version(),
        "capability_digest": digest_text(binding.capability_digest()),
        "endpoint_profile_ref": binding.endpoint_profile_ref(),
        "credential_ref": binding.credential_ref(),
        "binding_role": role_tag(binding.role()),
        "binding_digest": digest_text(binding.identity_digest()),
        "zero_index": u64_hex(0),
    })
}

pub(crate) fn fenced_parameters(binding: &ReplicaBinding) -> Map<String, Value> {
    let mut parameters = Map::new();
    parameters.insert(
        "namespace_id".into(),
        Value::String(binding.namespace_id().as_str().to_owned()),
    );
    parameters.insert(
        "backend_generation".into(),
        Value::String(u64_hex(binding.backend_generation().get())),
    );
    parameters.insert(
        "binding_digest".into(),
        Value::String(digest_text(binding.identity_digest())),
    );
    parameters
}

pub(crate) fn exact_owner_match() -> &'static str {
    "MATCH (owner:DtgOwner {
       namespace_id: $namespace_id,
       backend_generation: $backend_generation,
       cluster_id: $cluster_id,
       graph_id: $graph_id,
       shard_id: $shard_id,
       placement_epoch: $placement_epoch,
       replica_id: $replica_id,
       backend_class_digest: $backend_class_digest,
       provider_kind: $provider_kind,
       contract_version: $contract_version,
       layout_version: $layout_version,
       capability_digest: $capability_digest,
       endpoint_profile_ref: $endpoint_profile_ref,
       credential_ref: $credential_ref,
       binding_role: $binding_role,
       binding_digest: $binding_digest
     })"
}

fn decode_single_owner(rows: &QueryRows) -> Result<ReplicaBinding, StorageError> {
    let row = rows
        .first()
        .ok_or_else(|| StorageError::Internal("Neo4j namespace has no owner node".into()))?;
    if row.len() != 16 {
        return Err(StorageError::Internal(
            "Neo4j owner query returned an invalid shape".into(),
        ));
    }
    if text(&row[7], "provider kind")? != "neo4j" {
        return Err(StorageError::Internal(
            "Neo4j owner node contains a foreign provider kind".into(),
        ));
    }
    let binding = ReplicaBinding::builder()
        .cluster_id(decode_u64_hex(text(&row[0], "cluster id")?)?)
        .graph_id(decode_u64_hex(text(&row[1], "graph id")?)?)
        .shard_id(decode_u64_hex(text(&row[2], "shard id")?)?)
        .placement_epoch(decode_u64_hex(text(&row[3], "placement epoch")?)?)
        .replica_id(decode_u64_hex(text(&row[4], "replica id")?)?)
        .backend_generation(decode_u64_hex(text(&row[5], "backend generation")?)?)
        .backend_class_digest(decode_digest(text(&row[6], "backend class digest")?)?)
        .provider_kind(ProviderKind::Neo4j)
        .contract_version(number_u32(&row[8], "contract version")?)
        .layout_version(number_u32(&row[9], "layout version")?)
        .capability_digest(decode_digest(text(&row[10], "capability digest")?)?)
        .namespace_id(text(&row[11], "namespace id")?)
        .endpoint_profile_ref(text(&row[12], "endpoint profile")?)
        .credential_ref(text(&row[13], "credential reference")?)
        .role(decode_role(text(&row[14], "binding role")?)?)
        .build()?;
    if binding.identity_digest() != decode_digest(text(&row[15], "binding digest")?)? {
        return Err(StorageError::Internal(
            "Neo4j owner binding digest is corrupt".into(),
        ));
    }
    Ok(binding)
}

pub(crate) fn u64_hex(value: u64) -> String {
    format!("{value:016x}")
}

pub(crate) fn u128_hex(value: u128) -> String {
    format!("{value:032x}")
}

pub(crate) fn decode_u64_hex(value: &str) -> Result<u64, StorageError> {
    u64::from_str_radix(value, 16)
        .map_err(|_| StorageError::Internal("invalid Neo4j u64 encoding".into()))
}

pub(crate) fn decode_u128_hex(value: &str) -> Result<u128, StorageError> {
    u128::from_str_radix(value, 16)
        .map_err(|_| StorageError::Internal("invalid Neo4j u128 encoding".into()))
}

pub(crate) fn digest_text(value: Digest32) -> String {
    STANDARD.encode(value.get())
}

pub(crate) fn decode_digest(value: &str) -> Result<Digest32, StorageError> {
    let bytes = STANDARD
        .decode(value)
        .map_err(|_| StorageError::Internal("invalid Neo4j digest encoding".into()))?;
    Ok(Digest32::new(bytes.try_into().map_err(|_| {
        StorageError::Internal("invalid Neo4j digest length".into())
    })?))
}

pub(crate) fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str, StorageError> {
    value
        .as_str()
        .ok_or_else(|| StorageError::Internal(format!("Neo4j query omitted or invalidated {name}")))
}

fn number_u32(value: &Value, name: &str) -> Result<u32, StorageError> {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| StorageError::Internal(format!("Neo4j query omitted or invalidated {name}")))
}

fn role_tag(role: BindingRole) -> &'static str {
    match role {
        BindingRole::Candidate => "candidate",
        BindingRole::Active => "active",
        BindingRole::Retiring => "retiring",
    }
}

fn decode_role(value: &str) -> Result<BindingRole, StorageError> {
    match value {
        "candidate" => Ok(BindingRole::Candidate),
        "active" => Ok(BindingRole::Active),
        "retiring" => Ok(BindingRole::Retiring),
        _ => Err(StorageError::Internal("invalid Neo4j binding role".into())),
    }
}
