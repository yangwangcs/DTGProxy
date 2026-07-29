use dtg_storage::{BindingRole, Digest32, ProviderKind, ReplicaBinding, StorageError};
use dtg_storage_remote_protocol::{DIGEST_BYTES, proto};

pub(crate) fn encode_binding(binding: &ReplicaBinding) -> proto::Binding {
    proto::Binding {
        cluster_id: binding.cluster_id().get(),
        graph_id: binding.graph_id().get(),
        shard_id: binding.shard_id().get(),
        placement_epoch: binding.placement_epoch().get(),
        replica_id: binding.replica_id().get(),
        backend_generation: binding.backend_generation().get(),
        backend_class_digest: binding.backend_class_digest().get().to_vec(),
        provider_kind: match binding.provider_kind() {
            ProviderKind::Fjall => "fjall".into(),
            ProviderKind::PostgreSql => "postgresql".into(),
            ProviderKind::Neo4j => "neo4j".into(),
            ProviderKind::Remote(name) => format!("remote:{name}"),
        },
        contract_version: binding.contract_version(),
        layout_version: binding.layout_version(),
        capability_digest: binding.capability_digest().get().to_vec(),
        namespace_id: binding.namespace_id().as_str().into(),
        endpoint_profile_ref: binding.endpoint_profile_ref().into(),
        credential_ref: binding.credential_ref().into(),
        role: match binding.role() {
            BindingRole::Candidate => "candidate",
            BindingRole::Active => "active",
            BindingRole::Retiring => "retiring",
        }
        .into(),
        binding_digest: binding.identity_digest().get().to_vec(),
    }
}

pub(crate) fn decode_binding(binding: &proto::Binding) -> Result<ReplicaBinding, StorageError> {
    let provider = match binding.provider_kind.as_str() {
        "fjall" => ProviderKind::Fjall,
        "postgresql" => ProviderKind::PostgreSql,
        "neo4j" => ProviderKind::Neo4j,
        value if value.starts_with("remote:") => ProviderKind::Remote(value[7..].into()),
        _ => {
            return Err(StorageError::InvalidBinding(
                "unknown remote provider kind".into(),
            ));
        }
    };
    let role = match binding.role.as_str() {
        "candidate" => BindingRole::Candidate,
        "active" => BindingRole::Active,
        "retiring" => BindingRole::Retiring,
        _ => {
            return Err(StorageError::InvalidBinding(
                "unknown remote binding role".into(),
            ));
        }
    };
    let decoded = ReplicaBinding::builder()
        .cluster_id(binding.cluster_id)
        .graph_id(binding.graph_id)
        .shard_id(binding.shard_id)
        .placement_epoch(binding.placement_epoch)
        .replica_id(binding.replica_id)
        .backend_generation(binding.backend_generation)
        .backend_class_digest(digest(&binding.backend_class_digest)?)
        .provider_kind(provider)
        .contract_version(binding.contract_version)
        .layout_version(binding.layout_version)
        .capability_digest(digest(&binding.capability_digest)?)
        .namespace_id(binding.namespace_id.clone())
        .endpoint_profile_ref(binding.endpoint_profile_ref.clone())
        .credential_ref(binding.credential_ref.clone())
        .role(role)
        .build()?;
    if decoded.identity_digest() != digest(&binding.binding_digest)? {
        return Err(StorageError::InvalidBinding(
            "remote binding digest is corrupt".into(),
        ));
    }
    Ok(decoded)
}

pub(crate) fn digest(bytes: &[u8]) -> Result<Digest32, StorageError> {
    Ok(Digest32::new(bytes.try_into().map_err(|_| {
        StorageError::InvalidBinding(format!("digest must contain {DIGEST_BYTES} bytes"))
    })?))
}
