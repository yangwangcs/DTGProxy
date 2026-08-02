use dtg_kernel::{
    BackendGeneration, ClusterId, Digest32, GraphId, PlacementEpoch, ReplicaId, ShardId,
};

use crate::{CapabilityManifest, StorageError, capability::encode_manifest};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum ProviderKind {
    Fjall,
    PostgreSql,
    Kuzu,
    Remote(String),
}

impl ProviderKind {
    fn encode(&self, hasher: &mut blake3::Hasher) -> Result<(), StorageError> {
        match self {
            Self::Fjall => {
                hasher.update(b"fjall");
            }
            Self::PostgreSql => {
                hasher.update(b"postgresql");
            }
            Self::Kuzu => {
                hasher.update(b"kuzu");
            }
            Self::Remote(name) => {
                if name.is_empty()
                    || !name.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'-' | b'.')
                    })
                {
                    return Err(StorageError::InvalidBinding(format!(
                        "remote provider name is not canonical: {name:?}"
                    )));
                }
                hasher.update(b"remote:");
                hasher.update(&(name.len() as u64).to_be_bytes());
                hasher.update(name.as_bytes());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum DurabilityPolicy {
    DurableCommit,
    DurableCommitWithReplicaSync,
}

impl DurabilityPolicy {
    const fn tag(self) -> u8 {
        match self {
            Self::DurableCommit => 1,
            Self::DurableCommitWithReplicaSync => 2,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendClass {
    provider_kind: ProviderKind,
    contract_version: u32,
    layout_version: u32,
    durability_policy: DurabilityPolicy,
    required_capabilities: CapabilityManifest,
    digest: Digest32,
}

impl BackendClass {
    pub fn new<I, S>(
        provider_kind: ProviderKind,
        contract_version: u32,
        layout_version: u32,
        required_capabilities: I,
    ) -> Result<Self, StorageError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::with_durability(
            provider_kind,
            contract_version,
            layout_version,
            DurabilityPolicy::DurableCommit,
            required_capabilities,
        )
    }

    pub fn with_durability<I, S>(
        provider_kind: ProviderKind,
        contract_version: u32,
        layout_version: u32,
        durability_policy: DurabilityPolicy,
        required_capabilities: I,
    ) -> Result<Self, StorageError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        if contract_version == 0 || layout_version == 0 {
            return Err(StorageError::InvalidBinding(
                "backend contract and layout versions must be nonzero".into(),
            ));
        }
        let required_capabilities = CapabilityManifest::from_names(required_capabilities)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"dtg-backend-class-v1");
        provider_kind.encode(&mut hasher)?;
        hasher.update(&contract_version.to_be_bytes());
        hasher.update(&layout_version.to_be_bytes());
        hasher.update(&[durability_policy.tag()]);
        encode_manifest(&mut hasher, &required_capabilities);
        let digest = Digest32::new(*hasher.finalize().as_bytes());
        Ok(Self {
            provider_kind,
            contract_version,
            layout_version,
            durability_policy,
            required_capabilities,
            digest,
        })
    }

    pub const fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
    }

    pub const fn contract_version(&self) -> u32 {
        self.contract_version
    }

    pub const fn layout_version(&self) -> u32 {
        self.layout_version
    }

    pub const fn durability_policy(&self) -> DurabilityPolicy {
        self.durability_policy
    }

    pub const fn required_capabilities(&self) -> &CapabilityManifest {
        &self.required_capabilities
    }

    pub const fn digest(&self) -> Digest32 {
        self.digest
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum BindingRole {
    Candidate,
    Active,
    Retiring,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct NamespaceId(String);

impl NamespaceId {
    pub fn new(value: impl Into<String>) -> Result<Self, StorageError> {
        let value = value.into();
        if value.is_empty() || value.len() > 255 || value.chars().any(char::is_whitespace) {
            return Err(StorageError::InvalidBinding(
                "namespace identifier must be nonempty, bounded, and whitespace-free".into(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBinding {
    cluster_id: ClusterId,
    graph_id: GraphId,
    shard_id: ShardId,
    placement_epoch: PlacementEpoch,
    replica_id: ReplicaId,
    backend_generation: BackendGeneration,
    backend_class_digest: Digest32,
    provider_kind: ProviderKind,
    contract_version: u32,
    layout_version: u32,
    capability_digest: Digest32,
    namespace_id: NamespaceId,
    endpoint_profile_ref: String,
    credential_ref: String,
    role: BindingRole,
}

impl ReplicaBinding {
    pub fn builder() -> ReplicaBindingBuilder {
        ReplicaBindingBuilder::default()
    }

    pub fn to_builder(&self) -> ReplicaBindingBuilder {
        ReplicaBindingBuilder {
            cluster_id: self.cluster_id.get(),
            graph_id: self.graph_id.get(),
            shard_id: self.shard_id.get(),
            placement_epoch: self.placement_epoch.get(),
            replica_id: self.replica_id.get(),
            backend_generation: self.backend_generation.get(),
            backend_class_digest: Some(self.backend_class_digest),
            provider_kind: Some(self.provider_kind.clone()),
            contract_version: self.contract_version,
            layout_version: self.layout_version,
            capability_digest: Some(self.capability_digest),
            namespace_id: Some(self.namespace_id.as_str().to_owned()),
            endpoint_profile_ref: Some(self.endpoint_profile_ref.clone()),
            credential_ref: Some(self.credential_ref.clone()),
            role: Some(self.role),
        }
    }

    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub const fn placement_epoch(&self) -> PlacementEpoch {
        self.placement_epoch
    }

    pub const fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    pub const fn backend_generation(&self) -> BackendGeneration {
        self.backend_generation
    }

    pub const fn backend_class_digest(&self) -> Digest32 {
        self.backend_class_digest
    }

    pub const fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
    }

    pub const fn contract_version(&self) -> u32 {
        self.contract_version
    }

    pub const fn layout_version(&self) -> u32 {
        self.layout_version
    }

    pub const fn capability_digest(&self) -> Digest32 {
        self.capability_digest
    }

    pub const fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn endpoint_profile_ref(&self) -> &str {
        &self.endpoint_profile_ref
    }

    pub fn credential_ref(&self) -> &str {
        &self.credential_ref
    }

    pub const fn role(&self) -> BindingRole {
        self.role
    }

    pub fn identity_digest(&self) -> Digest32 {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"dtg-replica-binding-v1");
        hasher.update(&self.cluster_id.get().to_be_bytes());
        hasher.update(&self.graph_id.get().to_be_bytes());
        hasher.update(&self.shard_id.get().to_be_bytes());
        hasher.update(&self.placement_epoch.get().to_be_bytes());
        hasher.update(&self.replica_id.get().to_be_bytes());
        hasher.update(&self.backend_generation.get().to_be_bytes());
        hasher.update(&self.backend_class_digest.get());
        self.provider_kind
            .encode(&mut hasher)
            .expect("validated provider kind remains canonical");
        hasher.update(&self.contract_version.to_be_bytes());
        hasher.update(&self.layout_version.to_be_bytes());
        hasher.update(&self.capability_digest.get());
        encode_string(&mut hasher, self.namespace_id.as_str());
        encode_string(&mut hasher, &self.endpoint_profile_ref);
        encode_string(&mut hasher, &self.credential_ref);
        hasher.update(&[match self.role {
            BindingRole::Candidate => 1,
            BindingRole::Active => 2,
            BindingRole::Retiring => 3,
        }]);
        Digest32::new(*hasher.finalize().as_bytes())
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReplicaBindingBuilder {
    cluster_id: u64,
    graph_id: u64,
    shard_id: u64,
    placement_epoch: u64,
    replica_id: u64,
    backend_generation: u64,
    backend_class_digest: Option<Digest32>,
    provider_kind: Option<ProviderKind>,
    contract_version: u32,
    layout_version: u32,
    capability_digest: Option<Digest32>,
    namespace_id: Option<String>,
    endpoint_profile_ref: Option<String>,
    credential_ref: Option<String>,
    role: Option<BindingRole>,
}

impl ReplicaBindingBuilder {
    pub const fn cluster_id(mut self, value: u64) -> Self {
        self.cluster_id = value;
        self
    }

    pub const fn graph_id(mut self, value: u64) -> Self {
        self.graph_id = value;
        self
    }

    pub const fn shard_id(mut self, value: u64) -> Self {
        self.shard_id = value;
        self
    }

    pub const fn placement_epoch(mut self, value: u64) -> Self {
        self.placement_epoch = value;
        self
    }

    pub const fn replica_id(mut self, value: u64) -> Self {
        self.replica_id = value;
        self
    }

    pub const fn backend_generation(mut self, value: u64) -> Self {
        self.backend_generation = value;
        self
    }

    pub const fn backend_class_digest(mut self, value: Digest32) -> Self {
        self.backend_class_digest = Some(value);
        self
    }

    pub fn provider_kind(mut self, value: ProviderKind) -> Self {
        self.provider_kind = Some(value);
        self
    }

    pub const fn contract_version(mut self, value: u32) -> Self {
        self.contract_version = value;
        self
    }

    pub const fn layout_version(mut self, value: u32) -> Self {
        self.layout_version = value;
        self
    }

    pub const fn capability_digest(mut self, value: Digest32) -> Self {
        self.capability_digest = Some(value);
        self
    }

    pub fn namespace_id(mut self, value: impl Into<String>) -> Self {
        self.namespace_id = Some(value.into());
        self
    }

    pub fn endpoint_profile_ref(mut self, value: impl Into<String>) -> Self {
        self.endpoint_profile_ref = Some(value.into());
        self
    }

    pub fn credential_ref(mut self, value: impl Into<String>) -> Self {
        self.credential_ref = Some(value.into());
        self
    }

    pub const fn role(mut self, value: BindingRole) -> Self {
        self.role = Some(value);
        self
    }

    pub fn build(self) -> Result<ReplicaBinding, StorageError> {
        let invalid = |field: &str| {
            StorageError::InvalidBinding(format!("missing or invalid binding field: {field}"))
        };
        let cluster_id = ClusterId::new(self.cluster_id).map_err(|_| invalid("cluster_id"))?;
        let graph_id = GraphId::new(self.graph_id).map_err(|_| invalid("graph_id"))?;
        let shard_id = ShardId::new(self.shard_id).map_err(|_| invalid("shard_id"))?;
        let placement_epoch =
            PlacementEpoch::new(self.placement_epoch).map_err(|_| invalid("placement_epoch"))?;
        let replica_id = ReplicaId::new(self.replica_id).map_err(|_| invalid("replica_id"))?;
        let backend_generation = BackendGeneration::new(self.backend_generation)
            .map_err(|_| invalid("backend_generation"))?;
        let backend_class_digest = self
            .backend_class_digest
            .filter(|digest| digest.get() != [0; 32])
            .ok_or_else(|| invalid("backend_class_digest"))?;
        let provider_kind = self.provider_kind.ok_or_else(|| invalid("provider_kind"))?;
        let mut provider_hasher = blake3::Hasher::new();
        provider_kind.encode(&mut provider_hasher)?;
        if self.contract_version == 0 {
            return Err(invalid("contract_version"));
        }
        if self.layout_version == 0 {
            return Err(invalid("layout_version"));
        }
        let capability_digest = self
            .capability_digest
            .filter(|digest| digest.get() != [0; 32])
            .ok_or_else(|| invalid("capability_digest"))?;
        let namespace_id =
            NamespaceId::new(self.namespace_id.ok_or_else(|| invalid("namespace_id"))?)?;
        let endpoint_profile_ref =
            nonempty_ref(self.endpoint_profile_ref, "endpoint_profile_ref", &invalid)?;
        let credential_ref = nonempty_ref(self.credential_ref, "credential_ref", &invalid)?;
        let role = self.role.ok_or_else(|| invalid("role"))?;
        Ok(ReplicaBinding {
            cluster_id,
            graph_id,
            shard_id,
            placement_epoch,
            replica_id,
            backend_generation,
            backend_class_digest,
            provider_kind,
            contract_version: self.contract_version,
            layout_version: self.layout_version,
            capability_digest,
            namespace_id,
            endpoint_profile_ref,
            credential_ref,
            role,
        })
    }
}

fn nonempty_ref<F>(value: Option<String>, field: &str, invalid: &F) -> Result<String, StorageError>
where
    F: Fn(&str) -> StorageError,
{
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid(field))
}

fn encode_string(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}
