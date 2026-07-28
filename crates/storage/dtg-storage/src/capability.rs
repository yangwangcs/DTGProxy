use std::collections::BTreeSet;

use dtg_kernel::Digest32;

use crate::{ReadFence, SnapshotRecord, StorageError, StoreFuture, VertexRead, VertexScan};

pub const SUPPORTED_PUSHDOWN_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityManifest {
    names: BTreeSet<String>,
    digest: Digest32,
}

impl CapabilityManifest {
    pub fn from_names<I, S>(names: I) -> Result<Self, StorageError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut canonical = BTreeSet::new();
        for name in names {
            let name = name.into();
            validate_capability_name(&name)?;
            canonical.insert(name);
        }
        let digest = digest_names(&canonical);
        Ok(Self {
            names: canonical,
            digest,
        })
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(String::as_str)
    }

    pub const fn digest(&self) -> Digest32 {
        self.digest
    }

    pub fn supports(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    pub fn contains_all(&self, required: &Self) -> bool {
        required.names.is_subset(&self.names)
    }

    pub fn intersection(&self, other: &Self) -> Self {
        let names = self.names.intersection(&other.names).cloned().collect();
        let digest = digest_names(&names);
        Self { names, digest }
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

fn validate_capability_name(name: &str) -> Result<(), StorageError> {
    let valid = !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.')
        })
        && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase);
    if valid {
        Ok(())
    } else {
        Err(StorageError::InvalidCapability(format!(
            "capability name is not canonical: {name:?}"
        )))
    }
}

fn digest_names(names: &BTreeSet<String>) -> Digest32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-capability-manifest-v1");
    hasher.update(&(names.len() as u64).to_be_bytes());
    for name in names {
        hasher.update(&(name.len() as u64).to_be_bytes());
        hasher.update(name.as_bytes());
    }
    Digest32::new(*hasher.finalize().as_bytes())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PushdownOperation {
    Vertex(VertexRead),
    VertexScan(VertexScan),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PushdownRequest {
    contract_version: u32,
    fence: ReadFence,
    required_capabilities: CapabilityManifest,
    operation: PushdownOperation,
}

impl PushdownRequest {
    pub fn new(
        contract_version: u32,
        fence: ReadFence,
        required_capabilities: CapabilityManifest,
        operation: PushdownOperation,
    ) -> Result<Self, StorageError> {
        let request = Self {
            contract_version,
            fence,
            required_capabilities,
            operation,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), StorageError> {
        if self.contract_version != SUPPORTED_PUSHDOWN_CONTRACT_VERSION {
            return Err(StorageError::InvalidCapability(
                "unsupported pushdown contract version".into(),
            ));
        }
        Ok(())
    }

    pub const fn contract_version(&self) -> u32 {
        self.contract_version
    }

    pub const fn fence(&self) -> &ReadFence {
        &self.fence
    }

    pub const fn required_capabilities(&self) -> &CapabilityManifest {
        &self.required_capabilities
    }

    pub const fn operation(&self) -> &PushdownOperation {
        &self.operation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PushdownOutcome {
    Exact(Vec<SnapshotRecord>),
    ResidualRequired {
        rows: Vec<SnapshotRecord>,
        guarantees: CapabilityManifest,
    },
    Unsupported,
}

pub trait PushdownExecutor: Send + Sync {
    fn binding(&self) -> &crate::ReplicaBinding;
    fn capabilities(&self) -> &CapabilityManifest;
    fn execute_pushdown(&self, request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome>;
}

pub(crate) fn encode_manifest(hasher: &mut blake3::Hasher, manifest: &CapabilityManifest) {
    hasher.update(&manifest.digest().get());
}
