use super::ValidationError;

pub const PLAN_VERSION: u16 = 1;
const MAX_SEMANTIC_BASELINE_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LanguageProfile {
    Cypher25,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanHeader {
    version: u16,
    graph_id: u64,
    schema_version: u64,
    topology_epoch: u64,
    language_profile: LanguageProfile,
    semantic_baseline: String,
    query_fingerprint: [u8; 32],
}

impl PlanHeader {
    pub fn new(
        graph_id: u64,
        schema_version: u64,
        topology_epoch: u64,
        language_profile: LanguageProfile,
        semantic_baseline: impl Into<String>,
        query_fingerprint: [u8; 32],
    ) -> Result<Self, ValidationError> {
        Self::with_version(
            PLAN_VERSION,
            graph_id,
            schema_version,
            topology_epoch,
            language_profile,
            semantic_baseline,
            query_fingerprint,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_version(
        version: u16,
        graph_id: u64,
        schema_version: u64,
        topology_epoch: u64,
        language_profile: LanguageProfile,
        semantic_baseline: impl Into<String>,
        query_fingerprint: [u8; 32],
    ) -> Result<Self, ValidationError> {
        let semantic_baseline = semantic_baseline.into();
        let header = Self {
            version,
            graph_id,
            schema_version,
            topology_epoch,
            language_profile,
            semantic_baseline,
            query_fingerprint,
        };
        header.validate_shape()?;
        Ok(header)
    }

    pub(crate) fn validate(&self) -> Result<(), ValidationError> {
        self.validate_shape()?;
        if self.version != PLAN_VERSION {
            return Err(ValidationError::UnsupportedVersion {
                expected: PLAN_VERSION,
                actual: self.version,
            });
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), ValidationError> {
        if self.graph_id == 0 {
            return Err(ValidationError::InvalidGraphId);
        }
        if self.schema_version == 0 {
            return Err(ValidationError::InvalidSchemaVersion);
        }
        if self.topology_epoch == 0 {
            return Err(ValidationError::InvalidTopologyEpoch);
        }
        if self.semantic_baseline.is_empty()
            || self.semantic_baseline.len() > MAX_SEMANTIC_BASELINE_BYTES
        {
            return Err(ValidationError::InvalidSemanticBaseline);
        }
        if self.query_fingerprint == [0; 32] {
            return Err(ValidationError::InvalidQueryFingerprint);
        }
        Ok(())
    }

    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn topology_epoch(&self) -> u64 {
        self.topology_epoch
    }

    #[must_use]
    pub const fn language_profile(&self) -> LanguageProfile {
        self.language_profile
    }

    #[must_use]
    pub fn semantic_baseline(&self) -> &str {
        &self.semantic_baseline
    }

    #[must_use]
    pub const fn query_fingerprint(&self) -> [u8; 32] {
        self.query_fingerprint
    }
}
