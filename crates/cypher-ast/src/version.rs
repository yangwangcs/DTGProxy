#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CypherVersion {
    V5,
    V25,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CypherProfile {
    version: CypherVersion,
    semantic_baseline: &'static str,
}

impl CypherProfile {
    #[must_use]
    pub const fn cypher_5() -> Self {
        Self {
            version: CypherVersion::V5,
            semantic_baseline: "Cypher 5 / frozen",
        }
    }

    #[must_use]
    pub const fn cypher_25() -> Self {
        Self {
            version: CypherVersion::V25,
            semantic_baseline: "Cypher 25 / 2026.07",
        }
    }

    #[must_use]
    pub const fn version(self) -> CypherVersion {
        self.version
    }

    #[must_use]
    pub const fn semantic_baseline(self) -> &'static str {
        self.semantic_baseline
    }
}

impl Default for CypherProfile {
    fn default() -> Self {
        Self::cypher_25()
    }
}
