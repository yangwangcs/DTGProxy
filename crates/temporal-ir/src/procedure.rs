use super::{RowSchema, ScalarExpr, SlotId};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProcedureIdentity {
    authority_key: [u8; 32],
    catalog_revision: u64,
    procedure_revision: u64,
}

impl ProcedureIdentity {
    #[must_use]
    pub const fn new(
        authority_key: [u8; 32],
        catalog_revision: u64,
        procedure_revision: u64,
    ) -> Self {
        Self {
            authority_key,
            catalog_revision,
            procedure_revision,
        }
    }

    #[must_use]
    pub const fn authority_key(&self) -> [u8; 32] {
        self.authority_key
    }

    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    #[must_use]
    pub const fn procedure_revision(&self) -> u64 {
        self.procedure_revision
    }

    #[must_use]
    pub const fn with_catalog_revision(self, catalog_revision: u64) -> Self {
        Self {
            catalog_revision,
            ..self
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcedureEffect {
    ReadOnly,
    Write,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcedurePlacement {
    Coordinator,
    ShardLocal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureArgument {
    name: String,
    expression: ScalarExpr,
}

impl ProcedureArgument {
    #[must_use]
    pub fn new(name: impl Into<String>, expression: ScalarExpr) -> Self {
        Self {
            name: name.into(),
            expression,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn expression(&self) -> &ScalarExpr {
        &self.expression
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcedureYieldBinding {
    source_index: u32,
    output_slot: SlotId,
}

impl ProcedureYieldBinding {
    #[must_use]
    pub const fn new(source_index: u32, output_slot: SlotId) -> Self {
        Self {
            source_index,
            output_slot,
        }
    }

    #[must_use]
    pub const fn source_index(&self) -> u32 {
        self.source_index
    }

    #[must_use]
    pub const fn output_slot(&self) -> SlotId {
        self.output_slot
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedProcedure {
    identity: ProcedureIdentity,
    name: String,
    arguments: Vec<ProcedureArgument>,
    yields: Vec<ProcedureYieldBinding>,
    provider_output: RowSchema,
    effect: ProcedureEffect,
    placement: ProcedurePlacement,
    max_invocations: u32,
    max_input_rows: u64,
    max_output_rows: u64,
    max_value_bytes: u64,
    max_result_bytes: u64,
    supports_overlay: bool,
}

impl ResolvedProcedure {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        identity: ProcedureIdentity,
        name: impl Into<String>,
        arguments: Vec<ProcedureArgument>,
        yields: Vec<ProcedureYieldBinding>,
        provider_output: RowSchema,
        effect: ProcedureEffect,
        placement: ProcedurePlacement,
        max_invocations: u32,
        max_input_rows: u64,
        max_output_rows: u64,
        max_value_bytes: u64,
        max_result_bytes: u64,
        supports_overlay: bool,
    ) -> Self {
        Self {
            identity,
            name: name.into(),
            arguments,
            yields,
            provider_output,
            effect,
            placement,
            max_invocations,
            max_input_rows,
            max_output_rows,
            max_value_bytes,
            max_result_bytes,
            supports_overlay,
        }
    }

    #[must_use]
    pub const fn identity(&self) -> &ProcedureIdentity {
        &self.identity
    }
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    #[must_use]
    pub fn arguments(&self) -> &[ProcedureArgument] {
        &self.arguments
    }
    #[must_use]
    pub fn yields(&self) -> &[ProcedureYieldBinding] {
        &self.yields
    }
    #[must_use]
    pub const fn provider_output(&self) -> &RowSchema {
        &self.provider_output
    }
    #[must_use]
    pub const fn effect(&self) -> ProcedureEffect {
        self.effect
    }
    #[must_use]
    pub const fn placement(&self) -> ProcedurePlacement {
        self.placement
    }
    #[must_use]
    pub const fn max_invocations(&self) -> u32 {
        self.max_invocations
    }
    #[must_use]
    pub const fn max_input_rows(&self) -> u64 {
        self.max_input_rows
    }
    #[must_use]
    pub const fn max_output_rows(&self) -> u64 {
        self.max_output_rows
    }
    #[must_use]
    pub const fn max_value_bytes(&self) -> u64 {
        self.max_value_bytes
    }
    #[must_use]
    pub const fn max_result_bytes(&self) -> u64 {
        self.max_result_bytes
    }
    #[must_use]
    pub const fn supports_overlay(&self) -> bool {
        self.supports_overlay
    }
}
