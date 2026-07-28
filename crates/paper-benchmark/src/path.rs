use crate::{ContractError, ResultIdentity};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentPath {
    BackendDirect,
    AdapterDirect,
    Proxy,
}

impl Display for ExperimentPath {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::BackendDirect => "backend_direct",
            Self::AdapterDirect => "adapter_direct",
            Self::Proxy => "proxy",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    Rocksdb,
    Postgresql,
    Neo4j,
}

impl Display for Backend {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Rocksdb => "rocksdb",
            Self::Postgresql => "postgresql",
            Self::Neo4j => "neo4j",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkRequest {
    pub workload: String,
    pub snapshot: String,
    pub parameters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendNativeRequest {
    pub backend: Backend,
    pub native_operation: String,
    pub native_request: Value,
}

impl BackendNativeRequest {
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.native_operation.trim().is_empty() {
            return Err(ContractError::InvalidField("native_operation"));
        }
        if self.native_request.is_null() {
            return Err(ContractError::InvalidField("native_request"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdapterPrimitive {
    PointLookup {
        key: String,
    },
    RangeScan {
        lower_bound: String,
        upper_bound: String,
    },
}

impl AdapterPrimitive {
    fn validate(&self) -> Result<(), ContractError> {
        match self {
            Self::PointLookup { key } => nonempty(key, "primitive.key"),
            Self::RangeScan {
                lower_bound,
                upper_bound,
            } => {
                nonempty(lower_bound, "primitive.lower_bound")?;
                nonempty(upper_bound, "primitive.upper_bound")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterSnapshotRequest {
    pub backend: Backend,
    pub pinned_snapshot: String,
    pub primitive: AdapterPrimitive,
}

impl AdapterSnapshotRequest {
    pub fn validate(&self) -> Result<(), ContractError> {
        nonempty(&self.pinned_snapshot, "pinned_snapshot")?;
        self.primitive.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyLoadgenRequest {
    pub backend: Backend,
    pub persisted_loadgen_input_json: String,
}

impl ProxyLoadgenRequest {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_json_object(
            &self.persisted_loadgen_input_json,
            "persisted_loadgen_input_json",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyLoadgenReport {
    pub backend: Backend,
    pub persisted_loadgen_report_json: String,
    pub execution: PathExecution,
}

impl ProxyLoadgenReport {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_json_object(
            &self.persisted_loadgen_report_json,
            "persisted_loadgen_report_json",
        )?;
        self.execution.validate()
    }
}

impl BenchmarkRequest {
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.workload.trim().is_empty() {
            return Err(ContractError::InvalidField("workload"));
        }
        if self.snapshot.trim().is_empty() {
            return Err(ContractError::InvalidField("snapshot"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum PathExecution {
    Available { identity: ResultIdentity },
    Unavailable { reason: String },
}

impl PathExecution {
    pub fn validate(&self) -> Result<(), ContractError> {
        match self {
            Self::Available { identity } => identity.validate(),
            Self::Unavailable { reason } if reason.trim().is_empty() => {
                Err(ContractError::InvalidField("unavailable.reason"))
            }
            Self::Unavailable { .. } => Ok(()),
        }
    }
}

pub trait BackendDirectRunner {
    fn backend(&self) -> Backend;
    fn execute_native(
        &mut self,
        request: &BackendNativeRequest,
    ) -> Result<PathExecution, PathError>;
}

pub trait AdapterDirectRunner {
    fn backend(&self) -> Backend;
    fn execute_snapshot(
        &mut self,
        snapshot: &dyn storage_api::ReadSnapshot,
        request: &AdapterSnapshotRequest,
    ) -> Result<PathExecution, PathError>;
}

pub trait ProxyRunner {
    fn backend(&self) -> Backend;
    fn execute_loadgen(
        &mut self,
        request: &ProxyLoadgenRequest,
    ) -> Result<ProxyLoadgenReport, PathError>;
}

fn nonempty(value: &str, field: &'static str) -> Result<(), ContractError> {
    if value.trim().is_empty() {
        return Err(ContractError::InvalidField(field));
    }
    Ok(())
}

fn validate_json_object(value: &str, field: &'static str) -> Result<(), ContractError> {
    let value: Value =
        serde_json::from_str(value).map_err(|_| ContractError::InvalidField(field))?;
    if value.is_object() {
        Ok(())
    } else {
        Err(ContractError::InvalidField(field))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathError {
    message: String,
}

impl PathError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for PathError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PathError {}
