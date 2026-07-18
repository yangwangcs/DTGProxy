#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use storage_api::AdapterRequirement;

const COMMAND_MAGIC: [u8; 4] = *b"DTCM";
const SNAPSHOT_MAGIC: [u8; 4] = *b"DTCS";
const LOG_MAGIC: [u8; 4] = *b"DTCL";
const FORMAT_VERSION: u16 = 1;
const CHECKSUM_BYTES: usize = 4;
const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;
const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const MAX_GRAPHS: usize = 65_536;
const MAX_PLACEMENTS: usize = 65_536;
const MAX_VOTERS: usize = 1_024;
const MAX_MAP_ENTRIES: usize = 1_024;
const MAX_NAME_BYTES: usize = 128;
const MAX_PARAMETER_NAME_BYTES: usize = 128;
const MAX_PARAMETER_VALUE_BYTES: usize = 16 * 1024;
const MAX_VIRTUAL_PARTITIONS: u32 = 1_048_576;
const SNAPSHOT_FILE: &str = "catalog.snapshot";
const LOG_FILE: &str = "catalog.log";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeploymentMode {
    PrimaryReplica,
    SharedNothing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Placement {
    shard_id: u32,
    epoch: u64,
    voters: Vec<u64>,
}

impl Placement {
    pub fn new(shard_id: u32, epoch: u64, mut voters: Vec<u64>) -> Result<Self, CatalogError> {
        if shard_id == 0 || epoch == 0 || voters.is_empty() || voters.len() > MAX_VOTERS {
            return Err(CatalogError::InvalidPlacement { shard_id });
        }
        voters.sort_unstable();
        if voters.windows(2).any(|pair| pair[0] == pair[1]) || voters[0] == 0 {
            return Err(CatalogError::InvalidPlacement { shard_id });
        }
        Ok(Self {
            shard_id,
            epoch,
            voters,
        })
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    #[must_use]
    pub fn voters(&self) -> &[u64] {
        &self.voters
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopologyDefinition {
    mode: DeploymentMode,
    route_seed: u64,
    virtual_partitions: u32,
    epoch: u64,
    placements: Vec<Placement>,
}

impl TopologyDefinition {
    pub fn new(
        mode: DeploymentMode,
        route_seed: u64,
        virtual_partitions: u32,
        epoch: u64,
        mut placements: Vec<Placement>,
    ) -> Result<Self, CatalogError> {
        if !(1..=MAX_VIRTUAL_PARTITIONS).contains(&virtual_partitions) {
            return Err(CatalogError::InvalidVirtualPartitionCount {
                actual: virtual_partitions,
            });
        }
        if epoch == 0 || placements.is_empty() || placements.len() > MAX_PLACEMENTS {
            return Err(CatalogError::InvalidTopology);
        }
        placements.sort_by_key(Placement::shard_id);
        if placements
            .windows(2)
            .any(|pair| pair[0].shard_id == pair[1].shard_id)
        {
            return Err(CatalogError::InvalidTopology);
        }
        match mode {
            DeploymentMode::PrimaryReplica if placements.len() != 1 => {
                return Err(CatalogError::InvalidTopology);
            }
            DeploymentMode::SharedNothing if placements.len() < 2 => {
                return Err(CatalogError::InvalidTopology);
            }
            DeploymentMode::PrimaryReplica | DeploymentMode::SharedNothing => {}
        }
        Ok(Self {
            mode,
            route_seed,
            virtual_partitions,
            epoch,
            placements,
        })
    }

    #[must_use]
    pub const fn mode(&self) -> DeploymentMode {
        self.mode
    }

    #[must_use]
    pub const fn route_seed(&self) -> u64 {
        self.route_seed
    }

    #[must_use]
    pub const fn virtual_partitions(&self) -> u32 {
        self.virtual_partitions
    }

    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    #[must_use]
    pub fn placements(&self) -> &[Placement] {
        &self.placements
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendProfile {
    provider: String,
    public_parameters: BTreeMap<String, String>,
    secret_references: BTreeMap<String, String>,
    requirement: AdapterRequirement,
    generation: u64,
}

impl BackendProfile {
    pub fn new(
        provider: impl Into<String>,
        public_parameters: BTreeMap<String, String>,
        secret_references: BTreeMap<String, String>,
        requirement: AdapterRequirement,
        generation: u64,
    ) -> Result<Self, CatalogError> {
        let provider = provider.into();
        validate_provider(&provider)?;
        if generation == 0 {
            return Err(CatalogError::InvalidBackendGeneration);
        }
        validate_map(&public_parameters, false)?;
        validate_map(&secret_references, true)?;
        Ok(Self {
            provider,
            public_parameters,
            secret_references,
            requirement,
            generation,
        })
    }

    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    #[must_use]
    pub const fn public_parameters(&self) -> &BTreeMap<String, String> {
        &self.public_parameters
    }

    #[must_use]
    pub const fn secret_references(&self) -> &BTreeMap<String, String> {
        &self.secret_references
    }

    #[must_use]
    pub const fn requirement(&self) -> AdapterRequirement {
        self.requirement
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphDefinition {
    graph_id: u64,
    name: String,
    schema_version: u64,
    topology: TopologyDefinition,
    backend: BackendProfile,
}

impl GraphDefinition {
    pub fn new(
        graph_id: u64,
        name: impl Into<String>,
        schema_version: u64,
        topology: TopologyDefinition,
        backend: BackendProfile,
    ) -> Result<Self, CatalogError> {
        let name = name.into();
        if graph_id == 0 {
            return Err(CatalogError::InvalidGraphId);
        }
        if name.is_empty()
            || name.len() > MAX_NAME_BYTES
            || name.chars().any(char::is_control)
            || schema_version == 0
        {
            return Err(CatalogError::InvalidGraphDefinition);
        }
        Ok(Self {
            graph_id,
            name,
            schema_version,
            topology,
            backend,
        })
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn topology(&self) -> &TopologyDefinition {
        &self.topology
    }

    #[must_use]
    pub const fn backend(&self) -> &BackendProfile {
        &self.backend
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogCommandBody {
    CreateGraph(GraphDefinition),
    PublishTopology {
        graph_id: u64,
        expected_epoch: u64,
        topology: TopologyDefinition,
    },
    PublishSchema {
        graph_id: u64,
        expected_version: u64,
        new_version: u64,
    },
    PublishBackend {
        graph_id: u64,
        expected_generation: u64,
        profile: BackendProfile,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogCommand {
    command_id: u128,
    expected_revision: u64,
    body: CatalogCommandBody,
}

impl CatalogCommand {
    #[must_use]
    pub const fn new(command_id: u128, expected_revision: u64, body: CatalogCommandBody) -> Self {
        Self {
            command_id,
            expected_revision,
            body,
        }
    }

    #[must_use]
    pub fn create_graph(command_id: u128, expected_revision: u64, graph: GraphDefinition) -> Self {
        Self::new(
            command_id,
            expected_revision,
            CatalogCommandBody::CreateGraph(graph),
        )
    }

    #[must_use]
    pub fn publish_topology(
        command_id: u128,
        expected_revision: u64,
        graph_id: u64,
        expected_epoch: u64,
        topology: TopologyDefinition,
    ) -> Self {
        Self::new(
            command_id,
            expected_revision,
            CatalogCommandBody::PublishTopology {
                graph_id,
                expected_epoch,
                topology,
            },
        )
    }

    #[must_use]
    pub fn publish_schema(
        command_id: u128,
        expected_revision: u64,
        graph_id: u64,
        expected_version: u64,
        new_version: u64,
    ) -> Self {
        Self::new(
            command_id,
            expected_revision,
            CatalogCommandBody::PublishSchema {
                graph_id,
                expected_version,
                new_version,
            },
        )
    }

    #[must_use]
    pub fn publish_backend(
        command_id: u128,
        expected_revision: u64,
        graph_id: u64,
        expected_generation: u64,
        profile: BackendProfile,
    ) -> Self {
        Self::new(
            command_id,
            expected_revision,
            CatalogCommandBody::PublishBackend {
                graph_id,
                expected_generation,
                profile,
            },
        )
    }

    #[must_use]
    pub const fn command_id(&self) -> u128 {
        self.command_id
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    #[must_use]
    pub const fn body(&self) -> &CatalogCommandBody {
        &self.body
    }

    pub fn encode(&self) -> Result<Vec<u8>, CatalogError> {
        if self.command_id == 0 {
            return Err(CatalogError::InvalidCommandId);
        }
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&COMMAND_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.command_id.to_be_bytes());
        bytes.extend_from_slice(&self.expected_revision.to_be_bytes());
        encode_command_body(&mut bytes, &self.body)?;
        append_checksum(&mut bytes);
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(CatalogError::RecordTooLarge);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CatalogError> {
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(CatalogError::RecordTooLarge);
        }
        verify_checksum(bytes)?;
        let mut reader = Reader::without_checksum(bytes)?;
        reader.expect_magic(COMMAND_MAGIC)?;
        reader.expect_version()?;
        let command_id = reader.u128()?;
        let expected_revision = reader.u64()?;
        let body = decode_command_body(&mut reader)?;
        reader.finish()?;
        let command = Self::new(command_id, expected_revision, body);
        if command.command_id == 0 || command.encode()? != bytes {
            return Err(CatalogError::NonCanonicalRecord);
        }
        Ok(command)
    }

    fn digest(&self) -> Result<[u8; 32], CatalogError> {
        Ok(*blake3::hash(&self.encode()?).as_bytes())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogApplyReceipt {
    revision: u64,
    duplicate: bool,
}

impl CatalogApplyReceipt {
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn duplicate(self) -> bool {
        self.duplicate
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AppliedCommand {
    digest: [u8; 32],
    revision: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogState {
    revision: u64,
    graphs: BTreeMap<u64, GraphDefinition>,
    applied_commands: BTreeMap<u128, AppliedCommand>,
}

impl CatalogState {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            revision: 0,
            graphs: BTreeMap::new(),
            applied_commands: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn graph(&self, graph_id: u64) -> Option<&GraphDefinition> {
        self.graphs.get(&graph_id)
    }

    #[must_use]
    pub const fn graphs(&self) -> &BTreeMap<u64, GraphDefinition> {
        &self.graphs
    }

    pub fn apply(&mut self, command: CatalogCommand) -> Result<CatalogApplyReceipt, CatalogError> {
        if command.command_id == 0 {
            return Err(CatalogError::InvalidCommandId);
        }
        let digest = command.digest()?;
        if let Some(applied) = self.applied_commands.get(&command.command_id) {
            if applied.digest != digest {
                return Err(CatalogError::CommandReplayMismatch {
                    command_id: command.command_id,
                });
            }
            return Ok(CatalogApplyReceipt {
                revision: applied.revision,
                duplicate: true,
            });
        }
        if command.expected_revision != self.revision {
            return Err(CatalogError::StaleCatalogRevision {
                expected: self.revision,
                actual: command.expected_revision,
            });
        }
        self.apply_body(command.body)?;
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(CatalogError::RevisionExhausted)?;
        self.applied_commands.insert(
            command.command_id,
            AppliedCommand {
                digest,
                revision: self.revision,
            },
        );
        Ok(CatalogApplyReceipt {
            revision: self.revision,
            duplicate: false,
        })
    }

    fn apply_body(&mut self, body: CatalogCommandBody) -> Result<(), CatalogError> {
        match body {
            CatalogCommandBody::CreateGraph(graph) => {
                if self.graphs.contains_key(&graph.graph_id) {
                    return Err(CatalogError::DuplicateGraph {
                        graph_id: graph.graph_id,
                    });
                }
                if graph.schema_version != 1
                    || graph.topology.epoch != 1
                    || graph.backend.generation != 1
                {
                    return Err(CatalogError::InvalidInitialEpoch);
                }
                self.graphs.insert(graph.graph_id, graph);
            }
            CatalogCommandBody::PublishTopology {
                graph_id,
                expected_epoch,
                topology,
            } => {
                let graph = self.graph_mut(graph_id)?;
                if graph.topology.epoch != expected_epoch {
                    return Err(CatalogError::StaleTopologyEpoch {
                        graph_id,
                        expected: graph.topology.epoch,
                        actual: expected_epoch,
                    });
                }
                if topology.epoch
                    != expected_epoch.checked_add(1).ok_or(
                        CatalogError::NonSequentialTopologyEpoch {
                            graph_id,
                            expected: expected_epoch,
                            actual: topology.epoch,
                        },
                    )?
                {
                    return Err(CatalogError::NonSequentialTopologyEpoch {
                        graph_id,
                        expected: expected_epoch + 1,
                        actual: topology.epoch,
                    });
                }
                validate_placement_transition(&graph.topology, &topology)?;
                graph.topology = topology;
            }
            CatalogCommandBody::PublishSchema {
                graph_id,
                expected_version,
                new_version,
            } => {
                let graph = self.graph_mut(graph_id)?;
                if graph.schema_version != expected_version {
                    return Err(CatalogError::StaleSchemaVersion {
                        graph_id,
                        expected: graph.schema_version,
                        actual: expected_version,
                    });
                }
                let expected_new = expected_version
                    .checked_add(1)
                    .ok_or(CatalogError::RevisionExhausted)?;
                if new_version != expected_new {
                    return Err(CatalogError::NonSequentialSchemaVersion {
                        graph_id,
                        expected: expected_new,
                        actual: new_version,
                    });
                }
                graph.schema_version = new_version;
            }
            CatalogCommandBody::PublishBackend {
                graph_id,
                expected_generation,
                profile,
            } => {
                let graph = self.graph_mut(graph_id)?;
                if graph.backend.generation != expected_generation {
                    return Err(CatalogError::StaleBackendGeneration {
                        graph_id,
                        expected: graph.backend.generation,
                        actual: expected_generation,
                    });
                }
                let expected_new = expected_generation
                    .checked_add(1)
                    .ok_or(CatalogError::RevisionExhausted)?;
                if profile.generation != expected_new {
                    return Err(CatalogError::NonSequentialBackendGeneration {
                        graph_id,
                        expected: expected_new,
                        actual: profile.generation,
                    });
                }
                graph.backend = profile;
            }
        }
        Ok(())
    }

    fn graph_mut(&mut self, graph_id: u64) -> Result<&mut GraphDefinition, CatalogError> {
        self.graphs
            .get_mut(&graph_id)
            .ok_or(CatalogError::UnknownGraph { graph_id })
    }
}

#[derive(Debug)]
pub struct Catalog {
    directory: PathBuf,
    snapshot_path: PathBuf,
    log_path: PathBuf,
    state: CatalogState,
}

impl Catalog {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, CatalogError> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory).map_err(io_error)?;
        let snapshot_path = directory.join(SNAPSHOT_FILE);
        let log_path = directory.join(LOG_FILE);
        let mut state = match fs::read(&snapshot_path) {
            Ok(bytes) => decode_snapshot(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => CatalogState::new(),
            Err(error) => return Err(io_error(error)),
        };
        match fs::read(&log_path) {
            Ok(bytes) => replay_log(&mut state, &bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
        Ok(Self {
            directory,
            snapshot_path,
            log_path,
            state,
        })
    }

    #[must_use]
    pub const fn state(&self) -> &CatalogState {
        &self.state
    }

    #[must_use]
    pub fn snapshot_path(&self) -> &Path {
        &self.snapshot_path
    }

    #[must_use]
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    pub fn execute(
        &mut self,
        command: CatalogCommand,
    ) -> Result<CatalogApplyReceipt, CatalogError> {
        let mut next = self.state.clone();
        let receipt = next.apply(command.clone())?;
        if receipt.duplicate {
            return Ok(receipt);
        }
        append_log_frame(&self.log_path, &command.encode()?)?;
        self.state = next;
        Ok(receipt)
    }

    pub fn checkpoint(&mut self) -> Result<(), CatalogError> {
        atomic_replace(&self.snapshot_path, &encode_snapshot(&self.state)?)?;
        atomic_replace(&self.log_path, &[])?;
        sync_directory(&self.directory)
    }
}

fn validate_placement_transition(
    old: &TopologyDefinition,
    new: &TopologyDefinition,
) -> Result<(), CatalogError> {
    let old = old
        .placements
        .iter()
        .map(|placement| (placement.shard_id, placement))
        .collect::<BTreeMap<_, _>>();
    for placement in &new.placements {
        if let Some(previous) = old.get(&placement.shard_id) {
            let expected =
                previous
                    .epoch
                    .checked_add(1)
                    .ok_or(CatalogError::InvalidPlacementTransition {
                        shard_id: placement.shard_id,
                    })?;
            if placement.epoch != expected {
                return Err(CatalogError::InvalidPlacementTransition {
                    shard_id: placement.shard_id,
                });
            }
        } else if placement.epoch != 1 {
            return Err(CatalogError::InvalidPlacementTransition {
                shard_id: placement.shard_id,
            });
        }
    }
    Ok(())
}

fn validate_provider(provider: &str) -> Result<(), CatalogError> {
    let valid = (1..=64).contains(&provider.len())
        && provider
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_lowercase)
        && provider.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        });
    if valid {
        Ok(())
    } else {
        Err(CatalogError::InvalidProviderName)
    }
}

fn validate_map(values: &BTreeMap<String, String>, secret: bool) -> Result<(), CatalogError> {
    if values.len() > MAX_MAP_ENTRIES {
        return Err(CatalogError::InvalidBackendParameters);
    }
    for (name, value) in values {
        let valid_name = !name.is_empty()
            && name.len() <= MAX_PARAMETER_NAME_BYTES
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte));
        let valid_value = !value.is_empty()
            && value.len() <= MAX_PARAMETER_VALUE_BYTES
            && !value.chars().any(char::is_control)
            && (!secret || value.starts_with("secret://"));
        if !valid_name || !valid_value {
            return Err(CatalogError::InvalidBackendParameters);
        }
    }
    Ok(())
}

fn encode_command_body(
    output: &mut Vec<u8>,
    body: &CatalogCommandBody,
) -> Result<(), CatalogError> {
    match body {
        CatalogCommandBody::CreateGraph(graph) => {
            output.push(1);
            encode_graph(output, graph)?;
        }
        CatalogCommandBody::PublishTopology {
            graph_id,
            expected_epoch,
            topology,
        } => {
            output.push(2);
            output.extend_from_slice(&graph_id.to_be_bytes());
            output.extend_from_slice(&expected_epoch.to_be_bytes());
            encode_topology(output, topology)?;
        }
        CatalogCommandBody::PublishSchema {
            graph_id,
            expected_version,
            new_version,
        } => {
            output.push(3);
            output.extend_from_slice(&graph_id.to_be_bytes());
            output.extend_from_slice(&expected_version.to_be_bytes());
            output.extend_from_slice(&new_version.to_be_bytes());
        }
        CatalogCommandBody::PublishBackend {
            graph_id,
            expected_generation,
            profile,
        } => {
            output.push(4);
            output.extend_from_slice(&graph_id.to_be_bytes());
            output.extend_from_slice(&expected_generation.to_be_bytes());
            encode_profile(output, profile)?;
        }
    }
    Ok(())
}

fn decode_command_body(reader: &mut Reader<'_>) -> Result<CatalogCommandBody, CatalogError> {
    match reader.u8()? {
        1 => Ok(CatalogCommandBody::CreateGraph(decode_graph(reader)?)),
        2 => Ok(CatalogCommandBody::PublishTopology {
            graph_id: reader.u64()?,
            expected_epoch: reader.u64()?,
            topology: decode_topology(reader)?,
        }),
        3 => Ok(CatalogCommandBody::PublishSchema {
            graph_id: reader.u64()?,
            expected_version: reader.u64()?,
            new_version: reader.u64()?,
        }),
        4 => Ok(CatalogCommandBody::PublishBackend {
            graph_id: reader.u64()?,
            expected_generation: reader.u64()?,
            profile: decode_profile(reader)?,
        }),
        tag => Err(CatalogError::UnknownCommandTag { tag }),
    }
}

fn encode_graph(output: &mut Vec<u8>, graph: &GraphDefinition) -> Result<(), CatalogError> {
    output.extend_from_slice(&graph.graph_id.to_be_bytes());
    write_string(output, &graph.name)?;
    output.extend_from_slice(&graph.schema_version.to_be_bytes());
    encode_topology(output, &graph.topology)?;
    encode_profile(output, &graph.backend)
}

fn decode_graph(reader: &mut Reader<'_>) -> Result<GraphDefinition, CatalogError> {
    GraphDefinition::new(
        reader.u64()?,
        reader.string(MAX_NAME_BYTES)?,
        reader.u64()?,
        decode_topology(reader)?,
        decode_profile(reader)?,
    )
}

fn encode_topology(
    output: &mut Vec<u8>,
    topology: &TopologyDefinition,
) -> Result<(), CatalogError> {
    output.push(match topology.mode {
        DeploymentMode::PrimaryReplica => 1,
        DeploymentMode::SharedNothing => 2,
    });
    output.extend_from_slice(&topology.route_seed.to_be_bytes());
    output.extend_from_slice(&topology.virtual_partitions.to_be_bytes());
    output.extend_from_slice(&topology.epoch.to_be_bytes());
    write_count(output, topology.placements.len())?;
    for placement in &topology.placements {
        output.extend_from_slice(&placement.shard_id.to_be_bytes());
        output.extend_from_slice(&placement.epoch.to_be_bytes());
        write_count(output, placement.voters.len())?;
        for voter in &placement.voters {
            output.extend_from_slice(&voter.to_be_bytes());
        }
    }
    Ok(())
}

fn decode_topology(reader: &mut Reader<'_>) -> Result<TopologyDefinition, CatalogError> {
    let mode = match reader.u8()? {
        1 => DeploymentMode::PrimaryReplica,
        2 => DeploymentMode::SharedNothing,
        tag => return Err(CatalogError::UnknownDeploymentMode { tag }),
    };
    let route_seed = reader.u64()?;
    let virtual_partitions = reader.u32()?;
    let epoch = reader.u64()?;
    let count = reader.count(MAX_PLACEMENTS)?;
    let mut placements = Vec::with_capacity(count);
    for _ in 0..count {
        let shard_id = reader.u32()?;
        let placement_epoch = reader.u64()?;
        let voter_count = reader.count(MAX_VOTERS)?;
        let mut voters = Vec::with_capacity(voter_count);
        for _ in 0..voter_count {
            voters.push(reader.u64()?);
        }
        placements.push(Placement::new(shard_id, placement_epoch, voters)?);
    }
    TopologyDefinition::new(mode, route_seed, virtual_partitions, epoch, placements)
}

fn encode_profile(output: &mut Vec<u8>, profile: &BackendProfile) -> Result<(), CatalogError> {
    write_string(output, &profile.provider)?;
    encode_map(output, &profile.public_parameters)?;
    encode_map(output, &profile.secret_references)?;
    output.push(match profile.requirement {
        AdapterRequirement::Development => 1,
        AdapterRequirement::ManagedReplica => 2,
        AdapterRequirement::HotPluggableReplica => 3,
    });
    output.extend_from_slice(&profile.generation.to_be_bytes());
    Ok(())
}

fn decode_profile(reader: &mut Reader<'_>) -> Result<BackendProfile, CatalogError> {
    let provider = reader.string(64)?;
    let public_parameters = decode_map(reader, false)?;
    let secret_references = decode_map(reader, true)?;
    let requirement = match reader.u8()? {
        1 => AdapterRequirement::Development,
        2 => AdapterRequirement::ManagedReplica,
        3 => AdapterRequirement::HotPluggableReplica,
        tag => return Err(CatalogError::UnknownAdapterRequirement { tag }),
    };
    let generation = reader.u64()?;
    BackendProfile::new(
        provider,
        public_parameters,
        secret_references,
        requirement,
        generation,
    )
}

fn encode_map(output: &mut Vec<u8>, values: &BTreeMap<String, String>) -> Result<(), CatalogError> {
    write_count(output, values.len())?;
    for (name, value) in values {
        write_string(output, name)?;
        write_string(output, value)?;
    }
    Ok(())
}

fn decode_map(
    reader: &mut Reader<'_>,
    secret: bool,
) -> Result<BTreeMap<String, String>, CatalogError> {
    let count = reader.count(MAX_MAP_ENTRIES)?;
    let mut values = BTreeMap::new();
    for _ in 0..count {
        let name = reader.string(MAX_PARAMETER_NAME_BYTES)?;
        let value = reader.string(MAX_PARAMETER_VALUE_BYTES)?;
        if values.insert(name, value).is_some() {
            return Err(CatalogError::NonCanonicalRecord);
        }
    }
    validate_map(&values, secret)?;
    Ok(values)
}

fn encode_snapshot(state: &CatalogState) -> Result<Vec<u8>, CatalogError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&state.revision.to_be_bytes());
    write_count(&mut bytes, state.graphs.len())?;
    for graph in state.graphs.values() {
        encode_graph(&mut bytes, graph)?;
    }
    write_count(&mut bytes, state.applied_commands.len())?;
    for (command_id, applied) in &state.applied_commands {
        bytes.extend_from_slice(&command_id.to_be_bytes());
        bytes.extend_from_slice(&applied.digest);
        bytes.extend_from_slice(&applied.revision.to_be_bytes());
    }
    append_checksum(&mut bytes);
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(CatalogError::RecordTooLarge);
    }
    Ok(bytes)
}

fn decode_snapshot(bytes: &[u8]) -> Result<CatalogState, CatalogError> {
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(CatalogError::RecordTooLarge);
    }
    verify_checksum(bytes)?;
    let mut reader = Reader::without_checksum(bytes)?;
    reader.expect_magic(SNAPSHOT_MAGIC)?;
    reader.expect_version()?;
    let revision = reader.u64()?;
    let graph_count = reader.count(MAX_GRAPHS)?;
    let mut graphs = BTreeMap::new();
    for _ in 0..graph_count {
        let graph = decode_graph(&mut reader)?;
        if graphs.insert(graph.graph_id, graph).is_some() {
            return Err(CatalogError::NonCanonicalRecord);
        }
    }
    let applied_count = reader.count(MAX_GRAPHS.saturating_mul(16))?;
    let mut applied_commands = BTreeMap::new();
    for _ in 0..applied_count {
        let command_id = reader.u128()?;
        let digest = reader.array::<32>()?;
        let applied_revision = reader.u64()?;
        if command_id == 0
            || applied_revision == 0
            || applied_revision > revision
            || applied_commands
                .insert(
                    command_id,
                    AppliedCommand {
                        digest,
                        revision: applied_revision,
                    },
                )
                .is_some()
        {
            return Err(CatalogError::NonCanonicalRecord);
        }
    }
    reader.finish()?;
    let state = CatalogState {
        revision,
        graphs,
        applied_commands,
    };
    if encode_snapshot(&state)? != bytes {
        return Err(CatalogError::NonCanonicalRecord);
    }
    Ok(state)
}

fn append_log_frame(path: &Path, command: &[u8]) -> Result<(), CatalogError> {
    let length = u32::try_from(command.len()).map_err(|_| CatalogError::RecordTooLarge)?;
    let mut frame = Vec::with_capacity(10 + command.len() + CHECKSUM_BYTES);
    frame.extend_from_slice(&LOG_MAGIC);
    frame.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(command);
    append_checksum(&mut frame);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(io_error)?;
    file.write_all(&frame)
        .and_then(|()| file.sync_data())
        .map_err(io_error)?;
    sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
}

fn replay_log(state: &mut CatalogState, bytes: &[u8]) -> Result<(), CatalogError> {
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        if remaining.len() < 10 + CHECKSUM_BYTES {
            return Err(CatalogError::TruncatedRecord);
        }
        if remaining[..4] != LOG_MAGIC {
            return Err(CatalogError::InvalidMagic);
        }
        if u16::from_be_bytes([remaining[4], remaining[5]]) != FORMAT_VERSION {
            return Err(CatalogError::UnsupportedVersion);
        }
        let command_length = usize::try_from(u32::from_be_bytes(
            remaining[6..10].try_into().expect("fixed log frame length"),
        ))
        .map_err(|_| CatalogError::RecordTooLarge)?;
        if command_length > MAX_COMMAND_BYTES {
            return Err(CatalogError::RecordTooLarge);
        }
        let frame_length = 10_usize
            .checked_add(command_length)
            .and_then(|length| length.checked_add(CHECKSUM_BYTES))
            .ok_or(CatalogError::RecordTooLarge)?;
        let frame = remaining
            .get(..frame_length)
            .ok_or(CatalogError::TruncatedRecord)?;
        verify_checksum(frame)?;
        let command = CatalogCommand::decode(&frame[10..frame_length - CHECKSUM_BYTES])?;
        state.apply(command)?;
        offset = offset
            .checked_add(frame_length)
            .ok_or(CatalogError::RecordTooLarge)?;
    }
    Ok(())
}

fn atomic_replace(path: &Path, bytes: &[u8]) -> Result<(), CatalogError> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(io_error)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(io_error)?;
    fs::rename(&temporary, path).map_err(io_error)?;
    sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
}

fn sync_directory(path: &Path) -> Result<(), CatalogError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_error)
}

fn write_count(output: &mut Vec<u8>, count: usize) -> Result<(), CatalogError> {
    let count = u32::try_from(count).map_err(|_| CatalogError::RecordTooLarge)?;
    output.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), CatalogError> {
    write_count(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn append_checksum(output: &mut Vec<u8>) {
    output.extend_from_slice(&crc32fast::hash(output).to_be_bytes());
}

fn verify_checksum(bytes: &[u8]) -> Result<(), CatalogError> {
    if bytes.len() < CHECKSUM_BYTES {
        return Err(CatalogError::TruncatedRecord);
    }
    let offset = bytes.len() - CHECKSUM_BYTES;
    let expected = u32::from_be_bytes(bytes[offset..].try_into().expect("fixed catalog checksum"));
    if crc32fast::hash(&bytes[..offset]) == expected {
        Ok(())
    } else {
        Err(CatalogError::ChecksumMismatch)
    }
}

fn io_error(error: std::io::Error) -> CatalogError {
    CatalogError::Io(error.to_string())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn without_checksum(bytes: &'a [u8]) -> Result<Self, CatalogError> {
        let length = bytes
            .len()
            .checked_sub(CHECKSUM_BYTES)
            .ok_or(CatalogError::TruncatedRecord)?;
        Ok(Self {
            bytes: &bytes[..length],
            offset: 0,
        })
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CatalogError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CatalogError::RecordTooLarge)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CatalogError::TruncatedRecord)?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CatalogError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CatalogError::TruncatedRecord)
    }

    fn u8(&mut self) -> Result<u8, CatalogError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CatalogError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, CatalogError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn u128(&mut self) -> Result<u128, CatalogError> {
        Ok(u128::from_be_bytes(self.array()?))
    }

    fn count(&mut self, maximum: usize) -> Result<usize, CatalogError> {
        let count = usize::try_from(self.u32()?).map_err(|_| CatalogError::RecordTooLarge)?;
        if count > maximum {
            Err(CatalogError::RecordTooLarge)
        } else {
            Ok(count)
        }
    }

    fn string(&mut self, maximum: usize) -> Result<String, CatalogError> {
        let length = self.count(maximum)?;
        String::from_utf8(self.take(length)?.to_vec()).map_err(|_| CatalogError::InvalidUtf8)
    }

    fn expect_magic(&mut self, magic: [u8; 4]) -> Result<(), CatalogError> {
        if self.take(4)? == magic {
            Ok(())
        } else {
            Err(CatalogError::InvalidMagic)
        }
    }

    fn expect_version(&mut self) -> Result<(), CatalogError> {
        if u16::from_be_bytes(self.array()?) == FORMAT_VERSION {
            Ok(())
        } else {
            Err(CatalogError::UnsupportedVersion)
        }
    }

    fn finish(self) -> Result<(), CatalogError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CatalogError::TrailingBytes)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogError {
    InvalidCommandId,
    CommandReplayMismatch {
        command_id: u128,
    },
    StaleCatalogRevision {
        expected: u64,
        actual: u64,
    },
    RevisionExhausted,
    InvalidGraphId,
    InvalidGraphDefinition,
    DuplicateGraph {
        graph_id: u64,
    },
    UnknownGraph {
        graph_id: u64,
    },
    InvalidInitialEpoch,
    InvalidVirtualPartitionCount {
        actual: u32,
    },
    InvalidTopology,
    InvalidPlacement {
        shard_id: u32,
    },
    InvalidPlacementTransition {
        shard_id: u32,
    },
    StaleTopologyEpoch {
        graph_id: u64,
        expected: u64,
        actual: u64,
    },
    NonSequentialTopologyEpoch {
        graph_id: u64,
        expected: u64,
        actual: u64,
    },
    StaleSchemaVersion {
        graph_id: u64,
        expected: u64,
        actual: u64,
    },
    NonSequentialSchemaVersion {
        graph_id: u64,
        expected: u64,
        actual: u64,
    },
    InvalidProviderName,
    InvalidBackendParameters,
    InvalidBackendGeneration,
    StaleBackendGeneration {
        graph_id: u64,
        expected: u64,
        actual: u64,
    },
    NonSequentialBackendGeneration {
        graph_id: u64,
        expected: u64,
        actual: u64,
    },
    InvalidMagic,
    UnsupportedVersion,
    UnknownCommandTag {
        tag: u8,
    },
    UnknownDeploymentMode {
        tag: u8,
    },
    UnknownAdapterRequirement {
        tag: u8,
    },
    TruncatedRecord,
    TrailingBytes,
    InvalidUtf8,
    NonCanonicalRecord,
    ChecksumMismatch,
    RecordTooLarge,
    Io(String),
}

impl Display for CatalogError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommandId => formatter.write_str("catalog command ID must be nonzero"),
            Self::CommandReplayMismatch { command_id } => {
                write!(
                    formatter,
                    "catalog command {command_id} was reused with different content"
                )
            }
            Self::StaleCatalogRevision { expected, actual } => write!(
                formatter,
                "catalog revision {actual} is stale; expected {expected}"
            ),
            Self::RevisionExhausted => formatter.write_str("catalog revision space exhausted"),
            Self::InvalidGraphId => formatter.write_str("graph ID must be nonzero"),
            Self::InvalidGraphDefinition => formatter.write_str("invalid graph definition"),
            Self::DuplicateGraph { graph_id } => {
                write!(formatter, "graph {graph_id} already exists")
            }
            Self::UnknownGraph { graph_id } => write!(formatter, "graph {graph_id} does not exist"),
            Self::InvalidInitialEpoch => formatter.write_str(
                "new graph schema, topology, and backend generations must all start at one",
            ),
            Self::InvalidVirtualPartitionCount { actual } => {
                write!(formatter, "invalid virtual partition count {actual}")
            }
            Self::InvalidTopology => formatter.write_str("invalid graph topology"),
            Self::InvalidPlacement { shard_id } => {
                write!(formatter, "invalid placement for Shard {shard_id}")
            }
            Self::InvalidPlacementTransition { shard_id } => write!(
                formatter,
                "placement epoch for Shard {shard_id} must advance exactly once"
            ),
            Self::StaleTopologyEpoch {
                graph_id,
                expected,
                actual,
            } => write!(
                formatter,
                "graph {graph_id} topology epoch {actual} is stale; expected {expected}"
            ),
            Self::NonSequentialTopologyEpoch {
                graph_id,
                expected,
                actual,
            } => write!(
                formatter,
                "graph {graph_id} topology epoch {actual} must be {expected}"
            ),
            Self::StaleSchemaVersion {
                graph_id,
                expected,
                actual,
            } => write!(
                formatter,
                "graph {graph_id} schema version {actual} is stale; expected {expected}"
            ),
            Self::NonSequentialSchemaVersion {
                graph_id,
                expected,
                actual,
            } => write!(
                formatter,
                "graph {graph_id} schema version {actual} must be {expected}"
            ),
            Self::InvalidProviderName => formatter.write_str("invalid backend provider name"),
            Self::InvalidBackendParameters => {
                formatter.write_str("invalid backend public parameter or secret reference")
            }
            Self::InvalidBackendGeneration => {
                formatter.write_str("backend generation must be nonzero")
            }
            Self::StaleBackendGeneration {
                graph_id,
                expected,
                actual,
            } => write!(
                formatter,
                "graph {graph_id} backend generation {actual} is stale; expected {expected}"
            ),
            Self::NonSequentialBackendGeneration {
                graph_id,
                expected,
                actual,
            } => write!(
                formatter,
                "graph {graph_id} backend generation {actual} must be {expected}"
            ),
            Self::InvalidMagic => formatter.write_str("invalid catalog record magic"),
            Self::UnsupportedVersion => formatter.write_str("unsupported catalog record version"),
            Self::UnknownCommandTag { tag } => {
                write!(formatter, "unknown catalog command tag {tag}")
            }
            Self::UnknownDeploymentMode { tag } => {
                write!(formatter, "unknown deployment mode tag {tag}")
            }
            Self::UnknownAdapterRequirement { tag } => {
                write!(formatter, "unknown adapter requirement tag {tag}")
            }
            Self::TruncatedRecord => formatter.write_str("catalog record is truncated"),
            Self::TrailingBytes => formatter.write_str("catalog record has trailing bytes"),
            Self::InvalidUtf8 => formatter.write_str("catalog string is not UTF-8"),
            Self::NonCanonicalRecord => formatter.write_str("catalog record is not canonical"),
            Self::ChecksumMismatch => formatter.write_str("catalog record checksum mismatch"),
            Self::RecordTooLarge => formatter.write_str("catalog record exceeds its size bound"),
            Self::Io(message) => write!(formatter, "catalog I/O failed: {message}"),
        }
    }
}

impl Error for CatalogError {}
