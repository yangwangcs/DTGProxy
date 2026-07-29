use dtg_kernel::{BackendGeneration, Digest32, PlacementEpoch, Version};
use dtg_storage::CommandId;

use crate::{CommandHeader, ShardError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationPhase {
    Prepare,
    Activate,
    Grace,
    Retire,
    Complete,
    Abort,
    Rollback,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OnlineMigrationContext {
    migration_id: u128,
    source_epoch: PlacementEpoch,
    source_generation: BackendGeneration,
    retained_generation: BackendGeneration,
    target_generation: BackendGeneration,
    target_backend_class_digest: Digest32,
    catalog_version: Version,
    verified_index: u64,
    logical_digest: Digest32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationCommand {
    header: CommandHeader,
    phase: MigrationPhase,
    online: Option<OnlineMigrationContext>,
}

impl MigrationCommand {
    pub fn new(
        command_id: CommandId,
        placement_epoch: u64,
        backend_generation: u64,
        phase: MigrationPhase,
    ) -> Result<Self, ShardError> {
        Ok(Self {
            header: CommandHeader::new(command_id, placement_epoch, backend_generation)?,
            phase,
            online: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn online(
        command_id: CommandId,
        migration_id: u128,
        source_epoch: u64,
        source_generation: u64,
        retained_generation: u64,
        target_generation: u64,
        target_backend_class_digest: Digest32,
        catalog_version: Version,
        verified_index: u64,
        logical_digest: Digest32,
        phase: MigrationPhase,
    ) -> Result<Self, ShardError> {
        let source_epoch = PlacementEpoch::new(source_epoch)
            .map_err(|_| invalid("migration source epoch must be nonzero"))?;
        let source_generation = BackendGeneration::new(source_generation)
            .map_err(|_| invalid("migration source generation must be nonzero"))?;
        let retained_generation = BackendGeneration::new(retained_generation)
            .map_err(|_| invalid("migration retained generation must be nonzero"))?;
        let target_generation = BackendGeneration::new(target_generation)
            .map_err(|_| invalid("migration target generation must be nonzero"))?;
        let next_epoch = source_epoch
            .get()
            .checked_add(1)
            .ok_or_else(|| invalid("migration placement epoch overflow"))?;
        let expected_generation = source_generation
            .get()
            .checked_add(1)
            .ok_or_else(|| invalid("migration backend generation overflow"))?;
        if migration_id == 0
            || target_generation.get() != expected_generation
            || target_backend_class_digest.get() == [0; 32]
            || verified_index == 0
            || logical_digest.get() == [0; 32]
            || (phase == MigrationPhase::Rollback
                && retained_generation.get() >= source_generation.get())
            || (phase != MigrationPhase::Rollback && retained_generation != source_generation)
        {
            return Err(invalid("online migration context is inconsistent"));
        }
        let uses_target_fence = !matches!(phase, MigrationPhase::Prepare | MigrationPhase::Abort);
        let header = CommandHeader::new(
            command_id,
            if uses_target_fence {
                next_epoch
            } else {
                source_epoch.get()
            },
            if uses_target_fence {
                target_generation.get()
            } else {
                source_generation.get()
            },
        )?;
        Ok(Self {
            header,
            phase,
            online: Some(OnlineMigrationContext {
                migration_id,
                source_epoch,
                source_generation,
                retained_generation,
                target_generation,
                target_backend_class_digest,
                catalog_version,
                verified_index,
                logical_digest,
            }),
        })
    }

    pub const fn header(&self) -> CommandHeader {
        self.header
    }

    pub const fn phase(&self) -> MigrationPhase {
        self.phase
    }

    pub const fn migration_id(&self) -> Option<u128> {
        match &self.online {
            Some(context) => Some(context.migration_id),
            None => None,
        }
    }

    pub const fn source_epoch(&self) -> Option<PlacementEpoch> {
        match &self.online {
            Some(context) => Some(context.source_epoch),
            None => None,
        }
    }

    pub const fn source_generation(&self) -> Option<BackendGeneration> {
        match &self.online {
            Some(context) => Some(context.source_generation),
            None => None,
        }
    }

    pub const fn retained_generation(&self) -> Option<BackendGeneration> {
        match &self.online {
            Some(context) => Some(context.retained_generation),
            None => None,
        }
    }

    pub const fn target_generation(&self) -> Option<BackendGeneration> {
        match &self.online {
            Some(context) => Some(context.target_generation),
            None => None,
        }
    }

    pub const fn target_backend_class_digest(&self) -> Option<Digest32> {
        match &self.online {
            Some(context) => Some(context.target_backend_class_digest),
            None => None,
        }
    }

    pub const fn catalog_version(&self) -> Option<Version> {
        match &self.online {
            Some(context) => Some(context.catalog_version),
            None => None,
        }
    }

    pub const fn verified_index(&self) -> Option<u64> {
        match &self.online {
            Some(context) => Some(context.verified_index),
            None => None,
        }
    }

    pub const fn logical_digest(&self) -> Option<Digest32> {
        match &self.online {
            Some(context) => Some(context.logical_digest),
            None => None,
        }
    }

    pub(crate) fn cuts_over_from(
        &self,
        epoch: PlacementEpoch,
        generation: BackendGeneration,
    ) -> bool {
        matches!(
            self.phase,
            MigrationPhase::Activate | MigrationPhase::Rollback
        ) && self.online.as_ref().is_some_and(|context| {
            context.source_epoch == epoch
                && context.source_generation == generation
                && self.header.placement_epoch().get() == epoch.get().checked_add(1).unwrap_or(0)
                && self.header.backend_generation() == context.target_generation
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn decode_online(
        header: CommandHeader,
        migration_id: u128,
        source_epoch: u64,
        source_generation: u64,
        retained_generation: u64,
        target_generation: u64,
        target_backend_class_digest: Digest32,
        catalog_version: Version,
        verified_index: u64,
        logical_digest: Digest32,
        phase: MigrationPhase,
    ) -> Result<Self, ShardError> {
        let command = Self::online(
            header.command_id(),
            migration_id,
            source_epoch,
            source_generation,
            retained_generation,
            target_generation,
            target_backend_class_digest,
            catalog_version,
            verified_index,
            logical_digest,
            phase,
        )?;
        if command.header != header {
            return Err(invalid("migration command header contradicts its context"));
        }
        Ok(command)
    }
}

fn invalid(message: &str) -> ShardError {
    ShardError::InvalidCommand(message.into())
}

#[cfg(test)]
mod tests {
    use dtg_kernel::{BackendGeneration, Digest32, PlacementEpoch, Version};
    use dtg_storage::{CommandId, LogicalMutation, Value};

    use super::{MigrationCommand, MigrationPhase};
    use crate::{SUPPORTED_SHARD_COMMAND_FORMAT_VERSION, ShardCommand};

    #[test]
    fn online_cutover_carries_the_full_generation_and_catalog_fence() {
        let command = MigrationCommand::online(
            CommandId::new(7).unwrap(),
            19,
            7,
            3,
            3,
            4,
            Digest32::new([5; 32]),
            Version::new(12),
            44,
            Digest32::new([9; 32]),
            MigrationPhase::Activate,
        )
        .unwrap();

        assert_eq!(
            command.header().placement_epoch(),
            PlacementEpoch::new(8).unwrap()
        );
        assert_eq!(
            command.header().backend_generation(),
            BackendGeneration::new(4).unwrap()
        );
        assert_eq!(command.migration_id(), Some(19));
        assert_eq!(command.source_generation().unwrap().get(), 3);
        assert_eq!(command.retained_generation().unwrap().get(), 3);
        assert_eq!(command.target_generation().unwrap().get(), 4);
        assert_eq!(command.catalog_version(), Some(Version::new(12)));
        assert_eq!(command.verified_index(), Some(44));
        assert_eq!(command.logical_digest(), Some(Digest32::new([9; 32])));
    }

    #[test]
    fn online_cutover_rejects_non_monotonic_epoch_or_generation_inputs() {
        let error = MigrationCommand::online(
            CommandId::new(7).unwrap(),
            19,
            u64::MAX,
            3,
            3,
            5,
            Digest32::new([5; 32]),
            Version::new(12),
            44,
            Digest32::new([9; 32]),
            MigrationPhase::Activate,
        )
        .unwrap_err();

        assert_eq!(error.code(), "DTG-SHARD-COMMAND");
    }

    #[test]
    fn online_command_round_trips_and_durably_records_its_full_context() {
        let migration = MigrationCommand::online(
            CommandId::new(7).unwrap(),
            19,
            7,
            3,
            3,
            4,
            Digest32::new([5; 32]),
            Version::new(12),
            44,
            Digest32::new([9; 32]),
            MigrationPhase::Activate,
        )
        .unwrap();
        assert!(migration.cuts_over_from(
            PlacementEpoch::new(7).unwrap(),
            BackendGeneration::new(3).unwrap()
        ));
        assert!(!migration.cuts_over_from(
            PlacementEpoch::new(7).unwrap(),
            BackendGeneration::new(2).unwrap()
        ));

        let command = ShardCommand::Migration(migration);
        let encoded = command.encode_current().unwrap();
        assert_eq!(
            u32::from_be_bytes(encoded[..4].try_into().unwrap()),
            SUPPORTED_SHARD_COMMAND_FORMAT_VERSION
        );
        assert_eq!(ShardCommand::decode(&encoded).unwrap(), command);

        let mutations = command.mutations().unwrap();
        let [LogicalMutation::PutReplicaMetadata(metadata)] = mutations.as_slice() else {
            panic!("online migration must persist one metadata record");
        };
        let Value::Map(fields) = metadata.value() else {
            panic!("online migration metadata must be a complete map");
        };
        assert_eq!(metadata.name(), "dtg.migration_phase");
        assert_eq!(fields.get("phase"), Some(&Value::String("activate".into())));
        assert!(fields.contains_key("migration_id"));
        assert!(fields.contains_key("source_generation"));
        assert!(fields.contains_key("retained_generation"));
        assert!(fields.contains_key("target_generation"));
        assert!(fields.contains_key("target_backend_class_digest"));
        assert!(fields.contains_key("catalog_version"));
        assert!(fields.contains_key("verified_index"));
        assert!(fields.contains_key("logical_digest"));
    }
}
