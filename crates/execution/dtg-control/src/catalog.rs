use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BackendGeneration, ControlError, Digest32, GraphId, RetentionPin, ShardId, ShardPlacement,
    Version,
};

type ShardKey = (GraphId, ShardId);
type GenerationKey = (GraphId, ShardId, BackendGeneration);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LineageEntry {
    generation: BackendGeneration,
    backend_class_digest: Digest32,
}

impl LineageEntry {
    pub const fn generation(self) -> BackendGeneration {
        self.generation
    }

    pub const fn backend_class_digest(self) -> Digest32 {
        self.backend_class_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogCommand {
    PutPlacement {
        expected_version: Version,
        placement: ShardPlacement,
    },
    PinRetention {
        expected_version: Version,
        graph_id: GraphId,
        shard_id: ShardId,
        generation: BackendGeneration,
        pin: RetentionPin,
    },
    UnpinRetention {
        expected_version: Version,
        graph_id: GraphId,
        shard_id: ShardId,
        generation: BackendGeneration,
        pin: RetentionPin,
    },
}

impl CatalogCommand {
    pub const fn put_placement(expected_version: Version, placement: ShardPlacement) -> Self {
        Self::PutPlacement {
            expected_version,
            placement,
        }
    }

    pub const fn pin_retention(
        expected_version: Version,
        graph_id: GraphId,
        shard_id: ShardId,
        generation: BackendGeneration,
        pin: RetentionPin,
    ) -> Self {
        Self::PinRetention {
            expected_version,
            graph_id,
            shard_id,
            generation,
            pin,
        }
    }

    pub const fn unpin_retention(
        expected_version: Version,
        graph_id: GraphId,
        shard_id: ShardId,
        generation: BackendGeneration,
        pin: RetentionPin,
    ) -> Self {
        Self::UnpinRetention {
            expected_version,
            graph_id,
            shard_id,
            generation,
            pin,
        }
    }

    pub const fn expected_version(&self) -> Version {
        match self {
            Self::PutPlacement {
                expected_version, ..
            }
            | Self::PinRetention {
                expected_version, ..
            }
            | Self::UnpinRetention {
                expected_version, ..
            } => *expected_version,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogState {
    version: Version,
    placements: BTreeMap<ShardKey, ShardPlacement>,
    lineage: BTreeMap<ShardKey, Vec<LineageEntry>>,
    retention_pins: BTreeMap<GenerationKey, BTreeSet<RetentionPin>>,
}

impl CatalogState {
    pub fn new() -> Self {
        Self {
            version: Version::new(0),
            placements: BTreeMap::new(),
            lineage: BTreeMap::new(),
            retention_pins: BTreeMap::new(),
        }
    }

    pub const fn version(&self) -> Version {
        self.version
    }

    pub fn placement(&self, graph_id: GraphId, shard_id: ShardId) -> Option<&ShardPlacement> {
        self.placements.get(&(graph_id, shard_id))
    }

    pub fn lineage(&self, graph_id: GraphId, shard_id: ShardId) -> &[LineageEntry] {
        self.lineage
            .get(&(graph_id, shard_id))
            .map_or(&[], Vec::as_slice)
    }

    pub fn is_generation_pinned(
        &self,
        graph_id: GraphId,
        shard_id: ShardId,
        generation: BackendGeneration,
    ) -> bool {
        self.retention_pins
            .get(&(graph_id, shard_id, generation))
            .is_some_and(|pins| !pins.is_empty())
    }

    pub fn apply(&self, command: CatalogCommand) -> Result<Self, ControlError> {
        if command.expected_version() != self.version {
            return Err(ControlError::StaleCatalog {
                expected: command.expected_version(),
                actual: self.version,
            });
        }

        let mut next = self.clone();
        match command {
            CatalogCommand::PutPlacement { placement, .. } => {
                next.put_placement(placement)?;
            }
            CatalogCommand::PinRetention {
                graph_id,
                shard_id,
                generation,
                pin,
                ..
            } => next.pin_retention(graph_id, shard_id, generation, pin)?,
            CatalogCommand::UnpinRetention {
                graph_id,
                shard_id,
                generation,
                pin,
                ..
            } => next.unpin_retention(graph_id, shard_id, generation, &pin)?,
        }
        next.version = Version::new(
            self.version
                .get()
                .checked_add(1)
                .ok_or(ControlError::VersionOverflow)?,
        );
        Ok(next)
    }

    fn put_placement(&mut self, placement: ShardPlacement) -> Result<(), ControlError> {
        placement.validate()?;
        let key = (placement.graph_id, placement.shard_id);
        let new_classes = placement.generation_classes();

        if let Some(previous) = self.placements.get(&key) {
            for generation in previous.generation_classes().keys() {
                if !new_classes.contains_key(generation)
                    && self.is_generation_pinned(key.0, key.1, *generation)
                {
                    return Err(ControlError::RetentionPinned);
                }
            }
        }

        let lineage = self.lineage.entry(key).or_default();
        for (generation, backend_class_digest) in new_classes {
            match lineage.iter().find(|entry| entry.generation == generation) {
                Some(entry) if entry.backend_class_digest != backend_class_digest => {
                    return Err(ControlError::LineageConflict);
                }
                Some(_) => {}
                None => lineage.push(LineageEntry {
                    generation,
                    backend_class_digest,
                }),
            }
        }
        lineage.sort_by_key(|entry| entry.generation);
        self.placements.insert(key, placement);
        Ok(())
    }

    fn pin_retention(
        &mut self,
        graph_id: GraphId,
        shard_id: ShardId,
        generation: BackendGeneration,
        pin: RetentionPin,
    ) -> Result<(), ControlError> {
        if !self.has_generation(graph_id, shard_id, generation) {
            return Err(ControlError::UnknownGeneration);
        }
        self.retention_pins
            .entry((graph_id, shard_id, generation))
            .or_default()
            .insert(pin);
        Ok(())
    }

    fn unpin_retention(
        &mut self,
        graph_id: GraphId,
        shard_id: ShardId,
        generation: BackendGeneration,
        pin: &RetentionPin,
    ) -> Result<(), ControlError> {
        if !self.has_generation(graph_id, shard_id, generation) {
            return Err(ControlError::UnknownGeneration);
        }
        let key = (graph_id, shard_id, generation);
        if let Some(pins) = self.retention_pins.get_mut(&key) {
            pins.remove(pin);
            if pins.is_empty() {
                self.retention_pins.remove(&key);
            }
        }
        Ok(())
    }

    fn has_generation(
        &self,
        graph_id: GraphId,
        shard_id: ShardId,
        generation: BackendGeneration,
    ) -> bool {
        self.lineage(graph_id, shard_id)
            .iter()
            .any(|entry| entry.generation == generation)
    }

    pub(crate) fn placements(&self) -> impl Iterator<Item = &ShardPlacement> {
        self.placements.values()
    }
}

impl Default for CatalogState {
    fn default() -> Self {
        Self::new()
    }
}
