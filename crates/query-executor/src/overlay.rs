use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_storage::{ElementKind, ElementRef};
use temporal_types::{Interval, ValidTime};

use crate::RuntimeValue;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphOverlayEntry {
    owner_shard: u32,
    adjacency_shards: Vec<u32>,
    element: ElementRef,
    valid: Interval<ValidTime>,
    replacement: Option<RuntimeValue>,
}

impl GraphOverlayEntry {
    pub fn put(
        owner_shard: u32,
        valid: Interval<ValidTime>,
        replacement: RuntimeValue,
    ) -> Result<Self, GraphOverlayError> {
        runtime_element(&replacement).ok_or(GraphOverlayError::NonGraphValue)?;
        Self::put_with_adjacency(owner_shard, [owner_shard], valid, replacement)
    }

    pub fn put_with_adjacency(
        owner_shard: u32,
        adjacency_shards: impl IntoIterator<Item = u32>,
        valid: Interval<ValidTime>,
        replacement: RuntimeValue,
    ) -> Result<Self, GraphOverlayError> {
        let element = runtime_element(&replacement).ok_or(GraphOverlayError::NonGraphValue)?;
        let mut adjacency_shards = adjacency_shards.into_iter().collect::<Vec<_>>();
        adjacency_shards.push(owner_shard);
        adjacency_shards.sort_unstable();
        adjacency_shards.dedup();
        Ok(Self {
            owner_shard,
            adjacency_shards,
            element,
            valid,
            replacement: Some(replacement),
        })
    }

    #[must_use]
    pub fn delete(owner_shard: u32, element: ElementRef, valid: Interval<ValidTime>) -> Self {
        Self {
            owner_shard,
            adjacency_shards: vec![owner_shard],
            element,
            valid,
            replacement: None,
        }
    }

    #[must_use]
    pub fn delete_with_adjacency(
        owner_shard: u32,
        adjacency_shards: impl IntoIterator<Item = u32>,
        element: ElementRef,
        valid: Interval<ValidTime>,
    ) -> Self {
        let mut adjacency_shards = adjacency_shards.into_iter().collect::<Vec<_>>();
        adjacency_shards.push(owner_shard);
        adjacency_shards.sort_unstable();
        adjacency_shards.dedup();
        Self {
            owner_shard,
            adjacency_shards,
            element,
            valid,
            replacement: None,
        }
    }

    #[must_use]
    pub const fn owner_shard(&self) -> u32 {
        self.owner_shard
    }

    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub fn adjacency_shards(&self) -> &[u32] {
        &self.adjacency_shards
    }

    #[must_use]
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }

    #[must_use]
    pub const fn replacement(&self) -> Option<&RuntimeValue> {
        self.replacement.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphOverlay {
    entries: Vec<GraphOverlayEntry>,
    maximum_entries: usize,
    selected_shard: Option<u32>,
}

impl GraphOverlay {
    pub fn new(maximum_entries: usize) -> Result<Self, GraphOverlayError> {
        if maximum_entries == 0 {
            return Err(GraphOverlayError::InvalidLimit);
        }
        Ok(Self {
            entries: Vec::new(),
            maximum_entries,
            selected_shard: None,
        })
    }

    pub fn stage(
        &mut self,
        entries: impl IntoIterator<Item = GraphOverlayEntry>,
    ) -> Result<(), GraphOverlayError> {
        let mut candidate = self.entries.clone();
        for entry in entries {
            candidate.retain(|previous| {
                previous.element != entry.element || previous.valid != entry.valid
            });
            candidate.push(entry);
        }
        if candidate.len() > self.maximum_entries {
            return Err(GraphOverlayError::EntryLimit {
                max: self.maximum_entries,
                actual: candidate.len(),
            });
        }
        self.entries = candidate;
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn for_shard(&self, shard_id: u32) -> Self {
        Self {
            entries: self.entries.clone(),
            maximum_entries: self.maximum_entries,
            selected_shard: Some(shard_id),
        }
    }

    #[must_use]
    pub fn contains_visible(&self, element: ElementRef, valid_time: ValidTime) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.element == element && entry.valid.contains(valid_time))
    }

    #[must_use]
    pub fn visible_relationship_endpoints(&self, valid_time: ValidTime) -> BTreeSet<ElementRef> {
        self.entries
            .iter()
            .filter(|entry| entry.valid.contains(valid_time))
            .filter_map(|entry| match entry.replacement.as_ref() {
                Some(RuntimeValue::Relationship(edge)) => {
                    Some([edge.source_ref(), edge.destination_ref()])
                }
                _ => None,
            })
            .flatten()
            .collect()
    }

    #[must_use]
    pub fn visible_projection(
        &self,
        valid_time: ValidTime,
    ) -> BTreeMap<ElementRef, Option<RuntimeValue>> {
        let mut visible = BTreeMap::new();
        for entry in &self.entries {
            if entry.valid.contains(valid_time) {
                visible.insert(entry.element, entry.replacement.clone());
            }
        }
        visible
    }

    pub(crate) fn visible_scan(
        &self,
        valid_time: ValidTime,
        kind: ElementKind,
    ) -> BTreeMap<ElementRef, Option<RuntimeValue>> {
        self.visible(valid_time, kind, |entry, shard| entry.owner_shard == shard)
    }

    pub(crate) fn visible_expand(
        &self,
        valid_time: ValidTime,
        kind: ElementKind,
    ) -> BTreeMap<ElementRef, Option<RuntimeValue>> {
        self.visible(valid_time, kind, |entry, shard| {
            kind == ElementKind::Vertex || entry.adjacency_shards.contains(&shard)
        })
    }

    fn visible(
        &self,
        valid_time: ValidTime,
        kind: ElementKind,
        selected: impl Fn(&GraphOverlayEntry, u32) -> bool,
    ) -> BTreeMap<ElementRef, Option<RuntimeValue>> {
        let mut visible = BTreeMap::new();
        for entry in &self.entries {
            if entry.element.kind() == kind
                && entry.valid.contains(valid_time)
                && self
                    .selected_shard
                    .is_none_or(|shard| selected(entry, shard))
            {
                visible.insert(entry.element, entry.replacement.clone());
            }
        }
        visible
    }
}

impl Default for GraphOverlay {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            maximum_entries: usize::MAX,
            selected_shard: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphOverlayError {
    InvalidLimit,
    NonGraphValue,
    EntryLimit { max: usize, actual: usize },
}

impl Display for GraphOverlayError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit => formatter.write_str("graph overlay limit must be non-zero"),
            Self::NonGraphValue => {
                formatter.write_str("graph overlay replacement is not an entity")
            }
            Self::EntryLimit { max, actual } => {
                write!(
                    formatter,
                    "graph overlay has {actual} entries, exceeding its limit of {max}"
                )
            }
        }
    }
}

impl Error for GraphOverlayError {}

fn runtime_element(value: &RuntimeValue) -> Option<ElementRef> {
    match value {
        RuntimeValue::Node(node) => Some(node.element()),
        RuntimeValue::Relationship(edge) => Some(edge.element()),
        _ => None,
    }
}
