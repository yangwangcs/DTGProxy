use std::collections::BTreeSet;

use super::ValidationError;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SlotId(u32);

impl SlotId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueType {
    Any,
    Null,
    Boolean,
    Integer,
    Float,
    String,
    Bytes,
    List(Box<ValueType>),
    Map,
    Node,
    Relationship,
    Path,
    Temporal,
    Spatial,
    Vector,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    slot: SlotId,
    name: String,
    value_type: ValueType,
    nullable: bool,
}

impl Column {
    #[must_use]
    pub fn new(
        slot: SlotId,
        name: impl Into<String>,
        value_type: ValueType,
        nullable: bool,
    ) -> Self {
        Self {
            slot,
            name: name.into(),
            value_type,
            nullable,
        }
    }

    #[must_use]
    pub const fn slot(&self) -> SlotId {
        self.slot
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowSchema {
    columns: Vec<Column>,
}

impl RowSchema {
    pub fn new(columns: Vec<Column>) -> Result<Self, ValidationError> {
        let mut slots = BTreeSet::new();
        for column in &columns {
            if !slots.insert(column.slot) {
                return Err(ValidationError::DuplicateSlot(column.slot));
            }
        }
        Ok(Self { columns })
    }

    #[must_use]
    pub const fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    #[must_use]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    #[must_use]
    pub fn contains(&self, slot: SlotId) -> bool {
        self.columns.iter().any(|column| column.slot == slot)
    }
}
