use std::collections::{BTreeMap, BTreeSet};

use temporal_ir::RowSchema;
use temporal_storage::ElementRef;
use temporal_types::{Interval, TransactionTime, ValidTime};

use super::{MAX_BATCH_ROWS, RecordBatch, RuntimeError, RuntimeValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemporalRegion {
    valid: Interval<ValidTime>,
    transaction: Interval<TransactionTime>,
}

impl TemporalRegion {
    #[must_use]
    pub const fn new(valid: Interval<ValidTime>, transaction: Interval<TransactionTime>) -> Self {
        Self { valid, transaction }
    }

    #[must_use]
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }

    #[must_use]
    pub const fn transaction(&self) -> Interval<TransactionTime> {
        self.transaction
    }

    #[must_use]
    pub fn intersection(self, other: Self) -> Option<Self> {
        Some(Self {
            valid: intersect(self.valid, other.valid)?,
            transaction: intersect(self.transaction, other.transaction)?,
        })
    }

    #[must_use]
    pub fn at_transaction(transaction: TransactionTime) -> Option<Self> {
        let next = transaction
            .logical()
            .checked_add(1)
            .map(|logical| TransactionTime::new(transaction.physical_micros(), logical))
            .or_else(|| {
                transaction
                    .physical_micros()
                    .checked_add(1)
                    .map(|physical| TransactionTime::new(physical, 0))
            })?;
        Some(Self::new(
            Interval::forever_from(ValidTime::from_micros(i64::MIN)),
            Interval::new(transaction, Some(next)).ok()?,
        ))
    }

    fn coalesce(self, other: Self) -> Option<Self> {
        if self.transaction == other.transaction {
            return connected_union(self.valid, other.valid)
                .map(|valid| Self::new(valid, self.transaction));
        }
        if self.valid == other.valid {
            return connected_union(self.transaction, other.transaction)
                .map(|transaction| Self::new(self.valid, transaction));
        }
        None
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalRow {
    values: Vec<RuntimeValue>,
    region: TemporalRegion,
    provenance: Vec<TemporalProvenance>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum TemporalProvenance {
    Element(ElementRef),
    Unwind(u32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalRecordBatch {
    schema: RowSchema,
    rows: Vec<TemporalRow>,
    estimated_bytes: u64,
}

impl TemporalRecordBatch {
    pub fn try_new(schema: RowSchema, rows: Vec<TemporalRow>) -> Result<Self, RuntimeError> {
        let values = rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>();
        let validated = RecordBatch::try_new(schema.clone(), values)?;
        Ok(Self {
            schema,
            rows,
            estimated_bytes: validated.estimated_bytes(),
        })
    }

    #[must_use]
    pub const fn schema(&self) -> &RowSchema {
        &self.schema
    }

    #[must_use]
    pub fn rows(&self) -> &[TemporalRow] {
        &self.rows
    }

    #[must_use]
    pub const fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    pub fn into_rows(self) -> Vec<TemporalRow> {
        self.rows
    }
}

impl TemporalRow {
    #[must_use]
    pub fn new(values: Vec<RuntimeValue>, region: TemporalRegion) -> Self {
        Self::with_provenance(values, region, Vec::new())
    }

    #[must_use]
    pub const fn with_provenance(
        values: Vec<RuntimeValue>,
        region: TemporalRegion,
        provenance: Vec<TemporalProvenance>,
    ) -> Self {
        Self {
            values,
            region,
            provenance,
        }
    }

    #[must_use]
    pub fn values(&self) -> &[RuntimeValue] {
        &self.values
    }

    #[must_use]
    pub const fn region(&self) -> TemporalRegion {
        self.region
    }

    #[must_use]
    pub fn provenance(&self) -> &[TemporalProvenance] {
        &self.provenance
    }

    #[must_use]
    pub fn with_values(&self, values: Vec<RuntimeValue>) -> Self {
        Self::with_provenance(values, self.region, self.provenance.clone())
    }

    #[must_use]
    pub fn with_appended_provenance(&self, provenance: TemporalProvenance) -> Self {
        let mut next = self.provenance.clone();
        next.push(provenance);
        Self::with_provenance(self.values.clone(), self.region, next)
    }
}

pub fn ensure_temporal_rows_memory(
    schema: &RowSchema,
    rows: &[TemporalRow],
    limit: u64,
) -> Result<(), RuntimeError> {
    let required = rows.chunks(MAX_BATCH_ROWS).try_fold(0_u64, |total, rows| {
        total
            .checked_add(
                TemporalRecordBatch::try_new(schema.clone(), rows.to_vec())?.estimated_bytes(),
            )
            .ok_or(RuntimeError::SizeOverflow)
    })?;
    if required > limit {
        return Err(RuntimeError::MemoryLimitExceeded { limit, required });
    }
    Ok(())
}

#[must_use]
pub fn temporal_join<F>(
    left: &[TemporalRow],
    right: &[TemporalRow],
    mut predicate: F,
) -> Vec<TemporalRow>
where
    F: FnMut(&TemporalRow, &TemporalRow) -> bool,
{
    let mut rows = Vec::new();
    for left_row in left {
        for right_row in right {
            if !predicate(left_row, right_row) {
                continue;
            }
            let Some(region) = left_row.region.intersection(right_row.region) else {
                continue;
            };
            let mut values = Vec::with_capacity(left_row.values.len() + right_row.values.len());
            values.extend(left_row.values.iter().cloned());
            values.extend(right_row.values.iter().cloned());
            let provenance = left_row
                .provenance
                .iter()
                .chain(&right_row.provenance)
                .cloned()
                .collect::<Vec<_>>();
            rows.push(TemporalRow::with_provenance(values, region, provenance));
        }
    }
    coalesce_temporal_rows(rows)
}

pub fn temporal_hash_join(
    left: &[TemporalRow],
    left_schema: &RowSchema,
    right: &[TemporalRow],
    right_schema: &RowSchema,
    keys: &[temporal_ir::SlotId],
    output: &RowSchema,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    temporal_hash_join_bounded(
        left,
        left_schema,
        right,
        right_schema,
        keys,
        output,
        u64::MAX,
    )
}

pub fn temporal_hash_join_bounded(
    left: &[TemporalRow],
    left_schema: &RowSchema,
    right: &[TemporalRow],
    right_schema: &RowSchema,
    keys: &[temporal_ir::SlotId],
    output: &RowSchema,
    memory_limit: u64,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    let left_keys = keys
        .iter()
        .map(|slot| {
            left_schema
                .columns()
                .iter()
                .position(|column| column.slot() == *slot)
                .ok_or(RuntimeError::MissingSlot(*slot))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let right_keys = keys
        .iter()
        .map(|slot| {
            right_schema
                .columns()
                .iter()
                .position(|column| column.slot() == *slot)
                .ok_or(RuntimeError::MissingSlot(*slot))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let left_slots = left_schema
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.slot(), index))
        .collect::<std::collections::BTreeMap<_, _>>();
    let right_slots = right_schema
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.slot(), index))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut rows = Vec::new();
    let mut retained_bytes = 0_u64;
    for left_row in left {
        for right_row in right {
            if !keys_match(left_row, right_row, &left_keys, &right_keys) {
                continue;
            }
            let Some(region) = left_row.region.intersection(right_row.region) else {
                continue;
            };
            let mut provenance = left_row.provenance.clone();
            provenance.extend(right_row.provenance.iter().cloned());
            let row = TemporalRow::with_provenance(
                output_values(left_row, Some(right_row), &left_slots, &right_slots, output)?,
                region,
                provenance,
            );
            retain_temporal_join_row(&mut rows, &mut retained_bytes, row, output, memory_limit)?;
        }
    }
    Ok(coalesce_temporal_rows(rows))
}

pub fn temporal_left_hash_join(
    left: &[TemporalRow],
    left_schema: &RowSchema,
    right: &[TemporalRow],
    right_schema: &RowSchema,
    keys: &[temporal_ir::SlotId],
    output: &RowSchema,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    temporal_left_hash_join_bounded(
        left,
        left_schema,
        right,
        right_schema,
        keys,
        output,
        u64::MAX,
    )
}

pub fn temporal_left_hash_join_bounded(
    left: &[TemporalRow],
    left_schema: &RowSchema,
    right: &[TemporalRow],
    right_schema: &RowSchema,
    keys: &[temporal_ir::SlotId],
    output: &RowSchema,
    memory_limit: u64,
) -> Result<Vec<TemporalRow>, RuntimeError> {
    let left_keys = slot_indices(left_schema, keys)?;
    let right_keys = slot_indices(right_schema, keys)?;
    let left_slots = slot_map(left_schema);
    let right_slots = slot_map(right_schema);
    let mut rows = Vec::new();
    let mut retained_bytes = 0_u64;
    for left_row in left {
        let matches = right
            .iter()
            .filter_map(|right_row| {
                keys_match(left_row, right_row, &left_keys, &right_keys)
                    .then(|| {
                        left_row
                            .region
                            .intersection(right_row.region)
                            .map(|region| (right_row, region))
                    })
                    .flatten()
            })
            .collect::<Vec<_>>();
        let valid_cells = region_cells(
            left_row.region.valid(),
            matches.iter().map(|(_, region)| region.valid()),
        );
        let transaction_cells = region_cells(
            left_row.region.transaction(),
            matches.iter().map(|(_, region)| region.transaction()),
        );
        for valid in valid_cells {
            for transaction in &transaction_cells {
                let region = TemporalRegion::new(valid, *transaction);
                let matched = matches
                    .iter()
                    .filter(|(_, matched)| region_contains(*matched, region))
                    .collect::<Vec<_>>();
                if matched.is_empty() {
                    let row = TemporalRow::with_provenance(
                        output_values(left_row, None, &left_slots, &right_slots, output)?,
                        region,
                        left_row.provenance.clone(),
                    );
                    retain_temporal_join_row(
                        &mut rows,
                        &mut retained_bytes,
                        row,
                        output,
                        memory_limit,
                    )?;
                } else {
                    for (right_row, _) in matched {
                        let mut provenance = left_row.provenance.clone();
                        provenance.extend(right_row.provenance.iter().cloned());
                        let row = TemporalRow::with_provenance(
                            output_values(
                                left_row,
                                Some(right_row),
                                &left_slots,
                                &right_slots,
                                output,
                            )?,
                            region,
                            provenance,
                        );
                        retain_temporal_join_row(
                            &mut rows,
                            &mut retained_bytes,
                            row,
                            output,
                            memory_limit,
                        )?;
                    }
                }
            }
        }
    }
    Ok(coalesce_temporal_rows(rows))
}

fn retain_temporal_join_row(
    rows: &mut Vec<TemporalRow>,
    retained_bytes: &mut u64,
    row: TemporalRow,
    output: &RowSchema,
    memory_limit: u64,
) -> Result<(), RuntimeError> {
    let row_bytes =
        TemporalRecordBatch::try_new(output.clone(), vec![row.clone()])?.estimated_bytes();
    let required = retained_bytes
        .checked_add(row_bytes)
        .ok_or(RuntimeError::SizeOverflow)?;
    if required > memory_limit {
        return Err(RuntimeError::MemoryLimitExceeded {
            limit: memory_limit,
            required,
        });
    }
    *retained_bytes = required;
    rows.push(row);
    Ok(())
}

fn slot_indices(
    schema: &RowSchema,
    slots: &[temporal_ir::SlotId],
) -> Result<Vec<usize>, RuntimeError> {
    slots
        .iter()
        .map(|slot| {
            schema
                .columns()
                .iter()
                .position(|column| column.slot() == *slot)
                .ok_or(RuntimeError::MissingSlot(*slot))
        })
        .collect()
}

fn slot_map(schema: &RowSchema) -> BTreeMap<temporal_ir::SlotId, usize> {
    schema
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.slot(), index))
        .collect()
}

fn keys_match(
    left: &TemporalRow,
    right: &TemporalRow,
    left_keys: &[usize],
    right_keys: &[usize],
) -> bool {
    left_keys
        .iter()
        .zip(right_keys)
        .all(|(left_index, right_index)| {
            let left = &left.values()[*left_index];
            let right = &right.values()[*right_index];
            !matches!(left, RuntimeValue::Null)
                && !matches!(right, RuntimeValue::Null)
                && left == right
        })
}

fn output_values(
    left: &TemporalRow,
    right: Option<&TemporalRow>,
    left_slots: &BTreeMap<temporal_ir::SlotId, usize>,
    right_slots: &BTreeMap<temporal_ir::SlotId, usize>,
    output: &RowSchema,
) -> Result<Vec<RuntimeValue>, RuntimeError> {
    output
        .columns()
        .iter()
        .map(|column| {
            if let Some(index) = left_slots.get(&column.slot()) {
                return Ok(left.values()[*index].clone());
            }
            if let Some(index) = right_slots.get(&column.slot()) {
                return right.map_or_else(
                    || {
                        if column.nullable() {
                            Ok(RuntimeValue::Null)
                        } else {
                            Err(RuntimeError::NullInNonNullableColumn {
                                slot: column.slot(),
                            })
                        }
                    },
                    |right| Ok(right.values()[*index].clone()),
                );
            }
            Err(RuntimeError::MissingSlot(column.slot()))
        })
        .collect()
}

pub(crate) fn region_cells<T>(
    base: Interval<T>,
    cuts: impl Iterator<Item = Interval<T>>,
) -> Vec<Interval<T>>
where
    T: Copy + Ord,
{
    let mut points = BTreeSet::from([base.start()]);
    let mut open = base.end().is_none();
    if let Some(end) = base.end() {
        points.insert(end);
    }
    for cut in cuts {
        points.insert(cut.start());
        if let Some(end) = cut.end() {
            points.insert(end);
        } else {
            open = true;
        }
    }
    let points = points.into_iter().collect::<Vec<_>>();
    points
        .iter()
        .enumerate()
        .filter_map(|(index, start)| {
            let end = points.get(index + 1).copied();
            (end.is_some() || open)
                .then(|| Interval::new(*start, end).ok())
                .flatten()
        })
        .filter(|cell| interval_contains(base, cell.start()))
        .collect()
}

pub(crate) fn region_contains(outer: TemporalRegion, inner: TemporalRegion) -> bool {
    interval_contains(outer.valid(), inner.valid().start())
        && interval_contains(outer.transaction(), inner.transaction().start())
}

fn interval_contains<T>(interval: Interval<T>, point: T) -> bool
where
    T: Copy + Ord,
{
    interval.start() <= point && interval.end().is_none_or(|end| point < end)
}

#[must_use]
pub fn coalesce_temporal_rows(rows: Vec<TemporalRow>) -> Vec<TemporalRow> {
    let mut output: Vec<TemporalRow> = Vec::with_capacity(rows.len());
    for mut row in rows {
        canonicalize_provenance(&mut row.provenance);
        let mut merged = row;
        let mut index = 0;
        while index < output.len() {
            if output[index].values == merged.values
                && output[index].provenance == merged.provenance
                && let Some(region) = output[index].region.coalesce(merged.region)
            {
                merged.region = region;
                output.remove(index);
                index = 0;
                continue;
            }
            index += 1;
        }
        output.push(merged);
    }
    output
}

fn canonicalize_provenance(provenance: &mut [TemporalProvenance]) {
    provenance.sort();
}

#[must_use]
pub fn distinct_temporal_rows(rows: Vec<TemporalRow>) -> Vec<TemporalRow> {
    let mut distinct: Vec<TemporalRow> = Vec::with_capacity(rows.len());
    for mut row in rows {
        if let Some(existing) = distinct
            .iter_mut()
            .find(|existing| existing.values == row.values && existing.region == row.region)
        {
            existing.provenance.append(&mut row.provenance);
            existing.provenance.sort();
            existing.provenance.dedup();
        } else {
            row.provenance.sort();
            row.provenance.dedup();
            distinct.push(row);
        }
    }
    distinct
}

fn intersect<T>(left: Interval<T>, right: Interval<T>) -> Option<Interval<T>>
where
    T: Copy + Ord,
{
    let start = left.start().max(right.start());
    let end = match (left.end(), right.end()) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(end), None) | (None, Some(end)) => Some(end),
        (None, None) => None,
    };
    Interval::new(start, end).ok()
}

fn connected_union<T>(left: Interval<T>, right: Interval<T>) -> Option<Interval<T>>
where
    T: Copy + Ord,
{
    if left.end().is_some_and(|end| end < right.start())
        || right.end().is_some_and(|end| end < left.start())
    {
        return None;
    }
    let start = left.start().min(right.start());
    let end = match (left.end(), right.end()) {
        (Some(left), Some(right)) => Some(left.max(right)),
        _ => None,
    };
    Interval::new(start, end).ok()
}
