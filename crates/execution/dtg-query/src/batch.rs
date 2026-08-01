use std::cmp::Ordering;
use std::collections::BTreeMap;

use dtg_language_ir::{LogicalType, RowSchema};
use dtg_storage::{EdgeId, EdgeVersion, Value, VertexId, VertexVersion};

use crate::QueryError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Self>),
    Map(BTreeMap<String, Self>),
    Vertex(VertexVersion),
    Relationship(EdgeVersion),
}

impl QueryValue {
    pub fn from_kernel(value: Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Boolean(value) => Self::Boolean(value),
            Value::Integer(value) => Self::Integer(value),
            Value::FloatBits(value) => Self::FloatBits(value),
            Value::Bytes(value) => Self::Bytes(value),
            Value::String(value) => Self::String(value),
            Value::List(values) => Self::List(values.into_iter().map(Self::from_kernel).collect()),
            Value::Map(values) => Self::Map(
                values
                    .into_iter()
                    .map(|(name, value)| (name, Self::from_kernel(value)))
                    .collect(),
            ),
        }
    }

    pub fn estimated_bytes(&self) -> u64 {
        match self {
            Self::Null => 1,
            Self::Boolean(_) => 1,
            Self::Integer(_) | Self::FloatBits(_) => 8,
            Self::Bytes(value) => value.len() as u64,
            Self::String(value) => value.len() as u64,
            Self::List(values) => values.iter().map(Self::estimated_bytes).sum(),
            Self::Map(values) => values
                .iter()
                .map(|(name, value)| name.len() as u64 + value.estimated_bytes())
                .sum(),
            Self::Vertex(vertex) => {
                64 + vertex
                    .properties()
                    .iter()
                    .map(|(name, value)| {
                        name.len() as u64 + Self::from_kernel(value.clone()).estimated_bytes()
                    })
                    .sum::<u64>()
            }
            Self::Relationship(edge) => {
                96 + edge.edge_type().len() as u64
                    + edge
                        .properties()
                        .iter()
                        .map(|(name, value)| {
                            name.len() as u64 + Self::from_kernel(value.clone()).estimated_bytes()
                        })
                        .sum::<u64>()
            }
        }
    }

    pub(crate) fn matches_type(&self, data_type: &LogicalType, nullable: bool) -> bool {
        if matches!(self, Self::Null) {
            return nullable || matches!(data_type, LogicalType::Null | LogicalType::Any);
        }
        match (self, data_type) {
            (_, LogicalType::Any) => true,
            (Self::Boolean(_), LogicalType::Boolean)
            | (Self::Integer(_), LogicalType::Integer)
            | (Self::FloatBits(_), LogicalType::Float)
            | (Self::Bytes(_), LogicalType::Bytes)
            | (Self::String(_), LogicalType::String)
            | (Self::Map(_), LogicalType::Map)
            | (Self::Vertex(_), LogicalType::Vertex)
            | (Self::Relationship(_), LogicalType::Relationship) => true,
            (Self::List(values), LogicalType::List(element_type)) => values
                .iter()
                .all(|value| value.matches_type(element_type, true)),
            _ => false,
        }
    }

    pub(crate) fn total_cmp(&self, other: &Self) -> Ordering {
        let tag = |value: &Self| match value {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Integer(_) => 2,
            Self::FloatBits(_) => 3,
            Self::Bytes(_) => 4,
            Self::String(_) => 5,
            Self::List(_) => 6,
            Self::Map(_) => 7,
            Self::Vertex(_) => 8,
            Self::Relationship(_) => 9,
        };
        tag(self)
            .cmp(&tag(other))
            .then_with(|| match (self, other) {
                (Self::Null, Self::Null) => Ordering::Equal,
                (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
                (Self::Integer(left), Self::Integer(right)) => left.cmp(right),
                (Self::FloatBits(left), Self::FloatBits(right)) => left.cmp(right),
                (Self::Bytes(left), Self::Bytes(right)) => left.cmp(right),
                (Self::String(left), Self::String(right)) => left.cmp(right),
                (Self::List(left), Self::List(right)) => compare_slices(left, right),
                (Self::Map(left), Self::Map(right)) => compare_maps(left, right),
                (Self::Vertex(left), Self::Vertex(right)) => {
                    vertex_key(left).cmp(&vertex_key(right))
                }
                (Self::Relationship(left), Self::Relationship(right)) => {
                    edge_key(left).cmp(&edge_key(right))
                }
                _ => Ordering::Equal,
            })
    }
}

fn compare_slices(left: &[QueryValue], right: &[QueryValue]) -> Ordering {
    left.iter()
        .zip(right)
        .find_map(|(left, right)| {
            let ordering = left.total_cmp(right);
            (ordering != Ordering::Equal).then_some(ordering)
        })
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

fn compare_maps(
    left: &BTreeMap<String, QueryValue>,
    right: &BTreeMap<String, QueryValue>,
) -> Ordering {
    let mut left = left.iter();
    let mut right = right.iter();
    loop {
        match (left.next(), right.next()) {
            (Some((left_name, left_value)), Some((right_name, right_value))) => {
                let ordering = left_name
                    .cmp(right_name)
                    .then_with(|| left_value.total_cmp(right_value));
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => return Ordering::Equal,
        }
    }
}

fn vertex_key(vertex: &VertexVersion) -> (VertexId, i64, i64, i64, dtg_storage::Version) {
    (
        vertex.id(),
        vertex.valid_time().start(),
        vertex.valid_time().end(),
        vertex.transaction_time().get(),
        vertex.version(),
    )
}

fn edge_key(
    edge: &EdgeVersion,
) -> (
    EdgeId,
    VertexId,
    VertexId,
    &str,
    i64,
    i64,
    i64,
    dtg_storage::Version,
) {
    (
        edge.id(),
        edge.source(),
        edge.target(),
        edge.edge_type(),
        edge.valid_time().start(),
        edge.valid_time().end(),
        edge.transaction_time().get(),
        edge.version(),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnBatch {
    schema: RowSchema,
    columns: Vec<Vec<QueryValue>>,
    row_count: usize,
}

impl ColumnBatch {
    pub fn try_new(schema: RowSchema, columns: Vec<Vec<QueryValue>>) -> Result<Self, QueryError> {
        if schema.fields.len() != columns.len() {
            return Err(QueryError::InvalidBatch(
                "column count does not match schema".into(),
            ));
        }
        let row_count = columns.first().map_or(0, Vec::len);
        if columns.iter().any(|column| column.len() != row_count) {
            return Err(QueryError::InvalidBatch(
                "column lengths are inconsistent".into(),
            ));
        }
        for (field, column) in schema.fields.iter().zip(&columns) {
            if column
                .iter()
                .any(|value| !value.matches_type(&field.data_type, field.nullable))
            {
                return Err(QueryError::InvalidBatch(format!(
                    "column {:?} contains a value outside its logical type",
                    field.name
                )));
            }
        }
        Ok(Self {
            schema,
            columns,
            row_count,
        })
    }

    pub fn from_rows(schema: RowSchema, rows: Vec<Vec<QueryValue>>) -> Result<Self, QueryError> {
        if rows.iter().any(|row| row.len() != schema.fields.len()) {
            return Err(QueryError::InvalidBatch(
                "row width does not match schema".into(),
            ));
        }
        let mut columns = vec![Vec::with_capacity(rows.len()); schema.fields.len()];
        for row in rows {
            for (column, value) in columns.iter_mut().zip(row) {
                column.push(value);
            }
        }
        Self::try_new(schema, columns)
    }

    pub fn empty(schema: RowSchema) -> Self {
        let columns = vec![Vec::new(); schema.fields.len()];
        Self {
            schema,
            columns,
            row_count: 0,
        }
    }

    pub fn concatenate(schema: RowSchema, batches: Vec<Self>) -> Result<Self, QueryError> {
        let mut row_count = 0_usize;
        for batch in &batches {
            if batch.schema != schema {
                return Err(QueryError::InvalidBatch(
                    "cannot concatenate batches with different schemas".into(),
                ));
            }
            row_count = row_count.checked_add(batch.row_count).ok_or_else(|| {
                QueryError::InvalidBatch("concatenated row count exceeds usize".into())
            })?;
        }
        let mut columns = (0..schema.fields.len())
            .map(|_| Vec::with_capacity(row_count))
            .collect::<Vec<_>>();
        for batch in batches {
            for (column, values) in columns.iter_mut().zip(batch.columns) {
                column.extend(values);
            }
        }
        Ok(Self {
            schema,
            columns,
            row_count,
        })
    }

    pub const fn schema(&self) -> &RowSchema {
        &self.schema
    }

    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn column(&self, index: usize) -> Option<&[QueryValue]> {
        self.columns.get(index).map(Vec::as_slice)
    }

    pub fn rows(&self) -> Vec<Vec<QueryValue>> {
        (0..self.row_count)
            .map(|row| {
                self.columns
                    .iter()
                    .map(|column| column[row].clone())
                    .collect()
            })
            .collect()
    }

    pub fn into_rows(self) -> Vec<Vec<QueryValue>> {
        let mut columns = self
            .columns
            .into_iter()
            .map(Vec::into_iter)
            .collect::<Vec<_>>();
        (0..self.row_count)
            .map(|_| {
                columns
                    .iter_mut()
                    .map(|column| column.next().expect("column length matches row count"))
                    .collect()
            })
            .collect()
    }

    pub fn estimated_bytes(&self) -> u64 {
        self.columns
            .iter()
            .flatten()
            .map(QueryValue::estimated_bytes)
            .sum()
    }

    pub fn vertex_ids(&self) -> Vec<VertexId> {
        self.columns
            .iter()
            .flatten()
            .filter_map(|value| match value {
                QueryValue::Vertex(vertex) => Some(vertex.id()),
                _ => None,
            })
            .collect()
    }

    pub fn relationship_ids(&self) -> Vec<EdgeId> {
        self.columns
            .iter()
            .flatten()
            .filter_map(|value| match value {
                QueryValue::Relationship(edge) => Some(edge.id()),
                _ => None,
            })
            .collect()
    }
}
