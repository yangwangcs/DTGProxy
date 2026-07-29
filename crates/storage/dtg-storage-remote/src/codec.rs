use std::collections::BTreeMap;

use dtg_storage::{
    AdjacencyDirection, CapabilityManifest, ChangeCursor, ChangePage, ChangeRecord, Digest32,
    EdgeId, EdgeTombstone, EdgeVersion, LogicalMutation, Properties, PushdownOperation,
    PushdownOutcome, PushdownRequest, ReadFence, ReplicaMetadata, ScanPage, SnapshotRecord,
    SnapshotReplayRecord, StorageError, TransactionId, TransactionRecord, TransactionState,
    TransactionTime, ValidInterval, Value, Version, VertexId, VertexRead, VertexScan,
    VertexTombstone, VertexVersion,
};

const MAX_NESTING: usize = 64;

pub(crate) const fn mutation_kind(mutation: &LogicalMutation) -> i16 {
    match mutation {
        LogicalMutation::PutVertex(_) => 1,
        LogicalMutation::DeleteVertex(_) => 2,
        LogicalMutation::PutEdge(_) => 3,
        LogicalMutation::DeleteEdge(_) => 4,
        LogicalMutation::PutTransaction(_) => 5,
        LogicalMutation::PutReplicaMetadata(_) => 6,
    }
}

pub(crate) fn encode_mutations(mutations: &[LogicalMutation]) -> Result<Vec<u8>, StorageError> {
    let mut encoder = Encoder::default();
    encoder.u32(length_u32(mutations.len(), "mutation count")?);
    for mutation in mutations {
        encoder.mutation(mutation)?;
    }
    Ok(encoder.finish())
}

pub(crate) fn decode_mutations(bytes: &[u8]) -> Result<Vec<LogicalMutation>, StorageError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()?;
    let mut mutations = Vec::with_capacity(count as usize);
    for _ in 0..count {
        mutations.push(decoder.mutation()?);
    }
    decoder.finish()?;
    Ok(mutations)
}

pub(crate) fn encode_point_request(id: u128, valid_at: i64, transaction_at: i64) -> Vec<u8> {
    let mut encoder = Encoder::default();
    encoder.u128(id);
    encoder.i64(valid_at);
    encoder.i64(transaction_at);
    encoder.finish()
}

pub(crate) fn decode_point_request(bytes: &[u8]) -> Result<(u128, i64, i64), StorageError> {
    let mut decoder = Decoder::new(bytes);
    let result = (decoder.u128()?, decoder.i64()?, decoder.i64()?);
    decoder.finish()?;
    Ok(result)
}

pub(crate) fn encode_history_request(id: u128, from: i64, through: i64, limit: u32) -> Vec<u8> {
    let mut encoder = Encoder::default();
    encoder.u128(id);
    encoder.i64(from);
    encoder.i64(through);
    encoder.u32(limit);
    encoder.finish()
}

pub(crate) fn decode_history_request(bytes: &[u8]) -> Result<(u128, i64, i64, u32), StorageError> {
    let mut decoder = Decoder::new(bytes);
    let result = (
        decoder.u128()?,
        decoder.i64()?,
        decoder.i64()?,
        decoder.u32()?,
    );
    decoder.finish()?;
    Ok(result)
}

pub(crate) fn encode_adjacency_request(
    vertex_id: u128,
    direction: AdjacencyDirection,
    valid_at: i64,
    transaction_at: i64,
    limit: u32,
) -> Vec<u8> {
    let mut encoder = Encoder::default();
    encoder.u128(vertex_id);
    encoder.u8(match direction {
        AdjacencyDirection::Outgoing => 1,
        AdjacencyDirection::Incoming => 2,
        AdjacencyDirection::Both => 3,
    });
    encoder.i64(valid_at);
    encoder.i64(transaction_at);
    encoder.u32(limit);
    encoder.finish()
}

pub(crate) fn decode_adjacency_request(
    bytes: &[u8],
) -> Result<(u128, AdjacencyDirection, i64, i64, u32), StorageError> {
    let mut decoder = Decoder::new(bytes);
    let vertex = decoder.u128()?;
    let direction = match decoder.u8()? {
        1 => AdjacencyDirection::Outgoing,
        2 => AdjacencyDirection::Incoming,
        3 => AdjacencyDirection::Both,
        _ => return Err(corrupt("invalid remote adjacency direction")),
    };
    let result = (
        vertex,
        direction,
        decoder.i64()?,
        decoder.i64()?,
        decoder.u32()?,
    );
    decoder.finish()?;
    Ok(result)
}

pub(crate) fn encode_changes_request(
    after: Option<ChangeCursor>,
    through: u64,
    limit: u32,
) -> Vec<u8> {
    let mut encoder = Encoder::default();
    encoder.u8(u8::from(after.is_some()));
    if let Some(after) = after {
        encoder.u64(after.raft_index());
        encoder.u64(after.mutation_ordinal());
    }
    encoder.u64(through);
    encoder.u32(limit);
    encoder.finish()
}

pub(crate) fn decode_changes_request(
    bytes: &[u8],
) -> Result<(Option<ChangeCursor>, u64, u32), StorageError> {
    let mut decoder = Decoder::new(bytes);
    let after = match decoder.u8()? {
        0 => None,
        1 => Some(ChangeCursor::new(decoder.u64()?, decoder.u64()?)),
        _ => return Err(corrupt("invalid remote change continuation flag")),
    };
    let result = (after, decoder.u64()?, decoder.u32()?);
    decoder.finish()?;
    Ok(result)
}

pub(crate) fn encode_scan_request(
    valid_at: i64,
    transaction_at: i64,
    after: Option<u128>,
    limit: u32,
) -> Vec<u8> {
    let mut encoder = Encoder::default();
    encoder.i64(valid_at);
    encoder.i64(transaction_at);
    encoder.u8(u8::from(after.is_some()));
    if let Some(after) = after {
        encoder.u128(after);
    }
    encoder.u32(limit);
    encoder.finish()
}

pub(crate) fn decode_scan_request(
    bytes: &[u8],
) -> Result<(i64, i64, Option<u128>, u32), StorageError> {
    let mut decoder = Decoder::new(bytes);
    let valid = decoder.i64()?;
    let transaction = decoder.i64()?;
    let after = match decoder.u8()? {
        0 => None,
        1 => Some(decoder.u128()?),
        _ => return Err(corrupt("invalid remote scan continuation flag")),
    };
    let result = (valid, transaction, after, decoder.u32()?);
    decoder.finish()?;
    Ok(result)
}

pub(crate) fn encode_text_request(value: &str) -> Result<Vec<u8>, StorageError> {
    let mut encoder = Encoder::default();
    encoder.string(value)?;
    Ok(encoder.finish())
}

pub(crate) fn decode_text_request(bytes: &[u8]) -> Result<String, StorageError> {
    let mut decoder = Decoder::new(bytes);
    let value = decoder.string()?;
    decoder.finish()?;
    Ok(value)
}

pub(crate) fn encode_change_page(page: &ChangePage) -> Result<Vec<u8>, StorageError> {
    let mut encoder = Encoder::default();
    encoder.u32(length_u32(page.rows().len(), "change page")?);
    for change in page.rows() {
        encoder.u64(change.raft_index());
        encoder.u64(change.mutation_ordinal());
        encoder.mutation(change.mutation())?;
    }
    encode_cursor(&mut encoder, page.next_after());
    Ok(encoder.finish())
}

pub(crate) fn decode_change_page(bytes: &[u8]) -> Result<ChangePage, StorageError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()?;
    let mut rows = Vec::with_capacity(count as usize);
    for _ in 0..count {
        rows.push(ChangeRecord::new(
            ChangeCursor::new(decoder.u64()?, decoder.u64()?),
            decoder.mutation()?,
        ));
    }
    let next = decode_cursor(&mut decoder)?;
    decoder.finish()?;
    Ok(ChangePage::new(rows, next))
}

pub(crate) fn encode_vertex_page(
    page: &ScanPage<VertexVersion, VertexId>,
) -> Result<Vec<u8>, StorageError> {
    let mutations = page
        .rows()
        .iter()
        .cloned()
        .map(LogicalMutation::PutVertex)
        .collect::<Vec<_>>();
    encode_page(&mutations, page.next_after().map(VertexId::get))
}

pub(crate) fn decode_vertex_page(
    bytes: &[u8],
) -> Result<ScanPage<VertexVersion, VertexId>, StorageError> {
    let (mutations, next) = decode_page(bytes)?;
    let rows = mutations
        .into_iter()
        .map(|mutation| match mutation {
            LogicalMutation::PutVertex(vertex) => Ok(vertex),
            _ => Err(corrupt(
                "remote vertex page contained a non-vertex mutation",
            )),
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    Ok(ScanPage::new(rows, next.map(VertexId::new).transpose()?))
}

pub(crate) fn encode_edge_page(
    page: &ScanPage<EdgeVersion, EdgeId>,
) -> Result<Vec<u8>, StorageError> {
    let mutations = page
        .rows()
        .iter()
        .cloned()
        .map(LogicalMutation::PutEdge)
        .collect::<Vec<_>>();
    encode_page(&mutations, page.next_after().map(EdgeId::get))
}

pub(crate) fn decode_edge_page(
    bytes: &[u8],
) -> Result<ScanPage<EdgeVersion, EdgeId>, StorageError> {
    let (mutations, next) = decode_page(bytes)?;
    let rows = mutations
        .into_iter()
        .map(|mutation| match mutation {
            LogicalMutation::PutEdge(edge) => Ok(edge),
            _ => Err(corrupt("remote edge page contained a non-edge mutation")),
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    Ok(ScanPage::new(rows, next.map(EdgeId::new).transpose()?))
}

pub(crate) fn encode_snapshot_records(records: &[SnapshotRecord]) -> Result<Vec<u8>, StorageError> {
    let mut encoder = Encoder::default();
    encoder.u32(length_u32(records.len(), "snapshot records")?);
    for record in records {
        match record {
            SnapshotRecord::Vertex(value) => {
                encoder.u8(1);
                encoder.vertex(value)?;
            }
            SnapshotRecord::VertexTombstone(value) => {
                encoder.u8(2);
                encoder.u128(value.id().get());
                encoder.u64(value.version().get());
                encoder.i64(value.transaction_time().get());
            }
            SnapshotRecord::Edge(value) => {
                encoder.u8(3);
                encoder.edge(value)?;
            }
            SnapshotRecord::EdgeTombstone(value) => {
                encoder.u8(4);
                encoder.u128(value.id().get());
                encoder.u64(value.version().get());
                encoder.i64(value.transaction_time().get());
            }
            SnapshotRecord::Transaction(value) => {
                encoder.u8(5);
                encoder.transaction(value);
            }
            SnapshotRecord::ReplicaMetadata(value) => {
                encoder.u8(6);
                encoder.string(value.name())?;
                encoder.value(value.value(), 0)?;
            }
            SnapshotRecord::Replay(value) => {
                encoder.u8(7);
                encoder.u64(value.raft_index());
                encoder.u64(value.raft_term());
                encoder.u128(value.command_id().get());
                encoder.digest(value.mutation_digest());
            }
            SnapshotRecord::Change(value) => {
                encoder.u8(8);
                encoder.u64(value.raft_index());
                encoder.u64(value.mutation_ordinal());
                encoder.mutation(value.mutation())?;
            }
        }
    }
    Ok(encoder.finish())
}

pub(crate) fn decode_snapshot_records(bytes: &[u8]) -> Result<Vec<SnapshotRecord>, StorageError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()?;
    let mut records = Vec::with_capacity(count as usize);
    for _ in 0..count {
        records.push(match decoder.u8()? {
            1 => SnapshotRecord::Vertex(decoder.vertex()?),
            2 => SnapshotRecord::VertexTombstone(VertexTombstone::new(
                VertexId::new(decoder.u128()?)?,
                Version::new(decoder.u64()?),
                TransactionTime::new(decoder.i64()?).map_err(kernel_error)?,
            )),
            3 => SnapshotRecord::Edge(decoder.edge()?),
            4 => SnapshotRecord::EdgeTombstone(EdgeTombstone::new(
                EdgeId::new(decoder.u128()?)?,
                Version::new(decoder.u64()?),
                TransactionTime::new(decoder.i64()?).map_err(kernel_error)?,
            )),
            5 => SnapshotRecord::Transaction(decoder.transaction()?),
            6 => SnapshotRecord::ReplicaMetadata(ReplicaMetadata::new(
                decoder.string()?,
                decoder.value(0)?,
            )?),
            7 => SnapshotRecord::Replay(SnapshotReplayRecord::new(
                decoder.u64()?,
                decoder.u64()?,
                dtg_storage::CommandId::new(decoder.u128()?)?,
                decoder.digest()?,
            )?),
            8 => SnapshotRecord::Change(ChangeRecord::new(
                ChangeCursor::new(decoder.u64()?, decoder.u64()?),
                decoder.mutation()?,
            )),
            _ => return Err(corrupt("unknown remote snapshot record tag")),
        });
    }
    decoder.finish()?;
    Ok(records)
}

pub(crate) fn encode_pushdown_request(request: &PushdownRequest) -> Result<Vec<u8>, StorageError> {
    let mut encoder = Encoder::default();
    encoder.u32(request.contract_version());
    let names = request.required_capabilities().names().collect::<Vec<_>>();
    encoder.u32(length_u32(names.len(), "pushdown capability count")?);
    for name in names {
        encoder.string(name)?;
    }
    match request.operation() {
        PushdownOperation::Vertex(read) => {
            encoder.u8(1);
            encoder.u128(read.id().get());
            encoder.i64(read.valid_at());
            encoder.i64(read.transaction_at().get());
        }
        PushdownOperation::VertexScan(scan) => {
            encoder.u8(2);
            encoder.i64(scan.valid_at());
            encoder.i64(scan.transaction_at().get());
            encoder.u8(u8::from(scan.after().is_some()));
            if let Some(after) = scan.after() {
                encoder.u128(after.get());
            }
            encoder.u32(scan.limit());
        }
    }
    Ok(encoder.finish())
}

pub(crate) fn decode_pushdown_request(
    bytes: &[u8],
    fence: ReadFence,
) -> Result<PushdownRequest, StorageError> {
    let mut decoder = Decoder::new(bytes);
    let contract_version = decoder.u32()?;
    let capability_count = decoder.u32()?;
    let mut names = Vec::with_capacity(capability_count as usize);
    for _ in 0..capability_count {
        names.push(decoder.string()?);
    }
    let operation = match decoder.u8()? {
        1 => PushdownOperation::Vertex(VertexRead::new(
            VertexId::new(decoder.u128()?)?,
            decoder.i64()?,
            TransactionTime::new(decoder.i64()?).map_err(kernel_error)?,
        )),
        2 => {
            let valid_at = decoder.i64()?;
            let transaction_at = TransactionTime::new(decoder.i64()?).map_err(kernel_error)?;
            let after = match decoder.u8()? {
                0 => None,
                1 => Some(VertexId::new(decoder.u128()?)?),
                _ => return Err(corrupt("invalid pushdown scan continuation flag")),
            };
            PushdownOperation::VertexScan(VertexScan::new(
                valid_at,
                transaction_at,
                after,
                decoder.u32()?,
            )?)
        }
        _ => return Err(corrupt("unknown remote pushdown operation")),
    };
    decoder.finish()?;
    PushdownRequest::new(
        contract_version,
        fence,
        CapabilityManifest::from_names(names)?,
        operation,
    )
}

pub(crate) fn encode_pushdown_outcome(outcome: &PushdownOutcome) -> Result<Vec<u8>, StorageError> {
    let mut encoder = Encoder::default();
    match outcome {
        PushdownOutcome::Exact(rows) => {
            encoder.u8(1);
            encoder.u32(0);
            encoder.bytes(&encode_snapshot_records(rows)?)?;
        }
        PushdownOutcome::ResidualRequired { rows, guarantees } => {
            encoder.u8(2);
            let names = guarantees.names().collect::<Vec<_>>();
            encoder.u32(length_u32(names.len(), "pushdown guarantee count")?);
            for name in names {
                encoder.string(name)?;
            }
            encoder.bytes(&encode_snapshot_records(rows)?)?;
        }
        PushdownOutcome::Unsupported => {
            encoder.u8(3);
            encoder.u32(0);
            encoder.bytes(&encode_snapshot_records(&[])?)?;
        }
    }
    Ok(encoder.finish())
}

pub(crate) fn decode_pushdown_outcome(bytes: &[u8]) -> Result<PushdownOutcome, StorageError> {
    let mut decoder = Decoder::new(bytes);
    let kind = decoder.u8()?;
    let guarantee_count = decoder.u32()?;
    let mut guarantees = Vec::with_capacity(guarantee_count as usize);
    for _ in 0..guarantee_count {
        guarantees.push(decoder.string()?);
    }
    let rows = decode_snapshot_records(&decoder.bytes()?)?;
    decoder.finish()?;
    match kind {
        1 if guarantees.is_empty() => Ok(PushdownOutcome::Exact(rows)),
        2 => Ok(PushdownOutcome::ResidualRequired {
            rows,
            guarantees: CapabilityManifest::from_names(guarantees)?,
        }),
        3 if guarantees.is_empty() && rows.is_empty() => Ok(PushdownOutcome::Unsupported),
        _ => Err(corrupt("invalid remote pushdown outcome")),
    }
}

fn encode_page(mutations: &[LogicalMutation], next: Option<u128>) -> Result<Vec<u8>, StorageError> {
    let mut encoder = Encoder::default();
    encoder.u32(length_u32(mutations.len(), "scan page")?);
    for mutation in mutations {
        encoder.mutation(mutation)?;
    }
    encoder.u8(u8::from(next.is_some()));
    if let Some(next) = next {
        encoder.u128(next);
    }
    Ok(encoder.finish())
}

fn decode_page(bytes: &[u8]) -> Result<(Vec<LogicalMutation>, Option<u128>), StorageError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()?;
    let mut mutations = Vec::with_capacity(count as usize);
    for _ in 0..count {
        mutations.push(decoder.mutation()?);
    }
    let next = match decoder.u8()? {
        0 => None,
        1 => Some(decoder.u128()?),
        _ => return Err(corrupt("invalid remote scan continuation flag")),
    };
    decoder.finish()?;
    Ok((mutations, next))
}

fn encode_cursor(encoder: &mut Encoder, cursor: Option<ChangeCursor>) {
    encoder.u8(u8::from(cursor.is_some()));
    if let Some(cursor) = cursor {
        encoder.u64(cursor.raft_index());
        encoder.u64(cursor.mutation_ordinal());
    }
}

fn decode_cursor(decoder: &mut Decoder<'_>) -> Result<Option<ChangeCursor>, StorageError> {
    match decoder.u8()? {
        0 => Ok(None),
        1 => Ok(Some(ChangeCursor::new(decoder.u64()?, decoder.u64()?))),
        _ => Err(corrupt("invalid remote change continuation flag")),
    }
}

fn length_u32(value: usize, name: &str) -> Result<u32, StorageError> {
    u32::try_from(value).map_err(|_| StorageError::Internal(format!("remote {name} is too large")))
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), StorageError> {
        let length = u32::try_from(value.len())
            .map_err(|_| StorageError::Internal("remote value is too large".into()))?;
        self.u32(length);
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), StorageError> {
        self.bytes(value.as_bytes())
    }

    fn digest(&mut self, value: Digest32) {
        self.bytes.extend_from_slice(&value.get());
    }

    fn properties(&mut self, properties: &Properties) -> Result<(), StorageError> {
        let length = u32::try_from(properties.len())
            .map_err(|_| StorageError::Internal("too many remote properties".into()))?;
        self.u32(length);
        for (name, value) in properties {
            self.string(name)?;
            self.value(value, 0)?;
        }
        Ok(())
    }

    fn value(&mut self, value: &Value, depth: usize) -> Result<(), StorageError> {
        if depth > MAX_NESTING {
            return Err(StorageError::Internal(
                "remote value nesting exceeds the provider bound".into(),
            ));
        }
        match value {
            Value::Null => self.u8(0),
            Value::Boolean(value) => {
                self.u8(1);
                self.u8(u8::from(*value));
            }
            Value::Integer(value) => {
                self.u8(2);
                self.i64(*value);
            }
            Value::FloatBits(value) => {
                self.u8(3);
                self.u64(*value);
            }
            Value::Bytes(value) => {
                self.u8(4);
                self.bytes(value)?;
            }
            Value::String(value) => {
                self.u8(5);
                self.string(value)?;
            }
            Value::List(values) => {
                self.u8(6);
                let length = u32::try_from(values.len())
                    .map_err(|_| StorageError::Internal("remote list is too large".into()))?;
                self.u32(length);
                for value in values {
                    self.value(value, depth + 1)?;
                }
            }
            Value::Map(values) => {
                self.u8(7);
                let length = u32::try_from(values.len())
                    .map_err(|_| StorageError::Internal("remote map is too large".into()))?;
                self.u32(length);
                for (name, value) in values {
                    self.string(name)?;
                    self.value(value, depth + 1)?;
                }
            }
        }
        Ok(())
    }

    fn vertex(&mut self, vertex: &VertexVersion) -> Result<(), StorageError> {
        self.u128(vertex.id().get());
        self.u64(vertex.version().get());
        self.i64(vertex.valid_time().start());
        self.i64(vertex.valid_time().end());
        self.i64(vertex.transaction_time().get());
        self.properties(vertex.properties())
    }

    fn edge(&mut self, edge: &EdgeVersion) -> Result<(), StorageError> {
        self.u128(edge.id().get());
        self.u128(edge.source().get());
        self.u128(edge.target().get());
        self.string(edge.edge_type())?;
        self.u64(edge.version().get());
        self.i64(edge.valid_time().start());
        self.i64(edge.valid_time().end());
        self.i64(edge.transaction_time().get());
        self.properties(edge.properties())
    }

    fn transaction(&mut self, transaction: &TransactionRecord) {
        self.u128(transaction.id().get());
        self.u8(match transaction.state() {
            TransactionState::Prepared => 1,
            TransactionState::Committed => 2,
            TransactionState::Aborted => 3,
        });
        self.i64(transaction.transaction_time().get());
        self.digest(transaction.record_digest());
    }

    fn mutation(&mut self, mutation: &LogicalMutation) -> Result<(), StorageError> {
        self.u8(mutation_kind(mutation) as u8);
        match mutation {
            LogicalMutation::PutVertex(vertex) => self.vertex(vertex)?,
            LogicalMutation::DeleteVertex(tombstone) => {
                self.u128(tombstone.id().get());
                self.u64(tombstone.version().get());
                self.i64(tombstone.transaction_time().get());
            }
            LogicalMutation::PutEdge(edge) => self.edge(edge)?,
            LogicalMutation::DeleteEdge(tombstone) => {
                self.u128(tombstone.id().get());
                self.u64(tombstone.version().get());
                self.i64(tombstone.transaction_time().get());
            }
            LogicalMutation::PutTransaction(transaction) => self.transaction(transaction),
            LogicalMutation::PutReplicaMetadata(metadata) => {
                self.string(metadata.name())?;
                self.value(metadata.value(), 0)?;
            }
        }
        Ok(())
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(&self) -> Result<(), StorageError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(corrupt("trailing bytes in remote typed value"))
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], StorageError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| corrupt("truncated remote typed value"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| corrupt("invalid remote u32"))?,
        ))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| corrupt("invalid remote u64"))?,
        ))
    }

    fn u128(&mut self) -> Result<u128, StorageError> {
        Ok(u128::from_be_bytes(
            self.take(16)?
                .try_into()
                .map_err(|_| corrupt("invalid remote u128"))?,
        ))
    }

    fn i64(&mut self) -> Result<i64, StorageError> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| corrupt("invalid remote i64"))?,
        ))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, StorageError> {
        let length =
            usize::try_from(self.u32()?).map_err(|_| corrupt("invalid remote byte length"))?;
        Ok(self.take(length)?.to_vec())
    }

    fn string(&mut self) -> Result<String, StorageError> {
        String::from_utf8(self.bytes()?).map_err(|_| corrupt("invalid remote UTF-8"))
    }

    fn digest(&mut self) -> Result<Digest32, StorageError> {
        Ok(Digest32::new(
            self.take(32)?
                .try_into()
                .map_err(|_| corrupt("invalid remote digest"))?,
        ))
    }

    fn properties(&mut self, depth: usize) -> Result<Properties, StorageError> {
        if depth > MAX_NESTING {
            return Err(corrupt("remote property nesting exceeds provider bound"));
        }
        let count = self.u32()?;
        let mut values = BTreeMap::new();
        for _ in 0..count {
            let name = self.string()?;
            let value = self.value(depth + 1)?;
            if values.insert(name, value).is_some() {
                return Err(corrupt("duplicate remote property name"));
            }
        }
        Ok(values)
    }

    fn value(&mut self, depth: usize) -> Result<Value, StorageError> {
        if depth > MAX_NESTING {
            return Err(corrupt("remote value nesting exceeds provider bound"));
        }
        match self.u8()? {
            0 => Ok(Value::Null),
            1 => match self.u8()? {
                0 => Ok(Value::Boolean(false)),
                1 => Ok(Value::Boolean(true)),
                _ => Err(corrupt("invalid remote boolean")),
            },
            2 => Ok(Value::Integer(self.i64()?)),
            3 => Ok(Value::FloatBits(self.u64()?)),
            4 => Ok(Value::Bytes(self.bytes()?)),
            5 => Ok(Value::String(self.string()?)),
            6 => {
                let count = self.u32()?;
                let mut values = Vec::new();
                for _ in 0..count {
                    values.push(self.value(depth + 1)?);
                }
                Ok(Value::List(values))
            }
            7 => Ok(Value::Map(self.properties(depth + 1)?)),
            _ => Err(corrupt("unknown remote value tag")),
        }
    }

    fn vertex(&mut self) -> Result<VertexVersion, StorageError> {
        VertexVersion::new(
            VertexId::new(self.u128()?)?,
            Version::new(self.u64()?),
            ValidInterval::new(self.i64()?, self.i64()?).map_err(kernel_error)?,
            TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            self.properties(0)?,
        )
    }

    fn edge(&mut self) -> Result<EdgeVersion, StorageError> {
        EdgeVersion::new(
            EdgeId::new(self.u128()?)?,
            VertexId::new(self.u128()?)?,
            VertexId::new(self.u128()?)?,
            self.string()?,
            Version::new(self.u64()?),
            ValidInterval::new(self.i64()?, self.i64()?).map_err(kernel_error)?,
            TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            self.properties(0)?,
        )
    }

    fn transaction(&mut self) -> Result<TransactionRecord, StorageError> {
        let id = TransactionId::new(self.u128()?).map_err(kernel_error)?;
        let state = match self.u8()? {
            1 => TransactionState::Prepared,
            2 => TransactionState::Committed,
            3 => TransactionState::Aborted,
            _ => return Err(corrupt("unknown remote transaction state")),
        };
        TransactionRecord::new(
            id,
            state,
            TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            self.digest()?,
        )
    }

    fn mutation(&mut self) -> Result<LogicalMutation, StorageError> {
        match self.u8()? {
            1 => Ok(LogicalMutation::PutVertex(self.vertex()?)),
            2 => Ok(LogicalMutation::DeleteVertex(VertexTombstone::new(
                VertexId::new(self.u128()?)?,
                Version::new(self.u64()?),
                TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            ))),
            3 => Ok(LogicalMutation::PutEdge(self.edge()?)),
            4 => Ok(LogicalMutation::DeleteEdge(EdgeTombstone::new(
                EdgeId::new(self.u128()?)?,
                Version::new(self.u64()?),
                TransactionTime::new(self.i64()?).map_err(kernel_error)?,
            ))),
            5 => Ok(LogicalMutation::PutTransaction(self.transaction()?)),
            6 => Ok(LogicalMutation::PutReplicaMetadata(ReplicaMetadata::new(
                self.string()?,
                self.value(0)?,
            )?)),
            _ => Err(corrupt("unknown remote mutation tag")),
        }
    }
}

fn corrupt(message: &str) -> StorageError {
    StorageError::Internal(message.into())
}

fn kernel_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(format!("invalid typed remote value: {error}"))
}
