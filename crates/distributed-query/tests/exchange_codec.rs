use std::collections::BTreeMap;

use distributed_query::{
    ExchangeCodecLimits, ExchangeDecodeExpectation, ExchangeFrame, SnapshotToken,
    schema_fingerprint,
};
use query_executor::{
    ChangeMetadata, ColumnBatch, EdgeRecord, RecordBatch, RuntimeValue, VertexRecord,
};
use temporal_ir::{Column, RowSchema, SlotId, ValueType};
use temporal_storage::{
    EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalEventOperation,
};
use temporal_types::{CanonicalElement, GraphValue, TransactionTime, ValidTime};

fn schema() -> RowSchema {
    RowSchema::new(vec![
        Column::new(SlotId::new(1), "active", ValueType::Boolean, true),
        Column::new(SlotId::new(2), "count", ValueType::Integer, false),
        Column::new(SlotId::new(3), "name", ValueType::String, true),
        Column::new(SlotId::new(4), "payload", ValueType::Bytes, true),
    ])
    .expect("schema")
}

fn snapshot() -> SnapshotToken {
    SnapshotToken::new(7, 3, 11, TransactionTime::new(100, 2), [9; 32]).expect("snapshot")
}

fn batch() -> ColumnBatch {
    ColumnBatch::from_record_batch(
        &RecordBatch::try_new(
            schema(),
            vec![
                vec![
                    RuntimeValue::Boolean(true),
                    RuntimeValue::Integer(42),
                    RuntimeValue::String("cedar-neutral".into()),
                    RuntimeValue::Bytes(vec![0, 1, 2, 255]),
                ],
                vec![
                    RuntimeValue::Null,
                    RuntimeValue::Integer(-7),
                    RuntimeValue::Null,
                    RuntimeValue::Bytes(Vec::new()),
                ],
            ],
        )
        .expect("record batch"),
    )
    .expect("column batch")
}

fn large_string_bytes_and_node_batch() -> ColumnBatch {
    let graph = GraphId::new(9);
    let partition = PartitionId::new(4);
    let element = ElementRef::vertex(graph, partition, ElementId::new(77));
    let node = VertexRecord::new(
        element,
        Some(LabelId::new(5)),
        CanonicalElement::new(
            11,
            BTreeMap::from([(42, GraphValue::String("boundary".repeat(32)))]),
        ),
    );
    let schema = RowSchema::new(vec![
        Column::new(SlotId::new(1), "text", ValueType::String, true),
        Column::new(SlotId::new(2), "payload", ValueType::Bytes, true),
        Column::new(SlotId::new(3), "node", ValueType::Node, true),
    ])
    .expect("schema");
    let long_text = "cedar-neutral-variable-width".repeat(256);
    let long_bytes = (0_u16..4096)
        .map(|value| (value % 251) as u8)
        .collect::<Vec<_>>();

    ColumnBatch::from_record_batch(
        &RecordBatch::try_new(
            schema,
            vec![
                vec![
                    RuntimeValue::String(long_text.clone()),
                    RuntimeValue::Bytes(long_bytes.clone()),
                    RuntimeValue::Node(node.clone()),
                ],
                vec![
                    RuntimeValue::Null,
                    RuntimeValue::Bytes(Vec::new()),
                    RuntimeValue::Null,
                ],
                vec![
                    RuntimeValue::String(long_text),
                    RuntimeValue::Bytes(long_bytes),
                    RuntimeValue::Node(node),
                ],
            ],
        )
        .expect("records"),
    )
    .expect("columns")
}

fn limits() -> ExchangeCodecLimits {
    ExchangeCodecLimits::new(16_384, 4 * 1024 * 1024).expect("limits")
}

#[test]
fn exchange_frame_round_trips_canonical_columns_without_exposing_column_batch_layout() {
    let snapshot = snapshot();
    let batch = batch();
    let frame = ExchangeFrame::encode(5, 8, false, &snapshot, &batch, limits()).expect("encode");

    assert_eq!(
        frame.schema_fingerprint(),
        schema_fingerprint(batch.schema())
    );
    assert_eq!(frame.row_count(), 2);

    let decoded = frame
        .decode(
            ExchangeDecodeExpectation::new(5, 8, snapshot, batch.schema().clone()),
            limits(),
        )
        .expect("decode");

    assert_eq!(decoded.shard_id(), 5);
    assert_eq!(decoded.sequence(), 8);
    assert!(!decoded.has_more());
    assert_eq!(decoded.batch(), &batch);
}

#[test]
fn large_variable_width_batch_round_trips_through_canonical_exchange() {
    let batch = large_string_bytes_and_node_batch();
    let frame = ExchangeFrame::encode(7, 0, false, &snapshot(), &batch, limits()).expect("encode");

    assert_eq!(
        frame
            .decode(
                ExchangeDecodeExpectation::new(7, 0, snapshot(), batch.schema().clone()),
                limits(),
            )
            .expect("decode")
            .into_batch(),
        batch
    );
}

#[test]
fn schema_fingerprint_is_defined_by_slots_types_and_nullability_not_display_names() {
    let renamed = RowSchema::new(vec![
        Column::new(SlotId::new(1), "renamed-a", ValueType::Boolean, true),
        Column::new(SlotId::new(2), "renamed-b", ValueType::Integer, false),
        Column::new(SlotId::new(3), "renamed-c", ValueType::String, true),
        Column::new(SlotId::new(4), "renamed-d", ValueType::Bytes, true),
    ])
    .expect("renamed schema");

    assert_eq!(schema_fingerprint(&schema()), schema_fingerprint(&renamed));
}

#[test]
fn exchange_frame_rejects_corruption_wrong_version_and_truncation() {
    let snapshot = snapshot();
    let frame = ExchangeFrame::encode(5, 8, true, &snapshot, &batch(), limits()).expect("encode");

    let mut corrupted = frame.as_bytes().to_vec();
    *corrupted.last_mut().expect("payload byte") ^= 0x80;
    assert_eq!(
        ExchangeFrame::from_bytes(corrupted, limits()),
        Err(distributed_query::DistributedQueryError::ExchangeChecksumMismatch)
    );

    let mut wrong_version = frame.as_bytes().to_vec();
    wrong_version[4..6].copy_from_slice(&2_u16.to_be_bytes());
    assert_eq!(
        ExchangeFrame::from_bytes(wrong_version, limits()),
        Err(distributed_query::DistributedQueryError::ExchangeVersionMismatch(2))
    );

    let truncated = frame.as_bytes()[..frame.as_bytes().len() - 1].to_vec();
    assert!(matches!(
        ExchangeFrame::from_bytes(truncated, limits()),
        Err(distributed_query::DistributedQueryError::PayloadLimit)
            | Err(distributed_query::DistributedQueryError::MalformedExchange)
    ));
}

#[test]
fn exchange_frame_rejects_schema_snapshot_and_sequence_mismatch() {
    let snapshot = snapshot();
    let frame = ExchangeFrame::encode(5, 8, false, &snapshot, &batch(), limits()).expect("encode");
    let wrong_snapshot = SnapshotToken::new(7, 3, 12, TransactionTime::new(100, 2), [9; 32])
        .expect("wrong snapshot");
    assert_eq!(
        frame.decode(
            ExchangeDecodeExpectation::new(5, 8, wrong_snapshot, schema()),
            limits(),
        ),
        Err(distributed_query::DistributedQueryError::ExchangeMetadataMismatch)
    );

    let wrong_schema = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "active",
        ValueType::Boolean,
        false,
    )])
    .expect("wrong schema");
    assert_eq!(
        frame.decode(
            ExchangeDecodeExpectation::new(5, 8, snapshot.clone(), wrong_schema),
            limits(),
        ),
        Err(distributed_query::DistributedQueryError::ExchangeSchemaMismatch)
    );
    assert_eq!(
        frame.decode(
            ExchangeDecodeExpectation::new(5, 9, snapshot, schema()),
            limits(),
        ),
        Err(distributed_query::DistributedQueryError::ExchangeMetadataMismatch)
    );
}

#[test]
fn exchange_frame_enforces_encoded_payload_and_row_limits() {
    let snapshot = snapshot();
    assert_eq!(
        ExchangeFrame::encode(
            5,
            8,
            false,
            &snapshot,
            &batch(),
            ExchangeCodecLimits::new(1, 4 * 1024 * 1024).expect("row limit"),
        ),
        Err(distributed_query::DistributedQueryError::PayloadLimit)
    );
    assert_eq!(
        ExchangeFrame::encode(
            5,
            8,
            false,
            &snapshot,
            &batch(),
            ExchangeCodecLimits::new(16_384, 8).expect("byte limit"),
        ),
        Err(distributed_query::DistributedQueryError::PayloadLimit)
    );
}

#[test]
fn exchange_frame_round_trips_nested_and_graph_boundary_values() {
    let graph = GraphId::new(7);
    let partition = PartitionId::new(3);
    let vertex_ref = ElementRef::vertex(graph, partition, ElementId::new(11));
    let edge_ref = ElementRef::edge(graph, partition, ElementId::new(12));
    let destination = ElementRef::vertex(graph, partition, ElementId::new(13));
    let payload =
        CanonicalElement::new(3, BTreeMap::from([(1, GraphValue::String("value".into()))]));
    let metadata = ChangeMetadata::new(
        ValidTime::from_micros(10),
        Some(ValidTime::from_micros(20)),
        TransactionTime::new(30, 4),
        5,
        TemporalEventOperation::Delete,
    );
    let node = VertexRecord::new(vertex_ref, Some(LabelId::new(6)), payload.clone())
        .with_change_metadata(metadata);
    let relationship = EdgeRecord::from_endpoints(
        edge_ref,
        EdgeTypeId::new(7),
        vertex_ref,
        destination,
        payload,
    )
    .with_change_metadata(metadata);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "value",
        ValueType::Any,
        false,
    )])
    .expect("schema");
    let batch = ColumnBatch::from_record_batch(
        &RecordBatch::try_new(
            schema.clone(),
            vec![
                vec![RuntimeValue::List(vec![
                    RuntimeValue::Node(node.clone()),
                    RuntimeValue::Integer(1),
                    RuntimeValue::Null,
                    RuntimeValue::String("nested".into()),
                ])],
                vec![RuntimeValue::Map(BTreeMap::from([(
                    9,
                    RuntimeValue::Bytes(vec![1, 2, 3]),
                )]))],
                vec![RuntimeValue::Node(node)],
                vec![RuntimeValue::Relationship(relationship)],
            ],
        )
        .expect("records"),
    )
    .expect("columns");
    let snapshot = snapshot();
    let frame = ExchangeFrame::encode(5, 0, false, &snapshot, &batch, limits()).expect("encode");
    let decoded = frame
        .decode(
            ExchangeDecodeExpectation::new(5, 0, snapshot, schema),
            limits(),
        )
        .expect("decode");

    assert_eq!(decoded.batch(), &batch);
}

#[test]
fn exchange_frame_rejects_noncanonical_null_payload_and_validity_bits() {
    const CHECKSUM_OFFSET: usize = 164;
    const HEADER_BYTES: usize = 168;
    const FIRST_VALIDITY_BYTE: usize = HEADER_BYTES + 10;
    const FIRST_BOOLEAN_DATA: usize = HEADER_BYTES + 15;

    let snapshot = snapshot();
    let frame = ExchangeFrame::encode(5, 8, false, &snapshot, &batch(), limits()).expect("encode");

    let mut hidden_null_value = frame.as_bytes().to_vec();
    hidden_null_value[FIRST_BOOLEAN_DATA + 1] = 1;
    rewrite_checksum(&mut hidden_null_value, CHECKSUM_OFFSET, HEADER_BYTES);
    let hidden_null_value =
        ExchangeFrame::from_bytes(hidden_null_value, limits()).expect("valid envelope");
    assert_eq!(
        hidden_null_value.decode(
            ExchangeDecodeExpectation::new(5, 8, snapshot.clone(), schema()),
            limits(),
        ),
        Err(distributed_query::DistributedQueryError::MalformedExchange)
    );

    let mut unused_validity_bit = frame.as_bytes().to_vec();
    unused_validity_bit[FIRST_VALIDITY_BYTE] |= 0x80;
    rewrite_checksum(&mut unused_validity_bit, CHECKSUM_OFFSET, HEADER_BYTES);
    let unused_validity_bit =
        ExchangeFrame::from_bytes(unused_validity_bit, limits()).expect("valid envelope");
    assert_eq!(
        unused_validity_bit.decode(
            ExchangeDecodeExpectation::new(5, 8, snapshot, schema()),
            limits(),
        ),
        Err(distributed_query::DistributedQueryError::MalformedExchange)
    );
}

#[test]
fn exchange_decode_rejects_compact_nested_values_before_runtime_expansion() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "value",
        ValueType::Any,
        false,
    )])
    .expect("schema");
    let batch = ColumnBatch::from_record_batch(
        &RecordBatch::try_new(
            schema.clone(),
            vec![vec![RuntimeValue::List(vec![RuntimeValue::Null; 100])]],
        )
        .expect("records"),
    )
    .expect("columns");
    let snapshot = snapshot();
    let frame = ExchangeFrame::encode(5, 0, false, &snapshot, &batch, limits()).expect("encode");
    let transported = ExchangeFrame::from_bytes(
        frame.as_bytes().to_vec(),
        ExchangeCodecLimits::new(16_384, 256).expect("tight decode budget"),
    )
    .expect("wire payload itself is bounded");

    assert_eq!(
        transported.into_decoded(
            ExchangeDecodeExpectation::new(5, 0, snapshot, schema),
            ExchangeCodecLimits::new(16_384, 256).expect("tight decode budget"),
        ),
        Err(distributed_query::DistributedQueryError::PayloadLimit)
    );
}

#[test]
fn exchange_encoder_rejects_invalid_temporal_change_metadata() {
    let element = ElementRef::vertex(GraphId::new(7), PartitionId::new(3), ElementId::new(11));
    let invalid = ChangeMetadata::new(
        ValidTime::from_micros(20),
        Some(ValidTime::from_micros(20)),
        TransactionTime::new(30, 0),
        0,
        TemporalEventOperation::Put,
    );
    let node = VertexRecord::new(
        element,
        Some(LabelId::new(1)),
        CanonicalElement::new(3, BTreeMap::new()),
    )
    .with_change_metadata(invalid);
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "node",
        ValueType::Node,
        false,
    )])
    .expect("schema");
    let batch = ColumnBatch::from_record_batch(
        &RecordBatch::try_new(schema, vec![vec![RuntimeValue::Node(node)]]).expect("records"),
    )
    .expect("columns");

    assert_eq!(
        ExchangeFrame::encode(5, 0, false, &snapshot(), &batch, limits()),
        Err(distributed_query::DistributedQueryError::UnsupportedExchangeValue)
    );
}

#[test]
fn exchange_encoder_rejects_schema_type_nesting_beyond_the_wire_limit() {
    let mut value_type = ValueType::Integer;
    for _ in 0..66 {
        value_type = ValueType::List(Box::new(value_type));
    }
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "nested",
        value_type,
        true,
    )])
    .expect("schema");
    let batch = ColumnBatch::from_record_batch(
        &RecordBatch::try_new(schema, Vec::new()).expect("empty records"),
    )
    .expect("empty columns");

    assert_eq!(
        ExchangeFrame::encode(5, 0, false, &snapshot(), &batch, limits()),
        Err(distributed_query::DistributedQueryError::PayloadLimit)
    );
}

fn rewrite_checksum(bytes: &mut [u8], checksum_offset: usize, header_bytes: usize) {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[..checksum_offset]);
    hasher.update(&bytes[header_bytes..]);
    bytes[checksum_offset..header_bytes].copy_from_slice(&hasher.finalize().to_be_bytes());
}
