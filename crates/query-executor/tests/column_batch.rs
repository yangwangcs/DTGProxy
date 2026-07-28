use query_executor::{ColumnBatch, ColumnVector, RecordBatch, RuntimeValue};
use temporal_ir::{Column, RowSchema, SlotId, ValueType};

fn schema() -> RowSchema {
    RowSchema::new(vec![
        Column::new(SlotId::new(4), "count", ValueType::Integer, false),
        Column::new(SlotId::new(9), "name", ValueType::String, true),
    ])
    .expect("schema")
}

#[test]
fn converts_adapter_rows_to_typed_columns_and_back_without_losing_nulls() {
    let rows = RecordBatch::try_new(
        schema(),
        vec![
            vec![RuntimeValue::Integer(7), RuntimeValue::String("Ada".into())],
            vec![RuntimeValue::Integer(11), RuntimeValue::Null],
        ],
    )
    .expect("adapter batch");

    let columns = ColumnBatch::from_record_batch(&rows).expect("columnar boundary");
    assert_eq!(columns.schema(), &schema());
    assert_eq!(columns.row_count(), 2);
    assert!(matches!(
        columns.column(0),
        Some(ColumnVector::Integer { .. })
    ));
    assert!(matches!(columns.column(1), Some(ColumnVector::Utf8 { .. })));
    assert_eq!(
        columns
            .column(1)
            .expect("string column")
            .validity()
            .is_valid(1),
        Some(false)
    );
    assert_eq!(
        columns.row(1).expect("row boundary"),
        vec![RuntimeValue::Integer(11), RuntimeValue::Null]
    );
    assert_eq!(columns.to_record_batch().expect("adapter boundary"), rows);
}

fn variable_schema() -> RowSchema {
    RowSchema::new(vec![
        Column::new(SlotId::new(1), "text", ValueType::String, true),
        Column::new(SlotId::new(2), "payload", ValueType::Bytes, true),
    ])
    .expect("schema")
}

fn batch_with_string_and_bytes(text: &str, payload: &[u8]) -> ColumnBatch {
    ColumnBatch::from_record_batch(
        &RecordBatch::try_new(
            variable_schema(),
            vec![vec![
                RuntimeValue::String(text.into()),
                RuntimeValue::Bytes(payload.to_vec()),
            ]],
        )
        .expect("record batch"),
    )
    .expect("column batch")
}

fn nullable_string_batch() -> ColumnBatch {
    ColumnBatch::from_record_batch(
        &RecordBatch::try_new(
            RowSchema::new(vec![Column::new(
                SlotId::new(3),
                "maybe_text",
                ValueType::String,
                true,
            )])
            .expect("schema"),
            vec![
                vec![RuntimeValue::String("visible".into())],
                vec![RuntimeValue::Null],
            ],
        )
        .expect("record batch"),
    )
    .expect("column batch")
}

#[test]
fn value_ref_exposes_variable_values_without_owned_copies() {
    let batch = batch_with_string_and_bytes("large payload", b"binary payload");

    let text = batch.value_ref(0, 0).expect("string value");
    assert_eq!(text.utf8(), Some("large payload"));
    let string_data = match batch.column(0).expect("string column") {
        ColumnVector::Utf8 { offsets, data, .. } => {
            let start = offsets[0] as usize;
            let end = offsets[1] as usize;
            &data[start..end]
        }
        other => panic!("expected utf8 column, found {other:?}"),
    };
    assert_eq!(text.utf8().expect("utf8").as_ptr(), string_data.as_ptr());

    let bytes = batch.value_ref(1, 0).expect("bytes value");
    assert_eq!(bytes.bytes(), Some(&b"binary payload"[..]));
    let payload_data = match batch.column(1).expect("bytes column") {
        ColumnVector::Bytes { offsets, data, .. } => {
            let start = offsets[0] as usize;
            let end = offsets[1] as usize;
            &data[start..end]
        }
        other => panic!("expected bytes column, found {other:?}"),
    };
    assert_eq!(
        bytes.bytes().expect("bytes").as_ptr(),
        payload_data.as_ptr()
    );
}

#[test]
fn value_ref_uses_validity_before_hidden_payload() {
    let batch = nullable_string_batch();
    let value = batch.value_ref(0, 1).expect("nullable row");

    assert!(value.is_null());
    assert_eq!(value.utf8(), None);
}
