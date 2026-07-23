use std::collections::BTreeMap;

use bolt_protocol::Value;
use cypher_engine::{BoltValueError, bolt_parameter_to_runtime, runtime_value_to_bolt};
use query_executor::RuntimeValue;

#[test]
fn converts_utc_datetime_parameters_to_exact_microseconds() {
    let bolt = Value::Structure {
        signature: 0x49,
        fields: vec![
            Value::Integer(1_700_000_000),
            Value::Integer(123_456_000),
            Value::Integer(0),
        ],
    };

    assert_eq!(
        bolt_parameter_to_runtime(&bolt).expect("datetime"),
        RuntimeValue::TimestampMicros(1_700_000_000_123_456)
    );
    assert_eq!(
        runtime_value_to_bolt(&RuntimeValue::TimestampMicros(-1)).expect("negative timestamp"),
        Value::Structure {
            signature: 0x49,
            fields: vec![
                Value::Integer(-1),
                Value::Integer(999_999_000),
                Value::Integer(0),
            ],
        }
    );
}

#[test]
fn converts_nested_parameters_and_rejects_lossy_or_local_time() {
    let value = Value::Map(BTreeMap::from([
        ("active".into(), Value::Boolean(true)),
        (
            "tags".into(),
            Value::List(vec![Value::String("a".into()), Value::Null]),
        ),
    ]));
    let RuntimeValue::Map(converted) = bolt_parameter_to_runtime(&value).expect("map") else {
        panic!("expected map");
    };
    assert_eq!(converted.len(), 2);

    let nanosecond_loss = Value::Structure {
        signature: 0x49,
        fields: vec![Value::Integer(1), Value::Integer(1), Value::Integer(0)],
    };
    assert_eq!(
        bolt_parameter_to_runtime(&nanosecond_loss).expect_err("precision loss"),
        BoltValueError::SubMicrosecondPrecision
    );
    let local = Value::Structure {
        signature: 0x64,
        fields: vec![Value::Integer(1), Value::Integer(0)],
    };
    assert_eq!(
        bolt_parameter_to_runtime(&local).expect_err("local time is ambiguous"),
        BoltValueError::UnsupportedStructure(0x64)
    );
}
