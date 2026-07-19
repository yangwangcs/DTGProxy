use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use bolt_protocol::Value;
use query_executor::v2::RuntimeValue;

const MAX_NESTING: usize = 64;
const UTC_DATETIME_OFFSET: u8 = 0x49;
const UTC_DATETIME_ZONE_ID: u8 = 0x69;

pub fn bolt_parameter_to_runtime(value: &Value) -> Result<RuntimeValue, BoltValueError> {
    from_bolt(value, 0)
}

pub fn runtime_value_to_bolt(value: &RuntimeValue) -> Result<Value, BoltValueError> {
    to_bolt(value, 0)
}

fn from_bolt(value: &Value, depth: usize) -> Result<RuntimeValue, BoltValueError> {
    if depth > MAX_NESTING {
        return Err(BoltValueError::NestingLimit);
    }
    match value {
        Value::Null => Ok(RuntimeValue::Null),
        Value::Boolean(value) => Ok(RuntimeValue::Boolean(*value)),
        Value::Integer(value) => Ok(RuntimeValue::Integer(*value)),
        Value::FloatBits(value) => Ok(RuntimeValue::FloatBits(*value)),
        Value::Bytes(value) => Ok(RuntimeValue::Bytes(value.clone())),
        Value::String(value) => Ok(RuntimeValue::String(value.clone())),
        Value::List(values) => values
            .iter()
            .map(|value| from_bolt(value, depth + 1))
            .collect::<Result<Vec<_>, _>>()
            .map(RuntimeValue::List),
        Value::Map(values) => {
            let mut converted = BTreeMap::new();
            for (key, value) in values {
                let property_id = stable_id(key);
                if converted
                    .insert(property_id, from_bolt(value, depth + 1)?)
                    .is_some()
                {
                    return Err(BoltValueError::DuplicatePropertyHash(property_id));
                }
            }
            Ok(RuntimeValue::Map(converted))
        }
        Value::Structure { signature, fields }
            if matches!(*signature, UTC_DATETIME_OFFSET | UTC_DATETIME_ZONE_ID) =>
        {
            datetime(*signature, fields)
        }
        Value::Structure { signature, .. } => Err(BoltValueError::UnsupportedStructure(*signature)),
    }
}

fn datetime(signature: u8, fields: &[Value]) -> Result<RuntimeValue, BoltValueError> {
    let [
        Value::Integer(seconds),
        Value::Integer(nanoseconds),
        timezone,
    ] = fields
    else {
        return Err(BoltValueError::InvalidTemporalStructure);
    };
    let timezone_is_valid = match (signature, timezone) {
        (UTC_DATETIME_OFFSET, Value::Integer(_)) => true,
        (UTC_DATETIME_ZONE_ID, Value::String(value)) => !value.is_empty(),
        _ => false,
    };
    if !timezone_is_valid || !(0..1_000_000_000).contains(nanoseconds) {
        return Err(BoltValueError::InvalidTemporalStructure);
    }
    if nanoseconds % 1_000 != 0 {
        return Err(BoltValueError::SubMicrosecondPrecision);
    }
    let micros = seconds
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(nanoseconds / 1_000))
        .ok_or(BoltValueError::TemporalOverflow)?;
    Ok(RuntimeValue::TimestampMicros(micros))
}

fn to_bolt(value: &RuntimeValue, depth: usize) -> Result<Value, BoltValueError> {
    if depth > MAX_NESTING {
        return Err(BoltValueError::NestingLimit);
    }
    match value {
        RuntimeValue::Null => Ok(Value::Null),
        RuntimeValue::Boolean(value) => Ok(Value::Boolean(*value)),
        RuntimeValue::Integer(value) => Ok(Value::Integer(*value)),
        RuntimeValue::FloatBits(value) => Ok(Value::FloatBits(*value)),
        RuntimeValue::String(value) => Ok(Value::String(value.clone())),
        RuntimeValue::Bytes(value) => Ok(Value::Bytes(value.clone())),
        RuntimeValue::TimestampMicros(micros) => {
            let seconds = micros.div_euclid(1_000_000);
            let nanoseconds = micros.rem_euclid(1_000_000) * 1_000;
            Ok(Value::Structure {
                signature: UTC_DATETIME_OFFSET,
                fields: vec![
                    Value::Integer(seconds),
                    Value::Integer(nanoseconds),
                    Value::Integer(0),
                ],
            })
        }
        RuntimeValue::List(values) => values
            .iter()
            .map(|value| to_bolt(value, depth + 1))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List),
        RuntimeValue::Map(values) => values
            .iter()
            .map(|(key, value)| Ok((key.to_string(), to_bolt(value, depth + 1)?)))
            .collect::<Result<BTreeMap<_, _>, BoltValueError>>()
            .map(Value::Map),
        RuntimeValue::Node(node) => {
            let properties = node
                .payload()
                .properties()
                .iter()
                .map(|(key, value)| {
                    Ok((key.to_string(), to_bolt(&value.clone().into(), depth + 1)?))
                })
                .collect::<Result<BTreeMap<_, _>, BoltValueError>>()?;
            Ok(Value::Map(BTreeMap::from([
                ("type".into(), Value::String("node".into())),
                (
                    "element_id".into(),
                    Value::String(node.element().id().value().to_string()),
                ),
                (
                    "label_id".into(),
                    node.label().map_or(Value::Null, |label| {
                        Value::Integer(i64::from(label.value()))
                    }),
                ),
                ("properties".into(), Value::Map(properties)),
            ])))
        }
        RuntimeValue::Relationship(relationship) => {
            let properties = relationship
                .payload()
                .properties()
                .iter()
                .map(|(key, value)| {
                    Ok((key.to_string(), to_bolt(&value.clone().into(), depth + 1)?))
                })
                .collect::<Result<BTreeMap<_, _>, BoltValueError>>()?;
            Ok(Value::Map(BTreeMap::from([
                ("type".into(), Value::String("relationship".into())),
                (
                    "element_id".into(),
                    Value::String(relationship.element().id().value().to_string()),
                ),
                (
                    "relationship_type_id".into(),
                    Value::Integer(i64::from(relationship.edge_type().value())),
                ),
                (
                    "source_id".into(),
                    Value::String(relationship.source().value().to_string()),
                ),
                (
                    "destination_id".into(),
                    Value::String(relationship.destination().value().to_string()),
                ),
                ("properties".into(), Value::Map(properties)),
            ])))
        }
    }
}

fn stable_id(value: &str) -> u32 {
    let digest = blake3::hash(value.as_bytes());
    u32::from_be_bytes(
        digest.as_bytes()[..4]
            .try_into()
            .expect("digest has four bytes"),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BoltValueError {
    UnsupportedStructure(u8),
    InvalidTemporalStructure,
    SubMicrosecondPrecision,
    TemporalOverflow,
    DuplicatePropertyHash(u32),
    NestingLimit,
}

impl Display for BoltValueError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "Bolt value conversion failed: {self:?}")
    }
}

impl Error for BoltValueError {}
