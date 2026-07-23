use std::collections::BTreeMap;

use query_executor::{
    ExecutionContext, ResolvedValidTime, RuntimeError, RuntimeValue, resolve_temporal_scope,
};
use temporal_ir::{ScalarExpr, TransactionTimeSpec, ValidTimeSpec};
use temporal_storage::GraphId;
use temporal_types::{TransactionTime, ValidTime};

fn parameters(values: &[(&str, i64)]) -> ExecutionContext {
    ExecutionContext::new(
        values
            .iter()
            .map(|(name, value)| ((*name).to_owned(), RuntimeValue::TimestampMicros(*value)))
            .collect::<BTreeMap<_, _>>(),
    )
}

#[test]
fn resolves_parameterized_point_scope_and_preserves_snapshot_fence() {
    let context = parameters(&[("valid", 101), ("tx", 202)]);
    let resolved = resolve_temporal_scope(
        GraphId::new(7),
        &ValidTimeSpec::AsOf(ScalarExpr::Parameter("valid".into())),
        &TransactionTimeSpec::AsOf(ScalarExpr::Parameter("tx".into())),
        ValidTime::from_micros(999),
        TransactionTime::new(888, 3),
        &context,
    )
    .expect("temporal parameters should resolve");

    assert_eq!(
        resolved.valid_time(),
        ResolvedValidTime::Point(ValidTime::from_micros(101))
    );
    assert_eq!(
        resolved.transaction_time(),
        TransactionTime::new(202, u32::MAX)
    );
}

#[test]
fn current_scope_uses_query_start_and_tso_snapshot() {
    let resolved = resolve_temporal_scope(
        GraphId::new(7),
        &ValidTimeSpec::Current,
        &TransactionTimeSpec::Current,
        ValidTime::from_micros(303),
        TransactionTime::new(404, 5),
        &ExecutionContext::default(),
    )
    .expect("current scope should resolve");

    assert_eq!(
        resolved.valid_time(),
        ResolvedValidTime::Point(ValidTime::from_micros(303))
    );
    assert_eq!(resolved.transaction_time(), TransactionTime::new(404, 5));
}

#[test]
fn rejects_reversed_intervals_and_non_temporal_parameters() {
    let reversed = resolve_temporal_scope(
        GraphId::new(7),
        &ValidTimeSpec::Between {
            start: ScalarExpr::Parameter("from".into()),
            end: ScalarExpr::Parameter("to".into()),
        },
        &TransactionTimeSpec::Current,
        ValidTime::from_micros(0),
        TransactionTime::new(1, 0),
        &parameters(&[("from", 20), ("to", 10)]),
    )
    .expect_err("reversed interval must fail");
    assert_eq!(reversed, RuntimeError::InvalidTemporalInterval);

    let wrong_type = ExecutionContext::new(BTreeMap::from([(
        "valid".to_owned(),
        RuntimeValue::String("tomorrow".into()),
    )]));
    let error = resolve_temporal_scope(
        GraphId::new(7),
        &ValidTimeSpec::AsOf(ScalarExpr::Parameter("valid".into())),
        &TransactionTimeSpec::Current,
        ValidTime::from_micros(0),
        TransactionTime::new(1, 0),
        &wrong_type,
    )
    .expect_err("string temporal parameter must fail");
    assert_eq!(error, RuntimeError::InvalidTemporalValue("STRING"));
}
