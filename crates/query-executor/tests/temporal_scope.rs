use std::collections::BTreeMap;

use query_executor::{
    ChangeWindow, ExecutionContext, ResolvedValidTime, RuntimeError, RuntimeValue,
    resolve_change_scope, resolve_temporal_scope,
};
use temporal_ir::{ChangeAxis, ScalarExpr, TransactionTimeSpec, ValidTimeSpec};
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

#[test]
fn resolves_parameterized_change_window_without_falling_back_to_current_state() {
    let scope = resolve_change_scope(
        GraphId::new(7),
        ChangeAxis::ValidTime,
        &ScalarExpr::Parameter("from".into()),
        &ScalarExpr::Parameter("to".into()),
        &TransactionTimeSpec::AsOf(ScalarExpr::Parameter("snapshot".into())),
        TransactionTime::new(900, 1),
        &parameters(&[("from", 10), ("to", 20), ("snapshot", 30)]),
    )
    .expect("change scope");

    assert_eq!(
        scope.window(),
        ChangeWindow::Valid(
            temporal_types::Interval::new(
                ValidTime::from_micros(10),
                Some(ValidTime::from_micros(20)),
            )
            .unwrap()
        )
    );
    assert_eq!(scope.snapshot(), TransactionTime::new(30, u32::MAX));
}
