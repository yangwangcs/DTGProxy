mod support;

use dtg_language_ir::{BinaryOperator, LogicalExpr, Value};
use dtg_query::{CancellationToken, Expression, QueryBudget, QueryRuntime, ResidualPredicate};
use dtg_storage::{CapabilityManifest, PushdownOutcome};

use support::{
    FixtureStore, block_on, point_plan, point_plan_with_residual, records, snapshot, storage_map,
    vertex,
};

#[test]
fn residual_filter_runs_after_backend_candidate_filter() {
    let capabilities = CapabilityManifest::from_names([
        support::CAP_VERTEX_POINT,
        support::CAP_TEMPORAL_EXACT,
        support::CAP_NULL_EXACT,
    ])
    .unwrap();
    let rejected = vertex(1, 10);
    let wanted = vertex(2, 20);
    let store = FixtureStore::new(
        capabilities.clone(),
        vec![rejected.clone(), wanted.clone()],
        Vec::new(),
    );
    store.with_pushdown_outcome(PushdownOutcome::ResidualRequired {
        rows: records(&[rejected, wanted.clone()]),
        guarantees: capabilities.clone(),
    });
    let mut stream = block_on(QueryRuntime::new(16).execute(
        &point_plan(capabilities, 2),
        storage_map(store.storage(true)),
        &snapshot(),
        QueryBudget::unlimited(),
        CancellationToken::new(),
        None,
    ))
    .unwrap();
    let rows = block_on(stream.collect()).unwrap();

    assert_eq!(rows.vertex_ids(), vec![wanted.id()]);
    assert_eq!(store.pushdown_calls(), 1);
}

#[test]
fn unsupported_provider_response_uses_only_the_bounded_logical_fallback() {
    let capabilities =
        CapabilityManifest::from_names([support::CAP_VERTEX_POINT, support::CAP_TEMPORAL_EXACT])
            .unwrap();
    let wanted = vertex(2, 20);
    let store = FixtureStore::new(capabilities.clone(), vec![wanted.clone()], Vec::new());
    store.with_pushdown_outcome(PushdownOutcome::Unsupported);
    let mut stream = block_on(QueryRuntime::new(16).execute(
        &point_plan(capabilities, 2),
        storage_map(store.storage(true)),
        &snapshot(),
        QueryBudget::unlimited(),
        CancellationToken::new(),
        None,
    ))
    .unwrap();

    assert_eq!(
        block_on(stream.collect()).unwrap().vertex_ids(),
        vec![wanted.id()]
    );
}

#[test]
fn provider_runtime_residual_cannot_be_downgraded_by_an_exact_plan() {
    let capabilities =
        CapabilityManifest::from_names(support::EXACT_VERTEX_POINT_CAPABILITIES).unwrap();
    let rejected = vertex(1, 10);
    let wanted = vertex(2, 20);
    let store = FixtureStore::new(
        capabilities.clone(),
        vec![rejected.clone(), wanted.clone()],
        Vec::new(),
    );
    store.with_pushdown_outcome(PushdownOutcome::ResidualRequired {
        rows: records(&[rejected, wanted.clone()]),
        guarantees: CapabilityManifest::from_names([support::CAP_TEMPORAL_EXACT]).unwrap(),
    });
    let mut stream = block_on(QueryRuntime::new(16).execute(
        &point_plan(capabilities, 2),
        storage_map(store.storage(true)),
        &snapshot(),
        QueryBudget::unlimited(),
        CancellationToken::new(),
        None,
    ))
    .unwrap();

    assert_eq!(
        block_on(stream.collect()).unwrap().vertex_ids(),
        vec![wanted.id()]
    );
}

#[test]
fn execution_expression_residual_filters_provider_candidates() {
    let capabilities =
        CapabilityManifest::from_names(support::EXACT_VERTEX_POINT_CAPABILITIES).unwrap();
    let rejected = vertex(1, 10);
    let wanted = vertex(2, 20);
    let store = FixtureStore::new(
        capabilities.clone(),
        vec![rejected.clone(), wanted.clone()],
        Vec::new(),
    );
    store.with_pushdown_outcome(PushdownOutcome::Exact(records(&[rejected, wanted.clone()])));
    let residual = ResidualPredicate::Expression(Expression::new(LogicalExpr::Binary {
        left: Box::new(LogicalExpr::Property {
            input: Box::new(LogicalExpr::Column("vertex".into())),
            name: "score".into(),
        }),
        operator: BinaryOperator::GreaterThan,
        right: Box::new(LogicalExpr::Literal(Value::Integer(15))),
    }));
    let mut stream = block_on(QueryRuntime::new(16).execute(
        &point_plan_with_residual(capabilities, 2, Some(residual)),
        storage_map(store.storage(true)),
        &snapshot(),
        QueryBudget::unlimited(),
        CancellationToken::new(),
        None,
    ))
    .unwrap();

    assert_eq!(
        block_on(stream.collect()).unwrap().vertex_ids(),
        vec![wanted.id()]
    );
}
