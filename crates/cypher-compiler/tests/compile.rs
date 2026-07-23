use cypher_compiler::{CompileSession, CypherCompiler};
use cypher_sema::QueryEffect;
use temporal_ir::{
    LanguageProfile, LogicalOperator, ScalarExpr, TransactionTimeSpec, ValidTimeSpec, ValueType,
};

fn session() -> CompileSession {
    CompileSession::new("accounts", 7, 3, 11).expect("session should be valid")
}

#[test]
fn compiles_temporal_match_into_valid_ir() {
    let compiled = CypherCompiler::new()
        .compile(
            "CYPHER 25 USE accounts \
             FOR VALID_TIME AS OF $valid \
             FOR SYSTEM_TIME AS OF $tx \
             MATCH (a:Account)-[r:TRANSFER]->(b:Account) \
             WHERE a.active = true RETURN a, r, b",
            &session(),
        )
        .expect("query should compile");

    compiled
        .logical_plan()
        .validate()
        .expect("compiled plan should validate");
    assert_eq!(
        compiled.logical_plan().header().language_profile(),
        LanguageProfile::Cypher25
    );
    assert_eq!(compiled.logical_plan().header().graph_id(), 7);
    assert_eq!(compiled.effect(), QueryEffect::ReadOnly);
    let operators = compiled
        .logical_plan()
        .nodes()
        .iter()
        .map(|node| node.operator())
        .collect::<Vec<_>>();
    assert!(operators.iter().any(|operator| matches!(
        operator,
        LogicalOperator::TemporalSlice { valid_time, transaction_time }
            if valid_time == &ValidTimeSpec::AsOf(ScalarExpr::Parameter("valid".into()))
                && transaction_time
                    == &TransactionTimeSpec::AsOf(ScalarExpr::Parameter("tx".into()))
    )));
    assert!(
        operators
            .iter()
            .any(|operator| matches!(operator, LogicalOperator::NodeScan { .. }))
    );
    assert!(
        operators
            .iter()
            .any(|operator| matches!(operator, LogicalOperator::Expand { .. }))
    );
    assert!(
        operators
            .iter()
            .any(|operator| matches!(operator, LogicalOperator::Filter { .. }))
    );
    assert_eq!(compiled.result_schema().columns().len(), 3);
}

#[test]
fn lowers_order_skip_and_limit_into_temporal_ir() {
    let session = CompileSession::new("accounts", 7, 3, 11).unwrap();
    let compiled = CypherCompiler::new()
        .compile("MATCH (n) RETURN n ORDER BY n SKIP 1 LIMIT 2", &session)
        .unwrap();
    let operators = compiled.logical_plan().nodes();
    assert!(
        operators
            .iter()
            .any(|node| matches!(node.operator(), temporal_ir::LogicalOperator::Sort { .. }))
    );
    assert!(
        operators
            .iter()
            .any(|node| matches!(node.operator(), temporal_ir::LogicalOperator::Skip { .. }))
    );
    assert!(
        operators
            .iter()
            .any(|node| matches!(node.operator(), temporal_ir::LogicalOperator::Limit { .. }))
    );
}

#[test]
fn preserves_interval_and_current_transaction_time_in_ir() {
    let compiled = CypherCompiler::new()
        .compile(
            "USE accounts FOR VALID_TIME BETWEEN $from AND $to MATCH (n) RETURN n",
            &session(),
        )
        .expect("interval query should compile");

    assert!(compiled.logical_plan().nodes().iter().any(|node| matches!(
        node.operator(),
        LogicalOperator::TemporalSlice { valid_time, transaction_time }
            if valid_time == &ValidTimeSpec::Between {
                start: ScalarExpr::Parameter("from".into()),
                end: ScalarExpr::Parameter("to".into()),
            } && transaction_time == &TransactionTimeSpec::Current
    )));
}

#[test]
fn rejects_a_query_for_another_graph() {
    let error = CypherCompiler::new()
        .compile("USE inventory MATCH (n) RETURN n", &session())
        .expect_err("graph mismatch must fail");

    assert_eq!(error.code(), "DTG-CYPHER-GRAPH-MISMATCH");
}

#[test]
fn lowers_unwind_into_a_typed_logical_operator() {
    let compiled = CypherCompiler::new()
        .compile("UNWIND [1, 2, 3] AS value RETURN value", &session())
        .expect("UNWIND should compile");
    assert!(
        compiled
            .logical_plan()
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Unwind { .. }))
    );
}

#[test]
fn lowers_union_all_into_a_two_input_logical_operator() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1] AS value RETURN value UNION ALL UNWIND [2] AS value RETURN value",
            &session(),
        )
        .expect("UNION ALL should compile");
    assert!(
        compiled
            .logical_plan()
            .nodes()
            .iter()
            .any(|node| { matches!(node.operator(), LogicalOperator::Union { all: true }) })
    );
}

#[test]
fn normalizes_union_branches_with_different_internal_slots() {
    let compiled = CypherCompiler::new()
        .compile(
            "RETURN 1 AS value UNION UNWIND [2] AS x RETURN x AS value",
            &session(),
        )
        .expect("legal UNION schemas must be normalized positionally");

    compiled
        .logical_plan()
        .validate()
        .expect("normalized UNION plan");
    assert!(
        compiled
            .logical_plan()
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Union { all: false }))
    );
}

#[test]
fn lowers_composite_aggregate_expressions_and_rejects_nested_aggregates() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1, 1, 2] AS value \
             WITH value, count(value) + 1 AS total RETURN value, total",
            &session(),
        )
        .expect("composite grouped aggregate should compile");
    assert!(
        compiled
            .logical_plan()
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Aggregate { .. }))
    );

    let error = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS value RETURN count(sum(value)) AS total",
            &session(),
        )
        .expect_err("nested aggregates must be rejected deterministically");
    assert_eq!(error.code(), "DTG-CYPHER-NESTED-AGGREGATE");
}

#[test]
fn lowers_every_boundary_in_a_mixed_union_chain_left_associatively() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1, 1] AS value RETURN value \
             UNION ALL UNWIND [1] AS value RETURN value \
             UNION UNWIND [1, 2] AS value RETURN value \
             UNION ALL UNWIND [2] AS value RETURN value",
            &session(),
        )
        .expect("mixed UNION chain should compile");
    let unions = compiled
        .logical_plan()
        .nodes()
        .iter()
        .filter_map(|node| match node.operator() {
            LogicalOperator::Union { all } => Some(*all),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(unions, vec![true, false, true]);
    compiled
        .logical_plan()
        .validate()
        .expect("mixed UNION plan should validate");
}

#[test]
fn lowers_distinct_grouped_with_and_attached_pipeline_operators() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [3, 1, 1, 2] AS value \
             WITH DISTINCT value AS grouped WHERE grouped > 0 \
             ORDER BY grouped SKIP 1 LIMIT 2 RETURN grouped",
            &session(),
        )
        .expect("complete WITH pipeline should compile");
    let nodes = compiled.logical_plan().nodes();

    assert!(nodes.iter().any(|node| matches!(
        node.operator(),
        LogicalOperator::Aggregate { grouping, aggregates }
            if !grouping.is_empty() && aggregates.is_empty()
    )));
    assert!(
        nodes
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Filter { .. }))
    );
    assert!(
        nodes
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Sort { .. }))
    );
    assert!(
        nodes
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Skip { .. }))
    );
    assert!(
        nodes
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Limit { .. }))
    );
}

#[test]
fn lowers_union_chain_inside_a_read_subquery() {
    let compiled = CypherCompiler::new()
        .compile(
            "CALL () { RETURN 1 AS value UNION ALL RETURN 2 AS value UNION RETURN 2 AS value } \
             RETURN value",
            &session(),
        )
        .expect("UNION chain in a read subquery should compile");

    assert_eq!(
        compiled
            .logical_plan()
            .nodes()
            .iter()
            .find_map(|node| match node.operator() {
                LogicalOperator::Apply { apply } => Some(apply.child_plan()),
                _ => None,
            })
            .expect("isolated read child plan")
            .nodes()
            .iter()
            .filter(|node| matches!(node.operator(), LogicalOperator::Union { .. }))
            .count(),
        2
    );
}

#[test]
fn lowers_call_yield_bindings_into_the_following_projection() {
    let compiled = CypherCompiler::new()
        .compile(
            "CALL dtg.graph.degree() YIELD degree AS score RETURN score",
            &session(),
        )
        .expect("YIELD aliases should be visible to the following clause");

    assert!(
        compiled
            .logical_plan()
            .nodes()
            .iter()
            .any(|node| { matches!(node.operator(), LogicalOperator::ProcedureCall { .. }) })
    );
    assert_eq!(compiled.result_schema().columns()[0].name(), "score");
}

#[test]
fn lowers_correlated_procedure_call_with_input_slots_and_output_schema() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS source \
             CALL dtg.graph.bfs(source) YIELD vertexId, distance \
             RETURN source, vertexId, distance",
            &session(),
        )
        .expect("correlated procedure should compile");
    let procedure = compiled
        .logical_plan()
        .nodes()
        .iter()
        .find_map(|node| match node.operator() {
            LogicalOperator::ProcedureCall { procedure } => Some((node, procedure)),
            _ => None,
        })
        .expect("resolved procedure node");

    assert_eq!(
        procedure.1.identity().catalog_revision(),
        compiled.catalog_revision()
    );
    assert_eq!(procedure.1.name(), "dtg.graph.bfs");
    assert!(matches!(
        procedure.1.arguments(),
        [argument] if argument.name() == "source"
            && matches!(argument.expression(), ScalarExpr::Slot(_))
    ));
    assert_eq!(
        procedure
            .1
            .provider_output()
            .columns()
            .iter()
            .map(|column| (column.name(), column.value_type(), column.nullable()))
            .collect::<Vec<_>>(),
        vec![
            ("vertexId", &ValueType::String, false),
            ("distance", &ValueType::Integer, false),
            ("predecessor", &ValueType::String, true),
        ]
    );
    assert_eq!(procedure.1.yields().len(), 2);
    assert_eq!(procedure.0.output().columns().len(), 3);
}

#[test]
fn lowers_a_parameter_inside_a_procedure_map_argument() {
    let compiled = CypherCompiler::new()
        .compile(
            "CALL dtg.graph.pageRank({damping: $damping}) YIELD score RETURN score",
            &session(),
        )
        .expect("a parameter inside a procedure map should compile");
    let procedure = compiled
        .logical_plan()
        .nodes()
        .iter()
        .find_map(|node| match node.operator() {
            LogicalOperator::ProcedureCall { procedure } => Some(procedure),
            _ => None,
        })
        .expect("resolved procedure node");

    assert!(matches!(
        procedure.arguments(),
        [argument]
            if argument.name() == "damping"
                && argument.expression() == &ScalarExpr::Parameter("damping".into())
    ));
}

#[test]
fn rejects_mixed_write_and_procedure_statements_before_planning() {
    for query in [
        "CREATE (:MustNotCommitBefore) CALL dtg.graph.degree({}) YIELD degree RETURN degree",
        "CALL dtg.graph.degree({}) YIELD degree CREATE (:MustNotCommitAfter)",
    ] {
        let error = CypherCompiler::new()
            .compile(query, &session())
            .expect_err("1.0 has no staged write/procedure statement pipeline");
        assert_eq!(
            error.code(),
            "DTG-CYPHER-WRITE-PROCEDURE-UNSUPPORTED",
            "{query}"
        );
    }
}

#[test]
fn lowers_a_read_subquery_before_the_outer_projection() {
    let compiled = CypherCompiler::new()
        .compile(
            "CALL () { UNWIND [1, 2] AS value RETURN value } RETURN value",
            &session(),
        )
        .expect("read subquery should compile");

    assert!(compiled.is_read_only());
    assert!(
        compiled
            .logical_plan()
            .nodes()
            .iter()
            .find_map(|node| match node.operator() {
                LogicalOperator::Apply { apply } => Some(apply.child_plan()),
                _ => None,
            })
            .expect("isolated read child plan")
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Unwind { .. }))
    );
    assert_eq!(compiled.result_schema().columns()[0].name(), "value");
}

#[test]
fn lowers_a_correlated_read_subquery_with_the_outer_scope() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS value CALL (value) { RETURN value AS copy } RETURN copy",
            &session(),
        )
        .expect("correlated read subquery should compile");

    assert!(compiled.is_read_only());
    assert_eq!(compiled.result_schema().columns()[0].name(), "copy");
}

#[test]
fn lowers_correlated_apply_with_separate_parent_child_schemas() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS value \
             CALL (value) { RETURN value AS copy } \
             RETURN value, copy",
            &session(),
        )
        .expect("correlated CALL should compile to isolated Apply");
    let (node, apply) = compiled
        .logical_plan()
        .nodes()
        .iter()
        .find_map(|node| match node.operator() {
            LogicalOperator::Apply { apply } => Some((node, apply)),
            _ => None,
        })
        .expect("Apply node");

    assert!(matches!(apply.kind(), temporal_ir::ApplyKind::Inner));
    assert_ne!(
        apply.child_plan().header().query_fingerprint(),
        compiled.logical_plan().header().query_fingerprint()
    );
    assert!(matches!(
        apply.child_plan().nodes()[0].operator(),
        LogicalOperator::Argument
    ));
    assert_eq!(apply.imports().len(), 1);
    assert_eq!(apply.exports().len(), 1);
    assert_eq!(
        apply.child_plan().nodes()[0].output().columns()[0].slot(),
        apply.imports()[0].child_slot()
    );
    assert!(node.output().columns().iter().any(|column| {
        column.name() == "copy" && column.slot() == apply.exports()[0].parent_slot()
    }));
    assert_eq!(
        apply.child_plan().output().columns()[0].slot(),
        apply.exports()[0].child_slot()
    );
}

#[test]
fn lowers_exists_to_semi_apply_and_count_to_scalar_aggregate() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS value \
             RETURN EXISTS { RETURN value AS visible } AS present, \
                    COUNT { UNWIND [value] AS item RETURN item } AS total",
            &session(),
        )
        .expect("EXISTS and COUNT should compile to isolated scalar Apply nodes");
    let apply_kinds = compiled
        .logical_plan()
        .nodes()
        .iter()
        .filter_map(|node| match node.operator() {
            LogicalOperator::Apply { apply } => Some(apply.kind()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert!(matches!(
        apply_kinds[0],
        temporal_ir::ApplyKind::Exists { .. }
    ));
    assert!(matches!(
        apply_kinds[1],
        temporal_ir::ApplyKind::Count { .. }
    ));
    assert_eq!(compiled.result_schema().columns()[0].name(), "present");
    assert_eq!(
        compiled.result_schema().columns()[0].value_type(),
        &ValueType::Boolean
    );
    assert!(!compiled.result_schema().columns()[0].nullable());
    assert_eq!(compiled.result_schema().columns()[1].name(), "total");
    assert_eq!(
        compiled.result_schema().columns()[1].value_type(),
        &ValueType::Integer
    );
    assert!(!compiled.result_schema().columns()[1].nullable());
}

#[test]
fn lowers_in_transactions_to_a_batch_subtransaction_operator() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS value \
             CALL (value) { CREATE (n:Item {id: value}) } IN TRANSACTIONS OF 1 ROWS",
            &session(),
        )
        .expect("IN TRANSACTIONS must lower to an explicit batch boundary");

    assert!(compiled.logical_plan().nodes().iter().any(|node| matches!(
        node.operator(),
        LogicalOperator::BatchSubtransaction { batch } if batch.batch_rows() == 1
    )));
    let [cypher_compiler::CompiledMutation::Subquery(subquery)] =
        compiled.mutation_plan().mutations()
    else {
        panic!("batch child write must remain a structured mutation");
    };
    assert_eq!(subquery.batch_rows(), Some(1));
}

#[test]
fn extracts_match_with_prefix_using_the_actual_alias_schema() {
    let compiled = CypherCompiler::new()
        .compile(
            "MATCH (n:Account) WITH n AS account SET account.active = true",
            &session(),
        )
        .expect("write should compile");

    let prefix = compiled.read_prefix_plan().expect("read prefix");
    prefix.validate().expect("prefix should validate");
    assert_eq!(prefix.output().columns().len(), 1);
    assert_eq!(prefix.output().columns()[0].name(), "account");
    assert!(
        prefix
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Project { .. }))
    );
    assert!(prefix.nodes().iter().all(|node| !matches!(
        node.operator(),
        LogicalOperator::Set
            | LogicalOperator::Create
            | LogicalOperator::Merge
            | LogicalOperator::Remove
            | LogicalOperator::Delete { .. }
    )));
}

#[test]
fn extracts_unwind_prefix_without_collapsing_its_rows() {
    let compiled = CypherCompiler::new()
        .compile("UNWIND [1, 2, 3] AS item CREATE (n:Account)", &session())
        .expect("write should compile");

    let prefix = compiled.read_prefix_plan().expect("read prefix");
    prefix.validate().expect("prefix should validate");
    assert_eq!(prefix.output().columns()[0].name(), "item");
    assert!(
        prefix
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Unwind { .. }))
    );
}

#[test]
fn extracts_standalone_merge_as_a_structured_match_prefix() {
    let compiled = CypherCompiler::new()
        .compile(
            "USE accounts FOR VALID_TIME AS OF 1000 MERGE (account:Account {id: 7})",
            &session(),
        )
        .expect("MERGE should compile");

    let prefix = compiled.read_prefix_plan().expect("MERGE match prefix");
    prefix.validate().expect("prefix should validate");
    assert_eq!(prefix.output().columns()[0].name(), "account");
    assert!(
        prefix
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::NodeScan { .. }))
    );
    assert!(
        prefix
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Filter { .. }))
    );
    assert!(prefix.nodes().iter().all(|node| !matches!(
        node.operator(),
        LogicalOperator::Create
            | LogicalOperator::Merge
            | LogicalOperator::Set
            | LogicalOperator::Remove
            | LogicalOperator::Delete { .. }
    )));
}
