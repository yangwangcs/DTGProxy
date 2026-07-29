#![forbid(unsafe_code)]

use dtg_storage::LogicalReplicaActivation;
use dtg_storage_neo4j::{Neo4jReplicaStore, QueryApiContract};

fn assert_activation_contract<T: LogicalReplicaActivation>() {}

#[test]
fn query_api_v2_transaction_routes_and_affinity_are_explicit() {
    let contract = QueryApiContract::v2();
    assert_eq!(contract.database_path("neo4j"), "/db/neo4j/query/v2");
    assert_eq!(contract.begin_suffix(), "/tx");
    assert_eq!(contract.continue_path("42"), "/tx/42");
    assert_eq!(contract.commit_path("42"), "/tx/42/commit");
    assert_eq!(contract.rollback_path("42"), "/tx/42");
    assert_eq!(contract.affinity_header(), "neo4j-cluster-affinity");
    assert_eq!(contract.transaction_id_pointer(), "/transaction/id");
    assert_eq!(contract.rows_pointer(), "/data/values");
    assert_eq!(contract.max_response_bytes(), 64 * 1024 * 1024);
}

#[test]
fn query_api_requests_are_single_statement_and_parameterized() {
    let body = QueryApiContract::v2().request_body(
        "RETURN $value",
        serde_json::json!({
            "value": 7,
        }),
    );
    assert_eq!(body["statement"], "RETURN $value");
    assert_eq!(body["parameters"]["value"], 7);
    assert_eq!(body.as_object().unwrap().len(), 2);
}

#[test]
fn activation_uses_the_exact_provider_neutral_signature_and_one_query_api_statement() {
    assert_activation_contract::<Neo4jReplicaStore>();

    let source = include_str!("../src/snapshot.rs");
    assert!(source.contains("const ACTIVATE_CANDIDATE_QUERY"));
    assert!(source.contains(".execute(ACTIVATE_CANDIDATE_QUERY"));
    assert!(!source.contains("begin_transaction(ACTIVATE_CANDIDATE_QUERY"));
    assert!(!source.contains("format!(ACTIVATE_CANDIDATE_QUERY"));
}
