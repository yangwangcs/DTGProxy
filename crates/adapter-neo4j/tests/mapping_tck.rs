use std::time::{SystemTime, UNIX_EPOCH};

use adapter_neo4j::Neo4jAdapterFactory;
use storage_api::{MappingRequirement, run_mapping_restore_tck, run_mapping_tck};
use temporal_storage::run_temporal_graph_mapping_tck;

#[test]
fn live_neo4j_passes_shared_mapping_tck_and_restore() {
    let endpoint = std::env::var("DTGPROXY_NEO4J_ENDPOINT")
        .expect("DTGPROXY_NEO4J_ENDPOINT must point to a disposable Neo4j database");
    let password = std::env::var("DTGPROXY_NEO4J_PASSWORD")
        .expect("DTGPROXY_NEO4J_PASSWORD must authenticate to the disposable Neo4j database");
    let username = std::env::var("DTGPROXY_NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".into());
    let database = std::env::var("DTGPROXY_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".into());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_id = format!("mapping-tck-source-{suffix}");
    let destination_id = format!("mapping-tck-destination-{suffix}");
    let source =
        Neo4jAdapterFactory::open_mapping(&endpoint, &database, &username, &password, &source_id)
            .unwrap();
    source
        .describe_schema()
        .validate(MappingRequirement::HotPluggableReplica)
        .unwrap();
    source.validate_mapping().unwrap();
    let destination = Neo4jAdapterFactory::open_restore_mapping(
        &endpoint,
        &database,
        &username,
        &password,
        &destination_id,
    )
    .unwrap();

    run_mapping_tck(source.as_ref());
    run_mapping_restore_tck(source.as_ref(), destination.as_ref());
}

#[test]
fn live_neo4j_passes_temporal_graph_mapping_tck() {
    let endpoint = std::env::var("DTGPROXY_NEO4J_ENDPOINT")
        .expect("DTGPROXY_NEO4J_ENDPOINT must point to a disposable Neo4j database");
    let password = std::env::var("DTGPROXY_NEO4J_PASSWORD")
        .expect("DTGPROXY_NEO4J_PASSWORD must authenticate to the disposable Neo4j database");
    let username = std::env::var("DTGPROXY_NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".into());
    let database = std::env::var("DTGPROXY_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".into());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source = Neo4jAdapterFactory::open_mapping(
        &endpoint,
        &database,
        &username,
        &password,
        &format!("graph-tck-source-{suffix}"),
    )
    .unwrap();
    let destination = Neo4jAdapterFactory::open_restore_mapping(
        &endpoint,
        &database,
        &username,
        &password,
        &format!("graph-tck-destination-{suffix}"),
    )
    .unwrap();

    run_temporal_graph_mapping_tck(source, destination);
}
