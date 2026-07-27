use paper_benchmark::{
    AblationConfig, AblationCounters, AblationEvidence, AdapterDirectRunner, AdapterPrimitive,
    AdapterSnapshotRequest, Backend, BackendDirectRunner, BackendNativeRequest, BenchmarkRequest,
    DatasetManifest, ExperimentPath, HostResourceEvidence, PathError, PathExecution,
    ProcessTopologyEvidence, ProxyLoadgenReport, ProxyLoadgenRequest, ProxyRunner, RawObservation,
    ResourceMetric, ResourceScope, ResultIdentity, RunManifest, RunMode, SCHEMA_VERSION,
    TopologyDeploymentMode, TopologyEvidence, WorkloadManifest, ablation_config_for_label,
    summarize_formal_repetitions, summarize_repetitions, summarize_samples,
    validate_comparable_identities, validate_report_contract,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use storage_api::{AdapterFuture, KeySpan, KeyValue, LogicalKey, ReadSnapshot};

fn valid_observation_value(path: &str) -> Value {
    json!({
        "schema_version": 1,
        "run_id": "2026-07-26-formal",
        "path": path,
        "backend": "rocksdb",
        "workload": "point_lookup",
        "dataset_digest": "1111111111111111111111111111111111111111111111111111111111111111",
        "workload_digest": "2222222222222222222222222222222222222222222222222222222222222222",
        "snapshot": "as_of:100",
        "parameters_digest": "3333333333333333333333333333333333333333333333333333333333333333",
        "topology": {"data_nodes": 1},
        "concurrency": 8,
        "ablation": "production",
        "repetition": 1,
        "timing": {
            "warmup_started_unix_ns": 100,
            "measurement_started_unix_ns": 200,
            "measurement_ended_unix_ns": 300
        },
        "operations": 2,
        "errors": 0,
        "samples": {
            "latency_ns": [10, 20],
            "ttfr_ns": [4, 8]
        },
        "resources": {
            "cpu_time_ns": {"status": "observed", "value": 50},
            "peak_rss_bytes": {"status": "observed", "value": 4096},
            "network_rx_bytes": {"status": "observed", "value": 128},
            "network_tx_bytes": {"status": "observed", "value": 64}
        },
        "resource_scope": if path == "proxy" {
            "client_and_proxy_processes"
        } else {
            "client_process"
        },
        "identity": {
            "digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "row_count": 2
        },
        "configuration_digest": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        ,"ablation_evidence": {
            "gateway_pid": 42,
            "cell_id": "2026-07-26-formal:1",
            "configuration_digest": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "config": {
                "native_pushdown": true,
                "column_batches": true,
                "bounded_lazy_pages": true,
                "parallel_shard_fanout": true,
                "batched_property_gather": true
            },
            "total_operations": 2,
            "queries_started": 2,
            "queries_completed": 2,
            "queries_failed": 0,
            "queries_in_flight": 0,
            "counters": {
                "canonical_residual_scans": 0,
                "row_column_conversion_boundaries": 0,
                "eager_page_collections": 0,
                "serial_shard_opens": 0,
                "singleton_property_gather_reads": 0
            }
        }
    })
}

fn valid_observation(path: &str) -> RawObservation {
    let mut value = valid_observation_value(path);
    if path != "proxy" {
        value.as_object_mut().unwrap().remove("ablation_evidence");
    }
    serde_json::from_value(value).expect("valid observation schema")
}

fn remote_topology_evidence(observation: &RawObservation) -> TopologyEvidence {
    let nodes = observation.topology.data_nodes;
    let cpu_per_node = 50 / u64::from(nodes);
    let rss_per_node = 4096 / u64::from(nodes);
    let rx_per_node = 128 / u64::from(nodes);
    let tx_per_node = 64 / u64::from(nodes);
    let mut cpu_remainder = 50 % u64::from(nodes);
    let mut rss_remainder = 4096 % u64::from(nodes);
    let mut rx_remainder = 128 % u64::from(nodes);
    let mut tx_remainder = 64 % u64::from(nodes);
    let mut data_nodes = Vec::new();
    let mut resource_samples = Vec::new();
    for index in 0..nodes {
        let host_id = format!("host-{index}");
        let boot_id = format!("boot-{index}");
        let process_start_id = format!("start-{index}");
        let pid = 1000 + index;
        data_nodes.push(ProcessTopologyEvidence {
            host_id: host_id.clone(),
            boot_id: boot_id.clone(),
            process_start_id: process_start_id.clone(),
            pid,
            executable: format!("/opt/dtgproxy/data-node-{index}").into(),
            executable_sha256: digest(char::from_digit(index + 1, 10).unwrap_or('a')),
            listen_address: format!("10.0.0.{}:{}", index + 10, 7000 + index),
            data_interface: "eth0".into(),
            management_interface: "eth1".into(),
        });
        resource_samples.push(HostResourceEvidence {
            host_id,
            boot_id,
            process_start_id,
            pid,
            sampled_before_unix_ns: observation.timing.warmup_started_unix_ns - 1,
            sampled_after_unix_ns: observation.timing.measurement_ended_unix_ns + 1,
            cpu_time_ns: cpu_per_node + u64::from(take_remainder(&mut cpu_remainder)),
            peak_rss_bytes: rss_per_node + u64::from(take_remainder(&mut rss_remainder)),
            network_rx_bytes: rx_per_node + u64::from(take_remainder(&mut rx_remainder)),
            network_tx_bytes: tx_per_node + u64::from(take_remainder(&mut tx_remainder)),
        });
    }
    TopologyEvidence {
        deployment_mode: TopologyDeploymentMode::RemoteFormal,
        data_nodes,
        resource_samples,
    }
}

fn attach_formal_remote_evidence(observation: &mut RawObservation) {
    observation.resource_scope = ResourceScope::DataNodeProcesses;
    observation.topology_evidence = Some(remote_topology_evidence(observation));
}

fn take_remainder(remainder: &mut u64) -> u8 {
    if *remainder == 0 {
        0
    } else {
        *remainder -= 1;
        1
    }
}

#[test]
fn ablation_labels_map_to_exact_single_axis_configs() {
    let production = AblationConfig {
        native_pushdown: true,
        column_batches: true,
        bounded_lazy_pages: true,
        parallel_shard_fanout: true,
        batched_property_gather: true,
    };
    for (label, expected) in [
        ("production", production),
        (
            "no_native_pushdown",
            AblationConfig {
                native_pushdown: false,
                ..production
            },
        ),
        (
            "no_column_batch",
            AblationConfig {
                column_batches: false,
                ..production
            },
        ),
        (
            "no_lazy_pages",
            AblationConfig {
                bounded_lazy_pages: false,
                ..production
            },
        ),
        (
            "no_parallel_fanout",
            AblationConfig {
                parallel_shard_fanout: false,
                ..production
            },
        ),
        (
            "no_batched_gather",
            AblationConfig {
                batched_property_gather: false,
                ..production
            },
        ),
    ] {
        assert_eq!(
            ablation_config_for_label(label).unwrap(),
            expected,
            "{label}"
        );
    }
    assert!(ablation_config_for_label("unknown").is_err());
}

#[test]
fn observation_requires_gateway_evidence_only_for_proxy() {
    let mut proxy = valid_observation("proxy");
    proxy.validate().unwrap();
    proxy.ablation_evidence = None;
    assert!(proxy.validate().is_err());

    let mut direct = valid_observation("backend_direct");
    direct.ablation_evidence = Some(AblationEvidence {
        gateway_pid: 42,
        cell_id: "run:1".into(),
        configuration_digest: direct.configuration_digest.clone(),
        config: ablation_config_for_label("production").unwrap(),
        total_operations: direct.operations,
        queries_started: direct.operations,
        queries_completed: direct.operations,
        queries_failed: 0,
        queries_in_flight: 0,
        counters: AblationCounters::default(),
    });
    assert!(direct.validate().is_err());
}

#[test]
fn proxy_evidence_validates_digest_config_counts_and_disabled_counter() {
    let mut observation = valid_observation("proxy");
    observation.ablation = "no_native_pushdown".into();
    let evidence = observation.ablation_evidence.as_mut().unwrap();
    evidence.config = ablation_config_for_label(&observation.ablation).unwrap();
    evidence.counters.canonical_residual_scans = 1;
    observation.validate().unwrap();

    for mutation in [
        |e: &mut AblationEvidence| e.configuration_digest = digest('9'),
        |e: &mut AblationEvidence| e.queries_completed += 1,
        |e: &mut AblationEvidence| e.counters.canonical_residual_scans = 0,
        |e: &mut AblationEvidence| e.counters.serial_shard_opens = 1,
    ] {
        let mut invalid = observation.clone();
        mutation(invalid.ablation_evidence.as_mut().unwrap());
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn production_evidence_rejects_any_ablation_counter() {
    let mut observation = valid_observation("proxy");
    observation
        .ablation_evidence
        .as_mut()
        .unwrap()
        .counters
        .eager_page_collections = 1;
    assert!(observation.validate().is_err());
}

fn digest(character: char) -> String {
    std::iter::repeat_n(character, 64).collect()
}

fn valid_dataset_manifest() -> DatasetManifest {
    DatasetManifest {
        schema_version: SCHEMA_VERSION,
        dataset_id: "paper-1m-5m".into(),
        seed: 42,
        vertex_count: 1_000_000,
        edge_count: 5_000_000,
        temporal_update_count: 600_000,
        content_digest: digest('1'),
    }
}

fn valid_workload_manifest() -> WorkloadManifest {
    let mut workload = WorkloadManifest {
        schema_version: SCHEMA_VERSION,
        workload_id: "point_lookup".into(),
        query: "MATCH (n {id: $id}) RETURN n.name".into(),
        parameters: BTreeMap::from([("id".into(), json!(7))]),
        available_paths: vec![
            ExperimentPath::BackendDirect,
            ExperimentPath::AdapterDirect,
            ExperimentPath::Proxy,
        ],
        digest: String::new(),
    };
    workload.digest = workload.computed_digest().expect("workload digest");
    workload
}

fn valid_run_manifest(dataset: &DatasetManifest, workload: &WorkloadManifest) -> RunManifest {
    let mut run = RunManifest {
        schema_version: SCHEMA_VERSION,
        mode: RunMode::Formal,
        selected_backend: Backend::Rocksdb,
        run_id: "2026-07-26-formal".into(),
        revision: "0123456789abcdef".into(),
        dirty_worktree_digest: digest('3'),
        dataset_digest: dataset.content_digest.clone(),
        workload_digest: workload.digest.clone(),
        environment_digest: digest('4'),
        configuration_digest: String::new(),
        warmup_seconds: 30,
        measurement_seconds: 60,
        repetitions: 5,
    };
    run.configuration_digest = run
        .computed_configuration_digest()
        .expect("configuration digest");
    run
}

fn valid_associated_observation_value(path: &str) -> Value {
    let mut value = valid_observation_value(path);
    let object = value.as_object_mut().expect("observation object");
    if path != "proxy" {
        object.remove("ablation_evidence");
    }
    object.insert("run_id".into(), json!("2026-07-26-formal"));
    object.insert("dataset_digest".into(), json!(digest('1')));
    object.insert("workload_digest".into(), json!(digest('2')));
    object.insert("snapshot".into(), json!("as_of:100"));
    object.insert("parameters_digest".into(), json!(digest('3')));
    value
}

fn valid_contract_observation(
    path: &str,
    repetition: u32,
    run: &RunManifest,
    dataset: &DatasetManifest,
    workload: &WorkloadManifest,
) -> RawObservation {
    let mut observation = valid_observation(path);
    observation.run_id = run.run_id.clone();
    observation.dataset_digest = dataset.content_digest.clone();
    observation.workload_digest = workload.digest.clone();
    observation.workload = workload.workload_id.clone();
    observation.configuration_digest = run.configuration_digest.clone();
    if let Some(evidence) = &mut observation.ablation_evidence {
        evidence.configuration_digest = run.configuration_digest.clone();
        evidence.total_operations = observation.operations;
        evidence.queries_started = observation.operations;
        evidence.queries_completed = observation.operations;
    }
    observation.repetition = repetition;
    observation.timing.warmup_started_unix_ns = 1_000;
    observation.timing.measurement_started_unix_ns = 30_000_001_000;
    observation.timing.measurement_ended_unix_ns = 90_000_001_000;
    observation
}

fn valid_formal_contract_observation(
    path: &str,
    repetition: u32,
    run: &RunManifest,
    dataset: &DatasetManifest,
    workload: &WorkloadManifest,
) -> RawObservation {
    let mut observation = valid_contract_observation(path, repetition, run, dataset, workload);
    if observation.path == ExperimentPath::Proxy {
        attach_formal_remote_evidence(&mut observation);
    }
    observation
}

fn complete_formal_cell(
    run: &RunManifest,
    dataset: &DatasetManifest,
    workload: &WorkloadManifest,
) -> Vec<RawObservation> {
    let mut observations = Vec::new();
    for repetition in 1..=5 {
        for path in ["backend_direct", "adapter_direct", "proxy"] {
            observations.push(valid_formal_contract_observation(
                path, repetition, run, dataset, workload,
            ));
        }
    }
    observations
}

#[test]
fn formal_proxy_only_workload_requires_only_proxy_repetitions() {
    let dataset = valid_dataset_manifest();
    let mut workload = valid_workload_manifest();
    workload.available_paths = vec![ExperimentPath::Proxy];
    workload.digest = workload.computed_digest().unwrap();
    let run = valid_run_manifest(&dataset, &workload);
    let observations = (1..=5)
        .map(|repetition| {
            valid_formal_contract_observation("proxy", repetition, &run, &dataset, &workload)
        })
        .collect::<Vec<_>>();
    validate_report_contract(&run, &dataset, &workload, &observations).unwrap();
}

#[test]
fn observation_requires_every_report_identity_and_measurement_field() {
    for field in [
        "run_id",
        "path",
        "backend",
        "workload",
        "dataset_digest",
        "workload_digest",
        "snapshot",
        "parameters_digest",
        "repetition",
        "timing",
        "identity",
        "samples",
        "resources",
        "configuration_digest",
    ] {
        let mut value = valid_observation_value("proxy");
        value.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<RawObservation>(value).is_err(),
            "missing {field} must be rejected"
        );
    }
    let mut missing_evidence = valid_observation_value("proxy");
    missing_evidence
        .as_object_mut()
        .unwrap()
        .remove("ablation_evidence");
    let missing_evidence: RawObservation = serde_json::from_value(missing_evidence).unwrap();
    assert!(missing_evidence.validate().is_err());
}

#[test]
fn observation_validation_rejects_invalid_boundaries_identity_and_zero_operations() {
    let mut zero_operations = valid_observation("proxy");
    zero_operations.operations = 0;
    assert!(zero_operations.validate().is_err());

    let mut reversed_timing = valid_observation("proxy");
    reversed_timing.timing.measurement_started_unix_ns = 301;
    assert!(reversed_timing.validate().is_err());

    let mut empty_digest = valid_observation("proxy");
    empty_digest.identity.digest.clear();
    assert!(empty_digest.validate().is_err());

    let mut empty_configuration_digest = valid_observation("proxy");
    empty_configuration_digest.configuration_digest.clear();
    assert!(empty_configuration_digest.validate().is_err());

    let mut empty_run_id = valid_observation("proxy");
    empty_run_id.run_id.clear();
    assert!(empty_run_id.validate().is_err());

    let mut invalid_dataset_digest = valid_observation("proxy");
    invalid_dataset_digest.dataset_digest = "not-a-digest".into();
    assert!(invalid_dataset_digest.validate().is_err());

    let mut invalid_workload_digest = valid_observation("proxy");
    invalid_workload_digest.workload_digest = "not-a-digest".into();
    assert!(invalid_workload_digest.validate().is_err());

    let mut empty_snapshot = valid_observation("proxy");
    empty_snapshot.snapshot.clear();
    assert!(empty_snapshot.validate().is_err());

    let mut invalid_parameters_digest = valid_observation("proxy");
    invalid_parameters_digest.parameters_digest = "not-a-digest".into();
    assert!(invalid_parameters_digest.validate().is_err());
}

#[test]
fn compared_paths_must_have_the_same_result_identity() {
    let backend = valid_observation("backend_direct");
    let adapter = valid_observation("adapter_direct");
    let proxy = valid_observation("proxy");
    validate_comparable_identities([&backend, &adapter, &proxy]).unwrap();

    let mut drifted = valid_observation("proxy");
    drifted.identity.row_count += 1;
    assert!(validate_comparable_identities([&backend, &adapter, &drifted]).is_err());

    let mut drifted = valid_observation("proxy");
    drifted.identity.digest = digest('f');
    assert!(validate_comparable_identities([&backend, &adapter, &drifted]).is_err());
}

#[test]
fn comparison_requires_one_complete_matching_three_path_cell() {
    let backend = valid_observation("backend_direct");
    let adapter = valid_observation("adapter_direct");
    let proxy = valid_observation("proxy");

    validate_comparable_identities([&backend, &adapter, &proxy]).unwrap();
    assert!(validate_comparable_identities([&backend, &adapter]).is_err());
    assert!(validate_comparable_identities([&backend, &backend, &proxy]).is_err());

    let mut cross_repetition = proxy.clone();
    cross_repetition.repetition = 2;
    assert!(validate_comparable_identities([&backend, &adapter, &cross_repetition]).is_err());

    let mut cross_configuration = proxy;
    cross_configuration.configuration_digest = digest('e');
    assert!(validate_comparable_identities([&backend, &adapter, &cross_configuration]).is_err());

    let mut cross_run = valid_observation("proxy");
    cross_run.run_id = "another-run".into();
    assert!(validate_comparable_identities([&backend, &adapter, &cross_run]).is_err());

    let mut cross_backend = valid_observation("proxy");
    cross_backend.backend = Backend::Neo4j;
    assert!(validate_comparable_identities([&backend, &adapter, &cross_backend]).is_err());

    let mut cross_workload = valid_observation("proxy");
    cross_workload.workload = "range_lookup".into();
    assert!(validate_comparable_identities([&backend, &adapter, &cross_workload]).is_err());

    let mut cross_dataset = valid_observation("proxy");
    cross_dataset.dataset_digest = digest('a');
    assert!(validate_comparable_identities([&backend, &adapter, &cross_dataset]).is_err());

    let mut cross_workload_digest = valid_observation("proxy");
    cross_workload_digest.workload_digest = digest('b');
    assert!(validate_comparable_identities([&backend, &adapter, &cross_workload_digest]).is_err());

    let mut cross_topology = valid_observation("proxy");
    cross_topology.topology.data_nodes = 2;
    assert!(validate_comparable_identities([&backend, &adapter, &cross_topology]).is_err());

    let mut cross_concurrency = valid_observation("proxy");
    cross_concurrency.concurrency = 9;
    assert!(validate_comparable_identities([&backend, &adapter, &cross_concurrency]).is_err());

    let mut cross_ablation = valid_observation("proxy");
    cross_ablation.ablation = "disabled-cache".into();
    assert!(validate_comparable_identities([&backend, &adapter, &cross_ablation]).is_err());

    let mut cross_snapshot = valid_observation("proxy");
    cross_snapshot.snapshot = "as_of:101".into();
    assert!(validate_comparable_identities([&backend, &adapter, &cross_snapshot]).is_err());

    let mut cross_parameters = valid_observation("proxy");
    cross_parameters.parameters_digest = digest('c');
    assert!(validate_comparable_identities([&backend, &adapter, &cross_parameters]).is_err());
}

#[test]
fn comparison_requires_matching_run_associations_except_path() {
    let backend = serde_json::from_value::<RawObservation>(valid_associated_observation_value(
        "backend_direct",
    ))
    .expect("associated observation schema");
    let adapter = serde_json::from_value::<RawObservation>(valid_associated_observation_value(
        "adapter_direct",
    ))
    .expect("associated observation schema");
    let proxy =
        serde_json::from_value::<RawObservation>(valid_associated_observation_value("proxy"))
            .expect("associated observation schema");

    validate_comparable_identities([&backend, &adapter, &proxy]).unwrap();

    let mut missing_digest = valid_associated_observation_value("proxy");
    missing_digest
        .as_object_mut()
        .expect("observation object")
        .remove("parameters_digest");
    assert!(serde_json::from_value::<RawObservation>(missing_digest).is_err());

    let mut cross_snapshot = valid_associated_observation_value("proxy");
    cross_snapshot
        .as_object_mut()
        .expect("observation object")
        .insert("snapshot".into(), json!("as_of:101"));
    let cross_snapshot = serde_json::from_value::<RawObservation>(cross_snapshot)
        .expect("associated observation schema");
    assert!(validate_comparable_identities([&backend, &adapter, &cross_snapshot]).is_err());
}

#[test]
fn observation_requires_one_latency_and_ttfr_pair_per_operation() {
    let mut too_few_pairs = valid_observation("proxy");
    too_few_pairs.operations = 3;
    assert!(too_few_pairs.validate().is_err());

    let mut ttfr_after_latency = valid_observation("proxy");
    ttfr_after_latency.samples.ttfr_ns[1] = 21;
    assert!(ttfr_after_latency.validate().is_err());
}

#[test]
fn resource_metrics_use_explicit_observed_or_reasoned_unavailable_values() {
    let mut value = valid_associated_observation_value("proxy");
    value.as_object_mut().expect("observation object").insert(
        "resources".into(),
        json!({
            "cpu_time_ns": {"status": "observed", "value": 0},
            "peak_rss_bytes": {"status": "unavailable", "reason": "runtime does not expose RSS"},
            "network_rx_bytes": {"status": "observed", "value": 128},
            "network_tx_bytes": {"status": "unavailable", "reason": "host counter is disabled"}
        }),
    );
    let observation =
        serde_json::from_value::<RawObservation>(value).expect("explicit resource metric schema");
    observation.validate().unwrap();

    let mut blank_reason = valid_associated_observation_value("proxy");
    blank_reason
        .as_object_mut()
        .expect("observation object")
        .insert(
            "resources".into(),
            json!({
                "cpu_time_ns": {"status": "unavailable", "reason": ""},
                "peak_rss_bytes": {"status": "observed", "value": 4096},
                "network_rx_bytes": {"status": "observed", "value": 128},
                "network_tx_bytes": {"status": "observed", "value": 64}
            }),
        );
    assert!(
        serde_json::from_value::<RawObservation>(blank_reason)
            .expect("explicit resource metric schema")
            .validate()
            .is_err()
    );
}

#[test]
fn resource_scope_is_explicit_and_has_stable_wire_names() {
    for (scope, wire_name) in [
        (ResourceScope::ClientProcess, "client_process"),
        (
            ResourceScope::ClientAndProxyProcesses,
            "client_and_proxy_processes",
        ),
        (ResourceScope::DataNodeProcesses, "data_node_processes"),
        (ResourceScope::FullSystem, "full_system"),
    ] {
        assert_eq!(
            serde_json::to_string(&scope).unwrap(),
            format!("\"{wire_name}\"")
        );
    }

    let mut missing = valid_observation_value("backend_direct");
    missing.as_object_mut().unwrap().remove("resource_scope");
    assert!(serde_json::from_value::<RawObservation>(missing).is_err());

    let mut direct = valid_observation("backend_direct");
    direct.resource_scope = ResourceScope::FullSystem;
    assert!(direct.validate().is_err());
}

#[test]
fn remote_topology_recomputes_full_system_resources_exactly() {
    let mut observation = valid_observation("proxy");
    observation.resource_scope = ResourceScope::DataNodeProcesses;
    observation.topology_evidence = Some(remote_topology_evidence(&observation));
    observation.validate().unwrap();

    for mutation in [
        |observation: &mut RawObservation| {
            observation.resources.cpu_time_ns = ResourceMetric::Observed { value: 51 };
        },
        |observation: &mut RawObservation| {
            observation.resources.peak_rss_bytes = ResourceMetric::Observed { value: 4097 };
        },
        |observation: &mut RawObservation| {
            observation.resources.network_rx_bytes = ResourceMetric::Observed { value: 129 };
        },
        |observation: &mut RawObservation| {
            observation.resources.network_tx_bytes = ResourceMetric::Observed { value: 65 };
        },
    ] {
        let mut invalid = observation.clone();
        mutation(&mut invalid);
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn remote_topology_rejects_identity_network_and_sampling_drift() {
    let mut observation = valid_observation("proxy");
    observation.resource_scope = ResourceScope::DataNodeProcesses;
    observation.topology.data_nodes = 2;
    observation.topology_evidence = Some(remote_topology_evidence(&observation));
    observation.resources.cpu_time_ns = ResourceMetric::Observed { value: 50 };
    observation.resources.peak_rss_bytes = ResourceMetric::Observed { value: 4096 };
    observation.resources.network_rx_bytes = ResourceMetric::Observed { value: 128 };
    observation.resources.network_tx_bytes = ResourceMetric::Observed { value: 64 };
    observation.validate().unwrap();

    let mutations: Vec<fn(&mut TopologyEvidence)> = vec![
        |evidence| evidence.data_nodes[1].host_id = evidence.data_nodes[0].host_id.clone(),
        |evidence| evidence.resource_samples.pop().map(drop).unwrap_or(()),
        |evidence| evidence.resource_samples[0].boot_id = "different-boot".into(),
        |evidence| evidence.resource_samples[0].process_start_id = "different-start".into(),
        |evidence| evidence.resource_samples[0].pid += 1,
        |evidence| evidence.data_nodes[0].listen_address = "127.0.0.1:7000".into(),
        |evidence| evidence.data_nodes[0].management_interface = "eth0".into(),
        |evidence| evidence.data_nodes[0].executable_sha256 = "not-sha256".into(),
        |evidence| evidence.resource_samples[0].sampled_before_unix_ns = 201,
        |evidence| evidence.resource_samples[0].sampled_after_unix_ns = 299,
        |evidence| {
            evidence.resource_samples[0].sampled_before_unix_ns = 400;
            evidence.resource_samples[0].sampled_after_unix_ns = 399;
        },
    ];
    for mutate in mutations {
        let mut invalid = observation.clone();
        mutate(invalid.topology_evidence.as_mut().unwrap());
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn formal_proxy_requires_remote_formal_data_node_evidence() {
    let dataset = valid_dataset_manifest();
    let mut workload = valid_workload_manifest();
    workload.available_paths = vec![ExperimentPath::Proxy];
    workload.digest = workload.computed_digest().unwrap();
    let run = valid_run_manifest(&dataset, &workload);
    let mut observations = (1..=5)
        .map(|repetition| {
            valid_contract_observation("proxy", repetition, &run, &dataset, &workload)
        })
        .collect::<Vec<_>>();

    assert!(validate_report_contract(&run, &dataset, &workload, &observations).is_err());

    for observation in &mut observations {
        attach_formal_remote_evidence(observation);
    }
    validate_report_contract(&run, &dataset, &workload, &observations).unwrap();

    observations[0].resource_scope = ResourceScope::FullSystem;
    assert!(validate_report_contract(&run, &dataset, &workload, &observations).is_err());
    observations[0].resource_scope = ResourceScope::DataNodeProcesses;

    observations[0]
        .topology_evidence
        .as_mut()
        .unwrap()
        .deployment_mode = TopologyDeploymentMode::LocalDiagnostic;
    assert!(validate_report_contract(&run, &dataset, &workload, &observations).is_err());
}

#[test]
fn run_mode_enforces_formal_durations_and_allows_nonzero_diagnostic_durations() {
    let dataset = valid_dataset_manifest();
    let workload = valid_workload_manifest();
    let formal = valid_run_manifest(&dataset, &workload);
    formal.validate().unwrap();

    let mut wrong_formal = formal.clone();
    wrong_formal.measurement_seconds = 59;
    assert!(wrong_formal.validate().is_err());

    let mut wrong_formal_repetitions = formal.clone();
    wrong_formal_repetitions.repetitions = 4;
    assert!(wrong_formal_repetitions.validate().is_err());

    let mut diagnostic = formal;
    diagnostic.mode = RunMode::Diagnostic;
    diagnostic.warmup_seconds = 1;
    diagnostic.measurement_seconds = 1;
    diagnostic.repetitions = 1;
    diagnostic.configuration_digest = diagnostic
        .computed_configuration_digest()
        .expect("diagnostic configuration digest");
    diagnostic.validate().unwrap();
}

#[test]
fn experiment_paths_have_exact_stable_wire_names() {
    assert_eq!(
        serde_json::to_string(&ExperimentPath::BackendDirect).unwrap(),
        "\"backend_direct\""
    );
    assert_eq!(
        serde_json::to_string(&ExperimentPath::AdapterDirect).unwrap(),
        "\"adapter_direct\""
    );
    assert_eq!(
        serde_json::to_string(&ExperimentPath::Proxy).unwrap(),
        "\"proxy\""
    );
    assert_eq!(ExperimentPath::BackendDirect.to_string(), "backend_direct");
    assert_eq!(ExperimentPath::AdapterDirect.to_string(), "adapter_direct");
    assert_eq!(ExperimentPath::Proxy.to_string(), "proxy");
    assert!(serde_json::from_str::<ExperimentPath>("\"fallback\"").is_err());
}

#[test]
fn manifests_are_versioned_validated_and_deterministically_digestible() {
    let dataset = valid_dataset_manifest();
    let workload = valid_workload_manifest();
    let run = valid_run_manifest(&dataset, &workload);

    dataset.validate().unwrap();
    workload.validate().unwrap();
    run.validate().unwrap();
    assert_eq!(
        dataset.canonical_digest().unwrap(),
        dataset.canonical_digest().unwrap()
    );

    let mut invalid = run;
    invalid.schema_version = 2;
    assert!(invalid.validate().is_err());
}

#[test]
fn statistics_are_deterministic_and_formal_ci_requires_five_repetitions() {
    let sample = summarize_samples(&[40, 10, 30, 20, 50]).unwrap();
    assert_eq!(sample.count, 5);
    assert_eq!(sample.p50, 30);
    assert_eq!(sample.p95, 50);
    assert_eq!(sample.p99, 50);
    assert_eq!(sample.median, 30.0);
    assert!((sample.mean - 30.0).abs() < f64::EPSILON);

    assert!(summarize_repetitions(&[1.0, 2.0, 3.0, 4.0]).is_err());
    assert!(summarize_formal_repetitions(&[1.0, 2.0, 3.0, 4.0]).is_err());
    assert!(summarize_repetitions(&[1.0, 2.0, 3.0, 4.0, f64::NAN]).is_err());

    let repetitions = summarize_repetitions(&[10.0, 11.0, 12.0, 13.0, 14.0]).unwrap();
    assert_eq!(repetitions.count, 5);
    assert_eq!(repetitions.median, 12.0);
    assert!(repetitions.ci95_lower < repetitions.mean);
    assert!(repetitions.ci95_upper > repetitions.mean);
    assert!(summarize_formal_repetitions(&[10.0, 11.0, 12.0, 13.0, 14.0]).is_ok());
}

#[test]
fn statistics_have_exact_boundaries_and_do_not_use_normal_ci_for_large_df() {
    let singleton = summarize_samples(&[7]).unwrap();
    assert_eq!((singleton.p50, singleton.p95, singleton.p99), (7, 7, 7));

    let boundary = summarize_samples(&[10, 20]).unwrap();
    assert_eq!((boundary.p50, boundary.p95, boundary.p99), (10, 20, 20));

    let repetitions = summarize_repetitions(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]).unwrap();
    assert!((repetitions.sample_standard_deviation - 2.138_089_935_299_395).abs() < 1e-12);

    let large_sample: Vec<f64> = (1..=32).map(f64::from).collect();
    let large_ci = summarize_repetitions(&large_sample).unwrap();
    let normal_margin = 1.96 * large_ci.sample_standard_deviation / (large_ci.count as f64).sqrt();
    assert!(large_ci.ci95_upper - large_ci.mean > normal_margin);
}

#[test]
fn manifests_have_stable_golden_digests_and_json_round_trips() {
    let dataset = valid_dataset_manifest();
    assert_eq!(
        dataset.canonical_digest().unwrap(),
        "7bf87e979b535b7d31e438ef5ad63caeb5773f258785e6b33da5db644c2873de"
    );
    let encoded = serde_json::to_string(&dataset).unwrap();
    assert_eq!(
        serde_json::from_str::<DatasetManifest>(&encoded).unwrap(),
        dataset
    );
}

#[test]
fn manifest_digests_are_recomputed_without_self_reference() {
    let dataset = valid_dataset_manifest();
    let mut workload = valid_workload_manifest();
    let mut run = valid_run_manifest(&dataset, &workload);

    assert_eq!(workload.digest, workload.computed_digest().unwrap());
    assert_eq!(
        run.configuration_digest,
        run.computed_configuration_digest().unwrap()
    );

    workload.digest = digest('f');
    assert_eq!(
        workload.computed_digest().unwrap(),
        valid_workload_manifest().digest
    );
    assert!(
        workload.validate().is_err(),
        "forged 64-hex workload digest"
    );

    run.configuration_digest = digest('e');
    assert!(
        run.validate().is_err(),
        "forged 64-hex configuration digest"
    );
}

#[test]
fn report_contract_binds_manifests_to_observations_and_exact_timing() {
    let dataset = valid_dataset_manifest();
    let workload = valid_workload_manifest();
    let run = valid_run_manifest(&dataset, &workload);
    let observations = complete_formal_cell(&run, &dataset, &workload);

    validate_report_contract(&run, &dataset, &workload, &observations).unwrap();

    let mut wrong_warmup = observations[0].clone();
    wrong_warmup.timing.measurement_started_unix_ns = 1_100;
    wrong_warmup.timing.measurement_ended_unix_ns = 1_200;
    assert!(
        validate_report_contract(&run, &dataset, &workload, [&wrong_warmup]).is_err(),
        "formal 100ns raw timing must not pass"
    );

    let mut sixth_repetition = observations[0].clone();
    sixth_repetition.repetition = 6;
    assert!(validate_report_contract(&run, &dataset, &workload, [&sixth_repetition]).is_err());

    let mut wrong_configuration = observations[0].clone();
    wrong_configuration.configuration_digest = digest('c');
    assert!(validate_report_contract(&run, &dataset, &workload, [&wrong_configuration]).is_err());

    let mut wrong_dataset = observations[0].clone();
    wrong_dataset.dataset_digest = digest('d');
    assert!(validate_report_contract(&run, &dataset, &workload, [&wrong_dataset]).is_err());

    let mut diagnostic = run.clone();
    diagnostic.mode = RunMode::Diagnostic;
    diagnostic.warmup_seconds = 1;
    diagnostic.measurement_seconds = 2;
    diagnostic.repetitions = 1;
    diagnostic.configuration_digest = diagnostic.computed_configuration_digest().unwrap();
    let mut diagnostic_observation =
        valid_contract_observation("proxy", 1, &diagnostic, &dataset, &workload);
    diagnostic_observation.timing.measurement_started_unix_ns = 1_000_001_000;
    diagnostic_observation.timing.measurement_ended_unix_ns = 3_000_001_000;
    validate_report_contract(&diagnostic, &dataset, &workload, [&diagnostic_observation]).unwrap();

    diagnostic_observation.timing.measurement_ended_unix_ns = 1_000_001_100;
    assert!(
        validate_report_contract(&diagnostic, &dataset, &workload, [&diagnostic_observation],)
            .is_err(),
        "diagnostic raw timing must match the manifest durations"
    );
}

#[test]
fn report_contract_rejects_observations_from_a_different_selected_backend() {
    let dataset = valid_dataset_manifest();
    let workload = valid_workload_manifest();
    let run = valid_run_manifest(&dataset, &workload);
    let mut observations = complete_formal_cell(&run, &dataset, &workload);
    for observation in &mut observations {
        observation.backend = Backend::Postgresql;
    }

    let error = validate_report_contract(&run, &dataset, &workload, &observations)
        .expect_err("observation backend must match the run selected_backend");
    assert!(error.to_string().contains("selected_backend"));
}

#[test]
fn formal_contract_requires_each_path_repetition_exactly_once_per_cell() {
    let dataset = valid_dataset_manifest();
    let workload = valid_workload_manifest();
    let run = valid_run_manifest(&dataset, &workload);
    let complete = complete_formal_cell(&run, &dataset, &workload);
    validate_report_contract(&run, &dataset, &workload, &complete).unwrap();

    let mut missing = complete.clone();
    missing.pop();
    assert!(
        validate_report_contract(&run, &dataset, &workload, &missing).is_err(),
        "a formal cell missing one path repetition must be rejected"
    );

    let mut duplicate = complete.clone();
    duplicate.push(complete[0].clone());
    assert!(
        validate_report_contract(&run, &dataset, &workload, &duplicate).is_err(),
        "a duplicate path repetition must be rejected"
    );
}

#[test]
fn formal_contract_compares_identity_for_every_three_path_repetition() {
    let dataset = valid_dataset_manifest();
    let workload = valid_workload_manifest();
    let run = valid_run_manifest(&dataset, &workload);
    let mut observations = complete_formal_cell(&run, &dataset, &workload);
    let proxy = observations
        .iter_mut()
        .find(|observation| {
            observation.path == ExperimentPath::Proxy && observation.repetition == 4
        })
        .expect("proxy repetition four");
    proxy.identity.digest = digest('f');
    proxy.identity.row_count += 1;

    assert!(
        validate_report_contract(&run, &dataset, &workload, &observations).is_err(),
        "each comparable repetition must reject digest or row-count drift"
    );
}

#[test]
fn path_contract_is_backend_neutral_and_unavailability_is_explicit() {
    let request = BenchmarkRequest {
        workload: "historical_lookup".into(),
        snapshot: "as_of:100".into(),
        parameters: BTreeMap::from([("vertex_id".into(), json!(9))]),
    };
    request.validate().unwrap();

    let unavailable = PathExecution::Unavailable {
        reason: "native equivalent is not available".into(),
    };
    unavailable.validate().unwrap();

    let available = PathExecution::Available {
        identity: ResultIdentity {
            digest: "5555555555555555555555555555555555555555555555555555555555555555".into(),
            row_count: 1,
        },
    };
    available.validate().unwrap();
    assert_eq!(Backend::Postgresql.to_string(), "postgresql");
}

struct BackendDirectTestRunner;
struct AdapterDirectTestRunner;
struct ProxyTestRunner;
struct TestReadSnapshot;

impl ReadSnapshot for TestReadSnapshot {
    fn applied_log_index(&self) -> u64 {
        100
    }

    fn multi_get<'a>(&'a self, _keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn scan<'a>(&'a self, _span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl BackendDirectRunner for BackendDirectTestRunner {
    fn backend(&self) -> Backend {
        Backend::Rocksdb
    }

    fn execute_native(
        &mut self,
        _request: &BackendNativeRequest,
    ) -> Result<PathExecution, PathError> {
        Ok(PathExecution::Unavailable {
            reason: "test backend direct runner".into(),
        })
    }
}

impl AdapterDirectRunner for AdapterDirectTestRunner {
    fn backend(&self) -> Backend {
        Backend::Rocksdb
    }

    fn execute_snapshot(
        &mut self,
        snapshot: &dyn ReadSnapshot,
        _request: &AdapterSnapshotRequest,
    ) -> Result<PathExecution, PathError> {
        assert_eq!(snapshot.applied_log_index(), 100);
        Ok(PathExecution::Unavailable {
            reason: "test adapter direct runner".into(),
        })
    }
}

impl ProxyRunner for ProxyTestRunner {
    fn backend(&self) -> Backend {
        Backend::Rocksdb
    }

    fn execute_loadgen(
        &mut self,
        request: &ProxyLoadgenRequest,
    ) -> Result<ProxyLoadgenReport, PathError> {
        Ok(ProxyLoadgenReport {
            backend: request.backend,
            persisted_loadgen_report_json: r#"{"status":"unavailable"}"#.into(),
            execution: PathExecution::Unavailable {
                reason: "test proxy runner".into(),
            },
        })
    }
}

#[test]
fn path_runners_have_non_interchangeable_strongly_typed_inputs() {
    fn execute_backend_direct(
        runner: &mut impl BackendDirectRunner,
        request: &BackendNativeRequest,
    ) -> PathExecution {
        runner.execute_native(request).unwrap()
    }
    fn execute_adapter_direct(
        runner: &mut impl AdapterDirectRunner,
        snapshot: &dyn ReadSnapshot,
        request: &AdapterSnapshotRequest,
    ) -> PathExecution {
        runner.execute_snapshot(snapshot, request).unwrap()
    }
    fn execute_proxy(
        runner: &mut impl ProxyRunner,
        request: &ProxyLoadgenRequest,
    ) -> ProxyLoadgenReport {
        runner.execute_loadgen(request).unwrap()
    }

    let benchmark_request = BenchmarkRequest {
        workload: "point_lookup".into(),
        snapshot: "latest".into(),
        parameters: BTreeMap::new(),
    };
    benchmark_request.validate().unwrap();
    let backend_request = BackendNativeRequest {
        backend: Backend::Rocksdb,
        native_operation: "get_vertex".into(),
        native_request: json!({"vertex_id": 7}),
    };
    let adapter_request = AdapterSnapshotRequest {
        backend: Backend::Rocksdb,
        pinned_snapshot: "as_of:100".into(),
        primitive: AdapterPrimitive::PointLookup {
            key: "vertex:7".into(),
        },
    };
    let proxy_request = ProxyLoadgenRequest {
        backend: Backend::Rocksdb,
        persisted_loadgen_input_json: r#"{"workload":"point_lookup","concurrency":8}"#.into(),
    };
    backend_request.validate().unwrap();
    adapter_request.validate().unwrap();
    proxy_request.validate().unwrap();
    assert!(
        ProxyLoadgenRequest {
            backend: Backend::Rocksdb,
            persisted_loadgen_input_json: "not json".into(),
        }
        .validate()
        .is_err()
    );
    assert!(
        ProxyLoadgenReport {
            backend: Backend::Rocksdb,
            persisted_loadgen_report_json: "[]".into(),
            execution: PathExecution::Unavailable {
                reason: "test proxy runner".into(),
            },
        }
        .validate()
        .is_err()
    );

    let mut backend = BackendDirectTestRunner;
    let mut adapter = AdapterDirectTestRunner;
    let mut proxy = ProxyTestRunner;
    let snapshot = TestReadSnapshot;

    assert!(matches!(
        execute_backend_direct(&mut backend, &backend_request),
        PathExecution::Unavailable { .. }
    ));
    assert!(matches!(
        execute_adapter_direct(&mut adapter, &snapshot, &adapter_request),
        PathExecution::Unavailable { .. }
    ));
    let proxy_report = execute_proxy(&mut proxy, &proxy_request);
    proxy_report.validate().unwrap();
    assert!(matches!(
        proxy_report.execution,
        PathExecution::Unavailable { .. }
    ));
}
