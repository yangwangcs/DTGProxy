use paper_benchmark::{
    Backend, DatasetManifest, EnvironmentFingerprint, ExperimentMatrix, ExperimentPath,
    ExperimentProtocol, ExperimentSpec, ExperimentSuite, ExperimentSuiteKind, RunMode,
    SCHEMA_VERSION, WorkloadCase, WorkloadManifest, run_experiment, verify_artifact,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = r#"Usage:
  dtgproxy-paper-benchmark simulate --output-root DIR --run-id ID [OPTIONS]
  dtgproxy-paper-benchmark validate-spec --spec FILE --executor PROGRAM
  dtgproxy-paper-benchmark run --spec FILE --output-root DIR --executor PROGRAM
  dtgproxy-paper-benchmark verify --artifact DIR --regenerate-dir DIR

simulate options:
  --concurrencies LIST       Comma-separated positive integers (default: 1,2)
  --repetitions COUNT        Positive repetition count (default: 2)
  --warmup-seconds SECONDS   Positive warmup duration (default: 1)
  --measurement-seconds SEC  Must be 1 for the built-in simulator (default: 1)
  --shuffle-seed SEED        Deterministic matrix shuffle seed (default: 42)

The simulator is always diagnostic and never starts DTGProxy or a backend.
Formal mode is derived from the fixed protocol; it is never a user label.
"#;

fn main() {
    if let Err(error) = execute() {
        eprintln!("paper performance error: {error}");
        std::process::exit(1);
    }
}

fn execute() -> Result<(), String> {
    let mut arguments = env::args().skip(1);
    let Some(command) = arguments.next() else {
        return Err(USAGE.into());
    };
    let remaining: Vec<_> = arguments.collect();
    if command == "-h" || command == "--help" || command == "help" {
        print!("{USAGE}");
        return Ok(());
    }
    let options = parse_options(&remaining)?;
    match command.as_str() {
        "simulate" => simulate(options),
        "validate-spec" => validate_spec(options),
        "run" => run(options),
        "verify" => verify(options),
        _ => Err(format!("unknown command: {command}\n\n{USAGE}")),
    }
}

fn validate_spec(mut options: BTreeMap<String, String>) -> Result<(), String> {
    let spec_path = required_path(&mut options, "--spec")?;
    let executor = required_path(&mut options, "--executor")?;
    reject_unknown(options)?;
    let spec = read_spec(&spec_path)?;
    let manifest = spec
        .into_manifest(false)
        .map_err(|error| error.to_string())?;
    if manifest.run.mode != RunMode::Formal {
        return Err("validate-spec requires the fixed formal experiment contract".into());
    }
    let actual_executor = paper_benchmark::sha256_file(&executor)
        .map_err(|error| format!("failed to hash {}: {error}", executor.display()))?;
    if actual_executor != manifest.environment.binaries.executor_sha256 {
        return Err("executor SHA-256 mismatch".into());
    }
    println!(
        "validated run={} mode={:?} environment={}",
        manifest.run.run_id, manifest.run.mode, manifest.environment.digest
    );
    Ok(())
}

fn simulate(mut options: BTreeMap<String, String>) -> Result<(), String> {
    let output_root = required_path(&mut options, "--output-root")?;
    let run_id = required(&mut options, "--run-id")?;
    let concurrencies = parse_u32_list(
        &optional(&mut options, "--concurrencies").unwrap_or_else(|| "1,2".into()),
        "--concurrencies",
    )?;
    let repetitions = parse_u32(
        &optional(&mut options, "--repetitions").unwrap_or_else(|| "2".into()),
        "--repetitions",
    )?;
    let warmup_seconds = parse_u64(
        &optional(&mut options, "--warmup-seconds").unwrap_or_else(|| "1".into()),
        "--warmup-seconds",
    )?;
    let measurement_seconds = parse_u64(
        &optional(&mut options, "--measurement-seconds").unwrap_or_else(|| "1".into()),
        "--measurement-seconds",
    )?;
    let shuffle_seed = parse_u64(
        &optional(&mut options, "--shuffle-seed").unwrap_or_else(|| "42".into()),
        "--shuffle-seed",
    )?;
    reject_unknown(options)?;
    if measurement_seconds != 1 {
        return Err("the built-in simulator requires --measurement-seconds 1".into());
    }

    let dataset_digest = blake3::hash(b"dtgproxy-paper-tiny-dataset-v1")
        .to_hex()
        .to_string();
    let dataset = DatasetManifest {
        schema_version: SCHEMA_VERSION,
        dataset_id: "tiny-synthetic-v1".into(),
        seed: 42,
        vertex_count: 100,
        edge_count: 500,
        temporal_update_count: 60,
        content_digest: dataset_digest,
    };
    let paths = vec![
        ExperimentPath::BackendDirect,
        ExperimentPath::AdapterDirect,
        ExperimentPath::Proxy,
    ];
    let mut workload = WorkloadManifest {
        schema_version: SCHEMA_VERSION,
        workload_id: "tiny_point_lookup".into(),
        query: "MATCH (n {id: $id}) RETURN n.id".into(),
        parameters: BTreeMap::from([("id".into(), json!(7))]),
        available_paths: paths.clone(),
        digest: String::new(),
    };
    workload.digest = workload
        .computed_digest()
        .map_err(|error| error.to_string())?;
    let spec = ExperimentSpec {
        schema_version: SCHEMA_VERSION,
        run_id,
        selected_backend: Backend::Rocksdb,
        revision: "diagnostic-simulator-v1".into(),
        dirty_worktree_digest: blake3::hash(b"diagnostic-simulator-worktree")
            .to_hex()
            .to_string(),
        environment: EnvironmentFingerprint::synthetic(),
        dataset,
        workloads: vec![WorkloadCase {
            manifest: workload,
            snapshot: "as_of:100".into(),
        }],
        matrix: ExperimentMatrix {
            suites: vec![ExperimentSuite {
                kind: ExperimentSuiteKind::Comparison,
                backends: vec![Backend::Rocksdb],
                paths,
                workloads: vec!["tiny_point_lookup".into()],
                data_nodes: vec![1],
                concurrencies,
                ablations: vec!["production".into()],
                workload_ablations: BTreeMap::new(),
            }],
        },
        protocol: ExperimentProtocol {
            warmup_seconds,
            measurement_seconds,
            repetitions,
        },
        shuffle_seed,
    };
    let artifact =
        run_experiment(spec, &output_root, true, None).map_err(|error| error.to_string())?;
    println!("diagnostic artifact: {}", artifact.display());
    Ok(())
}

fn run(mut options: BTreeMap<String, String>) -> Result<(), String> {
    let spec_path = required_path(&mut options, "--spec")?;
    let output_root = required_path(&mut options, "--output-root")?;
    let executor = required_path(&mut options, "--executor")?;
    reject_unknown(options)?;
    let spec = read_spec(&spec_path)?;
    let artifact = run_experiment(spec, &output_root, false, Some(&executor))
        .map_err(|error| error.to_string())?;
    println!("experiment artifact: {}", artifact.display());
    Ok(())
}

fn read_spec(path: &Path) -> Result<ExperimentSpec, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("invalid experiment spec: {error}"))
}

fn verify(mut options: BTreeMap<String, String>) -> Result<(), String> {
    let artifact = required_path(&mut options, "--artifact")?;
    let regenerate_dir = required_path(&mut options, "--regenerate-dir")?;
    reject_unknown(options)?;
    let report = verify_artifact(&artifact, &regenerate_dir).map_err(|error| error.to_string())?;
    println!(
        "verified run={} mode={:?} raw={} summary_cells={} regenerated={}",
        report.run_id,
        report.mode,
        report.raw_observations,
        report.summary_cells,
        report.regenerated_directory.display()
    );
    Ok(())
}

fn parse_options(arguments: &[String]) -> Result<BTreeMap<String, String>, String> {
    let mut options = BTreeMap::new();
    let mut index = 0;
    while index < arguments.len() {
        let key = &arguments[index];
        if key == "-h" || key == "--help" {
            print!("{USAGE}");
            std::process::exit(0);
        }
        if !key.starts_with("--") {
            return Err(format!("expected an option, found: {key}"));
        }
        let Some(value) = arguments.get(index + 1) else {
            return Err(format!("missing value for {key}"));
        };
        if value.starts_with("--") {
            return Err(format!("missing value for {key}"));
        }
        if options.insert(key.clone(), value.clone()).is_some() {
            return Err(format!("duplicate option: {key}"));
        }
        index += 2;
    }
    Ok(options)
}

fn required(options: &mut BTreeMap<String, String>, name: &str) -> Result<String, String> {
    options
        .remove(name)
        .ok_or_else(|| format!("missing required option: {name}"))
}

fn required_path(options: &mut BTreeMap<String, String>, name: &str) -> Result<PathBuf, String> {
    Ok(Path::new(&required(options, name)?).to_path_buf())
}

fn optional(options: &mut BTreeMap<String, String>, name: &str) -> Option<String> {
    options.remove(name)
}

fn reject_unknown(options: BTreeMap<String, String>) -> Result<(), String> {
    if options.is_empty() {
        return Ok(());
    }
    Err(format!(
        "unknown option(s): {}",
        options.keys().cloned().collect::<Vec<_>>().join(", ")
    ))
}

fn parse_u32(value: &str, name: &str) -> Result<u32, String> {
    let parsed = value
        .parse::<u32>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be a positive integer"));
    }
    Ok(parsed)
}

fn parse_u64(value: &str, name: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be a positive integer"));
    }
    Ok(parsed)
}

fn parse_u32_list(value: &str, name: &str) -> Result<Vec<u32>, String> {
    let parsed: Vec<_> = value
        .split(',')
        .map(|item| parse_u32(item, name))
        .collect::<Result<_, _>>()?;
    if parsed.iter().copied().collect::<BTreeSet<_>>().len() != parsed.len() {
        return Err(format!("{name} must not contain duplicates"));
    }
    Ok(parsed)
}
