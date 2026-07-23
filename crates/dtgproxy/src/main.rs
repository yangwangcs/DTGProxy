#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::future::Future;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use dtgproxy::config::NodeConfig;
use dtgproxy::control_plane::{
    BackendProfile, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use dtgproxy::gateway::{
    GATEWAY_API_VERSION, GatewayOperation, GatewayRequest, GatewayService, initialize_node,
    send_request, serve,
};
use storage_api::AdapterRequirement;

const USAGE: &str = "DTGProxy 1.0 prototype\n\
Usage:\n\
  dtgproxy init --config <file> --root <dir> --graph-id <id> --mode <primary-replica|shared-nothing> --shards <spec> --backend <provider> [--graph-name <name>] [--listen <addr>]\n\
  dtgproxy serve --config <file>\n\
  dtgproxy status --config <file>\n\
  dtgproxy transaction --config <file> --file <request.json>\n\
  dtgproxy backend verify --config <file>\n\
  dtgproxy backend migrate --config <file> --provider <name>\n\
Shard spec: shard_id:placement_epoch:voter+voter[,shard_id:placement_epoch:voter]";

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match run(arguments) {
        Ok(output) => {
            if let Some(output) = output {
                println!("{output}");
            }
            ExitCode::SUCCESS
        }
        Err(CliError::Usage(message)) => {
            eprintln!("{message}");
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
        Err(CliError::Runtime(message)) => {
            eprintln!("dtgproxy error: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: Vec<String>) -> Result<Option<String>, CliError> {
    match arguments.as_slice() {
        [] => Ok(Some(USAGE.to_owned())),
        [argument] if argument == "--version" || argument == "-V" => {
            Ok(Some(format!("DTGProxy {}", env!("CARGO_PKG_VERSION"))))
        }
        [command, rest @ ..] if command == "init" => {
            initialize(rest).map(|()| Some("DTGProxy root initialized".to_owned()))
        }
        [command, rest @ ..] if command == "status" => status(rest).map(Some),
        [command, rest @ ..] if command == "serve" => serve_command(rest).map(|()| None),
        [command, rest @ ..] if command == "transaction" => transaction(rest).map(Some),
        [command, subcommand, rest @ ..] if command == "backend" && subcommand == "verify" => {
            backend_verify(rest).map(Some)
        }
        [command, subcommand, rest @ ..] if command == "backend" && subcommand == "migrate" => {
            backend_migrate(rest).map(Some)
        }
        [argument, ..] => Err(CliError::Usage(format!("unknown argument: {argument}"))),
    }
}

fn initialize(arguments: &[String]) -> Result<(), CliError> {
    let options = options(arguments)?;
    let config_path = required(&options, "--config")?;
    let root = PathBuf::from(required(&options, "--root")?);
    let graph_id = parse_required(&options, "--graph-id")?;
    let graph_name = options
        .get("--graph-name")
        .cloned()
        .unwrap_or_else(|| format!("graph-{graph_id}"));
    let listen = parse_or(&options, "--listen", "127.0.0.1:7070".parse().unwrap())?;
    let max_ticks = parse_or(&options, "--max-raft-ticks", 100_usize)?;
    let reservation = parse_or(&options, "--timestamp-reservation", 4_096_u32)?;
    let route_seed = parse_or(&options, "--route-seed", 1_u64)?;
    let virtual_partitions = parse_or(&options, "--virtual-partitions", 65_536_u32)?;
    let mode = match required(&options, "--mode")? {
        "primary-replica" => DeploymentMode::PrimaryReplica,
        "shared-nothing" => DeploymentMode::SharedNothing,
        mode => return Err(CliError::Usage(format!("invalid deployment mode {mode}"))),
    };
    let placements = parse_placements(required(&options, "--shards")?)?;
    let provider = required(&options, "--backend")?;
    let mut parameters = BTreeMap::new();
    if provider == "rocksdb" {
        parameters.insert("path".into(), format!("backends/graph-{graph_id}"));
    }
    if let Some(endpoint) = options.get("--backend-endpoint") {
        parameters.insert("endpoint".into(), endpoint.clone());
    }
    if let Some(database) = options.get("--backend-database") {
        parameters.insert("database".into(), database.clone());
    }
    if let Some(username) = options.get("--backend-user") {
        parameters.insert("username".into(), username.clone());
    }
    let mut secret_references = BTreeMap::new();
    if let Some(reference) = options.get("--backend-secret-ref") {
        let name = match provider {
            "postgresql" => "connection_string",
            "neo4j" => "password",
            _ => "credential",
        };
        secret_references.insert(name.into(), reference.clone());
    }
    let graph = GraphDefinition::new(
        graph_id,
        graph_name,
        1,
        TopologyDefinition::new(mode, route_seed, virtual_partitions, 1, placements)
            .map_err(runtime_error)?,
        BackendProfile::new(
            provider,
            parameters,
            secret_references,
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .map_err(runtime_error)?,
    )
    .map_err(runtime_error)?;
    let config =
        NodeConfig::new(root, graph_id, listen, max_ticks, reservation).map_err(runtime_error)?;
    initialize_node(config_path, &config, graph).map_err(runtime_error)
}

fn status(arguments: &[String]) -> Result<String, CliError> {
    let config = load_config(arguments)?;
    let gateway = block_on(GatewayService::open(config)).map_err(runtime_error)?;
    serde_json::to_string(&gateway.status().map_err(runtime_error)?).map_err(runtime_error)
}

fn serve_command(arguments: &[String]) -> Result<(), CliError> {
    let config = load_config(arguments)?;
    let listener = TcpListener::bind(config.listen()).map_err(runtime_error)?;
    println!(
        "DTGProxy Gateway listening on {}",
        listener.local_addr().map_err(runtime_error)?
    );
    let gateway = block_on(GatewayService::open(config)).map_err(runtime_error)?;
    serve(gateway, listener, None).map_err(runtime_error)
}

fn transaction(arguments: &[String]) -> Result<String, CliError> {
    let options = options(arguments)?;
    let config = NodeConfig::load(required(&options, "--config")?).map_err(runtime_error)?;
    let request = std::fs::read(required(&options, "--file")?).map_err(runtime_error)?;
    let response = send_request(config.listen(), &request).map_err(runtime_error)?;
    String::from_utf8(response).map_err(runtime_error)
}

fn backend_verify(arguments: &[String]) -> Result<String, CliError> {
    let config = load_config(arguments)?;
    let gateway = block_on(GatewayService::open(config)).map_err(runtime_error)?;
    let graph = gateway
        .catalog()
        .state()
        .graph(gateway.config().graph_id())
        .expect("Gateway open validated graph existence");
    Ok(serde_json::json!({
        "version": 1,
        "provider": graph.backend().provider(),
        "generation": graph.backend().generation(),
        "configuration_valid": true,
        "live_backend_verified": true,
        "replicas": gateway.backend_replicas(),
    })
    .to_string())
}

fn backend_migrate(arguments: &[String]) -> Result<String, CliError> {
    let options = options(arguments)?;
    let config = NodeConfig::load(required(&options, "--config")?).map_err(runtime_error)?;
    let provider = required(&options, "--provider")?;
    let mut public_parameters = BTreeMap::new();
    for (option, parameter) in [
        ("--backend-path", "path"),
        ("--backend-endpoint", "endpoint"),
        ("--backend-database", "database"),
        ("--backend-user", "username"),
        ("--backend-pool-size", "pool_size"),
    ] {
        if let Some(value) = options.get(option) {
            public_parameters.insert(parameter.to_owned(), value.clone());
        }
    }
    let mut secret_references = BTreeMap::new();
    if let Some(reference) = options.get("--backend-secret-ref") {
        let name = match provider {
            "postgresql" => "connection_string",
            "neo4j" => "password",
            _ => "credential",
        };
        secret_references.insert(name.to_owned(), reference.clone());
    }
    let request = GatewayRequest {
        version: GATEWAY_API_VERSION,
        request_id: "cli-backend-migrate".into(),
        operation: GatewayOperation::MigrateBackend {
            provider: provider.to_owned(),
            public_parameters,
            secret_references,
        },
    };
    let request = serde_json::to_vec(&request).map_err(runtime_error)?;
    let response = send_request(config.listen(), &request).map_err(runtime_error)?;
    String::from_utf8(response).map_err(runtime_error)
}

fn load_config(arguments: &[String]) -> Result<NodeConfig, CliError> {
    let options = options(arguments)?;
    NodeConfig::load(required(&options, "--config")?).map_err(runtime_error)
}

fn options(arguments: &[String]) -> Result<BTreeMap<String, String>, CliError> {
    if !arguments.len().is_multiple_of(2) {
        return Err(CliError::Usage("every option requires a value".into()));
    }
    let mut options = BTreeMap::new();
    for pair in arguments.chunks_exact(2) {
        if !pair[0].starts_with("--") {
            return Err(CliError::Usage(format!("invalid option {}", pair[0])));
        }
        if options.insert(pair[0].clone(), pair[1].clone()).is_some() {
            return Err(CliError::Usage(format!("duplicate option {}", pair[0])));
        }
    }
    Ok(options)
}

fn required<'a>(options: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str, CliError> {
    options
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| CliError::Usage(format!("missing required option {name}")))
}

fn parse_required<T>(options: &BTreeMap<String, String>, name: &str) -> Result<T, CliError>
where
    T: std::str::FromStr,
    T::Err: Display,
{
    required(options, name)?
        .parse()
        .map_err(|error| CliError::Usage(format!("invalid {name}: {error}")))
}

fn parse_or<T>(options: &BTreeMap<String, String>, name: &str, default: T) -> Result<T, CliError>
where
    T: std::str::FromStr,
    T::Err: Display,
{
    options.get(name).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|error| CliError::Usage(format!("invalid {name}: {error}")))
    })
}

fn parse_placements(value: &str) -> Result<Vec<Placement>, CliError> {
    value
        .split(',')
        .map(|entry| {
            let mut fields = entry.split(':');
            let shard_id = fields
                .next()
                .ok_or_else(|| CliError::Usage(format!("invalid Shard spec {entry}")))?
                .parse()
                .map_err(|error| CliError::Usage(format!("invalid Shard ID: {error}")))?;
            let epoch = fields
                .next()
                .ok_or_else(|| CliError::Usage(format!("invalid Shard spec {entry}")))?
                .parse()
                .map_err(|error| CliError::Usage(format!("invalid placement epoch: {error}")))?;
            let voters = fields
                .next()
                .ok_or_else(|| CliError::Usage(format!("invalid Shard spec {entry}")))?
                .split('+')
                .map(|voter| {
                    voter
                        .parse()
                        .map_err(|error| CliError::Usage(format!("invalid voter ID: {error}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if fields.next().is_some() {
                return Err(CliError::Usage(format!("invalid Shard spec {entry}")));
            }
            Placement::new(shard_id, epoch, voters).map_err(runtime_error)
        })
        .collect()
}

fn runtime_error(error: impl Display) -> CliError {
    CliError::Runtime(error.to_string())
}

enum CliError {
    Usage(String),
    Runtime(String),
}

use std::fmt::Display;

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}
