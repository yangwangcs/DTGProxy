use std::collections::BTreeMap;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bolt_server::{ExternalLoadConfig, run_external_load};

const USAGE: &str = "Usage: dtgproxy-bolt-loadgen --address HOST:PORT --query QUERY --connections COUNT --warmup-seconds SECONDS --duration-seconds SECONDS --timeout-ms MILLISECONDS [--benchmark-session TOKEN] --output FILE";

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() == 1 && matches!(arguments[0].as_str(), "--help" | "-h") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let options = match Options::parse(&arguments) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let config = ExternalLoadConfig::new(
        options.address,
        options.query,
        BTreeMap::new(),
        options.connections,
        options.warmup,
        options.duration,
        options.timeout,
    );
    let config = match (config, options.benchmark_session) {
        (Ok(config), Some(session)) => config.with_benchmark_session(session),
        (config, None) => config,
        (Err(error), Some(_)) => Err(error),
    };
    let config = match config {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let report = match run_external_load(config).await {
        Ok(report) => report,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let bytes = match serde_json::to_vec_pretty(&report) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("failed to encode load report: {error}");
            return ExitCode::FAILURE;
        }
    };
    match write_new_atomic(&options.output, &bytes) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("failed to write load report: {error}");
            ExitCode::FAILURE
        }
    }
}

struct Options {
    address: SocketAddr,
    query: String,
    connections: usize,
    warmup: Duration,
    duration: Duration,
    timeout: Duration,
    benchmark_session: Option<String>,
    output: PathBuf,
}

impl Options {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let mut address = None;
        let mut query = None;
        let mut connections = None;
        let mut warmup_seconds = None;
        let mut duration_seconds = None;
        let mut timeout_ms = None;
        let mut benchmark_session = None;
        let mut output = None;
        let mut index = 0;
        while index < arguments.len() {
            let option = arguments[index].as_str();
            let value = arguments
                .get(index + 1)
                .ok_or_else(|| format!("missing value for {option}"))?;
            match option {
                "--address" => address = Some(parse(value, option)?),
                "--query" => query = Some(value.clone()),
                "--connections" => connections = Some(parse(value, option)?),
                "--warmup-seconds" => warmup_seconds = Some(parse::<u64>(value, option)?),
                "--duration-seconds" => duration_seconds = Some(parse::<u64>(value, option)?),
                "--timeout-ms" => timeout_ms = Some(parse::<u64>(value, option)?),
                "--benchmark-session" => {
                    benchmark_session = Some(parse_benchmark_session(value, option)?)
                }
                "--output" => output = Some(PathBuf::from(value)),
                _ => return Err(format!("unknown option {option}")),
            }
            index += 2;
        }
        Ok(Self {
            address: required(address, "--address")?,
            query: required(query, "--query")?,
            connections: required(connections, "--connections")?,
            warmup: Duration::from_secs(required(warmup_seconds, "--warmup-seconds")?),
            duration: Duration::from_secs(required(duration_seconds, "--duration-seconds")?),
            timeout: Duration::from_millis(required(timeout_ms, "--timeout-ms")?),
            benchmark_session,
            output: required(output, "--output")?,
        })
    }
}

fn parse_benchmark_session(value: &str, option: &str) -> Result<String, String> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        Ok(value.to_owned())
    } else {
        Err(format!("invalid value for {option}"))
    }
}

fn parse<T>(value: &str, option: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| format!("invalid value for {option}"))
}

fn required<T>(value: Option<T>, option: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("missing required option {option}"))
}

fn write_new_atomic(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("report.json");
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        sequence
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    drop(file);

    let publish = std::fs::hard_link(&temporary, path);
    let cleanup = std::fs::remove_file(&temporary);
    publish?;
    cleanup?;
    OpenOptions::new().read(true).open(parent)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::{Options, write_new_atomic};
    use std::fs;

    fn required_arguments() -> Vec<String> {
        [
            "--address",
            "127.0.0.1:7687",
            "--query",
            "RETURN 1",
            "--connections",
            "1",
            "--warmup-seconds",
            "0",
            "--duration-seconds",
            "1",
            "--timeout-ms",
            "1000",
            "--output",
            "report.json",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn parses_a_valid_benchmark_session() {
        let mut arguments = required_arguments();
        arguments.extend([
            "--benchmark-session".to_owned(),
            "paper.Session-1_2".to_owned(),
        ]);

        let options = Options::parse(&arguments).expect("valid benchmark session");

        assert_eq!(
            options.benchmark_session.as_deref(),
            Some("paper.Session-1_2")
        );
    }

    #[test]
    fn rejects_empty_or_non_token_benchmark_sessions() {
        for token in ["", "contains space", "slash/value", "非ASCII"] {
            let mut arguments = required_arguments();
            arguments.extend(["--benchmark-session".to_owned(), token.to_owned()]);

            let error = match Options::parse(&arguments) {
                Ok(_) => panic!("invalid benchmark session must be rejected"),
                Err(error) => error,
            };

            assert!(error.contains("invalid value for --benchmark-session"));
        }
    }

    #[test]
    fn existing_output_is_preserved_without_leaking_a_temporary_file() {
        let directory = std::env::temp_dir().join(format!(
            "dtgproxy-loadgen-atomic-write-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("create test directory");
        let output = directory.join("report.json");
        fs::write(&output, b"original\n").expect("seed output");

        let error = write_new_atomic(&output, b"replacement").expect_err("refuse overwrite");

        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&output).expect("read output"), b"original\n");
        assert_eq!(
            fs::read_dir(&directory).expect("list directory").count(),
            1,
            "failed writes must remove their temporary file"
        );
        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
