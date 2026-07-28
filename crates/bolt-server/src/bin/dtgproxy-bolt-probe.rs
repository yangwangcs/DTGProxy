use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use bolt_server::{ExternalTtfrProbeConfig, ExternalTtfrProbeReport, probe_external_ttfr};

const USAGE: &str = "Usage: dtgproxy-bolt-probe --address HOST:PORT --query QUERY --warmups COUNT --samples COUNT --timeout-ms MILLISECONDS";

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
    let config = match ExternalTtfrProbeConfig::new(
        options.address,
        options.query,
        BTreeMap::new(),
        options.warmups,
        options.samples,
        options.timeout,
    ) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    match probe_external_ttfr(config).await {
        Ok(report) => {
            println!("{}", report_json(&report));
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

struct Options {
    address: SocketAddr,
    query: String,
    warmups: usize,
    samples: usize,
    timeout: Duration,
}

impl Options {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let mut address = None;
        let mut query = None;
        let mut warmups = None;
        let mut samples = None;
        let mut timeout_ms = None;
        let mut index = 0;
        while index < arguments.len() {
            let option = arguments[index].as_str();
            let value = arguments
                .get(index + 1)
                .ok_or_else(|| format!("missing value for {option}"))?;
            match option {
                "--address" => address = Some(parse(value, option)?),
                "--query" => query = Some(value.clone()),
                "--warmups" => warmups = Some(parse(value, option)?),
                "--samples" => samples = Some(parse(value, option)?),
                "--timeout-ms" => timeout_ms = Some(parse::<u64>(value, option)?),
                _ => return Err(format!("unknown option {option}")),
            }
            index += 2;
        }
        Ok(Self {
            address: required(address, "--address")?,
            query: required(query, "--query")?,
            warmups: required(warmups, "--warmups")?,
            samples: required(samples, "--samples")?,
            timeout: Duration::from_millis(required(timeout_ms, "--timeout-ms")?),
        })
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

fn report_json(report: &ExternalTtfrProbeReport) -> String {
    let mut json = String::from("{\"row_count\":");
    write!(json, "{}", report.row_count()).expect("write to string");
    json.push_str(",\"result_digest\":\"");
    push_hex(&mut json, &report.result_digest());
    json.push_str("\",\"samples\":[");
    for (index, sample) in report.samples().iter().enumerate() {
        if index != 0 {
            json.push(',');
        }
        write!(
            json,
            "{{\"ttfr_ns\":{},\"total_latency_ns\":{}}}",
            sample.ttfr().as_nanos(),
            sample.total_latency().as_nanos()
        )
        .expect("write to string");
    }
    json.push_str("]}");
    json
}

fn push_hex(output: &mut String, digest: &[u8; 32]) {
    for byte in digest {
        write!(output, "{byte:02x}").expect("write to string");
    }
}
