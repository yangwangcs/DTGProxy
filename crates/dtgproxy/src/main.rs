#![forbid(unsafe_code)]

use std::process::ExitCode;

const USAGE: &str = "Usage: dtgproxy [--version]";

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match arguments.as_slice() {
        [] => {
            println!("DTGProxy Phase 0 semantic kernel");
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        [argument] if argument == "--version" || argument == "-V" => {
            println!("DTGProxy {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        [argument, ..] => {
            eprintln!("unknown argument: {argument}");
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}
