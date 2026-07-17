use std::process::Command;

fn command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_dtgproxy"))
}

#[test]
fn version_reports_product_name_and_workspace_version() {
    let output = command().arg("--version").output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "DTGProxy 0.1.0\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn no_arguments_reports_phase_status_and_usage() {
    let output = command().output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("DTGProxy Phase 0 semantic kernel"));
    assert!(stdout.contains("Usage: dtgproxy [--version]"));
    assert!(output.stderr.is_empty());
}

#[test]
fn unknown_argument_exits_with_usage_error() {
    let output = command().arg("--unknown").output().unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unknown argument: --unknown"));
    assert!(stderr.contains("Usage: dtgproxy [--version]"));
}
