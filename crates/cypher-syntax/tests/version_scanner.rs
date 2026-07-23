use cypher_syntax::{CypherProfile, CypherVersion, scan_version};

#[test]
fn defaults_to_cypher_25() {
    let scanned = scan_version("MATCH (n) RETURN n").expect("query should scan");

    assert_eq!(scanned.profile().version(), CypherVersion::V25);
    assert_eq!(scanned.body(), "MATCH (n) RETURN n");
}

#[test]
fn rejects_removed_cypher_5_profile() {
    let error = scan_version("CYPHER 5\nMATCH (n) RETURN n")
        .expect_err("the removed Cypher 5 profile must not be accepted");

    assert_eq!(error.code(), "DTG-CYPHER-UNSUPPORTED-VERSION");
}

#[test]
fn scans_explicit_cypher_25_prefix_case_insensitively() {
    let scanned = scan_version("cypher 25 MATCH (n) RETURN n").expect("query should scan");

    assert_eq!(scanned.profile(), CypherProfile::cypher_25());
    assert_eq!(scanned.body(), "MATCH (n) RETURN n");
}

#[test]
fn rejects_unknown_language_version() {
    let error = scan_version("CYPHER 7 MATCH (n) RETURN n").expect_err("version must fail");

    assert_eq!(error.code(), "DTG-CYPHER-UNSUPPORTED-VERSION");
}
