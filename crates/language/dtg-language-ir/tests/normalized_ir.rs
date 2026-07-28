use dtg_language_ir::{
    BuiltInAlgorithmId, IrVersion, LogicalPlan, LogicalProgram, LogicalStatement, validate_program,
};

#[test]
fn current_ir_has_one_fresh_major_version() {
    assert_eq!(IrVersion::CURRENT.major(), 1);
}

#[test]
fn analytics_accepts_only_closed_built_in_ids() {
    assert!(BuiltInAlgorithmId::try_from("page_rank").is_ok());
    assert!(BuiltInAlgorithmId::try_from("user.uploaded_code").is_err());
}

#[test]
fn empty_query_is_rejected() {
    let program = LogicalProgram::new(LogicalStatement::Query(LogicalPlan::empty()));
    assert!(validate_program(&program).is_err());
}
