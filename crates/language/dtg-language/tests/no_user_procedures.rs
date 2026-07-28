use dtg_language::{EmptySchemaCatalog, compile};

#[test]
fn user_procedures_are_never_runtime_fallbacks() {
    for source in [
        "CALL user.code()",
        "CALL dbms.anything()",
        "CALL $procedure()",
    ] {
        let error = compile(source, &EmptySchemaCatalog).unwrap_err();
        assert_eq!(error.code(), "DTG-LANG-UNKNOWN-BUILTIN");
    }
}

#[test]
fn only_explicit_async_builtin_submission_is_accepted() {
    compile("SUBMIT ANALYTICS bfs ASYNC", &EmptySchemaCatalog).unwrap();
    let error = compile("SUBMIT ANALYTICS does_not_exist ASYNC", &EmptySchemaCatalog).unwrap_err();
    assert_eq!(error.code(), "DTG-LANG-UNKNOWN-BUILTIN");
}
