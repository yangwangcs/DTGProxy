use std::collections::BTreeSet;

use dtg_language_ir::{GraphId, LogicalType, Parameter};

use crate::{
    LanguageError, SchemaCatalog,
    ast::{Axis, Expr, Mode, Program, Scope, Statement},
};

pub(crate) struct TypedProgram {
    pub(crate) graph: Option<GraphId>,
    pub(crate) parameters: Vec<Parameter>,
    pub(crate) statement: Statement,
}

pub(crate) fn analyze(
    program: Program,
    catalog: &dyn SchemaCatalog,
) -> Result<TypedProgram, LanguageError> {
    let graph = match program.graph {
        Some(name) => Some(catalog.graph_id(&name).ok_or_else(|| {
            LanguageError::semantic("DTG-LANG-UNKNOWN-GRAPH", format!("unknown graph: {name}"))
        })?),
        None => None,
    };
    let mut parameters = BTreeSet::new();
    collect_statement_parameters(&program.statement, &mut parameters);
    validate_statement(&program.statement)?;
    Ok(TypedProgram {
        graph,
        parameters: parameters
            .into_iter()
            .map(|name| Parameter {
                name,
                data_type: LogicalType::Any,
                required: true,
            })
            .collect(),
        statement: program.statement,
    })
}

fn validate_statement(statement: &Statement) -> Result<(), LanguageError> {
    match statement {
        Statement::Procedure { name } => Err(LanguageError::semantic(
            "DTG-LANG-UNKNOWN-BUILTIN",
            format!("procedure '{name}' is not a built-in analytics submission"),
        )),
        Statement::SubmitAnalytics { algorithm } => {
            dtg_language_ir::BuiltInAlgorithmId::try_from(algorithm.as_str()).map_err(|_| {
                LanguageError::semantic(
                    "DTG-LANG-UNKNOWN-BUILTIN",
                    format!("unknown built-in analytics: {algorithm}"),
                )
            })?;
            Ok(())
        }
        Statement::Query(query) => {
            validate_scopes(&query.scopes)?;
            for matching in &query.matches {
                validate_scopes(&matching.scopes)?;
            }
            Ok(())
        }
        Statement::Write(write) => {
            let time = match write {
                crate::ast::Write::Create { valid_from, .. } => valid_from,
                crate::ast::Write::Set { valid_from, .. }
                | crate::ast::Write::Delete { valid_from, .. } => valid_from,
            };
            if matches!(time, Expr::Integer(_)) || matches!(time, Expr::Parameter(_)) {
                return Ok(());
            }
            Err(LanguageError::semantic(
                "DTG-LANG-TYPE",
                "VALID FROM must be an integer timestamp or parameter",
            ))
        }
        Statement::Boundary(_) => Ok(()),
    }
}

fn validate_scopes(scopes: &[Scope]) -> Result<(), LanguageError> {
    let mut axes = BTreeSet::new();
    for scope in scopes {
        if !axes.insert(scope.axis as u8) {
            return Err(LanguageError::semantic(
                "DTG-LANG-DUPLICATE-SCOPE",
                "at most one temporal scope per axis is allowed",
            ));
        }
        if scope.axis == Axis::System && matches!(scope.mode, Mode::Between(_, _)) {
            return Err(LanguageError::semantic(
                "DTG-LANG-SCOPE",
                "SYSTEM_TIME supports only AS OF or CHANGES",
            ));
        }
        if let Mode::Between(Expr::Integer(start), Expr::Integer(end))
        | Mode::Changes(Expr::Integer(start), Expr::Integer(end)) = &scope.mode
            && start >= end
        {
            return Err(LanguageError::semantic(
                "DTG-LANG-INVALID-INTERVAL",
                "temporal interval must have start < end",
            ));
        }
    }
    Ok(())
}

fn collect_statement_parameters(statement: &Statement, output: &mut BTreeSet<String>) {
    match statement {
        Statement::Query(query) => {
            for scope in &query.scopes {
                collect_scope(scope, output);
            }
            for matching in &query.matches {
                for scope in &matching.scopes {
                    collect_scope(scope, output);
                }
                for node in &matching.pattern.nodes {
                    for value in node.properties.values() {
                        collect_expr(value, output);
                    }
                }
            }
            for value in &query.returns {
                collect_expr(value, output);
            }
            if let Some((left, right)) = &query.where_clause {
                collect_expr(left, output);
                collect_expr(right, output);
            }
        }
        Statement::Write(write) => match write {
            crate::ast::Write::Create { node, valid_from } => {
                for value in node.properties.values() {
                    collect_expr(value, output);
                }
                collect_expr(valid_from, output);
            }
            crate::ast::Write::Set {
                properties,
                valid_from,
                ..
            } => {
                for value in properties.values() {
                    collect_expr(value, output);
                }
                collect_expr(valid_from, output);
            }
            crate::ast::Write::Delete { valid_from, .. } => collect_expr(valid_from, output),
        },
        Statement::Boundary(_)
        | Statement::SubmitAnalytics { .. }
        | Statement::Procedure { .. } => {}
    }
}
fn collect_scope(scope: &Scope, output: &mut BTreeSet<String>) {
    match &scope.mode {
        Mode::AsOf(value) => collect_expr(value, output),
        Mode::Between(left, right) | Mode::Changes(left, right) => {
            collect_expr(left, output);
            collect_expr(right, output);
        }
    }
}
fn collect_expr(expr: &Expr, output: &mut BTreeSet<String>) {
    if let Expr::Parameter(name) = expr {
        output.insert(name.clone());
    }
}

pub(crate) fn effective_scope(
    defaults: &[Scope],
    overrides: &[Scope],
) -> Result<(Option<Scope>, Option<Scope>), LanguageError> {
    let mut valid = None;
    let mut system = None;
    for scope in defaults.iter().chain(overrides) {
        match scope.axis {
            Axis::Valid => valid = Some(scope.clone()),
            Axis::System => system = Some(scope.clone()),
        }
    }
    Ok((valid, system))
}
