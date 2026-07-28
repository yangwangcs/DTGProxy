use dtg_kernel::{TransactionTime, ValidInterval, Value};
use dtg_language_ir::{
    AnalyticsExecutionMode, AnalyticsSubmission, BuiltInAlgorithmId, Expand, ExpandDirection,
    GraphScope, LogicalExpr, LogicalMutation, LogicalNode, LogicalNodeId, LogicalNodeKind,
    LogicalPlan, LogicalProgram, LogicalStatement, LogicalWrite, NodeScan, Projection, ReadScope,
    TemporalScope, TimeExpr, ValidIntervalExpr, ValidTimeExpr, ValidTimePredicate,
};

use crate::{
    LanguageError,
    ast::{Boundary, Expr, Mode, RelationshipDirection, Scope, Statement},
    sema::{TypedProgram, effective_scope},
};

pub(crate) fn normalize(typed: TypedProgram) -> Result<LogicalProgram, LanguageError> {
    let graph_scope = typed
        .graph
        .map(GraphScope::Explicit)
        .unwrap_or(GraphScope::SessionDefault);
    let statement = match typed.statement {
        Statement::Boundary(Boundary::Begin) => LogicalStatement::BeginTransaction,
        Statement::Boundary(Boundary::Commit) => LogicalStatement::CommitTransaction,
        Statement::Boundary(Boundary::Rollback) => LogicalStatement::RollbackTransaction,
        Statement::SubmitAnalytics { algorithm } => {
            LogicalStatement::SubmitAnalytics(AnalyticsSubmission {
                algorithm: BuiltInAlgorithmId::try_from(algorithm.as_str()).map_err(|_| {
                    LanguageError::semantic(
                        "DTG-LANG-UNKNOWN-BUILTIN",
                        "unknown built-in analytics",
                    )
                })?,
                execution_mode: AnalyticsExecutionMode::Asynchronous,
                read_scope: ReadScope::current(),
                arguments: Default::default(),
                result_schema: Default::default(),
                request_identity: None,
            })
        }
        Statement::Query(query) => LogicalStatement::Query(normalize_query(&query)?),
        Statement::Write(write) => LogicalStatement::Write(normalize_write(&write)?),
        Statement::Procedure { .. } => {
            return Err(LanguageError::semantic(
                "DTG-LANG-INTERNAL",
                "semantic analysis accepted an unsupported statement",
            ));
        }
    };
    Ok(LogicalProgram {
        version: dtg_language_ir::IrVersion::CURRENT,
        graph_scope,
        parameters: typed.parameters,
        statement,
        result_schema: Default::default(),
    })
}

fn normalize_write(write: &crate::ast::Write) -> Result<LogicalWrite, LanguageError> {
    let mutation = match write {
        crate::ast::Write::Create { node, valid_from } => LogicalMutation::CreateVertex {
            variable: node.variable.clone(),
            labels: node.labels.clone(),
            properties: node
                .properties
                .iter()
                .map(|(name, value)| (name.clone(), logical_expr(value)))
                .collect(),
            valid_from: valid_expr(valid_from)?,
        },
        crate::ast::Write::Set {
            variable,
            properties,
            valid_from,
        } => LogicalMutation::SetProperties {
            variable: variable.clone(),
            properties: properties
                .iter()
                .map(|(name, value)| (name.clone(), logical_expr(value)))
                .collect(),
            valid_from: valid_expr(valid_from)?,
        },
        crate::ast::Write::Delete {
            variable,
            valid_from,
        } => LogicalMutation::Delete {
            variable: variable.clone(),
            valid_from: valid_expr(valid_from)?,
        },
    };
    Ok(LogicalWrite {
        mutations: vec![mutation],
    })
}

fn normalize_query(query: &crate::ast::Query) -> Result<LogicalPlan, LanguageError> {
    let mut nodes = Vec::new();
    let mut root = None;
    for matching in &query.matches {
        let (valid, system) = effective_scope(&query.scopes, &matching.scopes)?;
        let scope = read_scope(valid.as_ref(), system.as_ref())?;
        let first = matching.pattern.nodes.first().ok_or_else(|| {
            LanguageError::semantic("DTG-LANG-PATTERN", "MATCH pattern requires a node")
        })?;
        let id = LogicalNodeId::new(nodes.len() as u32);
        nodes.push(LogicalNode {
            id,
            kind: LogicalNodeKind::NodeScan(NodeScan {
                variable: first.variable.clone(),
                labels: first.labels.clone(),
                read_scope: scope.clone(),
            }),
        });
        let mut match_root = filter_properties(&mut nodes, id, &first.variable, &first.properties);
        for (index, relationship) in matching.pattern.relationships.iter().enumerate() {
            let destination = &matching.pattern.nodes[index + 1];
            let expand_id = LogicalNodeId::new(nodes.len() as u32);
            nodes.push(LogicalNode {
                id: expand_id,
                kind: LogicalNodeKind::Expand(Expand {
                    input: match_root,
                    source: matching.pattern.nodes[index].variable.clone(),
                    relationship: relationship.variable.clone(),
                    destination: destination.variable.clone(),
                    direction: match relationship.direction {
                        RelationshipDirection::Outgoing => ExpandDirection::Outgoing,
                        RelationshipDirection::Incoming => ExpandDirection::Incoming,
                        RelationshipDirection::Either => ExpandDirection::Either,
                    },
                    relationship_types: relationship.types.clone(),
                    read_scope: scope.clone(),
                }),
            });
            match_root = filter_properties(
                &mut nodes,
                expand_id,
                &destination.variable,
                &destination.properties,
            );
        }
        root = Some(match root {
            None => match_root,
            Some(left) => {
                let id = LogicalNodeId::new(nodes.len() as u32);
                nodes.push(LogicalNode {
                    id,
                    kind: LogicalNodeKind::Join(dtg_language_ir::Join {
                        left,
                        right: match_root,
                        kind: dtg_language_ir::JoinKind::Inner,
                        predicate: None,
                    }),
                });
                id
            }
        });
    }
    let mut root = root.ok_or_else(|| {
        LanguageError::semantic("DTG-LANG-EMPTY-QUERY", "query requires a MATCH pattern")
    })?;
    if let Some((left, right)) = &query.where_clause {
        let id = LogicalNodeId::new(nodes.len() as u32);
        nodes.push(LogicalNode {
            id,
            kind: LogicalNodeKind::Filter {
                input: root,
                predicate: LogicalExpr::Binary {
                    left: Box::new(logical_expr(left)),
                    operator: dtg_language_ir::BinaryOperator::Equal,
                    right: Box::new(logical_expr(right)),
                },
            },
        });
        root = id;
    }
    if !query.returns.is_empty() {
        let id = LogicalNodeId::new(nodes.len() as u32);
        let projections = query
            .returns
            .iter()
            .enumerate()
            .map(|(index, expression)| Projection {
                expression: logical_expr(expression),
                alias: format!("column_{index}"),
            })
            .collect();
        nodes.push(LogicalNode {
            id,
            kind: LogicalNodeKind::Project {
                input: root,
                projections,
            },
        });
        root = id;
    }
    Ok(LogicalPlan { root, nodes })
}
fn filter_properties(
    nodes: &mut Vec<LogicalNode>,
    input: LogicalNodeId,
    variable: &str,
    properties: &std::collections::BTreeMap<String, Expr>,
) -> LogicalNodeId {
    let mut predicate = None;
    for (name, value) in properties {
        let equality = LogicalExpr::Binary {
            left: Box::new(LogicalExpr::Property {
                input: Box::new(LogicalExpr::Column(variable.to_owned())),
                name: name.clone(),
            }),
            operator: dtg_language_ir::BinaryOperator::Equal,
            right: Box::new(logical_expr(value)),
        };
        predicate = Some(match predicate {
            None => equality,
            Some(previous) => LogicalExpr::Binary {
                left: Box::new(previous),
                operator: dtg_language_ir::BinaryOperator::And,
                right: Box::new(equality),
            },
        });
    }
    match predicate {
        None => input,
        Some(predicate) => {
            let id = LogicalNodeId::new(nodes.len() as u32);
            nodes.push(LogicalNode {
                id,
                kind: LogicalNodeKind::Filter { input, predicate },
            });
            id
        }
    }
}
fn read_scope(valid: Option<&Scope>, system: Option<&Scope>) -> Result<ReadScope, LanguageError> {
    let transaction_time = match system {
        None => TemporalScope::Current,
        Some(Scope {
            mode: Mode::AsOf(value),
            ..
        }) => TemporalScope::AsOf(time_expr(value)?),
        Some(Scope {
            mode: Mode::Changes(from, to),
            ..
        }) => TemporalScope::Changes {
            from: time_expr(from)?,
            to: time_expr(to)?,
        },
        _ => {
            return Err(LanguageError::semantic(
                "DTG-LANG-SCOPE",
                "invalid SYSTEM_TIME scope",
            ));
        }
    };
    let valid_time = match valid {
        None => None,
        Some(Scope {
            mode: Mode::AsOf(value),
            ..
        }) => Some(ValidTimePredicate::At(valid_expr(value)?)),
        Some(Scope {
            mode: Mode::Between(start, end),
            ..
        }) => Some(ValidTimePredicate::Overlaps(valid_interval(start, end)?)),
        Some(Scope {
            mode: Mode::Changes(from, to),
            ..
        }) => Some(ValidTimePredicate::Changes {
            from: valid_expr(from)?,
            to: valid_expr(to)?,
        }),
    };
    Ok(ReadScope {
        transaction_time,
        valid_time,
    })
}
fn time_expr(expr: &Expr) -> Result<TimeExpr, LanguageError> {
    match expr {
        Expr::Parameter(name) => Ok(TimeExpr::Parameter(name.clone())),
        Expr::Integer(value) => TransactionTime::new(*value)
            .map(TimeExpr::Literal)
            .map_err(|_| {
                LanguageError::semantic("DTG-LANG-TIME", "SYSTEM_TIME must be non-negative")
            }),
        _ => Err(LanguageError::semantic(
            "DTG-LANG-TYPE",
            "SYSTEM_TIME expression must be an integer or parameter",
        )),
    }
}
fn valid_expr(expr: &Expr) -> Result<ValidTimeExpr, LanguageError> {
    match expr {
        Expr::Parameter(name) => Ok(ValidTimeExpr::Parameter(name.clone())),
        Expr::Integer(value) => Ok(ValidTimeExpr::Literal(*value)),
        _ => Err(LanguageError::semantic(
            "DTG-LANG-TYPE",
            "VALID_TIME expression must be an integer or parameter",
        )),
    }
}
fn valid_interval(start: &Expr, end: &Expr) -> Result<ValidIntervalExpr, LanguageError> {
    match (start, end) {
        (Expr::Integer(start), Expr::Integer(end)) => ValidInterval::new(*start, *end)
            .map(ValidIntervalExpr::Literal)
            .map_err(|_| {
                LanguageError::semantic(
                    "DTG-LANG-INVALID-INTERVAL",
                    "VALID_TIME BETWEEN requires start < end",
                )
            }),
        _ => Ok(ValidIntervalExpr::Bounds {
            start: valid_expr(start)?,
            end: valid_expr(end)?,
        }),
    }
}
fn logical_expr(expr: &Expr) -> LogicalExpr {
    match expr {
        Expr::Parameter(name) => LogicalExpr::Parameter(name.clone()),
        Expr::Integer(value) => LogicalExpr::Literal(Value::Integer(*value)),
        Expr::String(value) => LogicalExpr::Literal(Value::String(value.clone())),
        Expr::Boolean(value) => LogicalExpr::Literal(Value::Boolean(*value)),
        Expr::Null => LogicalExpr::Literal(Value::Null),
        Expr::Column(value) => LogicalExpr::Column(value.clone()),
        Expr::Property { input, name } => LogicalExpr::Property {
            input: Box::new(LogicalExpr::Column(input.clone())),
            name: name.clone(),
        },
    }
}
