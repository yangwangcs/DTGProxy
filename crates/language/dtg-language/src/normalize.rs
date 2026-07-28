use std::collections::{BTreeMap, BTreeSet};

use dtg_kernel::{TransactionTime, ValidInterval, Value};
use dtg_language_ir::{
    AnalyticsExecutionMode, AnalyticsSubmission, BuiltInAlgorithmId, Expand, ExpandDirection,
    Field, GraphScope, LogicalExpr, LogicalMutation, LogicalNode, LogicalNodeId, LogicalNodeKind,
    LogicalPlan, LogicalProgram, LogicalStatement, LogicalType, LogicalWrite, NodeScan, Projection,
    ReadScope, RowSchema, TemporalScope, TimeExpr, ValidIntervalExpr, ValidTimeExpr,
    ValidTimePredicate,
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
    let (statement, result_schema) = match typed.statement {
        Statement::Boundary(Boundary::Begin) => {
            (LogicalStatement::BeginTransaction, RowSchema::empty())
        }
        Statement::Boundary(Boundary::Commit) => {
            (LogicalStatement::CommitTransaction, RowSchema::empty())
        }
        Statement::Boundary(Boundary::Rollback) => {
            (LogicalStatement::RollbackTransaction, RowSchema::empty())
        }
        Statement::SubmitAnalytics { algorithm } => (
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
            }),
            RowSchema::empty(),
        ),
        Statement::Query(query) => {
            let (plan, schema) = normalize_query(&query)?;
            (LogicalStatement::Query(plan), schema)
        }
        Statement::Write(write) => (
            LogicalStatement::Write(normalize_write(&write)?),
            RowSchema::empty(),
        ),
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
        result_schema,
    })
}

fn normalize_write(write: &crate::ast::Write) -> Result<LogicalWrite, LanguageError> {
    let (input, mutation) = match write {
        crate::ast::Write::Create { node, valid_from } => (
            None,
            LogicalMutation::CreateVertex {
                variable: node.variable.clone(),
                labels: node.labels.clone(),
                properties: logical_properties(&node.properties),
                valid_from: valid_expr(valid_from)?,
            },
        ),
        crate::ast::Write::CreateRelationship {
            matches,
            pattern,
            valid_from,
        } => {
            let relationship = &pattern.relationships[0];
            let left = &pattern.nodes[0].variable;
            let right = &pattern.nodes[1].variable;
            let (source, destination) = match relationship.direction {
                RelationshipDirection::Outgoing => (left.clone(), right.clone()),
                RelationshipDirection::Incoming => (right.clone(), left.clone()),
                RelationshipDirection::Either => unreachable!("semantic analysis rejects this"),
            };
            (
                Some(normalize_selection(&[], matches, None)?),
                LogicalMutation::CreateRelationship {
                    variable: relationship.variable.clone(),
                    relationship_type: relationship.types[0].clone(),
                    source,
                    destination,
                    properties: logical_properties(&relationship.properties),
                    valid_from: valid_expr(valid_from)?,
                },
            )
        }
        crate::ast::Write::Set {
            matches,
            variable,
            properties,
            valid_from,
        } => (
            Some(normalize_selection(&[], matches, None)?),
            LogicalMutation::SetProperties {
                variable: variable.clone(),
                properties: logical_properties(properties),
                valid_from: valid_expr(valid_from)?,
            },
        ),
        crate::ast::Write::Delete {
            matches,
            variable,
            valid_from,
        } => (
            Some(normalize_selection(&[], matches, None)?),
            LogicalMutation::Delete {
                variable: variable.clone(),
                valid_from: valid_expr(valid_from)?,
            },
        ),
    };
    Ok(LogicalWrite {
        input,
        mutations: vec![mutation],
    })
}

fn normalize_query(query: &crate::ast::Query) -> Result<(LogicalPlan, RowSchema), LanguageError> {
    let bindings = query_bindings(query);
    let (projections, result_schema) = normalize_projections(&query.returns, &bindings);
    let mut plan = normalize_selection(&query.scopes, &query.matches, query.where_clause.as_ref())?;
    if !projections.is_empty() {
        let id = LogicalNodeId::new(plan.nodes.len() as u32);
        plan.nodes.push(LogicalNode {
            id,
            kind: LogicalNodeKind::Project {
                input: plan.root,
                projections,
            },
        });
        plan.root = id;
    }
    Ok((plan, result_schema))
}

fn normalize_selection(
    scopes: &[Scope],
    matches: &[crate::ast::Match],
    where_clause: Option<&(Expr, Expr)>,
) -> Result<LogicalPlan, LanguageError> {
    let mut nodes = Vec::new();
    let mut root = None;
    let mut bound = BTreeSet::new();
    for matching in matches {
        let (valid, system) = effective_scope(scopes, &matching.scopes)?;
        let scope = read_scope(valid.as_ref(), system.as_ref())?;
        let pattern = &matching.pattern;
        let anchor = pattern
            .nodes
            .iter()
            .position(|node| bound.contains(&node.variable));
        let first = pattern.nodes.first().ok_or_else(|| {
            LanguageError::semantic("DTG-LANG-PATTERN", "MATCH pattern requires a node")
        })?;
        let correlated = anchor.is_some();
        let anchor_index = anchor.unwrap_or(0);
        let mut match_root = if correlated {
            root.expect("a bound anchor requires an existing root")
        } else {
            let id = LogicalNodeId::new(nodes.len() as u32);
            nodes.push(LogicalNode {
                id,
                kind: LogicalNodeKind::NodeScan(NodeScan {
                    variable: first.variable.clone(),
                    labels: first.labels.clone(),
                    read_scope: scope.clone(),
                }),
            });
            bound.insert(first.variable.clone());
            id
        };
        let anchor_node = &pattern.nodes[anchor_index];
        match_root = filter_properties(
            &mut nodes,
            match_root,
            &anchor_node.variable,
            &anchor_node.properties,
        );
        for index in anchor_index..pattern.relationships.len() {
            match_root = expand_edge(&mut nodes, match_root, pattern, index, false, &scope);
            bound.insert(pattern.relationships[index].variable.clone());
            bound.insert(pattern.nodes[index + 1].variable.clone());
        }
        for index in (0..anchor_index).rev() {
            match_root = expand_edge(&mut nodes, match_root, pattern, index, true, &scope);
            bound.insert(pattern.relationships[index].variable.clone());
            bound.insert(pattern.nodes[index].variable.clone());
        }
        root = Some(if correlated {
            match_root
        } else {
            match root {
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
            }
        });
    }
    let mut root = root.ok_or_else(|| {
        LanguageError::semantic("DTG-LANG-EMPTY-QUERY", "query requires a MATCH pattern")
    })?;
    if let Some((left, right)) = where_clause {
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
    Ok(LogicalPlan { root, nodes })
}

fn expand_edge(
    nodes: &mut Vec<LogicalNode>,
    input: LogicalNodeId,
    pattern: &crate::ast::Pattern,
    index: usize,
    reverse: bool,
    scope: &ReadScope,
) -> LogicalNodeId {
    let relationship = &pattern.relationships[index];
    let (source, destination, direction) = if reverse {
        (
            &pattern.nodes[index + 1],
            &pattern.nodes[index],
            reverse_direction(relationship.direction),
        )
    } else {
        (
            &pattern.nodes[index],
            &pattern.nodes[index + 1],
            relationship.direction,
        )
    };
    let id = LogicalNodeId::new(nodes.len() as u32);
    nodes.push(LogicalNode {
        id,
        kind: LogicalNodeKind::Expand(Expand {
            input,
            source: source.variable.clone(),
            relationship: relationship.variable.clone(),
            destination: destination.variable.clone(),
            direction: match direction {
                RelationshipDirection::Outgoing => ExpandDirection::Outgoing,
                RelationshipDirection::Incoming => ExpandDirection::Incoming,
                RelationshipDirection::Either => ExpandDirection::Either,
            },
            relationship_types: relationship.types.clone(),
            read_scope: scope.clone(),
        }),
    });
    let id = filter_properties(nodes, id, &relationship.variable, &relationship.properties);
    filter_properties(nodes, id, &destination.variable, &destination.properties)
}

fn reverse_direction(direction: RelationshipDirection) -> RelationshipDirection {
    match direction {
        RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
        RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
        RelationshipDirection::Either => RelationshipDirection::Either,
    }
}

fn logical_properties(properties: &BTreeMap<String, Expr>) -> BTreeMap<String, LogicalExpr> {
    properties
        .iter()
        .map(|(name, value)| (name.clone(), logical_expr(value)))
        .collect()
}

fn query_bindings(query: &crate::ast::Query) -> BTreeMap<String, LogicalType> {
    let mut bindings = BTreeMap::new();
    for matching in &query.matches {
        for node in &matching.pattern.nodes {
            bindings.insert(node.variable.clone(), LogicalType::Vertex);
        }
        for relationship in &matching.pattern.relationships {
            bindings.insert(relationship.variable.clone(), LogicalType::Relationship);
        }
    }
    bindings
}

fn normalize_projections(
    expressions: &[Expr],
    bindings: &BTreeMap<String, LogicalType>,
) -> (Vec<Projection>, RowSchema) {
    let mut aliases = BTreeSet::new();
    let mut projections = Vec::new();
    let mut fields = Vec::new();
    for (index, expression) in expressions.iter().enumerate() {
        let base = expression_alias(expression).unwrap_or_else(|| format!("expression_{index}"));
        let mut alias = base.clone();
        let mut suffix = 2;
        while !aliases.insert(alias.clone()) {
            alias = format!("{base}_{suffix}");
            suffix += 1;
        }
        let (data_type, nullable) = expression_type(expression, bindings);
        projections.push(Projection {
            expression: logical_expr(expression),
            alias: alias.clone(),
        });
        fields.push(Field {
            name: alias,
            data_type,
            nullable,
        });
    }
    (projections, RowSchema { fields })
}

fn expression_alias(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Parameter(name) => Some(format!("${name}")),
        Expr::Column(name) => Some(name.clone()),
        Expr::Property { input, name } => Some(format!("{input}.{name}")),
        _ => None,
    }
}

fn expression_type(
    expression: &Expr,
    bindings: &BTreeMap<String, LogicalType>,
) -> (LogicalType, bool) {
    match expression {
        Expr::Parameter(_) => (LogicalType::Any, true),
        Expr::Integer(_) => (LogicalType::Integer, false),
        Expr::String(_) => (LogicalType::String, false),
        Expr::Boolean(_) => (LogicalType::Boolean, false),
        Expr::Null => (LogicalType::Null, true),
        Expr::Column(name) => (
            bindings.get(name).cloned().unwrap_or(LogicalType::Any),
            false,
        ),
        Expr::Property { .. } => (LogicalType::Any, true),
    }
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
