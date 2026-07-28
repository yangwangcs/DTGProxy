use std::collections::BTreeSet;
use std::fmt;

use crate::{
    LogicalExpr, LogicalMutation, LogicalNodeId, LogicalNodeKind, LogicalPlan, LogicalProgram,
    LogicalStatement, TimeExpr, ValidIntervalExpr, ValidTimeExpr, ValidTimePredicate,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct IrVersion {
    major: u16,
    minor: u16,
}

impl IrVersion {
    pub const CURRENT: Self = Self { major: 1, minor: 0 };

    pub const fn major(self) -> u16 {
        self.major
    }

    pub const fn minor(self) -> u16 {
        self.minor
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IrError {
    UnsupportedVersion(IrVersion),
    EmptyQuery,
    DuplicateParameter(String),
    DuplicateResultField(String),
    UnknownParameter(String),
    DuplicateNode(LogicalNodeId),
    MissingNode(LogicalNodeId),
}

impl fmt::Display for IrError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported logical IR version {}.{}",
                version.major(),
                version.minor()
            ),
            Self::EmptyQuery => formatter.write_str("logical query must contain a root node"),
            Self::DuplicateParameter(name) => write!(formatter, "duplicate parameter: {name}"),
            Self::DuplicateResultField(name) => write!(formatter, "duplicate result field: {name}"),
            Self::UnknownParameter(name) => write!(formatter, "unknown parameter: {name}"),
            Self::DuplicateNode(id) => write!(formatter, "duplicate logical node: {}", id.get()),
            Self::MissingNode(id) => write!(formatter, "unknown logical node: {}", id.get()),
        }
    }
}

impl std::error::Error for IrError {}

pub fn validate_program(program: &LogicalProgram) -> Result<(), IrError> {
    if program.version != IrVersion::CURRENT {
        return Err(IrError::UnsupportedVersion(program.version));
    }

    let parameters = unique_names(
        program
            .parameters
            .iter()
            .map(|parameter| parameter.name.as_str()),
        true,
    )?;
    unique_names(
        program
            .result_schema
            .fields
            .iter()
            .map(|field| field.name.as_str()),
        false,
    )?;

    match &program.statement {
        LogicalStatement::Query(plan) => validate_plan(plan, &parameters),
        LogicalStatement::SubmitAnalytics(submission) => {
            unique_names(
                submission
                    .result_schema
                    .fields
                    .iter()
                    .map(|field| field.name.as_str()),
                false,
            )?;
            validate_read_scope(&submission.read_scope, &parameters)?;
            for input in submission.arguments.values() {
                validate_expr(input, &parameters)?;
            }
            Ok(())
        }
        LogicalStatement::Write(write) => validate_write(write, &parameters),
        LogicalStatement::BeginTransaction
        | LogicalStatement::CommitTransaction
        | LogicalStatement::RollbackTransaction => Ok(()),
    }
}

fn unique_names<'a>(
    names: impl Iterator<Item = &'a str>,
    parameter: bool,
) -> Result<BTreeSet<&'a str>, IrError> {
    let mut unique = BTreeSet::new();
    for name in names {
        if !unique.insert(name) {
            return if parameter {
                Err(IrError::DuplicateParameter(name.to_owned()))
            } else {
                Err(IrError::DuplicateResultField(name.to_owned()))
            };
        }
    }
    Ok(unique)
}

fn validate_plan(plan: &LogicalPlan, parameters: &BTreeSet<&str>) -> Result<(), IrError> {
    if plan.nodes.is_empty() {
        return Err(IrError::EmptyQuery);
    }

    let node_ids: BTreeSet<_> = plan.nodes.iter().map(|node| node.id).collect();
    if node_ids.len() != plan.nodes.len() {
        let mut seen = BTreeSet::new();
        for node in &plan.nodes {
            if !seen.insert(node.id) {
                return Err(IrError::DuplicateNode(node.id));
            }
        }
    }
    if !node_ids.contains(&plan.root) {
        return Err(IrError::MissingNode(plan.root));
    }

    for node in &plan.nodes {
        validate_node(&node.kind, &node_ids, parameters)?;
    }
    Ok(())
}

fn validate_node(
    node: &LogicalNodeKind,
    node_ids: &BTreeSet<LogicalNodeId>,
    parameters: &BTreeSet<&str>,
) -> Result<(), IrError> {
    let require = |id| {
        node_ids
            .contains(&id)
            .then_some(())
            .ok_or(IrError::MissingNode(id))
    };
    match node {
        LogicalNodeKind::NodeScan(scan) => validate_read_scope(&scan.read_scope, parameters),
        LogicalNodeKind::RelationshipScan(scan) => {
            validate_read_scope(&scan.read_scope, parameters)
        }
        LogicalNodeKind::VertexLookup(lookup) => {
            validate_expr(&lookup.id, parameters)?;
            validate_read_scope(&lookup.read_scope, parameters)
        }
        LogicalNodeKind::RelationshipLookup(lookup) => {
            validate_expr(&lookup.id, parameters)?;
            validate_read_scope(&lookup.read_scope, parameters)
        }
        LogicalNodeKind::Expand(expand) => {
            require(expand.input)?;
            validate_read_scope(&expand.read_scope, parameters)
        }
        LogicalNodeKind::Filter { input, predicate } => {
            require(*input)?;
            validate_expr(predicate, parameters)
        }
        LogicalNodeKind::Project { input, projections } => {
            require(*input)?;
            validate_projections(projections, parameters)
        }
        LogicalNodeKind::Aggregate(aggregate) => {
            require(aggregate.input)?;
            validate_projections(&aggregate.groups, parameters)?;
            for function in &aggregate.aggregates {
                if let Some(argument) = &function.argument {
                    validate_expr(argument, parameters)?;
                }
            }
            Ok(())
        }
        LogicalNodeKind::Sort(sort) => {
            require(sort.input)?;
            for key in &sort.keys {
                validate_expr(&key.expression, parameters)?;
            }
            Ok(())
        }
        LogicalNodeKind::Limit(limit) => {
            require(limit.input)?;
            if let Some(skip) = &limit.skip {
                validate_expr(skip, parameters)?;
            }
            if let Some(limit) = &limit.limit {
                validate_expr(limit, parameters)?;
            }
            Ok(())
        }
        LogicalNodeKind::Unwind(unwind) => {
            require(unwind.input)?;
            validate_expr(&unwind.expression, parameters)
        }
        LogicalNodeKind::Join(join) => {
            require(join.left)?;
            require(join.right)?;
            if let Some(predicate) = &join.predicate {
                validate_expr(predicate, parameters)?;
            }
            Ok(())
        }
        LogicalNodeKind::Subquery(subquery) => {
            if let Some(input) = subquery.input {
                require(input)?;
            }
            validate_plan(&subquery.plan, parameters)
        }
    }
}

fn validate_projections(
    projections: &[crate::Projection],
    parameters: &BTreeSet<&str>,
) -> Result<(), IrError> {
    for projection in projections {
        validate_expr(&projection.expression, parameters)?;
    }
    Ok(())
}

fn validate_write(write: &crate::LogicalWrite, parameters: &BTreeSet<&str>) -> Result<(), IrError> {
    for mutation in &write.mutations {
        let properties = match mutation {
            LogicalMutation::CreateVertex { properties, .. }
            | LogicalMutation::CreateRelationship { properties, .. }
            | LogicalMutation::SetProperties { properties, .. } => Some(properties),
            LogicalMutation::Delete { .. } => None,
        };
        if let Some(properties) = properties {
            for expression in properties.values() {
                validate_expr(expression, parameters)?;
            }
        }
    }
    Ok(())
}

fn validate_expr(expression: &LogicalExpr, parameters: &BTreeSet<&str>) -> Result<(), IrError> {
    match expression {
        LogicalExpr::Literal(_) | LogicalExpr::Column(_) => Ok(()),
        LogicalExpr::Parameter(name) if parameters.contains(name.as_str()) => Ok(()),
        LogicalExpr::Parameter(name) => Err(IrError::UnknownParameter(name.clone())),
        LogicalExpr::Property { input, .. } | LogicalExpr::Unary { input, .. } => {
            validate_expr(input, parameters)
        }
        LogicalExpr::Binary { left, right, .. } => {
            validate_expr(left, parameters)?;
            validate_expr(right, parameters)
        }
        LogicalExpr::List(values) => {
            for value in values {
                validate_expr(value, parameters)?;
            }
            Ok(())
        }
        LogicalExpr::Map(values) => {
            for (_, value) in values {
                validate_expr(value, parameters)?;
            }
            Ok(())
        }
    }
}

fn validate_read_scope(
    scope: &crate::ReadScope,
    parameters: &BTreeSet<&str>,
) -> Result<(), IrError> {
    validate_scope(&scope.transaction_time, parameters)?;
    match &scope.valid_time {
        None => Ok(()),
        Some(ValidTimePredicate::At(time)) => validate_valid_time(time, parameters),
        Some(ValidTimePredicate::Overlaps(interval)) => {
            validate_valid_interval(interval, parameters)
        }
    }
}

fn validate_scope(
    scope: &crate::TemporalScope,
    parameters: &BTreeSet<&str>,
) -> Result<(), IrError> {
    match scope {
        crate::TemporalScope::Current => Ok(()),
        crate::TemporalScope::AsOf(time) => validate_time(time, parameters),
        crate::TemporalScope::Changes { from, to } => {
            validate_time(from, parameters)?;
            validate_time(to, parameters)
        }
    }
}

fn validate_valid_time(time: &ValidTimeExpr, parameters: &BTreeSet<&str>) -> Result<(), IrError> {
    match time {
        ValidTimeExpr::Literal(_) => Ok(()),
        ValidTimeExpr::Parameter(name) if parameters.contains(name.as_str()) => Ok(()),
        ValidTimeExpr::Parameter(name) => Err(IrError::UnknownParameter(name.clone())),
    }
}

fn validate_valid_interval(
    interval: &ValidIntervalExpr,
    parameters: &BTreeSet<&str>,
) -> Result<(), IrError> {
    match interval {
        ValidIntervalExpr::Literal(_) => Ok(()),
        ValidIntervalExpr::Parameter(name) if parameters.contains(name.as_str()) => Ok(()),
        ValidIntervalExpr::Parameter(name) => Err(IrError::UnknownParameter(name.clone())),
    }
}

fn validate_time(time: &TimeExpr, parameters: &BTreeSet<&str>) -> Result<(), IrError> {
    match time {
        TimeExpr::Literal(_) => Ok(()),
        TimeExpr::Parameter(name) if parameters.contains(name.as_str()) => Ok(()),
        TimeExpr::Parameter(name) => Err(IrError::UnknownParameter(name.clone())),
    }
}
