use std::collections::{BTreeMap, BTreeSet};

use dtg_language_ir::{GraphId, LogicalType, Parameter};

use crate::{
    LanguageError, SchemaCatalog,
    ast::{
        Axis, Expr, Match, Mode, Pattern, Program, RelationshipDirection, Scope, Statement, Write,
    },
};

pub(crate) struct TypedProgram {
    pub(crate) graph: Option<GraphId>,
    pub(crate) parameters: Vec<Parameter>,
    pub(crate) statement: Statement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BindingKind {
    Node,
    Relationship,
}

#[derive(Clone, Debug)]
struct Binding {
    kind: BindingKind,
    constraints: BTreeSet<String>,
    read_scope: EffectiveReadScope,
}

type Bindings = BTreeMap<String, Binding>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct EffectiveReadScope {
    valid: Option<Scope>,
    system: Option<Scope>,
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
            validate_statement_changes_axes(&query.scopes, &query.matches)?;
            let mut bindings = Bindings::new();
            validate_matches(&query.matches, &query.scopes, &mut bindings)?;
            if let Some((left, right)) = &query.where_clause {
                validate_expr(left, &bindings)?;
                validate_expr(right, &bindings)?;
            }
            for expression in &query.returns {
                validate_expr(expression, &bindings)?;
            }
            for key in &query.order_by {
                validate_expr(&key.expression, &bindings)?;
            }
            Ok(())
        }
        Statement::Write(write) => validate_write(write),
        Statement::Boundary(_) => Ok(()),
    }
}

fn validate_write(write: &Write) -> Result<(), LanguageError> {
    let valid_from = match write {
        Write::Create { node, valid_from } => {
            let bindings = Bindings::new();
            for expression in node.properties.values() {
                validate_expr(expression, &bindings)?;
            }
            valid_from
        }
        Write::CreateRelationship {
            matches,
            pattern,
            valid_from,
        } => {
            let mut bindings = Bindings::new();
            reject_historical_write_selection(matches)?;
            validate_matches(matches, &[], &mut bindings)?;
            validate_relationship_create(pattern, &bindings)?;
            valid_from
        }
        Write::Set {
            matches,
            variable,
            properties,
            valid_from,
        } => {
            let mut bindings = Bindings::new();
            reject_historical_write_selection(matches)?;
            validate_matches(matches, &[], &mut bindings)?;
            require_binding(variable, &bindings)?;
            for expression in properties.values() {
                validate_expr(expression, &bindings)?;
            }
            valid_from
        }
        Write::Delete {
            matches,
            variable,
            valid_from,
        } => {
            let mut bindings = Bindings::new();
            reject_historical_write_selection(matches)?;
            validate_matches(matches, &[], &mut bindings)?;
            require_binding(variable, &bindings)?;
            valid_from
        }
    };
    if matches!(valid_from, Expr::Integer(_) | Expr::Parameter(_)) {
        Ok(())
    } else {
        Err(LanguageError::semantic(
            "DTG-LANG-TYPE",
            "VALID FROM must be an integer timestamp or parameter",
        ))
    }
}

fn reject_historical_write_selection(matches: &[Match]) -> Result<(), LanguageError> {
    if matches
        .iter()
        .flat_map(|matching| &matching.scopes)
        .any(|scope| scope.axis == Axis::System)
    {
        return Err(LanguageError::semantic(
            "DTG-LANG-HISTORICAL-WRITE-SELECTION",
            "write-selection MATCH clauses cannot override SYSTEM_TIME",
        ));
    }
    Ok(())
}

fn validate_matches(
    matches: &[Match],
    defaults: &[Scope],
    bindings: &mut Bindings,
) -> Result<(), LanguageError> {
    for matching in matches {
        validate_scopes(&matching.scopes)?;
        let (valid, system) = effective_scope(defaults, &matching.scopes)?;
        let read_scope = EffectiveReadScope { valid, system };
        validate_correlation(&matching.pattern, bindings, &read_scope)?;
        validate_pattern(&matching.pattern, bindings, &read_scope)?;
    }
    Ok(())
}

fn validate_correlation(
    pattern: &Pattern,
    bindings: &Bindings,
    read_scope: &EffectiveReadScope,
) -> Result<(), LanguageError> {
    validate_current_pattern_uniqueness(pattern)?;

    let mut prebound = BTreeSet::new();
    for variable in pattern
        .nodes
        .iter()
        .map(|node| node.variable.as_str())
        .chain(
            pattern
                .relationships
                .iter()
                .map(|relationship| relationship.variable.as_str()),
        )
    {
        if bindings.contains_key(variable) {
            prebound.insert(variable);
        }
    }

    if prebound.len() > 1
        || prebound.iter().any(|variable| {
            bindings
                .get(*variable)
                .is_some_and(|binding| binding.kind == BindingKind::Relationship)
        })
    {
        return Err(LanguageError::semantic(
            "DTG-LANG-UNSUPPORTED-CORRELATION",
            "logical IR cannot faithfully express this MATCH correlation",
        ));
    }

    if let Some(variable) = prebound.first()
        && bindings
            .get(*variable)
            .is_some_and(|binding| binding.read_scope != *read_scope)
    {
        return Err(LanguageError::semantic(
            "DTG-LANG-INCOMPATIBLE-SCOPE",
            format!("incompatible temporal scope for reused binding '{variable}'"),
        ));
    }
    Ok(())
}

fn validate_current_pattern_uniqueness(pattern: &Pattern) -> Result<(), LanguageError> {
    let mut current = BTreeMap::new();
    for (variable, kind) in pattern
        .nodes
        .iter()
        .map(|node| (node.variable.as_str(), BindingKind::Node))
        .chain(
            pattern
                .relationships
                .iter()
                .map(|relationship| (relationship.variable.as_str(), BindingKind::Relationship)),
        )
    {
        if let Some(seen) = current.get(variable) {
            if *seen == kind {
                return Err(LanguageError::semantic(
                    "DTG-LANG-UNSUPPORTED-CORRELATION",
                    "logical IR cannot faithfully express repeated bindings within one pattern",
                ));
            }
        } else {
            current.insert(variable, kind);
        }
    }
    Ok(())
}

fn validate_pattern(
    pattern: &Pattern,
    bindings: &mut Bindings,
    read_scope: &EffectiveReadScope,
) -> Result<(), LanguageError> {
    let first = pattern.nodes.first().ok_or_else(|| {
        LanguageError::semantic("DTG-LANG-PATTERN", "MATCH pattern requires a node")
    })?;
    bind(
        bindings,
        &first.variable,
        BindingKind::Node,
        &first.labels,
        read_scope,
    )?;
    validate_properties(&first.properties, bindings)?;

    for (index, relationship) in pattern.relationships.iter().enumerate() {
        bind(
            bindings,
            &relationship.variable,
            BindingKind::Relationship,
            &relationship.types,
            read_scope,
        )?;
        validate_properties(&relationship.properties, bindings)?;
        let destination = &pattern.nodes[index + 1];
        bind(
            bindings,
            &destination.variable,
            BindingKind::Node,
            &destination.labels,
            read_scope,
        )?;
        validate_properties(&destination.properties, bindings)?;
    }
    Ok(())
}

fn bind(
    bindings: &mut Bindings,
    variable: &str,
    kind: BindingKind,
    constraints: &[String],
    read_scope: &EffectiveReadScope,
) -> Result<(), LanguageError> {
    if let Some(existing) = bindings.get(variable) {
        let adds_constraints = constraints
            .iter()
            .any(|constraint| !existing.constraints.contains(constraint));
        if existing.kind != kind || adds_constraints {
            return Err(LanguageError::semantic(
                "DTG-LANG-CONFLICTING-BINDING",
                format!("conflicting binding for variable '{variable}'"),
            ));
        }
        return Ok(());
    }
    bindings.insert(
        variable.to_owned(),
        Binding {
            kind,
            constraints: constraints.iter().cloned().collect(),
            read_scope: read_scope.clone(),
        },
    );
    Ok(())
}

fn validate_relationship_create(
    pattern: &Pattern,
    bindings: &Bindings,
) -> Result<(), LanguageError> {
    if pattern.nodes.len() != 2 || pattern.relationships.len() != 1 {
        return Err(LanguageError::semantic(
            "DTG-LANG-CREATE-RELATIONSHIP",
            "relationship CREATE requires exactly one relationship between two bound nodes",
        ));
    }
    let relationship = &pattern.relationships[0];
    if relationship.types.len() != 1 {
        return Err(LanguageError::semantic(
            "DTG-LANG-RELATIONSHIP-TYPE",
            "relationship CREATE requires exactly one explicit type",
        ));
    }
    if relationship.direction == RelationshipDirection::Either {
        return Err(LanguageError::semantic(
            "DTG-LANG-RELATIONSHIP-DIRECTION",
            "relationship CREATE requires an explicit direction",
        ));
    }
    if bindings.contains_key(&relationship.variable) {
        return Err(LanguageError::semantic(
            "DTG-LANG-CONFLICTING-BINDING",
            format!(
                "relationship CREATE variable '{}' is already bound",
                relationship.variable
            ),
        ));
    }
    for endpoint in &pattern.nodes {
        if !endpoint.labels.is_empty() || !endpoint.properties.is_empty() {
            return Err(LanguageError::semantic(
                "DTG-LANG-CREATE-ENDPOINT",
                "relationship CREATE endpoints must be bound variable references",
            ));
        }
        match bindings.get(&endpoint.variable) {
            Some(binding) if binding.kind == BindingKind::Node => {}
            _ => return unbound(&endpoint.variable),
        }
    }
    validate_properties(&relationship.properties, bindings)
}

fn validate_properties(
    properties: &BTreeMap<String, Expr>,
    bindings: &Bindings,
) -> Result<(), LanguageError> {
    for expression in properties.values() {
        validate_expr(expression, bindings)?;
    }
    Ok(())
}

fn validate_expr(expr: &Expr, bindings: &Bindings) -> Result<(), LanguageError> {
    match expr {
        Expr::Parameter(_) | Expr::Integer(_) | Expr::String(_) | Expr::Boolean(_) | Expr::Null => {
            Ok(())
        }
        Expr::Column(variable) => require_binding(variable, bindings),
        Expr::Property { input, .. } => require_binding(input, bindings),
    }
}

fn require_binding(variable: &str, bindings: &Bindings) -> Result<(), LanguageError> {
    if bindings.contains_key(variable) {
        Ok(())
    } else {
        unbound(variable)
    }
}

fn unbound(variable: &str) -> Result<(), LanguageError> {
    Err(LanguageError::semantic(
        "DTG-LANG-UNBOUND-VARIABLE",
        format!("unbound variable: {variable}"),
    ))
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

fn validate_statement_changes_axes(
    defaults: &[Scope],
    matches: &[Match],
) -> Result<(), LanguageError> {
    let changes_axes = defaults
        .iter()
        .chain(matches.iter().flat_map(|matching| matching.scopes.iter()))
        .filter(|scope| matches!(scope.mode, Mode::Changes(_, _)))
        .map(|scope| scope.axis as u8)
        .collect::<BTreeSet<_>>();
    if changes_axes.len() > 1 {
        return Err(LanguageError::semantic(
            "DTG-LANG-UNSUPPORTED-CHANGES",
            "at most one temporal axis per statement may use CHANGES",
        ));
    }
    Ok(())
}

fn collect_statement_parameters(statement: &Statement, output: &mut BTreeSet<String>) {
    match statement {
        Statement::Query(query) => {
            collect_scopes(&query.scopes, output);
            collect_matches(&query.matches, output);
            for value in &query.returns {
                collect_expr(value, output);
            }
            for key in &query.order_by {
                collect_expr(&key.expression, output);
            }
            if let Some((left, right)) = &query.where_clause {
                collect_expr(left, output);
                collect_expr(right, output);
            }
        }
        Statement::Write(write) => match write {
            Write::Create { node, valid_from } => {
                collect_properties(&node.properties, output);
                collect_expr(valid_from, output);
            }
            Write::CreateRelationship {
                matches,
                pattern,
                valid_from,
            } => {
                collect_matches(matches, output);
                collect_pattern(pattern, output);
                collect_expr(valid_from, output);
            }
            Write::Set {
                matches,
                properties,
                valid_from,
                ..
            } => {
                collect_matches(matches, output);
                collect_properties(properties, output);
                collect_expr(valid_from, output);
            }
            Write::Delete {
                matches,
                valid_from,
                ..
            } => {
                collect_matches(matches, output);
                collect_expr(valid_from, output);
            }
        },
        Statement::Boundary(_)
        | Statement::SubmitAnalytics { .. }
        | Statement::Procedure { .. } => {}
    }
}

fn collect_matches(matches: &[Match], output: &mut BTreeSet<String>) {
    for matching in matches {
        collect_scopes(&matching.scopes, output);
        collect_pattern(&matching.pattern, output);
    }
}

fn collect_pattern(pattern: &Pattern, output: &mut BTreeSet<String>) {
    for node in &pattern.nodes {
        collect_properties(&node.properties, output);
    }
    for relationship in &pattern.relationships {
        collect_properties(&relationship.properties, output);
    }
}

fn collect_properties(properties: &BTreeMap<String, Expr>, output: &mut BTreeSet<String>) {
    for expression in properties.values() {
        collect_expr(expression, output);
    }
}

fn collect_scopes(scopes: &[Scope], output: &mut BTreeSet<String>) {
    for scope in scopes {
        collect_scope(scope, output);
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
    let changes_axes = [valid.as_ref(), system.as_ref()]
        .into_iter()
        .flatten()
        .filter(|scope| matches!(scope.mode, Mode::Changes(_, _)))
        .count();
    if changes_axes > 1 {
        return Err(LanguageError::semantic(
            "DTG-LANG-UNSUPPORTED-CHANGES",
            "at most one effective temporal axis may use CHANGES",
        ));
    }
    Ok((valid, system))
}
