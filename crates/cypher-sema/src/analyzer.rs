use std::collections::BTreeMap;

use analytics_api::AlgorithmType;
use cypher_ast::{
    BinaryOperator, Clause, ClauseKind, CypherProfile, Expression, NodePattern, Pattern,
    ProcedureYield, RelationshipPattern, Statement, TemporalAxis, UnaryOperator,
};
use cypher_syntax::{ParsedQuery, TokenKind, lex, parse_expression, parse_pattern};
use procedure_runtime::{
    ProcedureAccess, ProcedureCatalog, ProcedureDescriptor, ProcedureEffect, ProcedureInput,
};
use temporal_ir::ValueType;

use crate::{CypherType, SemanticError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryEffect {
    ReadOnly,
    Write,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputField {
    name: String,
    cypher_type: CypherType,
}

impl OutputField {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn cypher_type(&self) -> &CypherType {
        &self.cypher_type
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzedQuery {
    output: Vec<OutputField>,
    effect: QueryEffect,
    procedures: Vec<AnalyzedProcedureCall>,
    subqueries: Vec<AnalyzedSubquery>,
}

impl AnalyzedQuery {
    #[must_use]
    pub fn output(&self) -> &[OutputField] {
        &self.output
    }

    #[must_use]
    pub const fn effect(&self) -> QueryEffect {
        self.effect
    }

    #[must_use]
    pub fn procedures(&self) -> &[AnalyzedProcedureCall] {
        &self.procedures
    }

    #[must_use]
    pub fn subquery(&self, clause_start: usize) -> Option<&AnalyzedQuery> {
        self.subqueries
            .iter()
            .find(|subquery| subquery.clause_start == clause_start)
            .map(|subquery| subquery.query.as_ref())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AnalyzedSubquery {
    clause_start: usize,
    query: Box<AnalyzedQuery>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzedProcedureArgument {
    name: String,
    expression: Expression,
}

impl AnalyzedProcedureArgument {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    #[must_use]
    pub const fn expression(&self) -> &Expression {
        &self.expression
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzedProcedureYield {
    source_index: u32,
    source_name: String,
    output_name: String,
    cypher_type: CypherType,
    nullable: bool,
}

impl AnalyzedProcedureYield {
    #[must_use]
    pub const fn source_index(&self) -> u32 {
        self.source_index
    }
    #[must_use]
    pub fn source_name(&self) -> &str {
        &self.source_name
    }
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }
    #[must_use]
    pub const fn cypher_type(&self) -> &CypherType {
        &self.cypher_type
    }
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzedProcedureCall {
    clause_start: usize,
    descriptor: ProcedureDescriptor,
    arguments: Vec<AnalyzedProcedureArgument>,
    yields: Vec<AnalyzedProcedureYield>,
}

impl AnalyzedProcedureCall {
    #[must_use]
    pub const fn clause_start(&self) -> usize {
        self.clause_start
    }
    #[must_use]
    pub const fn descriptor(&self) -> &ProcedureDescriptor {
        &self.descriptor
    }
    #[must_use]
    pub fn arguments(&self) -> &[AnalyzedProcedureArgument] {
        &self.arguments
    }
    #[must_use]
    pub fn yields(&self) -> &[AnalyzedProcedureYield] {
        &self.yields
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SemanticAnalyzer;

#[derive(Clone, Copy)]
struct InferenceContext<'a> {
    analyzer: &'a SemanticAnalyzer,
    profile: CypherProfile,
    catalog: &'a ProcedureCatalog,
    access: &'a ProcedureAccess,
}

impl SemanticAnalyzer {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn analyze(&self, parsed: &ParsedQuery) -> Result<AnalyzedQuery, SemanticError> {
        let catalog = default_catalog()?;
        self.analyze_internal(
            parsed,
            BTreeMap::new(),
            &catalog,
            &ProcedureAccess::allow_all(),
        )
    }

    pub fn analyze_with_bindings(
        &self,
        parsed: &ParsedQuery,
        bindings: BTreeMap<String, CypherType>,
    ) -> Result<AnalyzedQuery, SemanticError> {
        let catalog = default_catalog()?;
        self.analyze_internal(parsed, bindings, &catalog, &ProcedureAccess::allow_all())
    }

    pub fn analyze_with_procedures(
        &self,
        parsed: &ParsedQuery,
        catalog: &ProcedureCatalog,
        access: &ProcedureAccess,
    ) -> Result<AnalyzedQuery, SemanticError> {
        self.analyze_internal(parsed, BTreeMap::new(), catalog, access)
    }

    pub fn analyze_query_with_bindings_and_procedures(
        &self,
        query: &cypher_ast::QueryStatement,
        profile: CypherProfile,
        bindings: BTreeMap<String, CypherType>,
        catalog: &ProcedureCatalog,
        access: &ProcedureAccess,
    ) -> Result<AnalyzedQuery, SemanticError> {
        self.analyze_statement_internal(
            &Statement::Query(query.clone()),
            profile,
            bindings,
            catalog,
            access,
        )
    }

    fn analyze_internal(
        &self,
        parsed: &ParsedQuery,
        bindings: BTreeMap<String, CypherType>,
        catalog: &ProcedureCatalog,
        access: &ProcedureAccess,
    ) -> Result<AnalyzedQuery, SemanticError> {
        self.analyze_statement_internal(
            parsed.statement(),
            parsed.profile(),
            bindings,
            catalog,
            access,
        )
    }

    fn analyze_statement_internal(
        &self,
        statement: &Statement,
        profile: CypherProfile,
        bindings: BTreeMap<String, CypherType>,
        catalog: &ProcedureCatalog,
        access: &ProcedureAccess,
    ) -> Result<AnalyzedQuery, SemanticError> {
        match statement {
            Statement::Query(query) => {
                let inference = InferenceContext {
                    analyzer: self,
                    profile,
                    catalog,
                    access,
                };
                let branch_bindings = bindings.clone();
                let mut scope = Scope { symbols: bindings };
                let mut output = Vec::new();
                let mut union_output = None;
                let mut effect = QueryEffect::ReadOnly;
                let mut procedures = Vec::new();
                let mut subqueries = Vec::new();
                let mut nested_procedure = false;
                for clause in query.clauses() {
                    match clause.kind() {
                        ClauseKind::Match | ClauseKind::OptionalMatch => {
                            let pattern = parse_pattern(clause_body(clause)?)?;
                            bind_pattern(&mut scope, &pattern, &inference)?;
                        }
                        ClauseKind::Where | ClauseKind::Filter => {
                            let expression = parse_expression(clause_body(clause)?)?;
                            let actual = infer(&expression, &scope, &inference)?;
                            if !actual.is_boolean() {
                                return Err(SemanticError::new(
                                    "DTG-CYPHER-NON-BOOLEAN-PREDICATE",
                                    format!("predicate has type {actual:?}"),
                                ));
                            }
                        }
                        ClauseKind::With => {
                            let fields = projections(clause_body(clause)?, &scope, &inference)?;
                            scope = Scope::from_fields(&fields)?;
                        }
                        ClauseKind::Return => {
                            output = projections(clause_body(clause)?, &scope, &inference)?;
                        }
                        ClauseKind::Let => {
                            bind_let(clause_body(clause)?, &mut scope, &inference)?;
                        }
                        ClauseKind::Unwind => {
                            bind_unwind(clause_body(clause)?, &mut scope, &inference)?;
                        }
                        ClauseKind::Create | ClauseKind::Merge => {
                            let pattern = parse_pattern(clause_body(clause)?)?;
                            bind_pattern(&mut scope, &pattern, &inference)?;
                            effect = QueryEffect::Write;
                        }
                        ClauseKind::Set
                        | ClauseKind::Remove
                        | ClauseKind::Delete { .. }
                        | ClauseKind::Foreach => effect = QueryEffect::Write,
                        ClauseKind::Call => {
                            if let Some(subquery) = clause.call_subquery() {
                                let mut imports = BTreeMap::new();
                                for import in subquery.imports() {
                                    if imports.contains_key(import.value()) {
                                        return Err(SemanticError::for_symbol(
                                            "DTG-CYPHER-DUPLICATE-SUBQUERY-IMPORT",
                                            format!(
                                                "subquery import {} is repeated",
                                                import.value()
                                            ),
                                            import.value(),
                                        ));
                                    }
                                    let cypher_type =
                                        scope.symbols.get(import.value()).cloned().ok_or_else(
                                            || {
                                                SemanticError::for_symbol(
                                                    "DTG-CYPHER-UNKNOWN-SUBQUERY-IMPORT",
                                                    format!(
                                                        "subquery import {} is not visible",
                                                        import.value()
                                                    ),
                                                    import.value(),
                                                )
                                            },
                                        )?;
                                    imports.insert(import.value().to_owned(), cypher_type);
                                }
                                let mut export_names = std::collections::BTreeSet::new();
                                for export in subquery.exports() {
                                    if !export_names.insert(export.value()) {
                                        return Err(SemanticError::for_symbol(
                                            "DTG-CYPHER-DUPLICATE-SUBQUERY-EXPORT",
                                            format!(
                                                "subquery export {} is repeated",
                                                export.value()
                                            ),
                                            export.value(),
                                        ));
                                    }
                                }
                                let child = Statement::Query(subquery.query().clone());
                                let analyzed = self.analyze_statement_internal(
                                    &child, profile, imports, catalog, access,
                                )?;
                                if analyzed.effect() == QueryEffect::Write {
                                    effect = QueryEffect::Write;
                                }
                                if !analyzed.procedures().is_empty() {
                                    nested_procedure = true;
                                    procedures.extend(analyzed.procedures().iter().cloned());
                                }
                                if analyzed.output().len() != subquery.exports().len()
                                    || analyzed
                                        .output()
                                        .iter()
                                        .zip(subquery.exports())
                                        .any(|(field, export)| field.name() != export.value())
                                {
                                    return Err(SemanticError::new(
                                        "DTG-CYPHER-SUBQUERY-EXPORT-SCHEMA",
                                        "structured subquery exports do not match its RETURN schema",
                                    ));
                                }
                                for field in analyzed.output() {
                                    if scope.symbols.contains_key(field.name()) {
                                        return Err(SemanticError::for_symbol(
                                            "DTG-CYPHER-SUBQUERY-EXPORT-COLLISION",
                                            format!(
                                                "subquery export {} collides with an outer variable",
                                                field.name()
                                            ),
                                            field.name(),
                                        ));
                                    }
                                    scope.bind(field.name(), field.cypher_type().clone())?;
                                }
                                subqueries.push(AnalyzedSubquery {
                                    clause_start: clause.span().start(),
                                    query: Box::new(analyzed),
                                });
                            } else {
                                let procedure = analyze_procedure(
                                    clause, &scope, profile, catalog, access, &inference,
                                )?;
                                if procedure.descriptor.effect() == ProcedureEffect::Write {
                                    return Err(SemanticError::new(
                                        "DTG-CYPHER-PROCEDURE-WRITE-UNSUPPORTED",
                                        "write procedures require a typed candidate/revision-fenced staging implementation",
                                    ));
                                }
                                for item in procedure.yields() {
                                    scope.bind(item.output_name(), item.cypher_type().clone())?;
                                }
                                procedures.push(procedure);
                            }
                        }
                        ClauseKind::Yield => {}
                        ClauseKind::Union { .. } => {
                            validate_union_branch(&mut union_output, &output)?;
                            scope = Scope {
                                symbols: branch_bindings.clone(),
                            };
                            output.clear();
                        }
                        ClauseKind::OrderBy => {
                            for item in split_top_level(clause_body(clause)?, TokenKind::Comma)? {
                                let expression = parse_expression(strip_sort_direction(item)?)?;
                                let _ = infer(&expression, &scope, &inference)?;
                            }
                        }
                        ClauseKind::Skip | ClauseKind::Offset | ClauseKind::Limit => {
                            let expression = parse_expression(clause_body(clause)?)?;
                            let actual = infer(&expression, &scope, &inference)?;
                            if !matches!(
                                actual,
                                CypherType::Integer | CypherType::Any | CypherType::Null
                            ) {
                                return Err(type_mismatch("row count", &actual));
                            }
                        }
                        ClauseKind::Finish
                        | ClauseKind::For
                        | ClauseKind::Next
                        | ClauseKind::When => {}
                    }
                }
                if union_output.is_some() {
                    validate_union_branch(&mut union_output, &output)?;
                    output = union_output.unwrap_or_default();
                }
                if effect == QueryEffect::Write && !procedures.is_empty() {
                    return Err(SemanticError::new(
                        "DTG-CYPHER-WRITE-PROCEDURE-UNSUPPORTED",
                        "mixed write and procedure statements require an ordered staged execution pipeline",
                    ));
                }
                if nested_procedure {
                    return Err(SemanticError::new(
                        "DTG-CYPHER-SUBQUERY-PROCEDURE-UNSUPPORTED",
                        "named procedures inside CALL subqueries require the Task 4C execution path",
                    ));
                }
                if effect == QueryEffect::Write
                    && query.temporal().scope(TemporalAxis::SystemTime).is_some()
                {
                    return Err(SemanticError::new(
                        "DTG-TEMPORAL-HISTORICAL-WRITE",
                        "writes cannot target a historical transaction-time snapshot",
                    ));
                }
                Ok(AnalyzedQuery {
                    output,
                    effect,
                    procedures,
                    subqueries,
                })
            }
        }
    }
}

fn default_catalog() -> Result<ProcedureCatalog, SemanticError> {
    ProcedureCatalog::builtin_analytics()
        .map_err(|error| SemanticError::new("DTG-CYPHER-PROCEDURE-CATALOG", error.to_string()))
}

fn analyze_procedure(
    clause: &Clause,
    scope: &Scope,
    profile: cypher_ast::CypherProfile,
    catalog: &ProcedureCatalog,
    access: &ProcedureAccess,
    inference: &InferenceContext<'_>,
) -> Result<AnalyzedProcedureCall, SemanticError> {
    let call = clause.procedure().ok_or_else(|| {
        SemanticError::new(
            "DTG-CYPHER-INVALID-PROCEDURE",
            "named CALL has no structured procedure descriptor",
        )
    })?;
    let descriptor = catalog.resolve(call.name()).ok_or_else(|| {
        SemanticError::new(
            "DTG-CYPHER-UNKNOWN-PROCEDURE",
            format!("procedure {} is not registered", call.name()),
        )
    })?;
    if !descriptor.supports_profile(profile) {
        return Err(SemanticError::new(
            "DTG-CYPHER-PROCEDURE-PROFILE",
            "procedure does not support the active Cypher profile",
        ));
    }
    if !access.allows(descriptor.permission()) {
        return Err(SemanticError::new(
            "DTG-CYPHER-PROCEDURE-PERMISSION",
            "procedure capability is not granted",
        ));
    }
    let arguments =
        bind_procedure_arguments(call.arguments(), descriptor.inputs(), scope, inference)?;
    let yields = bind_procedure_yields(call.yield_selection(), descriptor)?;
    Ok(AnalyzedProcedureCall {
        clause_start: clause.span().start(),
        descriptor: descriptor.clone(),
        arguments,
        yields,
    })
}

fn bind_procedure_arguments(
    expressions: &[Expression],
    inputs: &[ProcedureInput],
    scope: &Scope,
    inference: &InferenceContext<'_>,
) -> Result<Vec<AnalyzedProcedureArgument>, SemanticError> {
    let mut arguments = Vec::new();
    if let [Expression::Map(entries)] = expressions {
        let mut names = std::collections::BTreeSet::new();
        for (key, expression) in entries {
            if !names.insert(key.value()) {
                return Err(SemanticError::new(
                    "DTG-CYPHER-DUPLICATE-PROCEDURE-ARGUMENT",
                    format!("procedure argument {} is repeated", key.value()),
                ));
            }
            let input = inputs
                .iter()
                .find(|input| input.name() == key.value())
                .ok_or_else(|| {
                    SemanticError::new(
                        "DTG-CYPHER-UNKNOWN-PROCEDURE-ARGUMENT",
                        format!("procedure has no input named {}", key.value()),
                    )
                })?;
            validate_procedure_argument(expression, input, scope, inference)?;
            arguments.push(AnalyzedProcedureArgument {
                name: key.value().to_owned(),
                expression: expression.clone(),
            });
        }
    } else {
        if expressions.len() > inputs.len() {
            return Err(SemanticError::new(
                "DTG-CYPHER-EXTRA-PROCEDURE-ARGUMENT",
                "procedure received more positional arguments than declared inputs",
            ));
        }
        for (expression, input) in expressions.iter().zip(inputs) {
            validate_procedure_argument(expression, input, scope, inference)?;
            arguments.push(AnalyzedProcedureArgument {
                name: input.name().to_owned(),
                expression: expression.clone(),
            });
        }
    }
    for input in inputs {
        if input.required()
            && !arguments
                .iter()
                .any(|argument| argument.name == input.name())
        {
            return Err(SemanticError::new(
                "DTG-CYPHER-MISSING-PROCEDURE-ARGUMENT",
                format!("required procedure input {} is missing", input.name()),
            ));
        }
    }
    Ok(arguments)
}

fn validate_procedure_argument(
    expression: &Expression,
    input: &ProcedureInput,
    scope: &Scope,
    inference: &InferenceContext<'_>,
) -> Result<(), SemanticError> {
    let actual = infer(expression, scope, inference)?;
    let compatible = match input.algorithm_type() {
        Some(AlgorithmType::Vertex) => matches!(
            actual,
            CypherType::Integer | CypherType::String | CypherType::Any
        ),
        Some(AlgorithmType::Time) => matches!(
            actual,
            CypherType::Temporal | CypherType::Integer | CypherType::Any
        ),
        Some(AlgorithmType::Float) => matches!(
            actual,
            CypherType::Integer | CypherType::Float | CypherType::Any
        ),
        _ => actual == cypher_type(input.value_type()) || actual == CypherType::Any,
    } || (actual == CypherType::Null && input.nullable());
    if compatible {
        Ok(())
    } else {
        Err(SemanticError::new(
            "DTG-CYPHER-PROCEDURE-ARGUMENT-TYPE",
            format!(
                "procedure input {} expects {:?}, got {actual:?}",
                input.name(),
                input.value_type()
            ),
        ))
    }
}

fn bind_procedure_yields(
    selection: &ProcedureYield,
    descriptor: &ProcedureDescriptor,
) -> Result<Vec<AnalyzedProcedureYield>, SemanticError> {
    let requested = match selection {
        ProcedureYield::None => return Ok(Vec::new()),
        ProcedureYield::All => descriptor
            .output()
            .columns()
            .iter()
            .map(|column| (column.name(), column.name()))
            .collect::<Vec<_>>(),
        ProcedureYield::Items(items) => items
            .iter()
            .map(|item| (item.name(), item.alias().unwrap_or_else(|| item.name())))
            .collect(),
    };
    requested
        .into_iter()
        .map(|(source_name, output_name)| {
            let (source_index, column) = descriptor
                .output()
                .columns()
                .iter()
                .enumerate()
                .find(|(_, column)| column.name() == source_name)
                .ok_or_else(|| {
                    SemanticError::new(
                        "DTG-CYPHER-UNKNOWN-YIELD",
                        format!("procedure has no output field {source_name}"),
                    )
                })?;
            Ok(AnalyzedProcedureYield {
                source_index: u32::try_from(source_index).map_err(|_| {
                    SemanticError::new(
                        "DTG-CYPHER-PROCEDURE-SCHEMA",
                        "procedure output schema is too wide",
                    )
                })?,
                source_name: source_name.to_owned(),
                output_name: output_name.to_owned(),
                cypher_type: cypher_type(column.value_type()),
                nullable: column.nullable(),
            })
        })
        .collect()
}

fn cypher_type(value_type: &ValueType) -> CypherType {
    match value_type {
        ValueType::Any => CypherType::Any,
        ValueType::Null => CypherType::Null,
        ValueType::Boolean => CypherType::Boolean,
        ValueType::Integer => CypherType::Integer,
        ValueType::Float => CypherType::Float,
        ValueType::String => CypherType::String,
        ValueType::Bytes => CypherType::Bytes,
        ValueType::List(item) => CypherType::List(Box::new(cypher_type(item))),
        ValueType::Map => CypherType::Map,
        ValueType::Node => CypherType::Node,
        ValueType::Relationship => CypherType::Relationship,
        ValueType::Path => CypherType::Path,
        ValueType::Temporal => CypherType::Temporal,
        ValueType::Spatial => CypherType::Spatial,
        ValueType::Vector => CypherType::Vector,
    }
}

#[derive(Clone, Debug, Default)]
struct Scope {
    symbols: BTreeMap<String, CypherType>,
}

impl Scope {
    fn from_fields(fields: &[OutputField]) -> Result<Self, SemanticError> {
        let mut scope = Self::default();
        for field in fields {
            scope.bind(&field.name, field.cypher_type.clone())?;
        }
        Ok(scope)
    }

    fn bind(&mut self, name: &str, cypher_type: CypherType) -> Result<(), SemanticError> {
        if self.symbols.insert(name.to_owned(), cypher_type).is_some() {
            return Err(SemanticError::for_symbol(
                "DTG-CYPHER-DUPLICATE-VARIABLE",
                format!("variable {name} is already bound"),
                name,
            ));
        }
        Ok(())
    }

    fn bind_compatible(
        &mut self,
        name: &str,
        cypher_type: CypherType,
    ) -> Result<(), SemanticError> {
        if let Some(existing) = self.symbols.get(name) {
            if existing != &cypher_type {
                return Err(SemanticError::for_symbol(
                    "DTG-CYPHER-VARIABLE-TYPE-CONFLICT",
                    format!("variable {name} has incompatible pattern roles"),
                    name,
                ));
            }
            return Ok(());
        }
        self.symbols.insert(name.to_owned(), cypher_type);
        Ok(())
    }

    fn resolve(&self, name: &str) -> Result<CypherType, SemanticError> {
        self.symbols.get(name).cloned().ok_or_else(|| {
            SemanticError::for_symbol(
                "DTG-CYPHER-UNBOUND-VARIABLE",
                format!("variable {name} is not in scope"),
                name,
            )
        })
    }
}

fn clause_body(clause: &Clause) -> Result<&str, SemanticError> {
    let words = match clause.kind() {
        ClauseKind::OptionalMatch | ClauseKind::Delete { detach: true } | ClauseKind::OrderBy => 2,
        _ => 1,
    };
    let lexed = lex(clause.text())
        .map_err(|error| SemanticError::new("DTG-CYPHER-INVALID-CLAUSE", error.to_string()))?;
    let token = lexed
        .tokens()
        .get(words)
        .ok_or_else(|| SemanticError::new("DTG-CYPHER-EMPTY-CLAUSE", "clause has no body"))?;
    Ok(&clause.text()[token.span().start()..])
}

fn bind_pattern(
    scope: &mut Scope,
    pattern: &Pattern,
    inference: &InferenceContext<'_>,
) -> Result<(), SemanticError> {
    for path in pattern.paths() {
        bind_node(scope, path.start(), inference)?;
        for chain in path.chains() {
            bind_relationship(scope, chain.relationship(), inference)?;
            bind_node(scope, chain.node(), inference)?;
        }
    }
    Ok(())
}

fn bind_node(
    scope: &mut Scope,
    node: &NodePattern,
    inference: &InferenceContext<'_>,
) -> Result<(), SemanticError> {
    if let Some(variable) = node.variable() {
        scope.bind_compatible(variable.value(), CypherType::Node)?;
    }
    if let Some(properties) = node.properties() {
        let _ = infer(properties, scope, inference)?;
    }
    Ok(())
}

fn bind_relationship(
    scope: &mut Scope,
    relationship: &RelationshipPattern,
    inference: &InferenceContext<'_>,
) -> Result<(), SemanticError> {
    if let Some(variable) = relationship.variable() {
        scope.bind_compatible(variable.value(), CypherType::Relationship)?;
    }
    if let Some(properties) = relationship.properties() {
        let _ = infer(properties, scope, inference)?;
    }
    Ok(())
}

fn projections(
    source: &str,
    scope: &Scope,
    inference: &InferenceContext<'_>,
) -> Result<Vec<OutputField>, SemanticError> {
    let (_, source) = projection_modifiers(source)?;
    let mut fields = Vec::new();
    for (index, item) in split_top_level(source, TokenKind::Comma)?
        .into_iter()
        .enumerate()
    {
        if is_star(item)? {
            fields.extend(scope.symbols.iter().map(|(name, cypher_type)| OutputField {
                name: name.clone(),
                cypher_type: cypher_type.clone(),
            }));
        } else {
            let (expression_source, alias) = split_alias(item)?;
            let expression = parse_expression(expression_source)?;
            let cypher_type = infer(&expression, scope, inference)?;
            let name = alias.unwrap_or_else(|| expression_name(&expression, index));
            fields.push(OutputField { name, cypher_type });
        }
    }
    let mut names = std::collections::BTreeSet::new();
    for field in &fields {
        if !names.insert(field.name.clone()) {
            return Err(SemanticError::for_symbol(
                "DTG-CYPHER-DUPLICATE-OUTPUT",
                format!("output name {} is repeated", field.name),
                &field.name,
            ));
        }
    }
    Ok(fields)
}

fn projection_modifiers(source: &str) -> Result<(bool, &str), SemanticError> {
    let lexed = lex(source)
        .map_err(|error| SemanticError::new("DTG-CYPHER-INVALID-PROJECTION", error.to_string()))?;
    let Some(first) = lexed.tokens().first() else {
        return Err(SemanticError::new(
            "DTG-CYPHER-EMPTY-PROJECTION",
            "projection requires at least one item",
        ));
    };
    if matches!(first.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case("DISTINCT")) {
        let body = source[first.span().end()..].trim();
        if body.is_empty() {
            return Err(SemanticError::new(
                "DTG-CYPHER-EMPTY-PROJECTION",
                "DISTINCT requires at least one projection item",
            ));
        }
        Ok((true, body))
    } else {
        Ok((false, source))
    }
}

fn is_star(source: &str) -> Result<bool, SemanticError> {
    let lexed = lex(source)
        .map_err(|error| SemanticError::new("DTG-CYPHER-INVALID-PROJECTION", error.to_string()))?;
    Ok(matches!(lexed.tokens(), [token] if token.kind() == &TokenKind::Star))
}

fn strip_sort_direction(source: &str) -> Result<&str, SemanticError> {
    let lexed = lex(source)
        .map_err(|error| SemanticError::new("DTG-CYPHER-INVALID-ORDER-BY", error.to_string()))?;
    if let Some(last) = lexed.tokens().last()
        && matches!(last.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case("ASC") || word.eq_ignore_ascii_case("DESC"))
    {
        return Ok(source[..last.span().start()].trim());
    }
    Ok(source)
}

fn validate_union_branch(
    expected: &mut Option<Vec<OutputField>>,
    actual: &[OutputField],
) -> Result<(), SemanticError> {
    let Some(expected) = expected else {
        *expected = Some(actual.to_vec());
        return Ok(());
    };
    if expected.len() != actual.len()
        || expected
            .iter()
            .zip(actual)
            .any(|(left, right)| left.name != right.name)
    {
        return Err(SemanticError::new(
            "DTG-CYPHER-UNION-SCHEMA-NAME-MISMATCH",
            "UNION query parts must return the same column names in the same order",
        ));
    }
    if expected
        .iter()
        .zip(actual)
        .any(|(left, right)| left.cypher_type != right.cypher_type)
    {
        return Err(SemanticError::new(
            "DTG-CYPHER-UNION-SCHEMA-TYPE-MISMATCH",
            "UNION query parts must return compatible column types",
        ));
    }
    Ok(())
}

fn split_alias(source: &str) -> Result<(&str, Option<String>), SemanticError> {
    let lexed = lex(source)
        .map_err(|error| SemanticError::new("DTG-CYPHER-INVALID-PROJECTION", error.to_string()))?;
    let mut depth = 0_usize;
    for (index, token) in lexed.tokens().iter().enumerate() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1);
            }
            TokenKind::Word(word) if depth == 0 && word.eq_ignore_ascii_case("AS") => {
                let alias = lexed.tokens().get(index + 1).ok_or_else(|| {
                    SemanticError::new("DTG-CYPHER-EXPECTED-ALIAS", "AS requires an alias")
                })?;
                let name = token_identifier(alias)?;
                if index + 2 != lexed.tokens().len() {
                    return Err(SemanticError::new(
                        "DTG-CYPHER-INVALID-ALIAS",
                        "unexpected token after projection alias",
                    ));
                }
                return Ok((&source[..token.span().start()], Some(name)));
            }
            _ => {}
        }
    }
    Ok((source, None))
}

fn split_top_level(source: &str, separator: TokenKind) -> Result<Vec<&str>, SemanticError> {
    let lexed = lex(source)
        .map_err(|error| SemanticError::new("DTG-CYPHER-INVALID-LIST", error.to_string()))?;
    let mut depth = 0_usize;
    let mut start = 0_usize;
    let mut parts = Vec::new();
    for token in lexed.tokens() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1);
            }
            kind if depth == 0 && kind == &separator => {
                parts.push(source[start..token.span().start()].trim());
                start = token.span().end();
            }
            _ => {}
        }
    }
    parts.push(source[start..].trim());
    if parts.iter().any(|part| part.is_empty()) {
        return Err(SemanticError::new(
            "DTG-CYPHER-EMPTY-LIST-ITEM",
            "comma-separated list contains an empty item",
        ));
    }
    Ok(parts)
}

fn bind_let(
    source: &str,
    scope: &mut Scope,
    inference: &InferenceContext<'_>,
) -> Result<(), SemanticError> {
    for binding in split_top_level(source, TokenKind::Comma)? {
        let lexed = lex(binding)
            .map_err(|error| SemanticError::new("DTG-CYPHER-INVALID-LET", error.to_string()))?;
        let equal = lexed
            .tokens()
            .iter()
            .position(|token| token.kind() == &TokenKind::Equal)
            .ok_or_else(|| SemanticError::new("DTG-CYPHER-INVALID-LET", "LET requires '='"))?;
        let name = token_identifier(&lexed.tokens()[0])?;
        let expression_start = lexed.tokens().get(equal + 1).ok_or_else(|| {
            SemanticError::new("DTG-CYPHER-INVALID-LET", "LET requires an expression")
        })?;
        let expression = parse_expression(&binding[expression_start.span().start()..])?;
        let cypher_type = infer(&expression, scope, inference)?;
        scope.bind(&name, cypher_type)?;
    }
    Ok(())
}

fn bind_unwind(
    source: &str,
    scope: &mut Scope,
    inference: &InferenceContext<'_>,
) -> Result<(), SemanticError> {
    let (expression_source, alias) = split_alias(source)?;
    let alias = alias.ok_or_else(|| {
        SemanticError::new("DTG-CYPHER-EXPECTED-ALIAS", "UNWIND requires AS alias")
    })?;
    let expression = parse_expression(expression_source)?;
    let item_type = match infer(&expression, scope, inference)? {
        CypherType::List(item) => *item,
        CypherType::Any | CypherType::Null => CypherType::Any,
        actual => {
            return Err(SemanticError::new(
                "DTG-CYPHER-TYPE-MISMATCH",
                format!("UNWIND requires a list, got {actual:?}"),
            ));
        }
    };
    scope.bind(&alias, item_type)
}

fn token_identifier(token: &cypher_syntax::Token) -> Result<String, SemanticError> {
    match token.kind() {
        TokenKind::Word(value) | TokenKind::EscapedIdentifier(value) => Ok(value.clone()),
        _ => Err(SemanticError::new(
            "DTG-CYPHER-EXPECTED-IDENTIFIER",
            "expected an identifier",
        )),
    }
}

fn expression_name(expression: &Expression, index: usize) -> String {
    match expression {
        Expression::Identifier(identifier) => identifier.value().to_owned(),
        Expression::Property { property, .. } => property.value().to_owned(),
        _ => format!("column{index}"),
    }
}

fn infer(
    expression: &Expression,
    scope: &Scope,
    inference: &InferenceContext<'_>,
) -> Result<CypherType, SemanticError> {
    match expression {
        Expression::Null => Ok(CypherType::Null),
        Expression::Boolean(_) => Ok(CypherType::Boolean),
        Expression::Integer(_) => Ok(CypherType::Integer),
        Expression::Float(_) => Ok(CypherType::Float),
        Expression::String(_) => Ok(CypherType::String),
        Expression::Parameter(_) => Ok(CypherType::Any),
        Expression::Identifier(identifier) => scope.resolve(identifier.value()),
        Expression::List(items) => {
            let mut item_type = CypherType::Null;
            for item in items {
                item_type = CypherType::unify(&item_type, &infer(item, scope, inference)?);
            }
            Ok(CypherType::List(Box::new(item_type)))
        }
        Expression::Map(entries) => {
            for (_, value) in entries {
                let _ = infer(value, scope, inference)?;
            }
            Ok(CypherType::Map)
        }
        Expression::Unary {
            operator,
            expression,
        } => {
            let actual = infer(expression, scope, inference)?;
            match operator {
                UnaryOperator::Not if actual.is_boolean() => Ok(CypherType::Boolean),
                UnaryOperator::Plus | UnaryOperator::Minus if actual.is_numeric() => Ok(actual),
                _ => Err(type_mismatch("invalid unary operand", &actual)),
            }
        }
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            let left = infer(left, scope, inference)?;
            let right = infer(right, scope, inference)?;
            infer_binary(*operator, left, right)
        }
        Expression::Property { value, .. } => {
            let actual = infer(value, scope, inference)?;
            if matches!(
                actual,
                CypherType::Node
                    | CypherType::Relationship
                    | CypherType::Map
                    | CypherType::Any
                    | CypherType::Null
            ) {
                Ok(CypherType::Any)
            } else {
                Err(type_mismatch("property access", &actual))
            }
        }
        Expression::Index { value, index } => {
            let value_type = infer(value, scope, inference)?;
            let _ = infer(index, scope, inference)?;
            match value_type {
                CypherType::List(item) => Ok(*item),
                CypherType::Map | CypherType::String | CypherType::Any | CypherType::Null => {
                    Ok(CypherType::Any)
                }
                actual => Err(type_mismatch("index access", &actual)),
            }
        }
        Expression::FunctionCall { name, arguments } => {
            let argument_types = arguments
                .iter()
                .map(|argument| infer(argument, scope, inference))
                .collect::<Result<Vec<_>, _>>()?;
            let function = name
                .iter()
                .map(|part| part.value())
                .collect::<Vec<_>>()
                .join(".")
                .to_ascii_lowercase();
            match function.as_str() {
                "count" | "size" => Ok(CypherType::Integer),
                "tostring" => Ok(CypherType::String),
                "coalesce" => Ok(argument_types
                    .iter()
                    .fold(CypherType::Null, |current, next| {
                        CypherType::unify(&current, next)
                    })),
                _ => Ok(CypherType::Any),
            }
        }
        Expression::ExistsSubquery(query) | Expression::CountSubquery(query) => {
            let child = Statement::Query((**query).clone());
            let analyzed = inference.analyzer.analyze_statement_internal(
                &child,
                inference.profile,
                scope.symbols.clone(),
                inference.catalog,
                inference.access,
            )?;
            if analyzed.effect() == QueryEffect::Write {
                return Err(SemanticError::new(
                    "DTG-CYPHER-SUBQUERY-EXPRESSION-WRITE",
                    "EXISTS and COUNT subqueries must be read-only",
                ));
            }
            if matches!(expression, Expression::ExistsSubquery(_)) {
                Ok(CypherType::Boolean)
            } else {
                Ok(CypherType::Integer)
            }
        }
    }
}

fn infer_binary(
    operator: BinaryOperator,
    left: CypherType,
    right: CypherType,
) -> Result<CypherType, SemanticError> {
    match operator {
        BinaryOperator::Or | BinaryOperator::Xor | BinaryOperator::And => {
            if left.is_boolean() && right.is_boolean() {
                Ok(CypherType::Boolean)
            } else {
                Err(type_mismatch("boolean operator", &left))
            }
        }
        BinaryOperator::Equal
        | BinaryOperator::NotEqual
        | BinaryOperator::Less
        | BinaryOperator::LessEqual
        | BinaryOperator::Greater
        | BinaryOperator::GreaterEqual
        | BinaryOperator::RegexMatch
        | BinaryOperator::In
        | BinaryOperator::Contains
        | BinaryOperator::StartsWith
        | BinaryOperator::EndsWith => Ok(CypherType::Boolean),
        BinaryOperator::Add => {
            if left == CypherType::String && right == CypherType::String {
                Ok(CypherType::String)
            } else if let (CypherType::List(left), CypherType::List(right)) = (&left, &right) {
                Ok(CypherType::List(Box::new(CypherType::unify(left, right))))
            } else {
                numeric_result(&left, &right)
            }
        }
        BinaryOperator::Subtract
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo
        | BinaryOperator::Power => numeric_result(&left, &right),
    }
}

fn numeric_result(left: &CypherType, right: &CypherType) -> Result<CypherType, SemanticError> {
    if !left.is_numeric() || !right.is_numeric() {
        return Err(type_mismatch(
            "numeric operator",
            if !left.is_numeric() { left } else { right },
        ));
    }
    if left == &CypherType::Float || right == &CypherType::Float {
        Ok(CypherType::Float)
    } else if left == &CypherType::Any || right == &CypherType::Any {
        Ok(CypherType::Any)
    } else {
        Ok(CypherType::Integer)
    }
}

fn type_mismatch(operation: &str, actual: &CypherType) -> SemanticError {
    SemanticError::new(
        "DTG-CYPHER-TYPE-MISMATCH",
        format!("{operation} does not accept {actual:?}"),
    )
}
