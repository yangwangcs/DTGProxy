use std::collections::BTreeMap;

use cypher_ast::{
    BinaryOperator, CallSubquery, Clause, ClauseKind, CypherProfile, Expression, NodePattern,
    Pattern, Statement, TemporalContext, TransactionTimeScope, UnaryOperator, ValidTimeScope,
};
use cypher_sema::{AnalyzedQuery, CypherType, QueryEffect, SemanticAnalyzer};
use cypher_syntax::{TokenKind, lex, parse, parse_expression, parse_pattern};
use procedure_runtime::{ProcedureAccess, ProcedureCatalog};
use temporal_ir::{
    ApplyKind, ApplySlotMapping, ChildPlanId, Column, LanguageProfile, LogicalApply,
    LogicalBatchSubtransaction, LogicalNodeId, LogicalOperator, LogicalPlan, LogicalPlanBuilder,
    MAX_APPLY_DEPTH, MAX_APPLY_INVOCATIONS, MAX_APPLY_OUTPUT_ROWS, PlanHeader, ProcedureArgument,
    ProcedureYieldBinding, ResolvedProcedure, RowSchema, ScalarExpr, SlotId, SortKey,
    TransactionTimeSpec, ValidTimeSpec, ValueType,
};
use temporal_types::GraphValue;

use crate::CompileError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompileSession {
    graph_name: String,
    graph_id: u64,
    schema_version: u64,
    topology_epoch: u64,
}

impl CompileSession {
    pub fn new(
        graph_name: impl Into<String>,
        graph_id: u64,
        schema_version: u64,
        topology_epoch: u64,
    ) -> Result<Self, CompileError> {
        let graph_name = graph_name.into();
        if graph_name.is_empty() || graph_name.len() > 255 {
            return Err(CompileError::new(
                "DTG-CYPHER-INVALID-GRAPH",
                "graph name must be non-empty and at most 255 bytes",
            ));
        }
        if graph_id == 0 || schema_version == 0 || topology_epoch == 0 {
            return Err(CompileError::new(
                "DTG-CYPHER-INVALID-COMPILE-SESSION",
                "graph, schema, and topology identities must be non-zero",
            ));
        }
        Ok(Self {
            graph_name,
            graph_id,
            schema_version,
            topology_epoch,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledQuery {
    fingerprint: [u8; 32],
    result_schema: RowSchema,
    effect: QueryEffect,
    logical_plan: LogicalPlan,
    mutation_plan: MutationPlan,
    catalog_revision: u64,
}

impl CompiledQuery {
    #[must_use]
    pub const fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    #[must_use]
    pub const fn result_schema(&self) -> &RowSchema {
        &self.result_schema
    }

    #[must_use]
    pub const fn effect(&self) -> QueryEffect {
        self.effect
    }

    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        matches!(self.effect, QueryEffect::ReadOnly)
    }

    #[must_use]
    pub const fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    #[must_use]
    pub const fn mutation_plan(&self) -> &MutationPlan {
        &self.mutation_plan
    }

    #[must_use]
    pub fn read_prefix_plan(&self) -> Option<LogicalPlan> {
        let root = write_boundary_input(&self.logical_plan)?;
        let prefix = logical_prefix_at(&self.logical_plan, root)?;
        if prefix
            .nodes()
            .iter()
            .any(|node| is_row_pipeline_operator(node.operator()))
        {
            return Some(prefix);
        }
        let [CompiledMutation::Merge(pattern)] = self.mutation_plan.mutations.as_slice() else {
            return None;
        };
        self.standalone_merge_match_plan(pattern)
    }

    #[must_use]
    pub fn uses_standalone_merge_match_prefix(&self) -> bool {
        let [CompiledMutation::Merge(_)] = self.mutation_plan.mutations.as_slice() else {
            return false;
        };
        let Some(write) = self.logical_plan.nodes().iter().find(|node| {
            matches!(
                node.operator(),
                LogicalOperator::Create
                    | LogicalOperator::Merge
                    | LogicalOperator::Set
                    | LogicalOperator::Remove
                    | LogicalOperator::Delete { .. }
            )
        }) else {
            return false;
        };
        let Some(root) = write.inputs().first() else {
            return false;
        };
        let Ok(root_index) = usize::try_from(root.value()) else {
            return false;
        };
        self.logical_plan
            .nodes()
            .get(..=root_index)
            .is_some_and(|nodes| {
                nodes
                    .iter()
                    .all(|node| !is_row_pipeline_operator(node.operator()))
            })
    }

    fn standalone_merge_match_plan(&self, pattern: &Pattern) -> Option<LogicalPlan> {
        let header = self.logical_plan.header().clone();
        let mut lowerer = Lowerer {
            builder: LogicalPlanBuilder::new(header.clone()),
            header,
            profile: CypherProfile::cypher_25(),
            catalog: None,
            access: None,
            root: None,
            schema: RowSchema::empty(),
            variables: BTreeMap::new(),
            next_slot: 0,
            temporal_inserted: false,
        };
        lowerer.pattern(pattern).ok()?;
        if let Some(LogicalOperator::TemporalSlice {
            valid_time,
            transaction_time,
        }) = self
            .logical_plan
            .nodes()
            .iter()
            .find_map(|node| match node.operator() {
                LogicalOperator::TemporalSlice {
                    valid_time,
                    transaction_time,
                } => Some(LogicalOperator::TemporalSlice {
                    valid_time: valid_time.clone(),
                    transaction_time: transaction_time.clone(),
                }),
                _ => None,
            })
        {
            lowerer
                .simple_unary(LogicalOperator::TemporalSlice {
                    valid_time,
                    transaction_time,
                })
                .ok()?;
        }
        let root = lowerer.ensure_root().ok()?;
        lowerer.builder.finish(root).ok()
    }

    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    #[must_use]
    pub fn uses_procedures(&self) -> bool {
        self.logical_plan
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::ProcedureCall { .. }))
    }
}

fn is_row_pipeline_operator(operator: &LogicalOperator) -> bool {
    matches!(
        operator,
        LogicalOperator::NodeScan { .. }
            | LogicalOperator::RelationshipScan { .. }
            | LogicalOperator::Expand { .. }
            | LogicalOperator::Filter { .. }
            | LogicalOperator::Project { .. }
            | LogicalOperator::Unwind { .. }
            | LogicalOperator::Aggregate { .. }
            | LogicalOperator::Sort { .. }
            | LogicalOperator::Skip { .. }
            | LogicalOperator::Limit { .. }
            | LogicalOperator::InnerJoin
            | LogicalOperator::LeftJoin
            | LogicalOperator::Union { .. }
            | LogicalOperator::Apply { .. }
            | LogicalOperator::BatchSubtransaction { .. }
    )
}

fn write_boundary_input(plan: &LogicalPlan) -> Option<LogicalNodeId> {
    plan.nodes().iter().find_map(|node| match node.operator() {
        LogicalOperator::Create
        | LogicalOperator::Merge
        | LogicalOperator::Set
        | LogicalOperator::Remove
        | LogicalOperator::Delete { .. } => node.inputs().first().copied(),
        LogicalOperator::Apply { apply } if write_boundary_input(apply.child_plan()).is_some() => {
            node.inputs().first().copied()
        }
        LogicalOperator::BatchSubtransaction { batch }
            if write_boundary_input(batch.apply().child_plan()).is_some() =>
        {
            node.inputs().first().copied()
        }
        _ => None,
    })
}

fn logical_prefix_at(plan: &LogicalPlan, root: LogicalNodeId) -> Option<LogicalPlan> {
    let root_index = usize::try_from(root.value()).ok()?;
    let nodes = plan.nodes().get(..=root_index)?.to_vec();
    let output = nodes.get(root_index)?.output().clone();
    Some(LogicalPlan::from_parts(
        plan.header().clone(),
        nodes,
        root,
        output,
    ))
}

fn read_prefix_plan(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let root = write_boundary_input(plan)?;
    let prefix = logical_prefix_at(plan, root)?;
    prefix
        .nodes()
        .iter()
        .any(|node| is_row_pipeline_operator(node.operator()))
        .then_some(prefix)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationPlan {
    mutations: Vec<CompiledMutation>,
}

impl MutationPlan {
    #[must_use]
    pub fn mutations(&self) -> &[CompiledMutation] {
        &self.mutations
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompiledMutation {
    Create(Pattern),
    Merge(Pattern),
    SetProperty {
        target: PropertyTarget,
        value: Expression,
    },
    RemoveProperty(PropertyTarget),
    Delete {
        variables: Vec<String>,
        detach: bool,
    },
    Subquery(CompiledSubqueryMutation),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledSubqueryMutation {
    clause_start: usize,
    imports: Vec<String>,
    exports: Vec<String>,
    export_expressions: Vec<CompiledSubqueryExport>,
    read_prefix_plan: Option<LogicalPlan>,
    mutation_plan: MutationPlan,
    batch_rows: Option<u32>,
}

impl CompiledSubqueryMutation {
    #[must_use]
    pub const fn clause_start(&self) -> usize {
        self.clause_start
    }

    #[must_use]
    pub fn imports(&self) -> &[String] {
        &self.imports
    }

    #[must_use]
    pub fn exports(&self) -> &[String] {
        &self.exports
    }

    #[must_use]
    pub fn export_expressions(&self) -> &[CompiledSubqueryExport] {
        &self.export_expressions
    }

    #[must_use]
    pub const fn read_prefix_plan(&self) -> Option<&LogicalPlan> {
        self.read_prefix_plan.as_ref()
    }

    #[must_use]
    pub const fn mutation_plan(&self) -> &MutationPlan {
        &self.mutation_plan
    }

    #[must_use]
    pub const fn batch_rows(&self) -> Option<u32> {
        self.batch_rows
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledSubqueryExport {
    name: String,
    expression: Expression,
}

impl CompiledSubqueryExport {
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
pub struct PropertyTarget {
    variable: String,
    property: String,
}

impl PropertyTarget {
    #[must_use]
    pub fn new(variable: impl Into<String>, property: impl Into<String>) -> Self {
        Self {
            variable: variable.into(),
            property: property.into(),
        }
    }

    #[must_use]
    pub fn variable(&self) -> &str {
        &self.variable
    }

    #[must_use]
    pub fn property(&self) -> &str {
        &self.property
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CypherCompiler;

impl CypherCompiler {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn compile(
        &self,
        text: &str,
        session: &CompileSession,
    ) -> Result<CompiledQuery, CompileError> {
        let catalog = ProcedureCatalog::builtin_analytics().map_err(|error| {
            CompileError::new("DTG-CYPHER-PROCEDURE-CATALOG", error.to_string())
        })?;
        self.compile_with_procedures(text, session, &catalog, &ProcedureAccess::allow_all())
    }

    pub fn compile_with_procedures(
        &self,
        text: &str,
        session: &CompileSession,
        catalog: &ProcedureCatalog,
        access: &ProcedureAccess,
    ) -> Result<CompiledQuery, CompileError> {
        let parsed = parse(text)?;
        if let Statement::Query(query) = parsed.statement()
            && let Some(graph) = query.graph()
            && graph.value() != session.graph_name
        {
            return Err(CompileError::new(
                "DTG-CYPHER-GRAPH-MISMATCH",
                format!(
                    "query selects graph {}, but the session is bound to {}",
                    graph.value(),
                    session.graph_name
                ),
            ));
        }
        let analyzed = SemanticAnalyzer::new().analyze_with_procedures(&parsed, catalog, access)?;
        let fingerprint = query_fingerprint(
            text,
            parsed.profile().semantic_baseline(),
            session.schema_version,
            catalog.revision(),
        );
        let header = PlanHeader::new(
            session.graph_id,
            session.schema_version,
            session.topology_epoch,
            LanguageProfile::Cypher25,
            parsed.profile().semantic_baseline(),
            fingerprint,
        )?;
        let logical_plan = lower(
            parsed.statement(),
            &analyzed,
            header,
            parsed.profile(),
            catalog,
            access,
        )?;
        let mutation_plan = compile_mutations(parsed.statement(), &logical_plan)?;
        let result_schema = logical_plan.output().clone();
        Ok(CompiledQuery {
            fingerprint,
            result_schema,
            effect: analyzed.effect(),
            logical_plan,
            mutation_plan,
            catalog_revision: catalog.revision(),
        })
    }
}

fn compile_mutations(
    statement: &Statement,
    logical_plan: &LogicalPlan,
) -> Result<MutationPlan, CompileError> {
    let mut mutations = Vec::new();
    let Statement::Query(query) = statement else {
        return Ok(MutationPlan { mutations });
    };
    for clause in query.clauses() {
        match clause.kind() {
            ClauseKind::Create => {
                mutations.push(CompiledMutation::Create(parse_pattern(clause_body(
                    clause,
                )?)?));
            }
            ClauseKind::Merge => {
                mutations.push(CompiledMutation::Merge(parse_pattern(clause_body(
                    clause,
                )?)?));
            }
            ClauseKind::Set => {
                for assignment in split_top_level(clause_body(clause)?)? {
                    let (target, value) = split_assignment(assignment)?;
                    mutations.push(CompiledMutation::SetProperty {
                        target: property_target(parse_expression(target)?)?,
                        value: parse_expression(value)?,
                    });
                }
            }
            ClauseKind::Remove => {
                for target in split_top_level(clause_body(clause)?)? {
                    mutations.push(CompiledMutation::RemoveProperty(property_target(
                        parse_expression(target)?,
                    )?));
                }
            }
            ClauseKind::Delete { detach } => {
                let variables = split_top_level(clause_body(clause)?)?
                    .into_iter()
                    .map(|source| match parse_expression(source)? {
                        Expression::Identifier(identifier) => Ok(identifier.value().to_owned()),
                        _ => Err(CompileError::new(
                            "DTG-CYPHER-INVALID-DELETE-TARGET",
                            "DELETE currently requires a bound node or relationship variable",
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                mutations.push(CompiledMutation::Delete { variables, detach });
            }
            ClauseKind::Call => {
                let Some(subquery) = clause.call_subquery() else {
                    continue;
                };
                let identity = child_plan_identity(logical_plan.header(), 0, clause.span().start());
                let child_plan = logical_plan
                    .nodes()
                    .iter()
                    .find_map(|node| match node.operator() {
                        LogicalOperator::Apply { apply } if apply.identity() == identity => {
                            Some(apply.child_plan())
                        }
                        LogicalOperator::BatchSubtransaction { batch }
                            if batch.apply().identity() == identity =>
                        {
                            Some(batch.apply().child_plan())
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        CompileError::new(
                            "DTG-CYPHER-SUBQUERY-PLAN-MISSING",
                            "write subquery has no isolated logical child plan",
                        )
                    })?;
                let mutation_plan =
                    compile_mutations(&Statement::Query(subquery.query().clone()), child_plan)?;
                if !mutation_plan.mutations.is_empty() {
                    mutations.push(CompiledMutation::Subquery(CompiledSubqueryMutation {
                        clause_start: clause.span().start(),
                        imports: subquery
                            .imports()
                            .iter()
                            .map(|name| name.value().to_owned())
                            .collect(),
                        exports: subquery
                            .exports()
                            .iter()
                            .map(|name| name.value().to_owned())
                            .collect(),
                        export_expressions: compile_subquery_exports(subquery)?,
                        read_prefix_plan: read_prefix_plan(child_plan),
                        mutation_plan,
                        batch_rows: subquery.in_transactions().map(|batch| batch.batch_rows()),
                    }));
                }
            }
            _ => {}
        }
    }
    Ok(MutationPlan { mutations })
}

fn compile_subquery_exports(
    subquery: &CallSubquery,
) -> Result<Vec<CompiledSubqueryExport>, CompileError> {
    if subquery.exports().is_empty() {
        return Ok(Vec::new());
    }
    let clause = subquery
        .query()
        .clauses()
        .iter()
        .rev()
        .find(|clause| clause.kind() == ClauseKind::Return)
        .ok_or_else(|| {
            CompileError::new(
                "DTG-CYPHER-SUBQUERY-EXPORT-SHAPE",
                "write subquery exports require a final RETURN projection",
            )
        })?;
    let (_, source) = projection_modifiers(clause_body(clause)?)?;
    let items = split_top_level(source)?;
    if items.len() == 1 && is_star(items[0])? {
        return Ok(subquery
            .exports()
            .iter()
            .map(|name| CompiledSubqueryExport {
                name: name.value().to_owned(),
                expression: Expression::Identifier(name.clone()),
            })
            .collect());
    }
    if items.len() != subquery.exports().len() {
        return Err(CompileError::new(
            "DTG-CYPHER-SUBQUERY-EXPORT-SHAPE",
            "write subquery export expressions do not match its declared output",
        ));
    }
    items
        .into_iter()
        .zip(subquery.exports())
        .map(|(item, export)| {
            let (expression, _) = split_alias(item)?;
            Ok(CompiledSubqueryExport {
                name: export.value().to_owned(),
                expression: parse_expression(expression)?,
            })
        })
        .collect()
}

fn split_assignment(source: &str) -> Result<(&str, &str), CompileError> {
    let lexed = lex(source)
        .map_err(|error| CompileError::new("DTG-CYPHER-INVALID-SET", error.to_string()))?;
    let mut depth = 0_usize;
    for token in lexed.tokens() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1);
            }
            TokenKind::Equal if depth == 0 => {
                let left = source[..token.span().start()].trim();
                let right = source[token.span().end()..].trim();
                if left.is_empty() || right.is_empty() {
                    break;
                }
                return Ok((left, right));
            }
            _ => {}
        }
    }
    Err(CompileError::new(
        "DTG-CYPHER-INVALID-SET",
        "SET requires a property assignment",
    ))
}

fn property_target(expression: Expression) -> Result<PropertyTarget, CompileError> {
    let Expression::Property { value, property } = expression else {
        return Err(CompileError::new(
            "DTG-CYPHER-INVALID-SET-TARGET",
            "property update target must have the form variable.property",
        ));
    };
    let Expression::Identifier(variable) = *value else {
        return Err(CompileError::new(
            "DTG-CYPHER-INVALID-SET-TARGET",
            "property update target must have the form variable.property",
        ));
    };
    Ok(PropertyTarget::new(variable.value(), property.value()))
}

fn query_fingerprint(
    text: &str,
    baseline: &str,
    schema_version: u64,
    catalog_revision: u64,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/CypherQuery/Latest");
    hasher.update(baseline.as_bytes());
    hasher.update(&schema_version.to_be_bytes());
    hasher.update(&catalog_revision.to_be_bytes());
    hasher.update(text.as_bytes());
    *hasher.finalize().as_bytes()
}

fn child_plan_identity(parent: &PlanHeader, kind: u8, discriminator: usize) -> ChildPlanId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/CypherChildPlan/Latest");
    hasher.update(&parent.query_fingerprint());
    hasher.update(&[kind]);
    hasher.update(&discriminator.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest.as_bytes()[..8]);
    let value = u64::from_be_bytes(bytes);
    ChildPlanId::new(value.max(1))
}

fn child_plan_header(
    parent: &PlanHeader,
    identity: ChildPlanId,
) -> Result<PlanHeader, CompileError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/CypherChildPlanFingerprint/Latest");
    hasher.update(&parent.query_fingerprint());
    hasher.update(&identity.value().to_be_bytes());
    PlanHeader::new(
        parent.graph_id(),
        parent.schema_version(),
        parent.topology_epoch(),
        parent.language_profile(),
        parent.semantic_baseline(),
        *hasher.finalize().as_bytes(),
    )
    .map_err(Into::into)
}

fn lower(
    statement: &Statement,
    analyzed: &AnalyzedQuery,
    header: PlanHeader,
    profile: CypherProfile,
    catalog: &ProcedureCatalog,
    access: &ProcedureAccess,
) -> Result<LogicalPlan, CompileError> {
    let mut lowerer = Lowerer {
        builder: LogicalPlanBuilder::new(header.clone()),
        header,
        profile,
        catalog: Some(catalog),
        access: Some(access),
        root: None,
        schema: RowSchema::empty(),
        variables: BTreeMap::new(),
        next_slot: 0,
        temporal_inserted: false,
    };
    match statement {
        Statement::Diff(_) => {
            lowerer.schema = output_schema(analyzed, &mut lowerer.next_slot)?;
            lowerer.root = Some(lowerer.builder.add(
                LogicalOperator::Diff,
                vec![],
                lowerer.schema.clone(),
            )?);
        }
        Statement::Query(query) => {
            lowerer.clause_chain(query.clauses(), analyzed, Some(query.temporal()))?;
        }
    }
    let root = lowerer.ensure_root()?;
    lowerer.builder.finish(root).map_err(Into::into)
}

struct Lowerer<'a> {
    builder: LogicalPlanBuilder,
    header: PlanHeader,
    profile: CypherProfile,
    catalog: Option<&'a ProcedureCatalog>,
    access: Option<&'a ProcedureAccess>,
    root: Option<LogicalNodeId>,
    schema: RowSchema,
    variables: BTreeMap<String, SlotId>,
    next_slot: u32,
    temporal_inserted: bool,
}

#[derive(Clone)]
struct LowerState {
    root: Option<LogicalNodeId>,
    schema: RowSchema,
    variables: BTreeMap<String, SlotId>,
    next_slot: u32,
    temporal_inserted: bool,
}

impl Lowerer<'_> {
    fn clause_chain(
        &mut self,
        clauses: &[Clause],
        analyzed: &AnalyzedQuery,
        temporal: Option<&TemporalContext>,
    ) -> Result<(), CompileError> {
        let branch_start = self.state();
        let mut accumulated: Option<(LogicalNodeId, RowSchema)> = None;
        let mut pending_boundary = None;
        for clause in clauses {
            if let ClauseKind::Union { all } = clause.kind() {
                self.finish_temporal_branch(temporal)?;
                let current = (self.ensure_root()?, self.schema.clone());
                let combined = if let Some(left) = accumulated.take() {
                    self.add_union(
                        left,
                        current,
                        pending_boundary.ok_or_else(|| {
                            CompileError::new(
                                "DTG-CYPHER-UNION-BOUNDARY",
                                "UNION boundary state is incomplete",
                            )
                        })?,
                    )?
                } else {
                    current
                };
                accumulated = Some(combined);
                pending_boundary = Some(all);
                self.restore(branch_start.clone());
                continue;
            }
            if let Some(temporal) = temporal
                && !self.temporal_inserted
                && has_explicit_temporal_scope(temporal)
                && !matches!(clause.kind(), ClauseKind::Match | ClauseKind::OptionalMatch)
            {
                self.temporal_slice(temporal)?;
            }
            self.clause(clause, analyzed)?;
        }
        self.finish_temporal_branch(temporal)?;
        if let Some(left) = accumulated {
            let right = (self.ensure_root()?, self.schema.clone());
            let combined = self.add_union(
                left,
                right,
                pending_boundary.ok_or_else(|| {
                    CompileError::new(
                        "DTG-CYPHER-UNION-BOUNDARY",
                        "UNION boundary state is incomplete",
                    )
                })?,
            )?;
            self.root = Some(combined.0);
            self.schema = combined.1;
            self.variables = self
                .schema
                .columns()
                .iter()
                .map(|column| (column.name().to_owned(), column.slot()))
                .collect();
        }
        Ok(())
    }

    fn add_union(
        &mut self,
        left: (LogicalNodeId, RowSchema),
        right: (LogicalNodeId, RowSchema),
        all: bool,
    ) -> Result<(LogicalNodeId, RowSchema), CompileError> {
        if !union_schemas_compatible(&left.1, &right.1) {
            return Err(CompileError::new(
                "DTG-CYPHER-UNION-SCHEMA-MISMATCH",
                "UNION query parts must return the same columns and types",
            ));
        }
        let schema = left.1;
        let right_root = if schema == right.1 {
            right.0
        } else {
            let expressions = schema
                .columns()
                .iter()
                .zip(right.1.columns())
                .map(|(output, input)| (output.slot(), ScalarExpr::Slot(input.slot())))
                .collect();
            self.builder.add(
                LogicalOperator::Project { expressions },
                vec![right.0],
                schema.clone(),
            )?
        };
        if let Some(next_slot) = schema
            .columns()
            .iter()
            .map(|column| column.slot().value().saturating_add(1))
            .max()
        {
            self.next_slot = self.next_slot.max(next_slot);
        }
        let root = self.builder.add(
            LogicalOperator::Union { all },
            vec![left.0, right_root],
            schema.clone(),
        )?;
        Ok((root, schema))
    }

    fn finish_temporal_branch(
        &mut self,
        temporal: Option<&TemporalContext>,
    ) -> Result<(), CompileError> {
        if let Some(temporal) = temporal
            && !self.temporal_inserted
            && has_explicit_temporal_scope(temporal)
        {
            self.temporal_slice(temporal)?;
        }
        Ok(())
    }

    fn state(&self) -> LowerState {
        LowerState {
            root: self.root,
            schema: self.schema.clone(),
            variables: self.variables.clone(),
            next_slot: self.next_slot,
            temporal_inserted: self.temporal_inserted,
        }
    }

    fn restore(&mut self, state: LowerState) {
        self.root = state.root;
        self.schema = state.schema;
        self.variables = state.variables;
        self.next_slot = state.next_slot;
        self.temporal_inserted = state.temporal_inserted;
    }

    fn clause(&mut self, clause: &Clause, analyzed: &AnalyzedQuery) -> Result<(), CompileError> {
        match clause.kind() {
            ClauseKind::Match | ClauseKind::OptionalMatch => {
                let pattern = parse_pattern(clause_body(clause)?)?;
                self.pattern(&pattern)?;
            }
            ClauseKind::Where | ClauseKind::Filter => {
                let expression = parse_expression(clause_body(clause)?)?;
                let predicate = self.scalar(&expression)?;
                let input = self.ensure_root()?;
                self.root = Some(self.builder.add(
                    LogicalOperator::Filter { predicate },
                    vec![input],
                    self.schema.clone(),
                )?);
            }
            ClauseKind::Return => self.project(clause_body(clause)?, analyzed)?,
            ClauseKind::Create => {
                self.bind_write_pattern(&parse_pattern(clause_body(clause)?)?)?;
                self.simple_unary(LogicalOperator::Create)?;
            }
            ClauseKind::Merge => {
                self.bind_write_pattern(&parse_pattern(clause_body(clause)?)?)?;
                self.simple_unary(LogicalOperator::Merge)?;
            }
            ClauseKind::Set => self.simple_unary(LogicalOperator::Set)?,
            ClauseKind::Remove => self.simple_unary(LogicalOperator::Remove)?,
            ClauseKind::Delete { detach } => {
                self.simple_unary(LogicalOperator::Delete { detach })?;
            }
            ClauseKind::OrderBy => {
                let visible_schema = self.schema.clone();
                let mut project_expressions = visible_schema
                    .columns()
                    .iter()
                    .map(|column| (column.slot(), ScalarExpr::Slot(column.slot())))
                    .collect::<Vec<_>>();
                let mut project_columns = visible_schema.columns().to_vec();
                let mut keys = Vec::new();
                for source in split_top_level(clause_body(clause)?)? {
                    let (source, ascending) = split_sort_direction(source)?;
                    let expression = parse_expression(source)?;
                    let scalar = self.scalar(&expression)?;
                    let slot = if let ScalarExpr::Slot(slot) = scalar {
                        slot
                    } else {
                        let slot = self.allocate_slot();
                        project_expressions.push((slot, scalar));
                        project_columns.push(Column::new(
                            slot,
                            format!("__sort{}", slot.value()),
                            ValueType::Any,
                            true,
                        ));
                        slot
                    };
                    keys.push(SortKey::new(slot, ascending));
                }
                let has_hidden_keys = project_columns.len() != visible_schema.columns().len();
                if has_hidden_keys {
                    let input = self.ensure_root()?;
                    self.schema = RowSchema::new(project_columns)?;
                    self.root = Some(self.builder.add(
                        LogicalOperator::Project {
                            expressions: project_expressions,
                        },
                        vec![input],
                        self.schema.clone(),
                    )?);
                }
                self.simple_unary(LogicalOperator::Sort { keys })?;
                if has_hidden_keys {
                    let input = self.ensure_root()?;
                    let expressions = visible_schema
                        .columns()
                        .iter()
                        .map(|column| (column.slot(), ScalarExpr::Slot(column.slot())))
                        .collect();
                    self.root = Some(self.builder.add(
                        LogicalOperator::Project { expressions },
                        vec![input],
                        visible_schema.clone(),
                    )?);
                    self.schema = visible_schema;
                }
            }
            ClauseKind::Skip | ClauseKind::Offset => {
                let count = self.scalar(&parse_expression(clause_body(clause)?)?)?;
                self.simple_unary(LogicalOperator::Skip { count })?;
            }
            ClauseKind::Limit => {
                let count = self.scalar(&parse_expression(clause_body(clause)?)?)?;
                self.simple_unary(LogicalOperator::Limit { count })?;
            }
            ClauseKind::Call => {
                if clause.call_subquery().is_some() {
                    self.subquery_call(clause, analyzed)?;
                } else {
                    self.procedure_call(clause, analyzed)?;
                }
            }
            ClauseKind::Unwind => self.unwind(clause_body(clause)?)?,
            ClauseKind::With => self.with_projection(clause_body(clause)?)?,
            ClauseKind::Finish => self.simple_unary(LogicalOperator::Finish)?,
            ClauseKind::Let => self.with_projection(clause_body(clause)?)?,
            ClauseKind::Yield => {}
            ClauseKind::Foreach
            | ClauseKind::For
            | ClauseKind::Union { .. }
            | ClauseKind::Next
            | ClauseKind::When => {
                return Err(CompileError::new(
                    "DTG-CYPHER-CLAUSE-NOT-YET-LOWERED",
                    format!(
                        "clause {:?} requires a dedicated runtime operator",
                        clause.kind()
                    ),
                ));
            }
        }
        Ok(())
    }

    fn pattern(&mut self, pattern: &Pattern) -> Result<(), CompileError> {
        for path in pattern.paths() {
            let (source, source_column) = self.binding(
                path.start().variable().map(cypher_ast::Identifier::value),
                ValueType::Node,
            );
            if self.root.is_none() {
                self.schema = RowSchema::new(vec![source_column])?;
                self.root = Some(
                    self.builder.add(
                        LogicalOperator::NodeScan {
                            binding: source,
                            labels: path
                                .start()
                                .labels()
                                .iter()
                                .map(|label| stable_id(label.value()))
                                .collect(),
                        },
                        vec![],
                        self.schema.clone(),
                    )?,
                );
                self.node_property_filter(source, path.start())?;
            } else if !self.schema.contains(source) {
                let input = self.ensure_root()?;
                let scan_schema = RowSchema::new(vec![source_column.clone()])?;
                let scan = self.builder.add(
                    LogicalOperator::NodeScan {
                        binding: source,
                        labels: path
                            .start()
                            .labels()
                            .iter()
                            .map(|label| stable_id(label.value()))
                            .collect(),
                    },
                    vec![],
                    scan_schema,
                )?;
                let mut columns = self.schema.columns().to_vec();
                columns.push(source_column);
                self.schema = RowSchema::new(columns)?;
                self.root = Some(self.builder.add(
                    LogicalOperator::InnerJoin,
                    vec![input, scan],
                    self.schema.clone(),
                )?);
                self.node_property_filter(source, path.start())?;
            } else {
                self.node_property_filter(source, path.start())?;
            }
            let mut current_source = source;
            for chain in path.chains() {
                let (relationship, relationship_column) = self.binding(
                    chain
                        .relationship()
                        .variable()
                        .map(cypher_ast::Identifier::value),
                    ValueType::Relationship,
                );
                let (destination, destination_column) = self.binding(
                    chain.node().variable().map(cypher_ast::Identifier::value),
                    ValueType::Node,
                );
                let mut columns = self.schema.columns().to_vec();
                if !self.schema.contains(relationship) {
                    columns.push(relationship_column);
                }
                if !self.schema.contains(destination) {
                    columns.push(destination_column);
                }
                self.schema = RowSchema::new(columns)?;
                let input = self.ensure_root()?;
                self.root = Some(
                    self.builder.add(
                        LogicalOperator::Expand {
                            source: current_source,
                            relationship,
                            destination,
                            outgoing: !matches!(
                                chain.relationship().direction(),
                                cypher_ast::RelationshipDirection::Incoming
                            ),
                            types: chain
                                .relationship()
                                .types()
                                .iter()
                                .map(|relationship_type| stable_id(relationship_type.value()))
                                .collect(),
                        },
                        vec![input],
                        self.schema.clone(),
                    )?,
                );
                self.node_property_filter(destination, chain.node())?;
                current_source = destination;
            }
        }
        Ok(())
    }

    fn unwind(&mut self, source: &str) -> Result<(), CompileError> {
        let (expression_source, name) = split_unwind_alias(source)?;
        let expression = parse_expression(expression_source)?;
        let scalar = self.scalar(&expression)?;
        let input = self.ensure_root()?;
        let (slot, column) = self.binding(Some(name), ValueType::Any);
        let column = Column::new(
            column.slot(),
            column.name(),
            column.value_type().clone(),
            true,
        );
        let mut columns = self.schema.columns().to_vec();
        if !columns.iter().any(|existing| existing.slot() == slot) {
            columns.push(column);
        }
        self.schema = RowSchema::new(columns)?;
        self.root = Some(self.builder.add(
            LogicalOperator::Unwind {
                expression: scalar,
                binding: slot,
            },
            vec![input],
            self.schema.clone(),
        )?);
        Ok(())
    }

    fn node_property_filter(
        &mut self,
        slot: SlotId,
        node: &NodePattern,
    ) -> Result<(), CompileError> {
        let Some(Expression::Map(items)) = node.properties() else {
            return Ok(());
        };
        let mut predicate = None;
        for (name, value) in items {
            let equality = ScalarExpr::Equal(
                Box::new(ScalarExpr::Property {
                    value: Box::new(ScalarExpr::Slot(slot)),
                    property_id: stable_id(name.value()),
                }),
                Box::new(self.scalar(value)?),
            );
            predicate = Some(match predicate {
                None => equality,
                Some(previous) => ScalarExpr::And(Box::new(previous), Box::new(equality)),
            });
        }
        if let Some(predicate) = predicate {
            let input = self.ensure_root()?;
            self.root = Some(self.builder.add(
                LogicalOperator::Filter { predicate },
                vec![input],
                self.schema.clone(),
            )?);
        }
        Ok(())
    }

    fn with_projection(&mut self, source: &str) -> Result<(), CompileError> {
        self.projection(source, None)
    }

    fn bind_write_pattern(&mut self, pattern: &Pattern) -> Result<(), CompileError> {
        let mut columns = self.schema.columns().to_vec();
        for path in pattern.paths() {
            self.bind_write_element(
                path.start().variable().map(cypher_ast::Identifier::value),
                ValueType::Node,
                &mut columns,
            );
            for chain in path.chains() {
                self.bind_write_element(
                    chain
                        .relationship()
                        .variable()
                        .map(cypher_ast::Identifier::value),
                    ValueType::Relationship,
                    &mut columns,
                );
                self.bind_write_element(
                    chain.node().variable().map(cypher_ast::Identifier::value),
                    ValueType::Node,
                    &mut columns,
                );
            }
        }
        self.schema = RowSchema::new(columns)?;
        Ok(())
    }

    fn bind_write_element(
        &mut self,
        name: Option<&str>,
        value_type: ValueType,
        columns: &mut Vec<Column>,
    ) {
        let (slot, column) = self.binding(name, value_type);
        if !columns.iter().any(|existing| existing.slot() == slot) {
            columns.push(column);
        }
    }

    fn project(&mut self, source: &str, analyzed: &AnalyzedQuery) -> Result<(), CompileError> {
        self.projection(source, Some(analyzed.output()))
    }

    fn projection(
        &mut self,
        source: &str,
        fields: Option<&[cypher_sema::OutputField]>,
    ) -> Result<(), CompileError> {
        let (distinct, source) = projection_modifiers(source)?;
        let mut items = Vec::new();
        for item in split_top_level(source)? {
            if is_star(item)? {
                for column in self.schema.columns() {
                    items.push((
                        column.name().to_owned(),
                        ScalarExpr::Slot(column.slot()),
                        column.value_type().clone(),
                        column.nullable(),
                    ));
                }
                continue;
            }
            let (expression_source, alias) = split_alias(item)?;
            let expression = parse_expression(expression_source)?;
            let (scalar, scalar_type, nullable) = match &expression {
                Expression::ExistsSubquery(query) => (
                    ScalarExpr::Slot(self.scalar_subquery(query, true)?),
                    ValueType::Boolean,
                    false,
                ),
                Expression::CountSubquery(query) => (
                    ScalarExpr::Slot(self.scalar_subquery(query, false)?),
                    ValueType::Integer,
                    false,
                ),
                _ => (self.scalar(&expression)?, ValueType::Any, true),
            };
            let name = alias
                .map(str::to_owned)
                .or_else(|| match &expression {
                    Expression::Identifier(identifier) => Some(identifier.value().to_owned()),
                    Expression::Property { property, .. } => Some(property.value().to_owned()),
                    _ => None,
                })
                .unwrap_or_else(|| format!("column{}", items.len()));
            let value_type = match &scalar {
                ScalarExpr::Slot(slot) => self
                    .schema
                    .columns()
                    .iter()
                    .find(|column| column.slot() == *slot)
                    .map_or(scalar_type, |column| column.value_type().clone()),
                _ => scalar_type,
            };
            items.push((name, scalar, value_type, nullable));
        }
        if let Some(fields) = fields {
            if items.len() != fields.len() {
                return Err(CompileError::new(
                    "DTG-CYPHER-PROJECTION-SHAPE",
                    "semantic and compiler projection counts differ",
                ));
            }
            for ((name, _, kind, _), field) in items.iter_mut().zip(fields) {
                *name = field.name().to_owned();
                *kind = value_type(field.cypher_type());
            }
        }
        let input = self.ensure_root()?;
        let has_aggregate = items
            .iter()
            .any(|(_, expression, _, _)| contains_aggregate_scalar(expression));
        let mut columns = Vec::with_capacity(items.len());
        if has_aggregate {
            let mut preproject = self
                .schema
                .columns()
                .iter()
                .map(|column| (column.slot(), ScalarExpr::Slot(column.slot())))
                .collect::<Vec<_>>();
            let mut preproject_columns = self.schema.columns().to_vec();
            let mut grouping = Vec::new();
            let mut aggregates = Vec::new();
            let mut aggregate_columns = Vec::new();
            let mut final_expressions = Vec::with_capacity(items.len());
            for (name, expression, value_type, nullable) in items {
                let slot = self.allocate_slot();
                let column = Column::new(slot, name, value_type, nullable);
                columns.push(column.clone());
                if contains_aggregate_scalar(&expression) {
                    let expression = self.rewrite_aggregate_expression(
                        expression,
                        &mut preproject,
                        &mut preproject_columns,
                        &mut grouping,
                        &mut aggregates,
                        &mut aggregate_columns,
                    )?;
                    final_expressions.push((slot, expression));
                } else {
                    grouping.push(slot);
                    preproject.push((slot, expression));
                    preproject_columns.push(column.clone());
                    aggregate_columns.push(column);
                    final_expressions.push((slot, ScalarExpr::Slot(slot)));
                }
            }
            let mut aggregate_input = input;
            if !grouping.is_empty() {
                let preproject_schema = RowSchema::new(preproject_columns)?;
                aggregate_input = self.builder.add(
                    LogicalOperator::Project {
                        expressions: preproject,
                    },
                    vec![input],
                    preproject_schema,
                )?;
            }
            let aggregate_output = RowSchema::new(aggregate_columns)?;
            let aggregate_root = self.builder.add(
                LogicalOperator::Aggregate {
                    grouping,
                    aggregates,
                },
                vec![aggregate_input],
                aggregate_output,
            )?;
            let output = RowSchema::new(columns)?;
            self.root = Some(self.builder.add(
                LogicalOperator::Project {
                    expressions: final_expressions,
                },
                vec![aggregate_root],
                output.clone(),
            )?);
            self.schema = output;
        } else {
            let mut expressions = Vec::with_capacity(items.len());
            for (name, expression, value_type, nullable) in items {
                let slot = self.allocate_slot();
                expressions.push((slot, expression));
                columns.push(Column::new(slot, name, value_type, nullable));
            }
            let output = RowSchema::new(columns)?;
            self.root = Some(self.builder.add(
                LogicalOperator::Project { expressions },
                vec![input],
                output.clone(),
            )?);
            self.schema = output;
        }
        if distinct {
            let input = self.ensure_root()?;
            let grouping = self.schema.columns().iter().map(Column::slot).collect();
            self.root = Some(self.builder.add(
                LogicalOperator::Aggregate {
                    grouping,
                    aggregates: Vec::new(),
                },
                vec![input],
                self.schema.clone(),
            )?);
        }
        self.variables = self
            .schema
            .columns()
            .iter()
            .map(|column| (column.name().to_owned(), column.slot()))
            .collect();
        Ok(())
    }

    fn scalar_subquery(
        &mut self,
        query: &cypher_ast::QueryStatement,
        exists: bool,
    ) -> Result<SlotId, CompileError> {
        let catalog = self.catalog.ok_or_else(|| {
            CompileError::new(
                "DTG-CYPHER-SUBQUERY-SEMANTIC-CONTEXT",
                "subquery expression has no procedure catalog context",
            )
        })?;
        let access = self.access.ok_or_else(|| {
            CompileError::new(
                "DTG-CYPHER-SUBQUERY-SEMANTIC-CONTEXT",
                "subquery expression has no procedure access context",
            )
        })?;
        let parent_input = self.ensure_root()?;
        let parent_schema = self.schema.clone();
        let bindings = parent_schema
            .columns()
            .iter()
            .map(|column| {
                (
                    column.name().to_owned(),
                    cypher_type_from_value_type(column.value_type()),
                )
            })
            .collect();
        let analyzed = SemanticAnalyzer::new().analyze_query_with_bindings_and_procedures(
            query,
            self.profile,
            bindings,
            catalog,
            access,
        )?;
        if !matches!(analyzed.effect(), QueryEffect::ReadOnly) {
            return Err(CompileError::new(
                "DTG-CYPHER-SUBQUERY-EXPRESSION-WRITE",
                "EXISTS and COUNT subqueries must be read-only",
            ));
        }
        let discriminator = usize::try_from(self.next_slot).unwrap_or(usize::MAX);
        let identity = child_plan_identity(&self.header, if exists { 1 } else { 2 }, discriminator);
        let child_header = child_plan_header(&self.header, identity)?;
        let mut child_columns = Vec::with_capacity(parent_schema.columns().len());
        let mut child_variables = BTreeMap::new();
        let mut imports = Vec::with_capacity(parent_schema.columns().len());
        for (index, parent_column) in parent_schema.columns().iter().enumerate() {
            let child_slot = SlotId::new(u32::try_from(index).map_err(|_| {
                CompileError::new(
                    "DTG-CYPHER-SUBQUERY-SCHEMA",
                    "subquery import schema is too wide",
                )
            })?);
            child_columns.push(Column::new(
                child_slot,
                parent_column.name(),
                parent_column.value_type().clone(),
                parent_column.nullable(),
            ));
            child_variables.insert(parent_column.name().to_owned(), child_slot);
            imports.push(ApplySlotMapping::new(parent_column.slot(), child_slot));
        }
        let child_schema = RowSchema::new(child_columns)?;
        let mut child = Lowerer {
            builder: LogicalPlanBuilder::new(child_header.clone()),
            header: child_header,
            profile: self.profile,
            catalog: self.catalog,
            access: self.access,
            root: None,
            schema: child_schema.clone(),
            variables: child_variables,
            next_slot: u32::try_from(child_schema.columns().len()).map_err(|_| {
                CompileError::new(
                    "DTG-CYPHER-SUBQUERY-SCHEMA",
                    "subquery import schema is too wide",
                )
            })?,
            temporal_inserted: true,
        };
        child.root = Some(child.builder.add(
            LogicalOperator::Argument,
            Vec::new(),
            child_schema,
        )?);
        child.clause_chain(query.clauses(), &analyzed, None)?;
        let child_root = child.ensure_root()?;
        let child_plan = child.builder.finish(child_root)?;

        let result = self.allocate_slot();
        let result_type = if exists {
            ValueType::Boolean
        } else {
            ValueType::Integer
        };
        let mut output = parent_schema.columns().to_vec();
        output.push(Column::new(
            result,
            if exists {
                format!("__exists{}", result.value())
            } else {
                format!("__count{}", result.value())
            },
            result_type,
            false,
        ));
        self.schema = RowSchema::new(output)?;
        self.root = Some(self.builder.add(
            LogicalOperator::Apply {
                apply: LogicalApply::new(
                    identity,
                    if exists {
                        ApplyKind::Exists { output: result }
                    } else {
                        ApplyKind::Count { output: result }
                    },
                    imports,
                    Vec::new(),
                    child_plan,
                    MAX_APPLY_INVOCATIONS,
                    MAX_APPLY_OUTPUT_ROWS,
                    MAX_APPLY_DEPTH,
                ),
            },
            vec![parent_input],
            self.schema.clone(),
        )?);
        Ok(result)
    }

    fn scalar(&self, expression: &Expression) -> Result<ScalarExpr, CompileError> {
        match expression {
            Expression::Null => Ok(ScalarExpr::Literal(GraphValue::Null)),
            Expression::Boolean(value) => Ok(ScalarExpr::Literal(GraphValue::Boolean(*value))),
            Expression::Integer(value) => value
                .parse::<i64>()
                .map(GraphValue::Integer)
                .map(ScalarExpr::Literal)
                .map_err(|_| CompileError::new("DTG-CYPHER-INTEGER-OVERFLOW", value)),
            Expression::Float(value) => value
                .parse::<f64>()
                .map(f64::to_bits)
                .map(GraphValue::FloatBits)
                .map(ScalarExpr::Literal)
                .map_err(|_| CompileError::new("DTG-CYPHER-INVALID-FLOAT", value)),
            Expression::String(value) => Ok(ScalarExpr::Literal(GraphValue::String(value.clone()))),
            Expression::Parameter(name) => Ok(ScalarExpr::Parameter(name.clone())),
            Expression::Identifier(identifier) => self
                .variables
                .get(identifier.value())
                .copied()
                .map(ScalarExpr::Slot)
                .ok_or_else(|| {
                    CompileError::new(
                        "DTG-CYPHER-UNBOUND-VARIABLE",
                        format!("variable {} has no slot", identifier.value()),
                    )
                }),
            Expression::List(items) => items
                .iter()
                .map(|item| self.scalar(item))
                .collect::<Result<Vec<_>, _>>()
                .map(ScalarExpr::List),
            Expression::Map(items) => Ok(ScalarExpr::Map(
                items
                    .iter()
                    .map(|(key, value)| Ok((stable_id(key.value()), self.scalar(value)?)))
                    .collect::<Result<Vec<_>, CompileError>>()?,
            )),
            Expression::Unary {
                operator,
                expression,
            } => {
                let value = Box::new(self.scalar(expression)?);
                match operator {
                    UnaryOperator::Not => Ok(ScalarExpr::Not(value)),
                    UnaryOperator::Minus => Ok(ScalarExpr::Negate(value)),
                    UnaryOperator::Plus => Ok(*value),
                }
            }
            Expression::Binary {
                operator,
                left,
                right,
            } => {
                let left = Box::new(self.scalar(left)?);
                let right = Box::new(self.scalar(right)?);
                scalar_binary(*operator, left, right)
            }
            Expression::Property { value, property } => Ok(ScalarExpr::Property {
                value: Box::new(self.scalar(value)?),
                property_id: stable_id(property.value()),
            }),
            Expression::Index { .. } => Err(CompileError::new(
                "DTG-CYPHER-INDEX-LOWERING",
                "index lowering is not available in the initial IR expression set",
            )),
            Expression::FunctionCall { name, arguments } => Ok(ScalarExpr::Function {
                function_id: stable_id(
                    &name
                        .iter()
                        .map(|part| part.value())
                        .collect::<Vec<_>>()
                        .join(".")
                        .to_ascii_lowercase(),
                ),
                arguments: arguments
                    .iter()
                    .map(|argument| self.scalar(argument))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            Expression::ExistsSubquery(_) | Expression::CountSubquery(_) => Err(CompileError::new(
                "DTG-CYPHER-SUBQUERY-EXPRESSION-NOT-LOWERED",
                "subquery expressions require an isolated Apply plan",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn rewrite_aggregate_expression(
        &mut self,
        expression: ScalarExpr,
        preproject: &mut Vec<(SlotId, ScalarExpr)>,
        preproject_columns: &mut Vec<Column>,
        grouping: &mut Vec<SlotId>,
        aggregates: &mut Vec<(SlotId, ScalarExpr)>,
        aggregate_columns: &mut Vec<Column>,
    ) -> Result<ScalarExpr, CompileError> {
        if let ScalarExpr::Function {
            function_id,
            arguments,
        } = &expression
            && is_aggregate_function(*function_id)
        {
            if arguments.iter().any(contains_aggregate_scalar) {
                return Err(CompileError::new(
                    "DTG-CYPHER-NESTED-AGGREGATE",
                    "aggregate function arguments cannot contain another aggregate function",
                ));
            }
            let slot = self.allocate_slot();
            aggregates.push((slot, expression));
            aggregate_columns.push(Column::new(
                slot,
                format!("__aggregate{}", slot.value()),
                ValueType::Any,
                true,
            ));
            return Ok(ScalarExpr::Slot(slot));
        }
        if !contains_aggregate_scalar(&expression) {
            if scalar_references_slot(&expression) {
                let slot = self.allocate_slot();
                grouping.push(slot);
                preproject.push((slot, expression));
                let column = Column::new(
                    slot,
                    format!("__group{}", slot.value()),
                    ValueType::Any,
                    true,
                );
                preproject_columns.push(column.clone());
                aggregate_columns.push(column);
                return Ok(ScalarExpr::Slot(slot));
            }
            return Ok(expression);
        }
        let mut rewrite = |expression| {
            self.rewrite_aggregate_expression(
                expression,
                preproject,
                preproject_columns,
                grouping,
                aggregates,
                aggregate_columns,
            )
        };
        Ok(match expression {
            ScalarExpr::List(items) => ScalarExpr::List(
                items
                    .into_iter()
                    .map(&mut rewrite)
                    .collect::<Result<_, _>>()?,
            ),
            ScalarExpr::Map(items) => ScalarExpr::Map(
                items
                    .into_iter()
                    .map(|(key, value)| Ok((key, rewrite(value)?)))
                    .collect::<Result<_, CompileError>>()?,
            ),
            ScalarExpr::Property { value, property_id } => ScalarExpr::Property {
                value: Box::new(rewrite(*value)?),
                property_id,
            },
            ScalarExpr::Not(value) => ScalarExpr::Not(Box::new(rewrite(*value)?)),
            ScalarExpr::Negate(value) => ScalarExpr::Negate(Box::new(rewrite(*value)?)),
            ScalarExpr::Equal(left, right) => {
                ScalarExpr::Equal(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::NotEqual(left, right) => {
                ScalarExpr::NotEqual(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::Less(left, right) => {
                ScalarExpr::Less(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::LessEqual(left, right) => {
                ScalarExpr::LessEqual(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::Greater(left, right) => {
                ScalarExpr::Greater(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::GreaterEqual(left, right) => {
                ScalarExpr::GreaterEqual(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::And(left, right) => {
                ScalarExpr::And(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::Or(left, right) => {
                ScalarExpr::Or(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::Add(left, right) => {
                ScalarExpr::Add(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::Subtract(left, right) => {
                ScalarExpr::Subtract(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::Multiply(left, right) => {
                ScalarExpr::Multiply(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::Divide(left, right) => {
                ScalarExpr::Divide(Box::new(rewrite(*left)?), Box::new(rewrite(*right)?))
            }
            ScalarExpr::Function {
                function_id,
                arguments,
            } => ScalarExpr::Function {
                function_id,
                arguments: arguments
                    .into_iter()
                    .map(&mut rewrite)
                    .collect::<Result<_, _>>()?,
            },
            ScalarExpr::Slot(_) | ScalarExpr::Parameter(_) | ScalarExpr::Literal(_) => {
                unreachable!("aggregate-containing expression must have a recursive child")
            }
        })
    }

    fn temporal_slice(&mut self, context: &TemporalContext) -> Result<(), CompileError> {
        let valid_time = match context.valid_time() {
            None => ValidTimeSpec::Current,
            Some(ValidTimeScope::AsOf(expression)) => ValidTimeSpec::AsOf(self.scalar(expression)?),
            Some(ValidTimeScope::Between { start, end }) => ValidTimeSpec::Between {
                start: self.scalar(start)?,
                end: self.scalar(end)?,
            },
        };
        let transaction_time = match context.transaction_time() {
            TransactionTimeScope::Current => TransactionTimeSpec::Current,
            TransactionTimeScope::AsOf(expression) => {
                TransactionTimeSpec::AsOf(self.scalar(expression)?)
            }
        };
        let input = self.ensure_root()?;
        self.root = Some(self.builder.add(
            LogicalOperator::TemporalSlice {
                valid_time,
                transaction_time,
            },
            vec![input],
            self.schema.clone(),
        )?);
        self.temporal_inserted = true;
        Ok(())
    }

    fn procedure_call(
        &mut self,
        clause: &Clause,
        analyzed: &AnalyzedQuery,
    ) -> Result<(), CompileError> {
        let analyzed = analyzed
            .procedures()
            .iter()
            .find(|procedure| procedure.clause_start() == clause.span().start())
            .ok_or_else(|| {
                CompileError::new(
                    "DTG-CYPHER-PROCEDURE-DESCRIPTOR-MISSING",
                    "CALL clause has no semantically resolved procedure descriptor",
                )
            })?;
        let input = self.ensure_root()?;
        let mut columns = self.schema.columns().to_vec();
        let mut yields = Vec::with_capacity(analyzed.yields().len());
        for item in analyzed.yields() {
            let slot = self.allocate_slot();
            self.variables.insert(item.output_name().to_owned(), slot);
            columns.push(Column::new(
                slot,
                item.output_name(),
                value_type(item.cypher_type()),
                item.nullable(),
            ));
            yields.push(ProcedureYieldBinding::new(item.source_index(), slot));
        }
        let arguments = analyzed
            .arguments()
            .iter()
            .map(|argument| {
                Ok(ProcedureArgument::new(
                    argument.name(),
                    self.scalar(argument.expression())?,
                ))
            })
            .collect::<Result<Vec<_>, CompileError>>()?;
        let descriptor = analyzed.descriptor();
        let limits = descriptor.limits();
        let procedure = ResolvedProcedure::new(
            *descriptor.identity(),
            descriptor.name(),
            arguments,
            yields,
            descriptor.output().clone(),
            descriptor.effect(),
            descriptor.placement(),
            limits.max_invocations(),
            limits.max_input_rows(),
            limits.max_output_rows(),
            limits.max_value_bytes(),
            limits.max_result_bytes(),
            descriptor.supports_overlay(),
        );
        self.schema = RowSchema::new(columns)?;
        self.root = Some(self.builder.add(
            LogicalOperator::ProcedureCall { procedure },
            vec![input],
            self.schema.clone(),
        )?);
        Ok(())
    }

    fn subquery_call(
        &mut self,
        clause: &Clause,
        analyzed: &AnalyzedQuery,
    ) -> Result<(), CompileError> {
        let subquery = clause.call_subquery().ok_or_else(|| {
            CompileError::new(
                "DTG-CYPHER-SUBQUERY-DESCRIPTOR-MISSING",
                "CALL subquery has no parsed body",
            )
        })?;
        let query = subquery.query();
        if query.graph().is_some()
            || query.temporal().valid_time().is_some()
            || !matches!(
                query.temporal().transaction_time(),
                TransactionTimeScope::Current
            )
        {
            return Err(CompileError::new(
                "DTG-CYPHER-SUBQUERY-SCOPE",
                "CALL subquery inherits the outer graph and temporal scope",
            ));
        }
        let analyzed = analyzed.subquery(clause.span().start()).ok_or_else(|| {
            CompileError::new(
                "DTG-CYPHER-SUBQUERY-SEMANTIC-MISSING",
                "CALL subquery has no authority-preserving semantic result",
            )
        })?;
        if !analyzed.procedures().is_empty() {
            return Err(CompileError::new(
                "DTG-CYPHER-SUBQUERY-PROCEDURE-UNSUPPORTED",
                "CALL subquery procedure calls require a dedicated staged execution boundary",
            ));
        }
        let parent_input = self.ensure_root()?;
        let parent_schema = self.schema.clone();
        let identity = child_plan_identity(&self.header, 0, clause.span().start());
        let child_header = child_plan_header(&self.header, identity)?;
        let mut child_columns = Vec::with_capacity(subquery.imports().len());
        let mut child_variables = BTreeMap::new();
        let mut imports = Vec::with_capacity(subquery.imports().len());
        for (index, import) in subquery.imports().iter().enumerate() {
            let parent_slot = self.variables.get(import.value()).copied().ok_or_else(|| {
                CompileError::new(
                    "DTG-CYPHER-UNKNOWN-SUBQUERY-IMPORT",
                    format!("subquery import {} has no parent slot", import.value()),
                )
            })?;
            let parent_column = parent_schema
                .columns()
                .iter()
                .find(|column| column.slot() == parent_slot)
                .ok_or_else(|| {
                    CompileError::new(
                        "DTG-CYPHER-UNKNOWN-SUBQUERY-IMPORT",
                        format!("subquery import {} has no parent column", import.value()),
                    )
                })?;
            let child_slot = SlotId::new(u32::try_from(index).map_err(|_| {
                CompileError::new(
                    "DTG-CYPHER-SUBQUERY-SCHEMA",
                    "subquery import schema is too wide",
                )
            })?);
            child_columns.push(Column::new(
                child_slot,
                import.value(),
                parent_column.value_type().clone(),
                parent_column.nullable(),
            ));
            child_variables.insert(import.value().to_owned(), child_slot);
            imports.push(ApplySlotMapping::new(parent_slot, child_slot));
        }
        let child_schema = RowSchema::new(child_columns)?;
        let mut child = Lowerer {
            builder: LogicalPlanBuilder::new(child_header.clone()),
            header: child_header,
            profile: self.profile,
            catalog: self.catalog,
            access: self.access,
            root: None,
            schema: child_schema.clone(),
            variables: child_variables,
            next_slot: u32::try_from(child_schema.columns().len()).map_err(|_| {
                CompileError::new(
                    "DTG-CYPHER-SUBQUERY-SCHEMA",
                    "subquery import schema is too wide",
                )
            })?,
            temporal_inserted: true,
        };
        child.root = Some(child.builder.add(
            LogicalOperator::Argument,
            Vec::new(),
            child_schema,
        )?);
        child.clause_chain(query.clauses(), analyzed, None)?;
        let child_root = child.ensure_root()?;
        let child_plan = child.builder.finish(child_root)?;

        let mut output_columns = parent_schema.columns().to_vec();
        let mut exports = Vec::with_capacity(child_plan.output().columns().len());
        for child_column in child_plan.output().columns() {
            let parent_slot = self.allocate_slot();
            output_columns.push(Column::new(
                parent_slot,
                child_column.name(),
                child_column.value_type().clone(),
                child_column.nullable(),
            ));
            self.variables
                .insert(child_column.name().to_owned(), parent_slot);
            exports.push(ApplySlotMapping::new(parent_slot, child_column.slot()));
        }
        self.schema = RowSchema::new(output_columns)?;
        let apply = LogicalApply::new(
            identity,
            ApplyKind::Inner,
            imports,
            exports,
            child_plan,
            MAX_APPLY_INVOCATIONS,
            MAX_APPLY_OUTPUT_ROWS,
            MAX_APPLY_DEPTH,
        );
        let operator = if let Some(batch) = subquery.in_transactions() {
            LogicalOperator::BatchSubtransaction {
                batch: LogicalBatchSubtransaction::new(apply, batch.batch_rows()),
            }
        } else {
            LogicalOperator::Apply { apply }
        };
        self.root = Some(
            self.builder
                .add(operator, vec![parent_input], self.schema.clone())?,
        );
        Ok(())
    }

    fn simple_unary(&mut self, operator: LogicalOperator) -> Result<(), CompileError> {
        let input = self.ensure_root()?;
        self.root = Some(
            self.builder
                .add(operator, vec![input], self.schema.clone())?,
        );
        Ok(())
    }

    fn binding(&mut self, name: Option<&str>, value_type: ValueType) -> (SlotId, Column) {
        if let Some(name) = name
            && let Some(slot) = self.variables.get(name).copied()
        {
            return (slot, Column::new(slot, name, value_type, false));
        }
        let slot = self.allocate_slot();
        let name = name.map_or_else(|| format!("__anon{}", slot.value()), str::to_owned);
        self.variables.insert(name.clone(), slot);
        (slot, Column::new(slot, name, value_type, false))
    }

    fn allocate_slot(&mut self) -> SlotId {
        let slot = SlotId::new(self.next_slot);
        self.next_slot = self.next_slot.saturating_add(1);
        slot
    }

    fn ensure_root(&mut self) -> Result<LogicalNodeId, CompileError> {
        if let Some(root) = self.root {
            return Ok(root);
        }
        let root = self
            .builder
            .add(LogicalOperator::Argument, vec![], self.schema.clone())?;
        self.root = Some(root);
        Ok(root)
    }
}

fn is_aggregate_function(function_id: u32) -> bool {
    ["count", "sum", "avg", "min", "max"]
        .iter()
        .any(|name| stable_id(name) == function_id)
}

fn contains_aggregate_scalar(expression: &ScalarExpr) -> bool {
    match expression {
        ScalarExpr::Function {
            function_id,
            arguments,
        } => is_aggregate_function(*function_id) || arguments.iter().any(contains_aggregate_scalar),
        ScalarExpr::List(items) => items.iter().any(contains_aggregate_scalar),
        ScalarExpr::Map(items) => items
            .iter()
            .any(|(_, value)| contains_aggregate_scalar(value)),
        ScalarExpr::Property { value, .. } | ScalarExpr::Not(value) | ScalarExpr::Negate(value) => {
            contains_aggregate_scalar(value)
        }
        ScalarExpr::Equal(left, right)
        | ScalarExpr::NotEqual(left, right)
        | ScalarExpr::Less(left, right)
        | ScalarExpr::LessEqual(left, right)
        | ScalarExpr::Greater(left, right)
        | ScalarExpr::GreaterEqual(left, right)
        | ScalarExpr::And(left, right)
        | ScalarExpr::Or(left, right)
        | ScalarExpr::Add(left, right)
        | ScalarExpr::Subtract(left, right)
        | ScalarExpr::Multiply(left, right)
        | ScalarExpr::Divide(left, right) => {
            contains_aggregate_scalar(left) || contains_aggregate_scalar(right)
        }
        ScalarExpr::Slot(_) | ScalarExpr::Parameter(_) | ScalarExpr::Literal(_) => false,
    }
}

fn scalar_references_slot(expression: &ScalarExpr) -> bool {
    match expression {
        ScalarExpr::Slot(_) => true,
        ScalarExpr::Parameter(_) | ScalarExpr::Literal(_) => false,
        ScalarExpr::List(items) => items.iter().any(scalar_references_slot),
        ScalarExpr::Map(items) => items.iter().any(|(_, value)| scalar_references_slot(value)),
        ScalarExpr::Property { value, .. } | ScalarExpr::Not(value) | ScalarExpr::Negate(value) => {
            scalar_references_slot(value)
        }
        ScalarExpr::Equal(left, right)
        | ScalarExpr::NotEqual(left, right)
        | ScalarExpr::Less(left, right)
        | ScalarExpr::LessEqual(left, right)
        | ScalarExpr::Greater(left, right)
        | ScalarExpr::GreaterEqual(left, right)
        | ScalarExpr::And(left, right)
        | ScalarExpr::Or(left, right)
        | ScalarExpr::Add(left, right)
        | ScalarExpr::Subtract(left, right)
        | ScalarExpr::Multiply(left, right)
        | ScalarExpr::Divide(left, right) => {
            scalar_references_slot(left) || scalar_references_slot(right)
        }
        ScalarExpr::Function { arguments, .. } => arguments.iter().any(scalar_references_slot),
    }
}

fn union_schemas_compatible(left: &RowSchema, right: &RowSchema) -> bool {
    left.columns().len() == right.columns().len()
        && left
            .columns()
            .iter()
            .zip(right.columns())
            .all(|(left, right)| {
                left.name() == right.name()
                    && left.value_type() == right.value_type()
                    && left.nullable() == right.nullable()
            })
}

fn output_schema(analyzed: &AnalyzedQuery, next_slot: &mut u32) -> Result<RowSchema, CompileError> {
    let mut columns = Vec::with_capacity(analyzed.output().len());
    for field in analyzed.output() {
        let slot = SlotId::new(*next_slot);
        *next_slot = next_slot.saturating_add(1);
        columns.push(Column::new(
            slot,
            field.name(),
            value_type(field.cypher_type()),
            true,
        ));
    }
    RowSchema::new(columns).map_err(Into::into)
}

fn value_type(cypher_type: &CypherType) -> ValueType {
    match cypher_type {
        CypherType::Any => ValueType::Any,
        CypherType::Null => ValueType::Null,
        CypherType::Boolean => ValueType::Boolean,
        CypherType::Integer => ValueType::Integer,
        CypherType::Float => ValueType::Float,
        CypherType::String => ValueType::String,
        CypherType::Bytes => ValueType::Bytes,
        CypherType::List(item) => ValueType::List(Box::new(value_type(item))),
        CypherType::Map => ValueType::Map,
        CypherType::Node => ValueType::Node,
        CypherType::Relationship => ValueType::Relationship,
        CypherType::Path => ValueType::Path,
        CypherType::Temporal => ValueType::Temporal,
        CypherType::Spatial => ValueType::Spatial,
        CypherType::Vector => ValueType::Vector,
    }
}

fn cypher_type_from_value_type(value_type: &ValueType) -> CypherType {
    match value_type {
        ValueType::Any => CypherType::Any,
        ValueType::Null => CypherType::Null,
        ValueType::Boolean => CypherType::Boolean,
        ValueType::Integer => CypherType::Integer,
        ValueType::Float => CypherType::Float,
        ValueType::String => CypherType::String,
        ValueType::Bytes => CypherType::Bytes,
        ValueType::List(item) => CypherType::List(Box::new(cypher_type_from_value_type(item))),
        ValueType::Map => CypherType::Map,
        ValueType::Node => CypherType::Node,
        ValueType::Relationship => CypherType::Relationship,
        ValueType::Path => CypherType::Path,
        ValueType::Temporal => CypherType::Temporal,
        ValueType::Spatial => CypherType::Spatial,
        ValueType::Vector => CypherType::Vector,
    }
}

fn clause_body(clause: &Clause) -> Result<&str, CompileError> {
    let words = match clause.kind() {
        ClauseKind::OptionalMatch | ClauseKind::Delete { detach: true } | ClauseKind::OrderBy => 2,
        _ => 1,
    };
    let lexed = lex(clause.text())
        .map_err(|error| CompileError::new("DTG-CYPHER-INVALID-CLAUSE", error.to_string()))?;
    let token = lexed
        .tokens()
        .get(words)
        .ok_or_else(|| CompileError::new("DTG-CYPHER-EMPTY-CLAUSE", "clause has no body"))?;
    Ok(&clause.text()[token.span().start()..])
}

fn has_explicit_temporal_scope(context: &TemporalContext) -> bool {
    context.valid_time().is_some()
        || !matches!(context.transaction_time(), TransactionTimeScope::Current)
}

fn projection_modifiers(source: &str) -> Result<(bool, &str), CompileError> {
    let lexed = lex(source)
        .map_err(|error| CompileError::new("DTG-CYPHER-INVALID-PROJECTION", error.to_string()))?;
    let Some(first) = lexed.tokens().first() else {
        return Err(CompileError::new(
            "DTG-CYPHER-EMPTY-PROJECTION",
            "projection requires at least one item",
        ));
    };
    if matches!(first.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case("DISTINCT")) {
        let body = source[first.span().end()..].trim();
        if body.is_empty() {
            return Err(CompileError::new(
                "DTG-CYPHER-EMPTY-PROJECTION",
                "DISTINCT requires at least one projection item",
            ));
        }
        Ok((true, body))
    } else {
        Ok((false, source))
    }
}

fn is_star(source: &str) -> Result<bool, CompileError> {
    let lexed = lex(source)
        .map_err(|error| CompileError::new("DTG-CYPHER-INVALID-PROJECTION", error.to_string()))?;
    Ok(matches!(lexed.tokens(), [token] if token.kind() == &TokenKind::Star))
}

fn split_sort_direction(source: &str) -> Result<(&str, bool), CompileError> {
    let lexed = lex(source)
        .map_err(|error| CompileError::new("DTG-CYPHER-INVALID-ORDER-BY", error.to_string()))?;
    if let Some(last) = lexed.tokens().last()
        && let TokenKind::Word(direction) = last.kind()
    {
        if direction.eq_ignore_ascii_case("ASC") {
            return Ok((source[..last.span().start()].trim(), true));
        }
        if direction.eq_ignore_ascii_case("DESC") {
            return Ok((source[..last.span().start()].trim(), false));
        }
    }
    Ok((source, true))
}

fn split_top_level(source: &str) -> Result<Vec<&str>, CompileError> {
    let lexed = lex(source)
        .map_err(|error| CompileError::new("DTG-CYPHER-INVALID-PROJECTION", error.to_string()))?;
    let mut depth = 0_usize;
    let mut start = 0_usize;
    let mut parts = Vec::new();
    for token in lexed.tokens() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1);
            }
            TokenKind::Comma if depth == 0 => {
                parts.push(source[start..token.span().start()].trim());
                start = token.span().end();
            }
            _ => {}
        }
    }
    parts.push(source[start..].trim());
    Ok(parts)
}

fn split_unwind_alias(source: &str) -> Result<(&str, &str), CompileError> {
    let lexed = lex(source)
        .map_err(|error| CompileError::new("DTG-CYPHER-INVALID-UNWIND", error.to_string()))?;
    let mut depth = 0_usize;
    for token in lexed.tokens() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1)
            }
            TokenKind::Word(value) if depth == 0 && value.eq_ignore_ascii_case("AS") => {
                let left = source[..token.span().start()].trim();
                let right = source[token.span().end()..].trim();
                if !left.is_empty() && !right.is_empty() {
                    return Ok((left, right));
                }
            }
            _ => {}
        }
    }
    Err(CompileError::new(
        "DTG-CYPHER-INVALID-UNWIND",
        "UNWIND requires an expression followed by AS variable",
    ))
}

fn split_alias(source: &str) -> Result<(&str, Option<&str>), CompileError> {
    let lexed = lex(source)
        .map_err(|error| CompileError::new("DTG-CYPHER-INVALID-WITH", error.to_string()))?;
    let mut depth = 0_usize;
    for token in lexed.tokens() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1)
            }
            TokenKind::Word(value) if depth == 0 && value.eq_ignore_ascii_case("AS") => {
                let left = source[..token.span().start()].trim();
                let right = source[token.span().end()..].trim();
                if left.is_empty() || right.is_empty() {
                    return Err(CompileError::new(
                        "DTG-CYPHER-INVALID-WITH",
                        "WITH alias requires an expression and a name",
                    ));
                }
                return Ok((left, Some(right)));
            }
            _ => {}
        }
    }
    Ok((source.trim(), None))
}

fn scalar_binary(
    operator: BinaryOperator,
    left: Box<ScalarExpr>,
    right: Box<ScalarExpr>,
) -> Result<ScalarExpr, CompileError> {
    Ok(match operator {
        BinaryOperator::Equal => ScalarExpr::Equal(left, right),
        BinaryOperator::NotEqual => ScalarExpr::NotEqual(left, right),
        BinaryOperator::Less => ScalarExpr::Less(left, right),
        BinaryOperator::LessEqual => ScalarExpr::LessEqual(left, right),
        BinaryOperator::Greater => ScalarExpr::Greater(left, right),
        BinaryOperator::GreaterEqual => ScalarExpr::GreaterEqual(left, right),
        BinaryOperator::And => ScalarExpr::And(left, right),
        BinaryOperator::Or => ScalarExpr::Or(left, right),
        BinaryOperator::Add => ScalarExpr::Add(left, right),
        BinaryOperator::Subtract => ScalarExpr::Subtract(left, right),
        BinaryOperator::Multiply => ScalarExpr::Multiply(left, right),
        BinaryOperator::Divide => ScalarExpr::Divide(left, right),
        unsupported => {
            return Err(CompileError::new(
                "DTG-CYPHER-OPERATOR-LOWERING",
                format!("operator {unsupported:?} is not in the initial scalar IR set"),
            ));
        }
    })
}

fn stable_id(value: &str) -> u32 {
    let digest = blake3::hash(value.as_bytes());
    u32::from_be_bytes(
        digest.as_bytes()[..4]
            .try_into()
            .expect("digest has four bytes"),
    )
}
