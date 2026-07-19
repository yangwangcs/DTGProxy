use std::collections::BTreeMap;

use cypher_ast::{
    BinaryOperator, Clause, ClauseKind, CypherVersion, Expression, Pattern, Statement,
    UnaryOperator,
};
use cypher_sema::{AnalyzedQuery, CypherType, QueryEffect, SemanticAnalyzer};
use cypher_syntax::{TokenKind, lex, parse, parse_expression, parse_pattern};
use temporal_ir::v2::{
    Column, LanguageProfile, LogicalNodeId, LogicalOperator, LogicalPlan, LogicalPlanBuilder,
    PlanHeaderV2, RowSchema, ScalarExpr, SlotId, ValueType,
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
    pub const fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
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
        let analyzed = SemanticAnalyzer::new().analyze(&parsed)?;
        let fingerprint = query_fingerprint(
            text,
            parsed.profile().semantic_baseline(),
            session.schema_version,
        );
        let profile = match parsed.profile().version() {
            CypherVersion::V5 => LanguageProfile::Cypher5,
            CypherVersion::V25 => LanguageProfile::Cypher25,
        };
        let header = PlanHeaderV2::new(
            session.graph_id,
            session.schema_version,
            session.topology_epoch,
            profile,
            parsed.profile().semantic_baseline(),
            fingerprint,
        )?;
        let logical_plan = lower(parsed.statement(), &analyzed, header)?;
        let result_schema = logical_plan.output().clone();
        Ok(CompiledQuery {
            fingerprint,
            result_schema,
            effect: analyzed.effect(),
            logical_plan,
        })
    }
}

fn query_fingerprint(text: &str, baseline: &str, schema_version: u64) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/CypherQuery/V2");
    hasher.update(baseline.as_bytes());
    hasher.update(&schema_version.to_be_bytes());
    hasher.update(text.as_bytes());
    *hasher.finalize().as_bytes()
}

fn lower(
    statement: &Statement,
    analyzed: &AnalyzedQuery,
    header: PlanHeaderV2,
) -> Result<LogicalPlan, CompileError> {
    let mut lowerer = Lowerer {
        builder: LogicalPlanBuilder::new(header),
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
            for clause in query.clauses() {
                if !lowerer.temporal_inserted
                    && (query.temporal().valid_time().is_some()
                        || !matches!(
                            query.temporal().transaction_time(),
                            cypher_ast::TransactionTimeScope::Current
                        ))
                    && !matches!(clause.kind(), ClauseKind::Match | ClauseKind::OptionalMatch)
                {
                    lowerer.temporal_slice()?;
                }
                lowerer.clause(clause, analyzed)?;
            }
            if !lowerer.temporal_inserted
                && (query.temporal().valid_time().is_some()
                    || !matches!(
                        query.temporal().transaction_time(),
                        cypher_ast::TransactionTimeScope::Current
                    ))
            {
                lowerer.temporal_slice()?;
            }
        }
    }
    let root = lowerer.ensure_root()?;
    lowerer.builder.finish(root).map_err(Into::into)
}

struct Lowerer {
    builder: LogicalPlanBuilder,
    root: Option<LogicalNodeId>,
    schema: RowSchema,
    variables: BTreeMap<String, SlotId>,
    next_slot: u32,
    temporal_inserted: bool,
}

impl Lowerer {
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
            ClauseKind::Create => self.simple_unary(LogicalOperator::Create)?,
            ClauseKind::Merge => self.simple_unary(LogicalOperator::Merge)?,
            ClauseKind::Set => self.simple_unary(LogicalOperator::Set)?,
            ClauseKind::Remove => self.simple_unary(LogicalOperator::Remove)?,
            ClauseKind::Delete { detach } => {
                self.simple_unary(LogicalOperator::Delete { detach })?;
            }
            ClauseKind::Call => self.simple_unary(LogicalOperator::ProcedureCall {
                procedure_id: stable_id(clause_body(clause)?),
            })?,
            ClauseKind::Finish => self.simple_unary(LogicalOperator::Finish)?,
            ClauseKind::With
            | ClauseKind::Let
            | ClauseKind::Unwind
            | ClauseKind::Yield
            | ClauseKind::Foreach
            | ClauseKind::For
            | ClauseKind::Union { .. }
            | ClauseKind::Next
            | ClauseKind::When
            | ClauseKind::OrderBy
            | ClauseKind::Skip
            | ClauseKind::Offset
            | ClauseKind::Limit => {}
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
                self.root = Some(self.builder.add(
                    LogicalOperator::Expand {
                        source: current_source,
                        relationship,
                        destination,
                        outgoing: !matches!(
                            chain.relationship().direction(),
                            cypher_ast::RelationshipDirection::Incoming
                        ),
                    },
                    vec![input],
                    self.schema.clone(),
                )?);
                current_source = destination;
            }
        }
        Ok(())
    }

    fn project(&mut self, source: &str, analyzed: &AnalyzedQuery) -> Result<(), CompileError> {
        let items = split_top_level(source)?;
        if items.len() != analyzed.output().len() {
            return Err(CompileError::new(
                "DTG-CYPHER-PROJECTION-SHAPE",
                "semantic and compiler projection counts differ",
            ));
        }
        let mut expressions = Vec::with_capacity(items.len());
        let mut columns = Vec::with_capacity(items.len());
        for (item, field) in items.into_iter().zip(analyzed.output()) {
            let expression_source = strip_alias(item)?;
            let expression = parse_expression(expression_source)?;
            let scalar = self.scalar(&expression)?;
            let slot = if let Expression::Identifier(identifier) = &expression {
                self.variables
                    .get(identifier.value())
                    .copied()
                    .unwrap_or_else(|| self.allocate_slot())
            } else {
                self.allocate_slot()
            };
            expressions.push((slot, scalar));
            columns.push(Column::new(
                slot,
                field.name(),
                value_type(field.cypher_type()),
                true,
            ));
        }
        let output = RowSchema::new(columns)?;
        let input = self.ensure_root()?;
        self.root = Some(self.builder.add(
            LogicalOperator::Project { expressions },
            vec![input],
            output.clone(),
        )?);
        self.schema = output;
        Ok(())
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
                .map(literal_value)
                .collect::<Result<Vec<_>, _>>()
                .map(GraphValue::List)
                .map(ScalarExpr::Literal),
            Expression::Map(_) => Err(CompileError::new(
                "DTG-CYPHER-NONLITERAL-MAP",
                "map lowering requires a typed map expression operator",
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
                        .join("."),
                ),
                arguments: arguments
                    .iter()
                    .map(|argument| self.scalar(argument))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
        }
    }

    fn temporal_slice(&mut self) -> Result<(), CompileError> {
        let input = self.ensure_root()?;
        self.root = Some(self.builder.add(
            LogicalOperator::TemporalSlice,
            vec![input],
            self.schema.clone(),
        )?);
        self.temporal_inserted = true;
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

fn strip_alias(source: &str) -> Result<&str, CompileError> {
    let lexed = lex(source)
        .map_err(|error| CompileError::new("DTG-CYPHER-INVALID-PROJECTION", error.to_string()))?;
    let mut depth = 0_usize;
    for token in lexed.tokens() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1);
            }
            TokenKind::Word(word) if depth == 0 && word.eq_ignore_ascii_case("AS") => {
                return Ok(source[..token.span().start()].trim());
            }
            _ => {}
        }
    }
    Ok(source)
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

fn literal_value(expression: &Expression) -> Result<GraphValue, CompileError> {
    match expression {
        Expression::Null => Ok(GraphValue::Null),
        Expression::Boolean(value) => Ok(GraphValue::Boolean(*value)),
        Expression::Integer(value) => value
            .parse()
            .map(GraphValue::Integer)
            .map_err(|_| CompileError::new("DTG-CYPHER-INTEGER-OVERFLOW", value)),
        Expression::Float(value) => value
            .parse::<f64>()
            .map(f64::to_bits)
            .map(GraphValue::FloatBits)
            .map_err(|_| CompileError::new("DTG-CYPHER-INVALID-FLOAT", value)),
        Expression::String(value) => Ok(GraphValue::String(value.clone())),
        Expression::List(items) => items
            .iter()
            .map(literal_value)
            .collect::<Result<Vec<_>, _>>()
            .map(GraphValue::List),
        _ => Err(CompileError::new(
            "DTG-CYPHER-NONLITERAL-LIST",
            "list literal contains a runtime expression",
        )),
    }
}

fn stable_id(value: &str) -> u32 {
    let digest = blake3::hash(value.as_bytes());
    u32::from_be_bytes(
        digest.as_bytes()[..4]
            .try_into()
            .expect("digest has four bytes"),
    )
}
