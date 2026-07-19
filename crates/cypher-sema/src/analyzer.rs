use std::collections::BTreeMap;

use cypher_ast::{
    BinaryOperator, Clause, ClauseKind, Expression, NodePattern, Pattern, RelationshipPattern,
    Statement, TransactionTimeScope, UnaryOperator,
};
use cypher_syntax::{ParsedQuery, TokenKind, lex, parse_expression, parse_pattern};

use crate::{CypherType, SemanticError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryEffect {
    ReadOnly,
    Write,
    Procedure,
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
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SemanticAnalyzer;

impl SemanticAnalyzer {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn analyze(&self, parsed: &ParsedQuery) -> Result<AnalyzedQuery, SemanticError> {
        match parsed.statement() {
            Statement::Diff(_) => Ok(AnalyzedQuery {
                output: ["element", "changeType", "before", "after"]
                    .into_iter()
                    .map(|name| OutputField {
                        name: name.into(),
                        cypher_type: CypherType::Any,
                    })
                    .collect(),
                effect: QueryEffect::ReadOnly,
            }),
            Statement::Query(query) => {
                let mut scope = Scope::default();
                let mut output = Vec::new();
                let mut effect = QueryEffect::ReadOnly;
                for clause in query.clauses() {
                    match clause.kind() {
                        ClauseKind::Match | ClauseKind::OptionalMatch => {
                            let pattern = parse_pattern(clause_body(clause)?)?;
                            bind_pattern(&mut scope, &pattern)?;
                        }
                        ClauseKind::Where | ClauseKind::Filter => {
                            let expression = parse_expression(clause_body(clause)?)?;
                            let actual = infer(&expression, &scope)?;
                            if !actual.is_boolean() {
                                return Err(SemanticError::new(
                                    "DTG-CYPHER-NON-BOOLEAN-PREDICATE",
                                    format!("predicate has type {actual:?}"),
                                ));
                            }
                        }
                        ClauseKind::With => {
                            let fields = projections(clause_body(clause)?, &scope)?;
                            scope = Scope::from_fields(&fields)?;
                        }
                        ClauseKind::Return => {
                            output = projections(clause_body(clause)?, &scope)?;
                        }
                        ClauseKind::Let => {
                            bind_let(clause_body(clause)?, &mut scope)?;
                        }
                        ClauseKind::Unwind => {
                            bind_unwind(clause_body(clause)?, &mut scope)?;
                        }
                        ClauseKind::Create | ClauseKind::Merge => {
                            let pattern = parse_pattern(clause_body(clause)?)?;
                            bind_pattern(&mut scope, &pattern)?;
                            effect = QueryEffect::Write;
                        }
                        ClauseKind::Set
                        | ClauseKind::Remove
                        | ClauseKind::Delete { .. }
                        | ClauseKind::Foreach => effect = QueryEffect::Write,
                        ClauseKind::Call => effect = QueryEffect::Procedure,
                        ClauseKind::Yield => bind_yield(clause_body(clause)?, &mut scope)?,
                        ClauseKind::Finish
                        | ClauseKind::For
                        | ClauseKind::Union { .. }
                        | ClauseKind::Next
                        | ClauseKind::When
                        | ClauseKind::OrderBy
                        | ClauseKind::Skip
                        | ClauseKind::Offset
                        | ClauseKind::Limit => {}
                    }
                }
                if effect == QueryEffect::Write
                    && matches!(
                        query.temporal().transaction_time(),
                        TransactionTimeScope::AsOf(_)
                    )
                {
                    return Err(SemanticError::new(
                        "DTG-TEMPORAL-HISTORICAL-WRITE",
                        "writes cannot target a historical transaction-time snapshot",
                    ));
                }
                Ok(AnalyzedQuery { output, effect })
            }
        }
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

fn bind_pattern(scope: &mut Scope, pattern: &Pattern) -> Result<(), SemanticError> {
    for path in pattern.paths() {
        bind_node(scope, path.start())?;
        for chain in path.chains() {
            bind_relationship(scope, chain.relationship())?;
            bind_node(scope, chain.node())?;
        }
    }
    Ok(())
}

fn bind_node(scope: &mut Scope, node: &NodePattern) -> Result<(), SemanticError> {
    if let Some(variable) = node.variable() {
        scope.bind_compatible(variable.value(), CypherType::Node)?;
    }
    if let Some(properties) = node.properties() {
        let _ = infer(properties, scope)?;
    }
    Ok(())
}

fn bind_relationship(
    scope: &mut Scope,
    relationship: &RelationshipPattern,
) -> Result<(), SemanticError> {
    if let Some(variable) = relationship.variable() {
        scope.bind_compatible(variable.value(), CypherType::Relationship)?;
    }
    if let Some(properties) = relationship.properties() {
        let _ = infer(properties, scope)?;
    }
    Ok(())
}

fn projections(source: &str, scope: &Scope) -> Result<Vec<OutputField>, SemanticError> {
    let fields = split_top_level(source, TokenKind::Comma)?
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            let (expression_source, alias) = split_alias(item)?;
            let expression = parse_expression(expression_source)?;
            let cypher_type = infer(&expression, scope)?;
            let name = alias.unwrap_or_else(|| expression_name(&expression, index));
            Ok(OutputField { name, cypher_type })
        })
        .collect::<Result<Vec<_>, SemanticError>>()?;
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

fn bind_let(source: &str, scope: &mut Scope) -> Result<(), SemanticError> {
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
        let cypher_type = infer(&expression, scope)?;
        scope.bind(&name, cypher_type)?;
    }
    Ok(())
}

fn bind_unwind(source: &str, scope: &mut Scope) -> Result<(), SemanticError> {
    let (expression_source, alias) = split_alias(source)?;
    let alias = alias.ok_or_else(|| {
        SemanticError::new("DTG-CYPHER-EXPECTED-ALIAS", "UNWIND requires AS alias")
    })?;
    let expression = parse_expression(expression_source)?;
    let item_type = match infer(&expression, scope)? {
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

fn bind_yield(source: &str, scope: &mut Scope) -> Result<(), SemanticError> {
    for item in split_top_level(source, TokenKind::Comma)? {
        let (_, alias) = split_alias(item)?;
        let lexed = lex(item)
            .map_err(|error| SemanticError::new("DTG-CYPHER-INVALID-YIELD", error.to_string()))?;
        let name = alias.unwrap_or(token_identifier(&lexed.tokens()[0])?);
        scope.bind(&name, CypherType::Any)?;
    }
    Ok(())
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

fn infer(expression: &Expression, scope: &Scope) -> Result<CypherType, SemanticError> {
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
                item_type = CypherType::unify(&item_type, &infer(item, scope)?);
            }
            Ok(CypherType::List(Box::new(item_type)))
        }
        Expression::Map(entries) => {
            for (_, value) in entries {
                let _ = infer(value, scope)?;
            }
            Ok(CypherType::Map)
        }
        Expression::Unary {
            operator,
            expression,
        } => {
            let actual = infer(expression, scope)?;
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
            let left = infer(left, scope)?;
            let right = infer(right, scope)?;
            infer_binary(*operator, left, right)
        }
        Expression::Property { value, .. } => {
            let actual = infer(value, scope)?;
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
            let value_type = infer(value, scope)?;
            let _ = infer(index, scope)?;
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
                .map(|argument| infer(argument, scope))
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
