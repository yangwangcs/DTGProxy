use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use cypher_ast::{
    CallSubquery, Clause, ClauseKind, CypherProfile, DEFAULT_SUBQUERY_BATCH_ROWS, Expression,
    Identifier, InTransactions, ProcedureCall, ProcedureYield, QueryStatement, Statement,
    SubqueryErrorPolicy, TemporalAxis, TemporalContext, TemporalMode, TemporalScope, TextSpan,
    YieldItem,
};

use crate::{
    Lexed, SourceSpan, SyntaxLimits, Token, TokenKind, lex, lex_with_limits, parse_expression,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedQuery {
    profile: CypherProfile,
    statement: Statement,
}

impl ParsedQuery {
    #[must_use]
    pub const fn profile(&self) -> CypherProfile {
        self.profile
    }

    #[must_use]
    pub const fn statement(&self) -> &Statement {
        &self.statement
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseError {
    code: &'static str,
    span: SourceSpan,
    message: String,
}

impl ParseError {
    pub(crate) fn new(code: &'static str, span: SourceSpan, message: impl Into<String>) -> Self {
        Self {
            code,
            span,
            message: message.into(),
        }
    }

    pub(crate) fn from_syntax(error: &crate::SyntaxError) -> Self {
        Self::new(error.code(), error.span(), error.to_string())
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub const fn span(&self) -> SourceSpan {
        self.span
    }
}

impl Display for ParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at bytes {}..{}: {}",
            self.code,
            self.span.start(),
            self.span.end(),
            self.message
        )
    }
}

impl Error for ParseError {}

pub fn parse(query: &str) -> Result<ParsedQuery, ParseError> {
    parse_with_limits(query, SyntaxLimits::default())
}

pub fn parse_with_limits(query: &str, limits: SyntaxLimits) -> Result<ParsedQuery, ParseError> {
    let lexed = lex_with_limits(query, limits).map_err(|source| ParseError {
        code: source.code(),
        span: source.span(),
        message: source.to_string(),
    })?;
    if lexed.tokens().is_empty() {
        return Err(parse_error(
            "DTG-CYPHER-EMPTY-QUERY",
            SourceSpan::new(0, query.len()),
            "query contains no statement",
        ));
    }
    let profile = lexed.profile();
    let mut parser = Parser {
        lexed: &lexed,
        position: 0,
        limits,
    };
    if parser.at_word("AT") || parser.at_word("DIFF") {
        return Err(parser.error_here(
            "DTG-CYPHER-REMOVED-TEMPORAL-SYNTAX",
            "use FOR VALID_TIME, FOR SYSTEM_TIME, or CHANGES",
        ));
    }
    let statement = Statement::Query(parser.parse_query()?);
    if ast_node_count(&statement) > limits.max_ast_nodes() {
        return Err(parse_error(
            "DTG-CYPHER-TOO-MANY-AST-NODES",
            SourceSpan::new(0, query.len()),
            "AST node count exceeds the configured limit",
        ));
    }
    Ok(ParsedQuery { profile, statement })
}

struct Parser<'lexed, 'source> {
    lexed: &'lexed Lexed<'source>,
    position: usize,
    limits: SyntaxLimits,
}

impl Parser<'_, '_> {
    fn parse_query(&mut self) -> Result<QueryStatement, ParseError> {
        let graph = if self.consume_word("USE") {
            Some(self.identifier()?)
        } else {
            None
        };
        let mut scopes = Vec::new();
        loop {
            let changes = self.consume_word("CHANGES");
            if !changes && !self.at_temporal_for() {
                break;
            }
            self.expect_word("FOR")?;
            let axis = self.temporal_axis()?;
            if scopes
                .iter()
                .any(|scope: &TemporalScope| scope.axis() == axis)
            {
                return Err(self.error_here(
                    "DTG-CYPHER-DUPLICATE-TEMPORAL-SCOPE",
                    "temporal axis is already specified",
                ));
            }
            scopes.push(self.temporal_scope(axis, changes)?);
        }
        let clauses = self.remaining_clauses()?;
        if clauses.is_empty() {
            return Err(self.error_here(
                "DTG-CYPHER-EXPECTED-CLAUSE",
                "query requires at least one Cypher clause",
            ));
        }
        Ok(QueryStatement::new(
            graph,
            TemporalContext::new(scopes),
            clauses,
        ))
    }

    fn at_temporal_for(&self) -> bool {
        self.at_word("FOR")
            && self
                .lexed
                .tokens()
                .get(self.position + 1)
                .is_some_and(|token| {
                    matches!(token.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case("VALID_TIME") || word.eq_ignore_ascii_case("SYSTEM_TIME"))
                })
    }

    fn temporal_axis(&mut self) -> Result<TemporalAxis, ParseError> {
        if self.consume_word("VALID_TIME") {
            Ok(TemporalAxis::ValidTime)
        } else if self.consume_word("SYSTEM_TIME") {
            Ok(TemporalAxis::SystemTime)
        } else {
            Err(self.error_here(
                "DTG-CYPHER-EXPECTED-TIME-DIMENSION",
                "FOR requires VALID_TIME or SYSTEM_TIME",
            ))
        }
    }

    fn temporal_scope(
        &mut self,
        axis: TemporalAxis,
        changes: bool,
    ) -> Result<TemporalScope, ParseError> {
        if changes {
            self.expect_word("BETWEEN")?;
            let start = self.expression()?;
            self.expect_word("AND")?;
            let end = self.expression()?;
            return Ok(TemporalScope::between(
                axis,
                TemporalMode::ChangesBetween,
                start,
                end,
            ));
        }
        if self.consume_word("AS") {
            self.expect_word("OF")?;
            Ok(TemporalScope::as_of(axis, self.expression()?))
        } else if self.consume_word("BETWEEN") {
            let start = self.expression()?;
            self.expect_word("AND")?;
            let end = self.expression()?;
            Ok(TemporalScope::between(
                axis,
                TemporalMode::StateBetween,
                start,
                end,
            ))
        } else {
            Err(self.error_here(
                "DTG-CYPHER-EXPECTED-TEMPORAL-SELECTOR",
                "temporal scope requires AS OF or BETWEEN ... AND",
            ))
        }
    }

    fn remaining_clauses(&mut self) -> Result<Vec<Clause>, ParseError> {
        let tokens = self.lexed.tokens();
        let mut starts = Vec::new();
        let mut depth = 0_usize;
        let mut index = self.position;
        while index < tokens.len() {
            match tokens[index].kind() {
                TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => {
                    depth += 1;
                }
                TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                    depth = depth.saturating_sub(1);
                }
                TokenKind::Word(word) if depth == 0 => {
                    if let Some((kind, consumed_words)) = clause_kind(tokens, index, word) {
                        starts.push((index, kind));
                        index += consumed_words.saturating_sub(1);
                    }
                }
                _ => {}
            }
            index += 1;
        }
        if starts
            .first()
            .is_none_or(|(start, _)| *start != self.position)
        {
            return Err(self.error_here(
                "DTG-CYPHER-EXPECTED-CLAUSE",
                "expected a supported Cypher clause",
            ));
        }
        let mut clauses = Vec::with_capacity(starts.len());
        for (offset, (start_index, kind)) in starts.iter().copied().enumerate() {
            let start = tokens[start_index].span().start();
            let end = starts.get(offset + 1).map_or_else(
                || self.lexed.source_text().len(),
                |(next, _)| tokens[*next].span().start(),
            );
            let text = self.lexed.source_text()[start..end].trim_end();
            let mut clause = Clause::new(kind, TextSpan::new(start, end), text);
            if kind == ClauseKind::Call {
                if let Some(subquery) = parse_subquery_call(text, self.limits)? {
                    clause = clause.with_call_subquery(subquery);
                } else {
                    let yield_text =
                        starts
                            .get(offset + 1)
                            .and_then(|(yield_start, yield_kind)| {
                                (*yield_kind == ClauseKind::Yield).then(|| {
                                    let yield_end = starts.get(offset + 2).map_or_else(
                                        || self.lexed.source_text().len(),
                                        |(next, _)| tokens[*next].span().start(),
                                    );
                                    &self.lexed.source_text()
                                        [tokens[*yield_start].span().start()..yield_end]
                                })
                            });
                    clause = clause.with_procedure(parse_procedure_call(text, yield_text)?);
                }
            }
            clauses.push(clause);
        }
        self.position = tokens.len();
        Ok(clauses)
    }

    fn expression(&mut self) -> Result<Expression, ParseError> {
        let token = self.current().ok_or_else(|| {
            self.error_here(
                "DTG-CYPHER-EXPECTED-EXPRESSION",
                "expected a temporal expression",
            )
        })?;
        let expression = match token.kind() {
            TokenKind::Parameter(value) => Expression::Parameter(value.clone()),
            TokenKind::String(value) => Expression::String(value.clone()),
            TokenKind::Integer(value) => Expression::Integer(value.clone()),
            TokenKind::Float(value) => Expression::Float(value.clone()),
            TokenKind::Word(value) if value.eq_ignore_ascii_case("NULL") => Expression::Null,
            TokenKind::Word(value) if value.eq_ignore_ascii_case("TRUE") => {
                Expression::Boolean(true)
            }
            TokenKind::Word(value) if value.eq_ignore_ascii_case("FALSE") => {
                Expression::Boolean(false)
            }
            TokenKind::Word(value) => Expression::Identifier(Identifier::new(value, false)),
            TokenKind::EscapedIdentifier(value) => {
                Expression::Identifier(Identifier::new(value, true))
            }
            _ => {
                return Err(parse_error(
                    "DTG-CYPHER-EXPECTED-EXPRESSION",
                    token.span(),
                    "expected a temporal literal, parameter, or identifier",
                ));
            }
        };
        self.position += 1;
        Ok(expression)
    }

    fn identifier(&mut self) -> Result<Identifier, ParseError> {
        let token = self.current().ok_or_else(|| {
            self.error_here("DTG-CYPHER-EXPECTED-IDENTIFIER", "expected an identifier")
        })?;
        let identifier = match token.kind() {
            TokenKind::Word(value) => Identifier::new(value, false),
            TokenKind::EscapedIdentifier(value) => Identifier::new(value, true),
            _ => {
                return Err(parse_error(
                    "DTG-CYPHER-EXPECTED-IDENTIFIER",
                    token.span(),
                    "expected an identifier",
                ));
            }
        };
        self.position += 1;
        Ok(identifier)
    }

    fn expect_word(&mut self, expected: &str) -> Result<(), ParseError> {
        if self.consume_word(expected) {
            Ok(())
        } else {
            Err(self.error_here(
                "DTG-CYPHER-EXPECTED-KEYWORD",
                format!("expected keyword {expected}"),
            ))
        }
    }

    fn consume_word(&mut self, expected: &str) -> bool {
        if self.at_word(expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn at_word(&self, expected: &str) -> bool {
        self.current().is_some_and(|token| {
            matches!(token.kind(), TokenKind::Word(actual) if actual.eq_ignore_ascii_case(expected))
        })
    }

    fn current(&self) -> Option<&Token> {
        self.lexed.tokens().get(self.position)
    }

    fn error_here(&self, code: &'static str, message: impl Into<String>) -> ParseError {
        let span = self.current().map_or_else(
            || {
                SourceSpan::new(
                    self.lexed.source_text().len(),
                    self.lexed.source_text().len(),
                )
            },
            Token::span,
        );
        parse_error(code, span, message)
    }
}

fn parse_subquery_call(
    text: &str,
    limits: SyntaxLimits,
) -> Result<Option<CallSubquery>, ParseError> {
    let lexed = lex_with_limits(text, limits).map_err(|error| ParseError::from_syntax(&error))?;
    let tokens = lexed.tokens();
    if !tokens.first().is_some_and(
        |token| matches!(token.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case("CALL")),
    ) {
        return Err(parse_error(
            "DTG-CYPHER-INVALID-CALL",
            SourceSpan::new(0, text.len()),
            "CALL clause is malformed",
        ));
    }
    match tokens.get(1).map(Token::kind) {
        Some(TokenKind::LeftBrace) => {
            return Err(parse_error(
                "DTG-CYPHER-SCOPED-SUBQUERY-REQUIRED",
                tokens[1].span(),
                "CALL subqueries require an explicit import scope: CALL (...) { ... }",
            ));
        }
        Some(TokenKind::LeftParen) => {}
        _ => return Ok(None),
    }

    let mut position = 2_usize;
    let mut imports = Vec::new();
    if tokens.get(position).map(Token::kind) != Some(&TokenKind::RightParen) {
        loop {
            let token = tokens.get(position).ok_or_else(|| {
                parse_error(
                    "DTG-CYPHER-INVALID-SUBQUERY-IMPORT",
                    SourceSpan::new(text.len(), text.len()),
                    "CALL import scope is not closed",
                )
            })?;
            imports.push(ast_identifier(token).ok_or_else(|| {
                parse_error(
                    "DTG-CYPHER-INVALID-SUBQUERY-IMPORT",
                    token.span(),
                    "CALL imports must be identifiers",
                )
            })?);
            position += 1;
            match tokens.get(position).map(Token::kind) {
                Some(TokenKind::Comma) => position += 1,
                Some(TokenKind::RightParen) => break,
                _ => {
                    return Err(parse_error(
                        "DTG-CYPHER-INVALID-SUBQUERY-IMPORT",
                        tokens
                            .get(position)
                            .map_or_else(|| SourceSpan::new(text.len(), text.len()), Token::span),
                        "CALL imports must be comma-separated identifiers",
                    ));
                }
            }
        }
    }
    position += 1;
    let open = tokens.get(position).ok_or_else(|| {
        parse_error(
            "DTG-CYPHER-INVALID-SUBQUERY",
            SourceSpan::new(text.len(), text.len()),
            "scoped CALL requires a query body",
        )
    })?;
    if open.kind() != &TokenKind::LeftBrace {
        return Err(parse_error(
            "DTG-CYPHER-INVALID-SUBQUERY",
            open.span(),
            "scoped CALL import list must be followed by a query body",
        ));
    }
    let open_index = position;
    let mut brace_depth = 0_usize;
    let close_index = (open_index..tokens.len())
        .find(|index| {
            match tokens[*index].kind() {
                TokenKind::LeftBrace => brace_depth += 1,
                TokenKind::RightBrace => {
                    brace_depth = brace_depth.saturating_sub(1);
                    if brace_depth == 0 {
                        return true;
                    }
                }
                _ => {}
            }
            false
        })
        .ok_or_else(|| {
            parse_error(
                "DTG-CYPHER-UNBALANCED-DELIMITER",
                open.span(),
                "CALL subquery body is not closed",
            )
        })?;
    let body = text[open.span().end()..tokens[close_index].span().start()].trim();
    if body.is_empty() {
        return Err(parse_error(
            "DTG-CYPHER-INVALID-SUBQUERY",
            open.span(),
            "CALL subquery cannot be empty",
        ));
    }
    let parsed = parse_with_limits(body, limits)?;
    let Statement::Query(query) = parsed.statement();
    let query = query.clone();
    let exports = subquery_exports(&query)?;

    position = close_index + 1;
    let in_transactions = if position == tokens.len() {
        None
    } else {
        if !token_is_word(tokens.get(position), "IN")
            || !token_is_word(tokens.get(position + 1), "TRANSACTIONS")
        {
            return Err(parse_error(
                "DTG-CYPHER-INVALID-SUBQUERY-SUFFIX",
                tokens[position].span(),
                "CALL subquery supports only an IN TRANSACTIONS suffix",
            ));
        }
        position += 2;
        let batch_rows = if token_is_word(tokens.get(position), "OF") {
            position += 1;
            let value = match tokens.get(position).map(Token::kind) {
                Some(TokenKind::Integer(value)) => value.parse::<u64>().ok(),
                _ => None,
            }
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                parse_error(
                    "DTG-CYPHER-INVALID-SUBQUERY-BATCH",
                    tokens
                        .get(position)
                        .map_or_else(|| SourceSpan::new(text.len(), text.len()), Token::span),
                    "IN TRANSACTIONS batch size must be a positive bounded integer",
                )
            })?;
            position += 1;
            if !token_is_word(tokens.get(position), "ROWS") {
                return Err(parse_error(
                    "DTG-CYPHER-INVALID-SUBQUERY-BATCH",
                    tokens
                        .get(position)
                        .map_or_else(|| SourceSpan::new(text.len(), text.len()), Token::span),
                    "IN TRANSACTIONS OF requires ROWS",
                ));
            }
            position += 1;
            value
        } else {
            DEFAULT_SUBQUERY_BATCH_ROWS
        };
        if position != tokens.len() {
            return Err(parse_error(
                "DTG-CYPHER-INVALID-SUBQUERY-SUFFIX",
                tokens[position].span(),
                "unexpected content after IN TRANSACTIONS",
            ));
        }
        Some(
            InTransactions::try_new(batch_rows, SubqueryErrorPolicy::Fail).ok_or_else(|| {
                parse_error(
                    "DTG-CYPHER-INVALID-SUBQUERY-BATCH",
                    tokens[close_index].span(),
                    "IN TRANSACTIONS batch size exceeds the configured maximum",
                )
            })?,
        )
    };
    Ok(Some(CallSubquery::new(
        imports,
        query,
        exports,
        in_transactions,
    )))
}

fn token_is_word(token: Option<&Token>, expected: &str) -> bool {
    token.is_some_and(|token| {
        matches!(token.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case(expected))
    })
}

fn ast_identifier(token: &Token) -> Option<Identifier> {
    match token.kind() {
        TokenKind::Word(value) => Some(Identifier::new(value, false)),
        TokenKind::EscapedIdentifier(value) => Some(Identifier::new(value, true)),
        _ => None,
    }
}

fn subquery_exports(query: &QueryStatement) -> Result<Vec<Identifier>, ParseError> {
    let Some(clause) = query
        .clauses()
        .iter()
        .rev()
        .find(|clause| clause.kind() == ClauseKind::Return)
    else {
        return Ok(Vec::new());
    };
    let lexed = lex(clause.text()).map_err(|error| ParseError::from_syntax(&error))?;
    let body_start = lexed.tokens().get(1).ok_or_else(|| {
        parse_error(
            "DTG-CYPHER-EMPTY-PROJECTION",
            SourceSpan::new(0, clause.text().len()),
            "RETURN requires at least one projection",
        )
    })?;
    let body = &clause.text()[body_start.span().start()..];
    let items = split_projection_items(body)?;
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| projection_name(item, index))
        .collect()
}

fn split_projection_items(source: &str) -> Result<Vec<&str>, ParseError> {
    let lexed = lex(source).map_err(|error| ParseError::from_syntax(&error))?;
    let mut depth = 0_usize;
    let mut start = 0_usize;
    let mut items = Vec::new();
    for token in lexed.tokens() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1);
            }
            TokenKind::Comma if depth == 0 => {
                items.push(source[start..token.span().start()].trim());
                start = token.span().end();
            }
            _ => {}
        }
    }
    items.push(source[start..].trim());
    if items.iter().any(|item| item.is_empty()) {
        return Err(parse_error(
            "DTG-CYPHER-EMPTY-LIST-ITEM",
            SourceSpan::new(0, source.len()),
            "RETURN contains an empty projection item",
        ));
    }
    Ok(items)
}

fn projection_name(source: &str, index: usize) -> Result<Identifier, ParseError> {
    let lexed = lex(source).map_err(|error| ParseError::from_syntax(&error))?;
    let mut depth = 0_usize;
    for (position, token) in lexed.tokens().iter().enumerate() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1);
            }
            TokenKind::Word(word) if depth == 0 && word.eq_ignore_ascii_case("AS") => {
                let alias = lexed
                    .tokens()
                    .get(position + 1)
                    .and_then(ast_identifier)
                    .ok_or_else(|| {
                        parse_error(
                            "DTG-CYPHER-EXPECTED-ALIAS",
                            token.span(),
                            "RETURN AS requires an alias",
                        )
                    })?;
                if position + 2 != lexed.tokens().len() {
                    return Err(parse_error(
                        "DTG-CYPHER-INVALID-ALIAS",
                        token.span(),
                        "unexpected token after RETURN alias",
                    ));
                }
                return Ok(alias);
            }
            _ => {}
        }
    }
    let expression = parse_expression(source)?;
    Ok(match expression {
        Expression::Identifier(identifier) => identifier,
        Expression::Property { property, .. } => property,
        _ => Identifier::new(format!("column{index}"), false),
    })
}

fn parse_procedure_call(text: &str, yield_text: Option<&str>) -> Result<ProcedureCall, ParseError> {
    let body = text
        .get(..4)
        .filter(|prefix| prefix.eq_ignore_ascii_case("CALL"))
        .and_then(|_| text.get(4..))
        .ok_or_else(|| {
            parse_error(
                "DTG-CYPHER-INVALID-CALL",
                SourceSpan::new(0, text.len()),
                "CALL clause is malformed",
            )
        })?
        .trim();
    let open = body.find('(').ok_or_else(|| {
        parse_error(
            "DTG-CYPHER-INVALID-CALL",
            SourceSpan::new(0, text.len()),
            "procedure call requires parentheses",
        )
    })?;
    let close = body.rfind(')').ok_or_else(|| {
        parse_error(
            "DTG-CYPHER-INVALID-CALL",
            SourceSpan::new(0, text.len()),
            "procedure call has no closing parenthesis",
        )
    })?;
    if close < open || !body[close + 1..].trim().is_empty() {
        return Err(parse_error(
            "DTG-CYPHER-INVALID-CALL",
            SourceSpan::new(0, text.len()),
            "invalid procedure call suffix",
        ));
    }
    let name = body[..open].trim();
    if name.is_empty() {
        return Err(parse_error(
            "DTG-CYPHER-INVALID-CALL",
            SourceSpan::new(0, text.len()),
            "procedure name is empty",
        ));
    }
    let yield_selection = yield_text
        .map(parse_yield_items)
        .transpose()?
        .unwrap_or(ProcedureYield::None);
    Ok(ProcedureCall::new(
        name,
        parse_procedure_arguments(body[open + 1..close].trim())?,
        yield_selection,
    ))
}

fn parse_procedure_arguments(source: &str) -> Result<Vec<Expression>, ParseError> {
    if source.is_empty() {
        return Ok(Vec::new());
    }
    let lexed = lex(source).map_err(|error| ParseError::from_syntax(&error))?;
    let mut depth = 0_usize;
    let mut start = 0_usize;
    let mut items = Vec::new();
    for token in lexed.tokens() {
        match token.kind() {
            TokenKind::LeftParen | TokenKind::LeftBracket | TokenKind::LeftBrace => depth += 1,
            TokenKind::RightParen | TokenKind::RightBracket | TokenKind::RightBrace => {
                depth = depth.saturating_sub(1);
            }
            TokenKind::Comma if depth == 0 => {
                items.push(parse_procedure_argument(
                    &source[start..token.span().start()],
                )?);
                start = token.span().end();
            }
            _ => {}
        }
    }
    items.push(parse_procedure_argument(&source[start..])?);
    Ok(items)
}

fn parse_procedure_argument(source: &str) -> Result<Expression, ParseError> {
    let source = source.trim();
    if source.is_empty() {
        return Err(parse_error(
            "DTG-CYPHER-INVALID-CALL",
            SourceSpan::new(0, 0),
            "procedure argument cannot be empty",
        ));
    }
    parse_expression(source)
}

fn parse_yield_items(text: &str) -> Result<ProcedureYield, ParseError> {
    let trimmed = text.trim();
    let body = trimmed
        .get(..5)
        .filter(|prefix| prefix.eq_ignore_ascii_case("YIELD"))
        .and_then(|_| trimmed.get(5..))
        .map(str::trim)
        .filter(|body| !body.is_empty())
        .ok_or_else(|| {
            parse_error(
                "DTG-CYPHER-INVALID-YIELD",
                SourceSpan::new(0, text.len()),
                "YIELD requires one or more result fields",
            )
        })?;
    let lexed = lex(body).map_err(|error| ParseError::from_syntax(&error))?;
    let tokens = lexed.tokens();
    if matches!(tokens, [token] if token.kind() == &TokenKind::Star) {
        return Ok(ProcedureYield::All);
    }
    if tokens.iter().any(|token| token.kind() == &TokenKind::Star) {
        return Err(parse_error(
            "DTG-CYPHER-INVALID-YIELD",
            SourceSpan::new(0, text.len()),
            "YIELD * cannot be combined with explicit result fields",
        ));
    }
    let mut items = Vec::new();
    let mut position = 0;
    while position < tokens.len() {
        let name = yield_identifier(&tokens[position]).ok_or_else(|| {
            parse_error(
                "DTG-CYPHER-INVALID-YIELD",
                SourceSpan::new(0, text.len()),
                "YIELD items must start with an identifier",
            )
        })?;
        position += 1;
        let alias = if tokens.get(position).is_some_and(|token| {
            matches!(token.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case("AS"))
        }) {
            position += 1;
            let alias = tokens.get(position).and_then(yield_identifier).ok_or_else(|| {
                parse_error(
                    "DTG-CYPHER-INVALID-YIELD",
                    SourceSpan::new(0, text.len()),
                    "YIELD AS requires an identifier",
                )
            })?;
            position += 1;
            Some(alias)
        } else {
            None
        };
        items.push(YieldItem::new(name, alias));
        if position == tokens.len() {
            break;
        }
        if tokens[position].kind() != &TokenKind::Comma {
            return Err(parse_error(
                "DTG-CYPHER-INVALID-YIELD",
                SourceSpan::new(0, text.len()),
                "YIELD items must be separated by commas",
            ));
        }
        position += 1;
        if position == tokens.len() {
            return Err(parse_error(
                "DTG-CYPHER-INVALID-YIELD",
                SourceSpan::new(0, text.len()),
                "YIELD cannot end with a comma",
            ));
        }
    }
    let mut outputs = BTreeSet::new();
    for item in &items {
        let output = item.alias().unwrap_or_else(|| item.name());
        if !outputs.insert(output.to_owned()) {
            return Err(parse_error(
                "DTG-CYPHER-DUPLICATE-YIELD",
                SourceSpan::new(0, text.len()),
                format!("YIELD output {output} is repeated"),
            ));
        }
    }
    Ok(ProcedureYield::Items(items))
}

fn yield_identifier(token: &Token) -> Option<String> {
    match token.kind() {
        TokenKind::Word(value) | TokenKind::EscapedIdentifier(value) => Some(value.clone()),
        _ => None,
    }
}

fn clause_kind(tokens: &[Token], index: usize, word: &str) -> Option<(ClauseKind, usize)> {
    let next_is = |expected: &str| {
        tokens.get(index + 1).is_some_and(|token| {
            matches!(token.kind(), TokenKind::Word(actual) if actual.eq_ignore_ascii_case(expected))
        })
    };
    let kind = if word.eq_ignore_ascii_case("OPTIONAL") && next_is("MATCH") {
        (ClauseKind::OptionalMatch, 2)
    } else if word.eq_ignore_ascii_case("DETACH") && next_is("DELETE") {
        (ClauseKind::Delete { detach: true }, 2)
    } else if word.eq_ignore_ascii_case("ORDER") && next_is("BY") {
        (ClauseKind::OrderBy, 2)
    } else if word.eq_ignore_ascii_case("UNION") && next_is("ALL") {
        (ClauseKind::Union { all: true }, 2)
    } else {
        let kind = match word.to_ascii_uppercase().as_str() {
            "MATCH" => ClauseKind::Match,
            "WHERE" => ClauseKind::Where,
            "FILTER" => ClauseKind::Filter,
            "WITH" => ClauseKind::With,
            "LET" => ClauseKind::Let,
            "RETURN" => ClauseKind::Return,
            "FINISH" => ClauseKind::Finish,
            "UNWIND" => ClauseKind::Unwind,
            "FOR" => ClauseKind::For,
            "UNION" => ClauseKind::Union { all: false },
            "NEXT" => ClauseKind::Next,
            "WHEN" => ClauseKind::When,
            "SKIP" => ClauseKind::Skip,
            "OFFSET" => ClauseKind::Offset,
            "LIMIT" => ClauseKind::Limit,
            "CREATE" => ClauseKind::Create,
            "MERGE" => ClauseKind::Merge,
            "SET" => ClauseKind::Set,
            "REMOVE" => ClauseKind::Remove,
            "DELETE" => ClauseKind::Delete { detach: false },
            "FOREACH" => ClauseKind::Foreach,
            "CALL" => ClauseKind::Call,
            "YIELD" => ClauseKind::Yield,
            _ => return None,
        };
        (kind, 1)
    };
    Some(kind)
}

fn parse_error(code: &'static str, span: SourceSpan, message: impl Into<String>) -> ParseError {
    ParseError::new(code, span, message)
}

fn ast_node_count(statement: &Statement) -> usize {
    match statement {
        Statement::Query(query) => query_ast_node_count(query),
    }
}

pub(crate) fn query_ast_node_count(query: &QueryStatement) -> usize {
    query.clauses().iter().fold(
        1_usize
            .saturating_add(query.clauses().len())
            .saturating_add(usize::from(query.graph().is_some()))
            .saturating_add(query.temporal().scopes().len()),
        |count, clause| {
            count.saturating_add(
                clause
                    .call_subquery()
                    .map_or(0, |subquery| query_ast_node_count(subquery.query())),
            )
        },
    )
}
