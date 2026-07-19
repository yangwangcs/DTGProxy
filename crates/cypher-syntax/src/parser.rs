use std::error::Error;
use std::fmt::{self, Display, Formatter};

use cypher_ast::{
    Clause, ClauseKind, CypherProfile, CypherVersion, DiffStatement, Expression, Identifier,
    QueryStatement, Statement, TemporalContext, TextSpan, TransactionTimeScope, ValidTimeScope,
};

use crate::{Lexed, SourceSpan, SyntaxLimits, Token, TokenKind, lex_with_limits};

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
    };
    let statement = if parser.at_word("DIFF") {
        Statement::Diff(parser.parse_diff()?)
    } else {
        Statement::Query(parser.parse_query()?)
    };
    Ok(ParsedQuery { profile, statement })
}

struct Parser<'lexed, 'source> {
    lexed: &'lexed Lexed<'source>,
    position: usize,
}

impl Parser<'_, '_> {
    fn parse_query(&mut self) -> Result<QueryStatement, ParseError> {
        let graph = if self.consume_word("USE") {
            Some(self.identifier()?)
        } else {
            None
        };
        let mut valid_time = None;
        let mut transaction_time = TransactionTimeScope::Current;
        while self.consume_word("AT") {
            if self.consume_word("VALID_TIME") {
                if valid_time.is_some() {
                    return Err(self.error_here(
                        "DTG-CYPHER-DUPLICATE-TEMPORAL-SCOPE",
                        "valid-time scope is already specified",
                    ));
                }
                valid_time = Some(self.valid_time_scope()?);
            } else if self.consume_word("TRANSACTION_TIME") {
                if !matches!(transaction_time, TransactionTimeScope::Current) {
                    return Err(self.error_here(
                        "DTG-CYPHER-DUPLICATE-TEMPORAL-SCOPE",
                        "transaction-time scope is already specified",
                    ));
                }
                self.expect_word("AS")?;
                self.expect_word("OF")?;
                transaction_time = TransactionTimeScope::AsOf(self.expression()?);
            } else {
                return Err(self.error_here(
                    "DTG-CYPHER-EXPECTED-TIME-DIMENSION",
                    "AT requires VALID_TIME or TRANSACTION_TIME",
                ));
            }
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
            TemporalContext::new(valid_time, transaction_time),
            clauses,
        ))
    }

    fn valid_time_scope(&mut self) -> Result<ValidTimeScope, ParseError> {
        if self.consume_word("AS") {
            self.expect_word("OF")?;
            Ok(ValidTimeScope::AsOf(self.expression()?))
        } else if self.consume_word("FROM") {
            let start = self.expression()?;
            self.expect_word("TO")?;
            let end = self.expression()?;
            Ok(ValidTimeScope::Between { start, end })
        } else {
            Err(self.error_here(
                "DTG-CYPHER-EXPECTED-TEMPORAL-SELECTOR",
                "VALID_TIME requires AS OF or FROM ... TO",
            ))
        }
    }

    fn parse_diff(&mut self) -> Result<DiffStatement, ParseError> {
        self.expect_word("DIFF")?;
        self.expect_word("GRAPH")?;
        let graph = self.identifier()?;
        self.expect_word("AT")?;
        self.expect_word("VALID_TIME")?;
        self.expect_word("AS")?;
        self.expect_word("OF")?;
        let from = self.expression()?;
        self.expect_word("AND")?;
        self.expect_word("AS")?;
        self.expect_word("OF")?;
        let to = self.expression()?;
        self.expect_word("AT")?;
        self.expect_word("TRANSACTION_TIME")?;
        self.expect_word("AS")?;
        self.expect_word("OF")?;
        let transaction = TransactionTimeScope::AsOf(self.expression()?);
        self.expect_word("YIELD")?;
        let mut yield_items = vec![self.identifier()?.value().to_owned()];
        while self.consume_kind(&TokenKind::Comma) {
            yield_items.push(self.identifier()?.value().to_owned());
        }
        if self.position != self.lexed.tokens().len() {
            return Err(self.error_here(
                "DTG-CYPHER-UNEXPECTED-TOKEN",
                "unexpected token after DIFF YIELD list",
            ));
        }
        Ok(DiffStatement::new(
            graph,
            from,
            to,
            transaction,
            yield_items,
        ))
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
            if self.lexed.profile().version() == CypherVersion::V5
                && matches!(
                    kind,
                    ClauseKind::Let | ClauseKind::Filter | ClauseKind::Finish
                )
            {
                return Err(parse_error(
                    "DTG-CYPHER-FEATURE-NOT-IN-PROFILE",
                    tokens[start_index].span(),
                    "clause is not available in the selected Cypher profile",
                ));
            }
            let start = tokens[start_index].span().start();
            let end = starts.get(offset + 1).map_or_else(
                || self.lexed.source_text().len(),
                |(next, _)| tokens[*next].span().start(),
            );
            clauses.push(Clause::new(
                kind,
                TextSpan::new(start, end),
                self.lexed.source_text()[start..end].trim_end(),
            ));
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

    fn consume_kind(&mut self, expected: &TokenKind) -> bool {
        if self.current().is_some_and(|token| token.kind() == expected) {
            self.position += 1;
            true
        } else {
            false
        }
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
    ParseError {
        code,
        span,
        message: message.into(),
    }
}
