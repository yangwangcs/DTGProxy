use cypher_ast::{BinaryOperator, Expression, Identifier, UnaryOperator};

use crate::parser::query_ast_node_count;
use crate::{
    ParseError, SourceSpan, SyntaxLimits, Token, TokenKind, lex_with_limits, parse_with_limits,
};

pub fn parse_expression(source: &str) -> Result<Expression, ParseError> {
    parse_expression_with_limits(source, SyntaxLimits::default())
}

pub(crate) fn parse_expression_with_limits(
    source: &str,
    limits: SyntaxLimits,
) -> Result<Expression, ParseError> {
    let lexed = lex_with_limits(source, limits).map_err(|error| ParseError::from_syntax(&error))?;
    let mut parser = ExpressionParser::new(lexed.tokens(), source, limits);
    let expression = parser.parse(0)?;
    if parser.position != lexed.tokens().len() {
        return Err(parser.error_here(
            "DTG-CYPHER-UNEXPECTED-TOKEN",
            "unexpected token after expression",
        ));
    }
    if expression_node_count(&expression) > limits.max_ast_nodes() {
        return Err(ParseError::new(
            "DTG-CYPHER-TOO-MANY-AST-NODES",
            SourceSpan::new(0, source.len()),
            "expression AST node count exceeds the configured limit",
        ));
    }
    Ok(expression)
}

pub(crate) struct ExpressionParser<'tokens> {
    tokens: &'tokens [Token],
    position: usize,
    source: &'tokens str,
    limits: SyntaxLimits,
}

impl<'tokens> ExpressionParser<'tokens> {
    pub(crate) const fn new(
        tokens: &'tokens [Token],
        source: &'tokens str,
        limits: SyntaxLimits,
    ) -> Self {
        Self {
            tokens,
            position: 0,
            source,
            limits,
        }
    }

    pub(crate) fn parse(&mut self, minimum_binding_power: u8) -> Result<Expression, ParseError> {
        let mut left = self.prefix()?;
        loop {
            if self.consume(&TokenKind::Dot) {
                let property = self.identifier()?;
                left = Expression::Property {
                    value: Box::new(left),
                    property,
                };
                continue;
            }
            if self.consume(&TokenKind::LeftBracket) {
                let index = self.parse(0)?;
                self.expect(&TokenKind::RightBracket)?;
                left = Expression::Index {
                    value: Box::new(left),
                    index: Box::new(index),
                };
                continue;
            }
            if self.consume(&TokenKind::LeftParen) {
                let name = qualified_name(left).ok_or_else(|| {
                    self.error_here(
                        "DTG-CYPHER-INVALID-FUNCTION-NAME",
                        "function call requires a qualified identifier",
                    )
                })?;
                let mut arguments = Vec::new();
                if self.consume(&TokenKind::Star) {
                    self.expect(&TokenKind::RightParen)?;
                    if !is_count_function(&name) {
                        return Err(self.error_here(
                            "DTG-CYPHER-INVALID-FUNCTION-ARGUMENT",
                            "only count accepts * as an argument",
                        ));
                    }
                } else if !self.consume(&TokenKind::RightParen) {
                    loop {
                        arguments.push(self.parse(0)?);
                        if self.consume(&TokenKind::Comma) {
                            continue;
                        }
                        self.expect(&TokenKind::RightParen)?;
                        break;
                    }
                }
                left = Expression::FunctionCall { name, arguments };
                continue;
            }
            let Some((operator, left_bp, right_bp, words)) = self.infix() else {
                break;
            };
            if left_bp < minimum_binding_power {
                break;
            }
            self.position += words;
            let right = self.parse(right_bp)?;
            left = Expression::binary(operator, left, right);
        }
        Ok(left)
    }

    fn prefix(&mut self) -> Result<Expression, ParseError> {
        if self.consume(&TokenKind::Plus) {
            return Ok(Expression::unary(UnaryOperator::Plus, self.parse(9)?));
        }
        if self.consume(&TokenKind::Minus) {
            return Ok(Expression::unary(UnaryOperator::Minus, self.parse(9)?));
        }
        if self.consume_word("NOT") {
            return Ok(Expression::unary(UnaryOperator::Not, self.parse(4)?));
        }
        if self.current_is_word("EXISTS") && self.next_kind() == Some(&TokenKind::LeftBrace) {
            return self.subquery_expression(true);
        }
        if self.current_is_word("COUNT") && self.next_kind() == Some(&TokenKind::LeftBrace) {
            return self.subquery_expression(false);
        }
        let token = self.current().ok_or_else(|| {
            self.error_here("DTG-CYPHER-EXPECTED-EXPRESSION", "expected an expression")
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
            TokenKind::LeftParen => {
                self.position += 1;
                let nested = self.parse(0)?;
                self.expect(&TokenKind::RightParen)?;
                return Ok(nested);
            }
            TokenKind::LeftBracket => {
                self.position += 1;
                return self.list();
            }
            TokenKind::LeftBrace => {
                self.position += 1;
                return self.map();
            }
            _ => {
                return Err(self.error_here(
                    "DTG-CYPHER-EXPECTED-EXPRESSION",
                    "token cannot start an expression",
                ));
            }
        };
        self.position += 1;
        Ok(expression)
    }

    fn subquery_expression(&mut self, exists: bool) -> Result<Expression, ParseError> {
        let keyword = self.position;
        let open = keyword + 1;
        let mut depth = 0_usize;
        let close = (open..self.tokens.len())
            .find(|index| {
                match self.tokens[*index].kind() {
                    TokenKind::LeftBrace => depth += 1,
                    TokenKind::RightBrace => {
                        depth = depth.saturating_sub(1);
                        if depth == 0 {
                            return true;
                        }
                    }
                    _ => {}
                }
                false
            })
            .ok_or_else(|| {
                self.error_here(
                    "DTG-CYPHER-UNBALANCED-DELIMITER",
                    "subquery expression body is not closed",
                )
            })?;
        let source =
            self.source[self.tokens[open].span().end()..self.tokens[close].span().start()].trim();
        if source.is_empty() {
            return Err(self.error_here(
                "DTG-CYPHER-INVALID-SUBQUERY",
                "subquery expression cannot be empty",
            ));
        }
        let parsed = parse_with_limits(source, self.limits)?;
        let cypher_ast::Statement::Query(query) = parsed.statement();
        self.position = close + 1;
        Ok(if exists {
            Expression::ExistsSubquery(Box::new(query.clone()))
        } else {
            Expression::CountSubquery(Box::new(query.clone()))
        })
    }

    fn list(&mut self) -> Result<Expression, ParseError> {
        let mut values = Vec::new();
        if self.consume(&TokenKind::RightBracket) {
            return Ok(Expression::List(values));
        }
        loop {
            values.push(self.parse(0)?);
            if self.consume(&TokenKind::Comma) {
                continue;
            }
            self.expect(&TokenKind::RightBracket)?;
            return Ok(Expression::List(values));
        }
    }

    fn map(&mut self) -> Result<Expression, ParseError> {
        let mut entries = Vec::new();
        if self.consume(&TokenKind::RightBrace) {
            return Ok(Expression::Map(entries));
        }
        loop {
            let key = self.identifier()?;
            self.expect(&TokenKind::Colon)?;
            entries.push((key, self.parse(0)?));
            if self.consume(&TokenKind::Comma) {
                continue;
            }
            self.expect(&TokenKind::RightBrace)?;
            return Ok(Expression::Map(entries));
        }
    }

    fn infix(&self) -> Option<(BinaryOperator, u8, u8, usize)> {
        let token = self.current()?;
        let left = |operator, precedence| Some((operator, precedence, precedence + 1, 1));
        match token.kind() {
            TokenKind::Word(word) if word.eq_ignore_ascii_case("OR") => left(BinaryOperator::Or, 1),
            TokenKind::Word(word) if word.eq_ignore_ascii_case("XOR") => {
                left(BinaryOperator::Xor, 2)
            }
            TokenKind::Word(word) if word.eq_ignore_ascii_case("AND") => {
                left(BinaryOperator::And, 3)
            }
            TokenKind::Equal => left(BinaryOperator::Equal, 5),
            TokenKind::NotEqual => left(BinaryOperator::NotEqual, 5),
            TokenKind::Less => left(BinaryOperator::Less, 5),
            TokenKind::LessEqual => left(BinaryOperator::LessEqual, 5),
            TokenKind::Greater => left(BinaryOperator::Greater, 5),
            TokenKind::GreaterEqual => left(BinaryOperator::GreaterEqual, 5),
            TokenKind::RegexMatch => left(BinaryOperator::RegexMatch, 5),
            TokenKind::Word(word) if word.eq_ignore_ascii_case("IN") => left(BinaryOperator::In, 5),
            TokenKind::Word(word) if word.eq_ignore_ascii_case("CONTAINS") => {
                left(BinaryOperator::Contains, 5)
            }
            TokenKind::Word(word)
                if word.eq_ignore_ascii_case("STARTS") && self.next_is_word("WITH") =>
            {
                Some((BinaryOperator::StartsWith, 5, 6, 2))
            }
            TokenKind::Word(word)
                if word.eq_ignore_ascii_case("ENDS") && self.next_is_word("WITH") =>
            {
                Some((BinaryOperator::EndsWith, 5, 6, 2))
            }
            TokenKind::Plus => left(BinaryOperator::Add, 6),
            TokenKind::Minus => left(BinaryOperator::Subtract, 6),
            TokenKind::Star => left(BinaryOperator::Multiply, 7),
            TokenKind::Slash => left(BinaryOperator::Divide, 7),
            TokenKind::Percent => left(BinaryOperator::Modulo, 7),
            TokenKind::Caret => Some((BinaryOperator::Power, 8, 8, 1)),
            _ => None,
        }
    }

    fn identifier(&mut self) -> Result<Identifier, ParseError> {
        let token = self.current().ok_or_else(|| {
            self.error_here("DTG-CYPHER-EXPECTED-IDENTIFIER", "expected an identifier")
        })?;
        let identifier = match token.kind() {
            TokenKind::Word(value) => Identifier::new(value, false),
            TokenKind::EscapedIdentifier(value) => Identifier::new(value, true),
            _ => {
                return Err(
                    self.error_here("DTG-CYPHER-EXPECTED-IDENTIFIER", "expected an identifier")
                );
            }
        };
        self.position += 1;
        Ok(identifier)
    }

    fn expect(&mut self, expected: &TokenKind) -> Result<(), ParseError> {
        if self.consume(expected) {
            Ok(())
        } else {
            Err(self.error_here(
                "DTG-CYPHER-EXPECTED-TOKEN",
                format!("expected {expected:?}"),
            ))
        }
    }

    fn consume(&mut self, expected: &TokenKind) -> bool {
        if self.current().is_some_and(|token| token.kind() == expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn consume_word(&mut self, expected: &str) -> bool {
        if self.current().is_some_and(|token| {
            matches!(token.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case(expected))
        }) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn next_is_word(&self, expected: &str) -> bool {
        self.tokens.get(self.position + 1).is_some_and(|token| {
            matches!(token.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case(expected))
        })
    }

    fn current_is_word(&self, expected: &str) -> bool {
        self.current().is_some_and(|token| {
            matches!(token.kind(), TokenKind::Word(word) if word.eq_ignore_ascii_case(expected))
        })
    }

    fn next_kind(&self) -> Option<&TokenKind> {
        self.tokens.get(self.position + 1).map(Token::kind)
    }

    fn current(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    pub(crate) const fn consumed(&self) -> usize {
        self.position
    }

    fn error_here(&self, code: &'static str, message: impl Into<String>) -> ParseError {
        ParseError::new(
            code,
            self.current().map_or_else(
                || SourceSpan::new(self.source.len(), self.source.len()),
                Token::span,
            ),
            message,
        )
    }
}

fn qualified_name(expression: Expression) -> Option<Vec<Identifier>> {
    match expression {
        Expression::Identifier(identifier) => Some(vec![identifier]),
        Expression::Property { value, property } => {
            let mut name = qualified_name(*value)?;
            name.push(property);
            Some(name)
        }
        _ => None,
    }
}

fn is_count_function(name: &[Identifier]) -> bool {
    matches!(name, [identifier] if identifier.value().eq_ignore_ascii_case("count"))
}

fn expression_node_count(expression: &Expression) -> usize {
    match expression {
        Expression::List(items) => items.iter().fold(1_usize, |count, item| {
            count.saturating_add(expression_node_count(item))
        }),
        Expression::Map(items) => items.iter().fold(1_usize, |count, (_, value)| {
            count.saturating_add(expression_node_count(value))
        }),
        Expression::Unary { expression, .. } => {
            1_usize.saturating_add(expression_node_count(expression))
        }
        Expression::Binary { left, right, .. } => 1_usize
            .saturating_add(expression_node_count(left))
            .saturating_add(expression_node_count(right)),
        Expression::Property { value, .. } => 1_usize.saturating_add(expression_node_count(value)),
        Expression::Index { value, index } => 1_usize
            .saturating_add(expression_node_count(value))
            .saturating_add(expression_node_count(index)),
        Expression::FunctionCall { arguments, .. } => {
            arguments.iter().fold(1_usize, |count, argument| {
                count.saturating_add(expression_node_count(argument))
            })
        }
        Expression::ExistsSubquery(query) | Expression::CountSubquery(query) => {
            1_usize.saturating_add(query_ast_node_count(query))
        }
        Expression::Null
        | Expression::Boolean(_)
        | Expression::Integer(_)
        | Expression::Float(_)
        | Expression::String(_)
        | Expression::Parameter(_)
        | Expression::Identifier(_) => 1,
    }
}
