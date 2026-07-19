use cypher_ast::{BinaryOperator, Expression, Identifier, UnaryOperator};

use crate::{ParseError, SourceSpan, Token, TokenKind, lex};

pub fn parse_expression(source: &str) -> Result<Expression, ParseError> {
    let lexed = lex(source).map_err(|error| ParseError::from_syntax(&error))?;
    let mut parser = ExpressionParser::new(lexed.tokens(), source.len());
    let expression = parser.parse(0)?;
    if parser.position != lexed.tokens().len() {
        return Err(parser.error_here(
            "DTG-CYPHER-UNEXPECTED-TOKEN",
            "unexpected token after expression",
        ));
    }
    Ok(expression)
}

pub(crate) struct ExpressionParser<'tokens> {
    tokens: &'tokens [Token],
    position: usize,
    source_len: usize,
}

impl<'tokens> ExpressionParser<'tokens> {
    pub(crate) const fn new(tokens: &'tokens [Token], source_len: usize) -> Self {
        Self {
            tokens,
            position: 0,
            source_len,
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
                if !self.consume(&TokenKind::RightParen) {
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
                || SourceSpan::new(self.source_len, self.source_len),
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
