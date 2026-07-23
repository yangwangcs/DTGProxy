use cypher_ast::{
    Expression, Identifier, NodePattern, PathPattern, Pattern, PatternLength, RelationshipChain,
    RelationshipDirection, RelationshipPattern,
};

use crate::expression_parser::ExpressionParser;
use crate::{ParseError, SourceSpan, SyntaxLimits, Token, TokenKind, lex};

pub fn parse_pattern(source: &str) -> Result<Pattern, ParseError> {
    let lexed = lex(source).map_err(|error| ParseError::from_syntax(&error))?;
    let mut parser = PatternParser {
        tokens: lexed.tokens(),
        position: 0,
        source,
    };
    let mut paths = vec![parser.path()?];
    while parser.consume(&TokenKind::Comma) {
        paths.push(parser.path()?);
    }
    if parser.position != parser.tokens.len() {
        return Err(parser.error_here(
            "DTG-CYPHER-UNEXPECTED-TOKEN",
            "unexpected token after pattern",
        ));
    }
    Ok(Pattern::new(paths))
}

struct PatternParser<'tokens> {
    tokens: &'tokens [Token],
    position: usize,
    source: &'tokens str,
}

impl PatternParser<'_> {
    fn path(&mut self) -> Result<PathPattern, ParseError> {
        let start = self.node()?;
        let mut chains = Vec::new();
        while self.at(&TokenKind::Minus) || self.at(&TokenKind::Less) {
            chains.push(self.chain()?);
        }
        Ok(PathPattern::new(start, chains))
    }

    fn node(&mut self) -> Result<NodePattern, ParseError> {
        if !self.consume(&TokenKind::LeftParen) {
            return Err(self.error_here(
                "DTG-CYPHER-EXPECTED-NODE-PATTERN",
                "expected '(' to start a node pattern",
            ));
        }
        let variable = if self.current_is_identifier() {
            Some(self.identifier()?)
        } else {
            None
        };
        let mut labels = Vec::new();
        while self.consume(&TokenKind::Colon) {
            labels.push(self.identifier()?);
        }
        let properties = if self.at(&TokenKind::LeftBrace) {
            Some(self.expression()?)
        } else {
            None
        };
        self.expect(&TokenKind::RightParen, "DTG-CYPHER-EXPECTED-NODE-PATTERN")?;
        Ok(NodePattern::new(variable, labels, properties))
    }

    fn chain(&mut self) -> Result<RelationshipChain, ParseError> {
        let incoming = self.consume(&TokenKind::Less);
        self.expect(&TokenKind::Minus, "DTG-CYPHER-EXPECTED-RELATIONSHIP")?;
        let (variable, types, length, properties) = if self.consume(&TokenKind::LeftBracket) {
            let variable = if self.current_is_identifier() {
                Some(self.identifier()?)
            } else {
                None
            };
            let mut types = Vec::new();
            if self.consume(&TokenKind::Colon) {
                types.push(self.identifier()?);
                while self.consume(&TokenKind::Pipe) {
                    let _ = self.consume(&TokenKind::Colon);
                    types.push(self.identifier()?);
                }
            }
            let length = if self.consume(&TokenKind::Star) {
                Some(self.length()?)
            } else {
                None
            };
            let properties = if self.at(&TokenKind::LeftBrace) {
                Some(self.expression()?)
            } else {
                None
            };
            self.expect(&TokenKind::RightBracket, "DTG-CYPHER-EXPECTED-RELATIONSHIP")?;
            (variable, types, length, properties)
        } else {
            (None, Vec::new(), None, None)
        };
        self.expect(&TokenKind::Minus, "DTG-CYPHER-EXPECTED-RELATIONSHIP")?;
        let direction = if incoming {
            RelationshipDirection::Incoming
        } else if self.consume(&TokenKind::Greater) {
            RelationshipDirection::Outgoing
        } else {
            RelationshipDirection::Undirected
        };
        let node = self.node()?;
        Ok(RelationshipChain::new(
            RelationshipPattern::new(variable, types, length, properties, direction),
            node,
        ))
    }

    fn length(&mut self) -> Result<PatternLength, ParseError> {
        let first = self.integer()?;
        if self.consume(&TokenKind::Dot) {
            self.expect(&TokenKind::Dot, "DTG-CYPHER-INVALID-PATTERN-LENGTH")?;
            let maximum = self.integer()?;
            Ok(PatternLength::new(first, maximum))
        } else if let Some(value) = first {
            Ok(PatternLength::new(Some(value), Some(value)))
        } else {
            Ok(PatternLength::new(Some(1), None))
        }
    }

    fn integer(&mut self) -> Result<Option<u32>, ParseError> {
        let Some(token) = self.current() else {
            return Ok(None);
        };
        let TokenKind::Integer(value) = token.kind() else {
            return Ok(None);
        };
        let parsed = value.parse::<u32>().map_err(|_| {
            self.error_here(
                "DTG-CYPHER-INVALID-PATTERN-LENGTH",
                "relationship length does not fit u32",
            )
        })?;
        self.position += 1;
        Ok(Some(parsed))
    }

    fn expression(&mut self) -> Result<Expression, ParseError> {
        let mut parser = ExpressionParser::new(
            &self.tokens[self.position..],
            self.source,
            SyntaxLimits::default(),
        );
        let expression = parser.parse(0)?;
        self.position += parser.consumed();
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
                return Err(
                    self.error_here("DTG-CYPHER-EXPECTED-IDENTIFIER", "expected an identifier")
                );
            }
        };
        self.position += 1;
        Ok(identifier)
    }

    fn current_is_identifier(&self) -> bool {
        self.current().is_some_and(|token| {
            matches!(
                token.kind(),
                TokenKind::Word(_) | TokenKind::EscapedIdentifier(_)
            )
        })
    }

    fn expect(&mut self, expected: &TokenKind, code: &'static str) -> Result<(), ParseError> {
        if self.consume(expected) {
            Ok(())
        } else {
            Err(self.error_here(code, format!("expected {expected:?}")))
        }
    }

    fn consume(&mut self, expected: &TokenKind) -> bool {
        if self.at(expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn at(&self, expected: &TokenKind) -> bool {
        self.current().is_some_and(|token| token.kind() == expected)
    }

    fn current(&self) -> Option<&Token> {
        self.tokens.get(self.position)
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
