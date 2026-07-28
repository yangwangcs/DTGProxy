use std::collections::BTreeMap;

use crate::{
    LanguageError,
    ast::{
        Axis, Boundary, Expr, Match, Mode, NodePattern, Pattern, Program, RelationshipDirection,
        RelationshipPattern, Scope, Statement, Write,
    },
    token::{Token, TokenKind},
};

const MAX_NESTING: usize = 128;

pub(crate) fn parse(tokens: &[Token]) -> Result<Program, LanguageError> {
    Parser { tokens, cursor: 0 }.program()
}

struct Parser<'a> {
    tokens: &'a [Token],
    cursor: usize,
}
impl Parser<'_> {
    fn program(&mut self) -> Result<Program, LanguageError> {
        if self.at_word("DIFF")
            || self.at_word("AT")
            || self.at_word("LEGACY")
            || self.at_word("COMPAT")
            || self.at_word("V1")
            || self.at_word("V2")
        {
            return Err(self.removed("removed compatibility or temporal syntax"));
        }
        let graph = if self.take_word("USE") {
            Some(self.identifier()?)
        } else {
            None
        };
        let statement = if self.take_word("BEGIN") {
            Statement::Boundary(Boundary::Begin)
        } else if self.take_word("COMMIT") {
            Statement::Boundary(Boundary::Commit)
        } else if self.take_word("ROLLBACK") {
            Statement::Boundary(Boundary::Rollback)
        } else if self.take_word("CALL") {
            Statement::Procedure {
                name: self.procedure_name()?,
            }
        } else if self.take_word("SUBMIT") {
            self.submit()?
        } else if self.at_word("CREATE") || self.at_word("MATCH") && self.has_write_keyword() {
            self.write()?
        } else {
            Statement::Query(self.query()?)
        };
        if !matches!(self.current().kind, TokenKind::End) {
            if self.at_word("AT")
                || self.at_word("DIFF")
                || self.at_word("LEGACY")
                || self.at_word("COMPAT")
                || self.at_word("V1")
                || self.at_word("V2")
            {
                return Err(self.removed("removed compatibility or temporal syntax"));
            }
            return Err(self.error("unexpected trailing input"));
        }
        Ok(Program { graph, statement })
    }
    fn query(&mut self) -> Result<crate::ast::Query, LanguageError> {
        let mut scopes = Vec::new();
        while self.at_word("FOR") || self.at_word("CHANGES") {
            scopes.push(self.scope()?);
        }
        let mut matches = Vec::new();
        while self.take_word("MATCH") {
            let pattern = self.pattern()?;
            let mut local_scopes = Vec::new();
            while self.at_word("FOR") {
                local_scopes.push(self.scope()?);
            }
            matches.push(Match {
                pattern,
                scopes: local_scopes,
            });
        }
        if matches.is_empty() {
            return Err(self.error("MATCH is required for a query"));
        }
        let where_clause = if self.take_word("WHERE") {
            let left = self.expr()?;
            self.expect_symbol('=')?;
            Some((left, self.expr()?))
        } else {
            None
        };
        let mut returns = Vec::new();
        if self.take_word("RETURN") {
            loop {
                returns.push(self.expr()?);
                if !self.take_symbol(',') {
                    break;
                }
            }
        }
        Ok(crate::ast::Query {
            scopes,
            matches,
            where_clause,
            returns,
        })
    }
    fn write(&mut self) -> Result<Statement, LanguageError> {
        if self.take_word("CREATE") {
            let pattern = self.node_pattern()?;
            let valid_from = self.valid_from()?;
            return Ok(Statement::Write(Write::Create {
                node: pattern,
                valid_from,
            }));
        }
        self.expect_word("MATCH")?;
        let pattern = self.pattern()?;
        let variable = pattern
            .nodes
            .first()
            .ok_or_else(|| self.error("write MATCH requires a node"))?
            .variable
            .clone();
        if self.take_word("SET") {
            let mut properties = BTreeMap::new();
            loop {
                let target = self.identifier()?;
                self.expect_symbol('.')?;
                let property = self.identifier()?;
                self.expect_symbol('=')?;
                if target != variable {
                    return Err(self.error("SET target must be the MATCH variable"));
                }
                properties.insert(property, self.expr()?);
                if !self.take_symbol(',') {
                    break;
                }
            }
            return Ok(Statement::Write(Write::Set {
                variable,
                properties,
                valid_from: self.valid_from()?,
            }));
        }
        self.expect_word("DELETE")?;
        let delete_variable = self.identifier()?;
        if delete_variable != variable {
            return Err(self.error("DELETE target must be the MATCH variable"));
        }
        Ok(Statement::Write(Write::Delete {
            variable,
            valid_from: self.valid_from()?,
        }))
    }
    fn submit(&mut self) -> Result<Statement, LanguageError> {
        self.expect_word("ANALYTICS")?;
        let algorithm = self.identifier()?;
        self.expect_word("ASYNC")?;
        Ok(Statement::SubmitAnalytics { algorithm })
    }
    fn scope(&mut self) -> Result<Scope, LanguageError> {
        if self.take_word("CHANGES") {
            self.expect_word("FOR")?;
            let axis = self.axis()?;
            self.expect_word("BETWEEN")?;
            let start = self.expr()?;
            self.expect_word("AND")?;
            let end = self.expr()?;
            return Ok(Scope {
                axis,
                mode: Mode::Changes(start, end),
            });
        }
        self.expect_word("FOR")?;
        let axis = self.axis()?;
        if self.take_word("AS") {
            self.expect_word("OF")?;
            return Ok(Scope {
                axis,
                mode: Mode::AsOf(self.expr()?),
            });
        }
        self.expect_word("BETWEEN")?;
        let start = self.expr()?;
        self.expect_word("AND")?;
        let end = self.expr()?;
        if axis == Axis::System {
            return Err(self.error("SYSTEM_TIME supports only AS OF"));
        }
        Ok(Scope {
            axis,
            mode: Mode::Between(start, end),
        })
    }
    fn axis(&mut self) -> Result<Axis, LanguageError> {
        if self.take_word("VALID_TIME") {
            Ok(Axis::Valid)
        } else if self.take_word("SYSTEM_TIME") {
            Ok(Axis::System)
        } else {
            Err(self.error("VALID_TIME or SYSTEM_TIME expected"))
        }
    }
    fn valid_from(&mut self) -> Result<Expr, LanguageError> {
        self.expect_word("VALID")?;
        if self.take_word("TO") {
            return Err(self.removed("VALID TO is removed; writes use VALID FROM"));
        }
        self.expect_word("FROM")?;
        self.expr()
    }
    fn pattern(&mut self) -> Result<Pattern, LanguageError> {
        let mut nodes = vec![self.node_pattern()?];
        let mut relationships = Vec::new();
        while self.at_symbol('-') || matches!(self.current().kind, TokenKind::ArrowLeft) {
            let incoming = self.take_arrow_left();
            if !incoming {
                self.expect_symbol('-')?;
            }
            self.expect_symbol('[')?;
            let relationship = self.relationship_pattern()?;
            self.expect_symbol(']')?;
            let direction = if incoming {
                self.expect_symbol('-')?;
                RelationshipDirection::Incoming
            } else if self.take_arrow_right() {
                RelationshipDirection::Outgoing
            } else if self.take_symbol('-') {
                RelationshipDirection::Either
            } else {
                return Err(self.error("relationship direction expected"));
            };
            relationships.push(RelationshipPattern {
                direction,
                ..relationship
            });
            nodes.push(self.node_pattern()?);
        }
        Ok(Pattern {
            nodes,
            relationships,
        })
    }
    fn node_pattern(&mut self) -> Result<NodePattern, LanguageError> {
        self.expect_symbol('(')?;
        let variable = if self.at_symbol(':') || self.at_symbol(')') {
            format!("_node_{}", self.cursor)
        } else {
            self.identifier()?
        };
        let mut labels = Vec::new();
        while self.take_symbol(':') {
            labels.push(self.identifier()?);
        }
        let properties = if self.take_symbol('{') {
            self.property_map()?
        } else {
            BTreeMap::new()
        };
        self.expect_symbol(')')?;
        Ok(NodePattern {
            variable,
            labels,
            properties,
        })
    }
    fn relationship_pattern(&mut self) -> Result<RelationshipPattern, LanguageError> {
        let variable = if self.at_symbol(':') || self.at_symbol(']') {
            format!("_rel_{}", self.cursor)
        } else {
            self.identifier()?
        };
        let mut types = Vec::new();
        while self.take_symbol(':') {
            types.push(self.identifier()?);
        }
        Ok(RelationshipPattern {
            variable,
            types,
            direction: RelationshipDirection::Outgoing,
        })
    }
    fn property_map(&mut self) -> Result<BTreeMap<String, Expr>, LanguageError> {
        let mut map = BTreeMap::new();
        let mut depth = 1;
        while !self.at_symbol('}') {
            if depth > MAX_NESTING {
                return Err(self.limit("property nesting exceeds limit"));
            }
            let key = self.identifier()?;
            self.expect_symbol(':')?;
            map.insert(key, self.expr()?);
            if !self.take_symbol(',') {
                break;
            }
            depth += 1;
        }
        self.expect_symbol('}')?;
        Ok(map)
    }
    fn expr(&mut self) -> Result<Expr, LanguageError> {
        let first = match &self.current().kind {
            TokenKind::Parameter(value) => {
                let value = Expr::Parameter(value.clone());
                self.cursor += 1;
                value
            }
            TokenKind::Integer(value) => {
                let value = Expr::Integer(*value);
                self.cursor += 1;
                value
            }
            TokenKind::String(value) => {
                let value = Expr::String(value.clone());
                self.cursor += 1;
                value
            }
            TokenKind::Word(value) if value.eq_ignore_ascii_case("true") => {
                self.cursor += 1;
                Expr::Boolean(true)
            }
            TokenKind::Word(value) if value.eq_ignore_ascii_case("false") => {
                self.cursor += 1;
                Expr::Boolean(false)
            }
            TokenKind::Word(value) if value.eq_ignore_ascii_case("null") => {
                self.cursor += 1;
                Expr::Null
            }
            TokenKind::Word(value) => {
                let value = value.clone();
                self.cursor += 1;
                Expr::Column(value)
            }
            _ => return Err(self.error("expression expected")),
        };
        if self.take_symbol('.') {
            let name = self.identifier()?;
            match first {
                Expr::Column(input) => Ok(Expr::Property { input, name }),
                _ => Err(self.error("property receiver must be a variable")),
            }
        } else {
            Ok(first)
        }
    }
    fn procedure_name(&mut self) -> Result<String, LanguageError> {
        let mut name = match self.current().kind.clone() {
            TokenKind::Parameter(value) => {
                self.cursor += 1;
                format!("${value}")
            }
            _ => self.identifier()?,
        };
        while self.take_symbol('.') {
            name.push('.');
            name.push_str(&self.identifier()?);
        }
        if self.take_symbol('(') {
            while !self.take_symbol(')') {
                if matches!(self.current().kind, TokenKind::End) {
                    return Err(self.error("unterminated procedure call"));
                }
                self.cursor += 1;
            }
        }
        Ok(name)
    }
    fn has_write_keyword(&self) -> bool {
        self.tokens[self.cursor..]
            .iter()
            .any(|token| token.is_word("SET") || token.is_word("DELETE"))
    }
    fn current(&self) -> &Token {
        &self.tokens[self.cursor]
    }
    fn take_word(&mut self, word: &str) -> bool {
        if self.current().is_word(word) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }
    fn at_word(&self, word: &str) -> bool {
        self.current().is_word(word)
    }
    fn expect_word(&mut self, word: &str) -> Result<(), LanguageError> {
        self.take_word(word)
            .then_some(())
            .ok_or_else(|| self.error(&format!("{word} expected")))
    }
    fn take_symbol(&mut self, symbol: char) -> bool {
        if matches!(&self.current().kind, TokenKind::Symbol(value) if *value == symbol) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }
    fn at_symbol(&self, symbol: char) -> bool {
        matches!(&self.current().kind, TokenKind::Symbol(value) if *value == symbol)
    }
    fn expect_symbol(&mut self, symbol: char) -> Result<(), LanguageError> {
        self.take_symbol(symbol)
            .then_some(())
            .ok_or_else(|| self.error(&format!("'{symbol}' expected")))
    }
    fn take_arrow_right(&mut self) -> bool {
        if matches!(self.current().kind, TokenKind::ArrowRight) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }
    fn take_arrow_left(&mut self) -> bool {
        if matches!(self.current().kind, TokenKind::ArrowLeft) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }
    fn identifier(&mut self) -> Result<String, LanguageError> {
        match self.current().kind.clone() {
            TokenKind::Word(value) => {
                self.cursor += 1;
                Ok(value)
            }
            _ => Err(self.error("identifier expected")),
        }
    }
    fn error(&self, message: &str) -> LanguageError {
        LanguageError::parse(message, self.current().start, self.current().end)
    }
    fn removed(&self, message: &str) -> LanguageError {
        LanguageError::removed(message, self.current().start, self.current().end)
    }
    fn limit(&self, message: &str) -> LanguageError {
        LanguageError::limit(message, self.current().start, self.current().end)
    }
}
