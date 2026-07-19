use std::error::Error;
use std::fmt::{self, Display, Formatter};

use cypher_ast::CypherProfile;

use crate::{SourceSpan, SyntaxLimits, Token, TokenKind, scan_version};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lexed<'query> {
    source: &'query str,
    profile: CypherProfile,
    tokens: Vec<Token>,
}

impl<'query> Lexed<'query> {
    #[must_use]
    pub const fn profile(&self) -> CypherProfile {
        self.profile
    }

    #[must_use]
    pub fn tokens(&self) -> &[Token] {
        &self.tokens
    }

    #[must_use]
    pub fn source(&self, token: &Token) -> &'query str {
        &self.source[token.span().start()..token.span().end()]
    }

    #[must_use]
    pub(crate) const fn source_text(&self) -> &'query str {
        self.source
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntaxError {
    code: &'static str,
    span: SourceSpan,
    message: String,
}

impl SyntaxError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub const fn span(&self) -> SourceSpan {
        self.span
    }
}

impl Display for SyntaxError {
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

impl Error for SyntaxError {}

pub fn lex(query: &str) -> Result<Lexed<'_>, SyntaxError> {
    lex_with_limits(query, SyntaxLimits::default())
}

pub fn lex_with_limits(query: &str, limits: SyntaxLimits) -> Result<Lexed<'_>, SyntaxError> {
    if query.len() > limits.max_query_bytes() {
        return Err(error(
            "DTG-CYPHER-QUERY-TOO-LARGE",
            0,
            query.len(),
            "query exceeds the configured byte limit",
        ));
    }
    let scan = scan_version(query).map_err(|source| {
        error(
            source.code(),
            source.offset(),
            source.offset(),
            "unsupported language version",
        )
    })?;
    let mut lexer = Lexer {
        source: query,
        cursor: scan.body_offset(),
        tokens: Vec::new(),
        limits,
        nesting_depth: 0,
    };
    lexer.run()?;
    Ok(Lexed {
        source: query,
        profile: scan.profile(),
        tokens: lexer.tokens,
    })
}

struct Lexer<'source> {
    source: &'source str,
    cursor: usize,
    tokens: Vec<Token>,
    limits: SyntaxLimits,
    nesting_depth: usize,
}

impl Lexer<'_> {
    fn run(&mut self) -> Result<(), SyntaxError> {
        while self.cursor < self.source.len() {
            self.skip_trivia()?;
            if self.cursor == self.source.len() {
                break;
            }
            let start = self.cursor;
            let character = self.current().expect("cursor is within source");
            match character {
                '`' => self.escaped_identifier(start)?,
                '\'' | '"' => self.string(start, character)?,
                '$' => self.parameter(start)?,
                '0'..='9' => self.number(start)?,
                character if is_identifier_start(character) => self.word(start)?,
                '(' => self.single(TokenKind::LeftParen, true)?,
                ')' => self.single(TokenKind::RightParen, false)?,
                '[' => self.single(TokenKind::LeftBracket, true)?,
                ']' => self.single(TokenKind::RightBracket, false)?,
                '{' => self.single(TokenKind::LeftBrace, true)?,
                '}' => self.single(TokenKind::RightBrace, false)?,
                ',' => self.single_plain(TokenKind::Comma)?,
                '.' => self.single_plain(TokenKind::Dot)?,
                ':' => self.single_plain(TokenKind::Colon)?,
                ';' => self.single_plain(TokenKind::Semicolon)?,
                '|' => self.single_plain(TokenKind::Pipe)?,
                '+' => self.single_plain(TokenKind::Plus)?,
                '-' => self.single_plain(TokenKind::Minus)?,
                '*' => self.single_plain(TokenKind::Star)?,
                '/' => self.single_plain(TokenKind::Slash)?,
                '%' => self.single_plain(TokenKind::Percent)?,
                '^' => self.single_plain(TokenKind::Caret)?,
                '=' => self.single_plain(TokenKind::Equal)?,
                '<' => self.less(start)?,
                '>' => self.greater(start)?,
                '!' if self.peek_ascii('=') => self.double(TokenKind::NotEqual)?,
                '~' => self.single_plain(TokenKind::RegexMatch)?,
                _ => {
                    let end = start + character.len_utf8();
                    return Err(error(
                        "DTG-CYPHER-INVALID-CHARACTER",
                        start,
                        end,
                        "character is not valid Cypher syntax",
                    ));
                }
            }
        }
        Ok(())
    }

    fn skip_trivia(&mut self) -> Result<(), SyntaxError> {
        loop {
            while self.current().is_some_and(char::is_whitespace) {
                self.bump();
            }
            if self.starts_with("//") {
                self.cursor += 2;
                while self.current().is_some_and(|character| character != '\n') {
                    self.bump();
                }
                continue;
            }
            if self.starts_with("/*") {
                let start = self.cursor;
                self.cursor += 2;
                while self.cursor < self.source.len() && !self.starts_with("*/") {
                    self.bump();
                }
                if self.cursor == self.source.len() {
                    return Err(error(
                        "DTG-CYPHER-UNTERMINATED-COMMENT",
                        start,
                        self.cursor,
                        "block comment is not terminated",
                    ));
                }
                self.cursor += 2;
                continue;
            }
            return Ok(());
        }
    }

    fn word(&mut self, start: usize) -> Result<(), SyntaxError> {
        self.bump();
        while self.current().is_some_and(is_identifier_continue) {
            self.bump();
        }
        let value = self.source[start..self.cursor].to_owned();
        self.push(TokenKind::Word(value), start)
    }

    fn parameter(&mut self, start: usize) -> Result<(), SyntaxError> {
        self.bump();
        let name_start = self.cursor;
        if !self.current().is_some_and(is_identifier_start) {
            return Err(error(
                "DTG-CYPHER-INVALID-PARAMETER",
                start,
                self.cursor,
                "parameter requires an identifier after '$'",
            ));
        }
        self.bump();
        while self.current().is_some_and(is_identifier_continue) {
            self.bump();
        }
        self.push(
            TokenKind::Parameter(self.source[name_start..self.cursor].to_owned()),
            start,
        )
    }

    fn number(&mut self, start: usize) -> Result<(), SyntaxError> {
        while self
            .current()
            .is_some_and(|character| character.is_ascii_digit())
        {
            self.bump();
        }
        let mut float = false;
        if self.current() == Some('.')
            && self
                .next_character()
                .is_some_and(|character| character.is_ascii_digit())
        {
            float = true;
            self.bump();
            while self
                .current()
                .is_some_and(|character| character.is_ascii_digit())
            {
                self.bump();
            }
        }
        if self
            .current()
            .is_some_and(|character| matches!(character, 'e' | 'E'))
        {
            float = true;
            self.bump();
            if self
                .current()
                .is_some_and(|character| matches!(character, '+' | '-'))
            {
                self.bump();
            }
            while self
                .current()
                .is_some_and(|character| character.is_ascii_digit())
            {
                self.bump();
            }
        }
        let value = self.source[start..self.cursor].to_owned();
        self.push(
            if float {
                TokenKind::Float(value)
            } else {
                TokenKind::Integer(value)
            },
            start,
        )
    }

    fn escaped_identifier(&mut self, start: usize) -> Result<(), SyntaxError> {
        self.bump();
        let mut value = String::new();
        loop {
            let Some(character) = self.current() else {
                return Err(error(
                    "DTG-CYPHER-UNTERMINATED-IDENTIFIER",
                    start,
                    self.cursor,
                    "escaped identifier is not terminated",
                ));
            };
            self.bump();
            if character != '`' {
                value.push(character);
            } else if self.current() == Some('`') {
                self.bump();
                value.push('`');
            } else {
                break;
            }
        }
        self.push(TokenKind::EscapedIdentifier(value), start)
    }

    fn string(&mut self, start: usize, quote: char) -> Result<(), SyntaxError> {
        self.bump();
        let mut value = String::new();
        loop {
            let Some(character) = self.current() else {
                return Err(error(
                    "DTG-CYPHER-UNTERMINATED-STRING",
                    start,
                    self.cursor,
                    "string literal is not terminated",
                ));
            };
            self.bump();
            if character == quote {
                break;
            }
            if character != '\\' {
                value.push(character);
                continue;
            }
            let Some(escaped) = self.current() else {
                return Err(error(
                    "DTG-CYPHER-UNTERMINATED-STRING",
                    start,
                    self.cursor,
                    "string escape is not terminated",
                ));
            };
            self.bump();
            value.push(match escaped {
                '\\' => '\\',
                '\'' => '\'',
                '"' => '"',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'b' => '\u{0008}',
                'f' => '\u{000c}',
                _ => {
                    return Err(error(
                        "DTG-CYPHER-INVALID-ESCAPE",
                        self.cursor - escaped.len_utf8() - 1,
                        self.cursor,
                        "string escape is not supported",
                    ));
                }
            });
        }
        self.push(TokenKind::String(value), start)
    }

    fn less(&mut self, _start: usize) -> Result<(), SyntaxError> {
        if self.peek_ascii('=') {
            self.double(TokenKind::LessEqual)
        } else if self.peek_ascii('>') {
            self.double(TokenKind::NotEqual)
        } else {
            self.single_plain(TokenKind::Less)
        }
    }

    fn greater(&mut self, _start: usize) -> Result<(), SyntaxError> {
        if self.peek_ascii('=') {
            self.double(TokenKind::GreaterEqual)
        } else {
            self.single_plain(TokenKind::Greater)
        }
    }

    fn single(&mut self, kind: TokenKind, opens: bool) -> Result<(), SyntaxError> {
        let start = self.cursor;
        if opens {
            self.nesting_depth += 1;
            if self.nesting_depth > self.limits.max_nesting_depth() {
                return Err(error(
                    "DTG-CYPHER-NESTING-TOO-DEEP",
                    start,
                    start + 1,
                    "syntax nesting exceeds the configured limit",
                ));
            }
        } else {
            self.nesting_depth = self.nesting_depth.saturating_sub(1);
        }
        self.bump();
        self.push(kind, start)
    }

    fn single_plain(&mut self, kind: TokenKind) -> Result<(), SyntaxError> {
        let start = self.cursor;
        self.bump();
        self.push(kind, start)
    }

    fn double(&mut self, kind: TokenKind) -> Result<(), SyntaxError> {
        let start = self.cursor;
        self.cursor += 2;
        self.push(kind, start)
    }

    fn push(&mut self, kind: TokenKind, start: usize) -> Result<(), SyntaxError> {
        self.tokens
            .push(Token::new(kind, SourceSpan::new(start, self.cursor)));
        if self.tokens.len() > self.limits.max_tokens() {
            return Err(error(
                "DTG-CYPHER-TOO-MANY-TOKENS",
                start,
                self.cursor,
                "token count exceeds the configured limit",
            ));
        }
        Ok(())
    }

    fn current(&self) -> Option<char> {
        self.source[self.cursor..].chars().next()
    }

    fn next_character(&self) -> Option<char> {
        let mut characters = self.source[self.cursor..].chars();
        characters.next()?;
        characters.next()
    }

    fn bump(&mut self) {
        self.cursor += self
            .current()
            .expect("bump requires a current character")
            .len_utf8();
    }

    fn starts_with(&self, value: &str) -> bool {
        self.source[self.cursor..].starts_with(value)
    }

    fn peek_ascii(&self, expected: char) -> bool {
        self.source
            .as_bytes()
            .get(self.cursor + 1)
            .is_some_and(|actual| *actual == expected as u8)
    }
}

fn is_identifier_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

fn is_identifier_continue(character: char) -> bool {
    character == '_' || character.is_alphanumeric()
}

fn error(code: &'static str, start: usize, end: usize, message: impl Into<String>) -> SyntaxError {
    SyntaxError {
        code,
        span: SourceSpan::new(start, end),
        message: message.into(),
    }
}
