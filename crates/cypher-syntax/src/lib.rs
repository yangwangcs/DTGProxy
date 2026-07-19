#![forbid(unsafe_code)]

mod expression_parser;
mod lexer;
mod limits;
mod parser;
mod pattern_parser;
mod scanner;
mod token;

pub use cypher_ast::{CypherProfile, CypherVersion};
pub use expression_parser::parse_expression;
pub use lexer::{Lexed, SyntaxError, lex, lex_with_limits};
pub use limits::{LimitsError, SyntaxLimits};
pub use parser::{ParseError, ParsedQuery, parse, parse_with_limits};
pub use pattern_parser::parse_pattern;
pub use scanner::{VersionScan, VersionScanError, scan_version};
pub use token::{SourceSpan, Token, TokenKind};
