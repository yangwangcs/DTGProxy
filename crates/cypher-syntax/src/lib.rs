#![forbid(unsafe_code)]

mod lexer;
mod limits;
mod parser;
mod scanner;
mod token;

pub use cypher_ast::{CypherProfile, CypherVersion};
pub use lexer::{Lexed, SyntaxError, lex, lex_with_limits};
pub use limits::{LimitsError, SyntaxLimits};
pub use parser::{ParseError, ParsedQuery, parse, parse_with_limits};
pub use scanner::{VersionScan, VersionScanError, scan_version};
pub use token::{SourceSpan, Token, TokenKind};
