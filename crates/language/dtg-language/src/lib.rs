#![forbid(unsafe_code)]

mod ast;
mod lexer;
mod normalize;
mod parser;
mod sema;
mod token;

use std::{fmt, sync::Arc};

pub use dtg_language_ir::LogicalProgram;

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LanguageError {
    code: &'static str,
    message: String,
    start: usize,
    end: usize,
}
impl LanguageError {
    pub fn code(&self) -> &'static str {
        self.code
    }
    pub fn span(&self) -> (usize, usize) {
        (self.start, self.end)
    }
    fn lex(message: impl Into<String>, start: usize, end: usize) -> Self {
        Self {
            code: "DTG-LANG-LEX",
            message: message.into(),
            start,
            end,
        }
    }
    fn parse(message: impl Into<String>, start: usize, end: usize) -> Self {
        Self {
            code: "DTG-LANG-PARSE",
            message: message.into(),
            start,
            end,
        }
    }
    fn removed(message: impl Into<String>, start: usize, end: usize) -> Self {
        Self {
            code: "DTG-LANG-REMOVED-SYNTAX",
            message: message.into(),
            start,
            end,
        }
    }
    fn limit(message: impl Into<String>, start: usize, end: usize) -> Self {
        Self {
            code: "DTG-LANG-LIMIT",
            message: message.into(),
            start,
            end,
        }
    }
    fn semantic(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            start: 0,
            end: 0,
        }
    }
}
impl fmt::Display for LanguageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at {}..{}: {}",
            self.code, self.start, self.end, self.message
        )
    }
}
impl std::error::Error for LanguageError {}

pub trait SchemaCatalog: Send + Sync {
    fn graph_id(&self, name: &str) -> Option<dtg_language_ir::GraphId>;
}
#[derive(Default)]
pub struct EmptySchemaCatalog;
impl SchemaCatalog for EmptySchemaCatalog {
    fn graph_id(&self, _name: &str) -> Option<dtg_language_ir::GraphId> {
        None
    }
}

pub struct Language {
    catalog: Arc<dyn SchemaCatalog>,
}
impl Language {
    pub fn new(catalog: Arc<dyn SchemaCatalog>) -> Self {
        Self { catalog }
    }
    pub fn compile(&self, source: &str) -> Result<LogicalProgram, LanguageError> {
        compile(source, self.catalog.as_ref())
    }
}

pub fn compile(source: &str, catalog: &dyn SchemaCatalog) -> Result<LogicalProgram, LanguageError> {
    let tokens = lexer::lex(source)?;
    let ast = parser::parse(&tokens)?;
    let typed = sema::analyze(ast, catalog)?;
    let program = normalize::normalize(typed)?;
    dtg_language_ir::validate_program(&program)
        .map_err(|error| LanguageError::semantic("DTG-LANG-IR-VALIDATION", error.to_string()))?;
    Ok(program)
}
