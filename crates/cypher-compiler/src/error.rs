use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompileError {
    code: &'static str,
    message: String,
}

impl CompileError {
    #[must_use]
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl Display for CompileError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for CompileError {}

impl From<cypher_syntax::ParseError> for CompileError {
    fn from(error: cypher_syntax::ParseError) -> Self {
        Self::new(error.code(), error.to_string())
    }
}

impl From<cypher_sema::SemanticError> for CompileError {
    fn from(error: cypher_sema::SemanticError) -> Self {
        Self::new(error.code(), error.to_string())
    }
}

impl From<temporal_ir::v2::ValidationError> for CompileError {
    fn from(error: temporal_ir::v2::ValidationError) -> Self {
        Self::new("DTG-IR-VALIDATION", error.to_string())
    }
}
