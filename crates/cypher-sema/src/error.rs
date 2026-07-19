use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticError {
    code: &'static str,
    message: String,
    symbol: Option<String>,
}

impl SemanticError {
    #[must_use]
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            symbol: None,
        }
    }

    #[must_use]
    pub(crate) fn for_symbol(
        code: &'static str,
        message: impl Into<String>,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            symbol: Some(symbol.into()),
        }
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }
}

impl Display for SemanticError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for SemanticError {}

impl From<cypher_syntax::ParseError> for SemanticError {
    fn from(error: cypher_syntax::ParseError) -> Self {
        Self::new(error.code(), error.to_string())
    }
}
