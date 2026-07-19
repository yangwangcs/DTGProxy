use std::error::Error;
use std::fmt::{self, Display, Formatter};

pub const MAX_QUERY_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_TOKENS: usize = 1_000_000;
pub const MAX_NESTING_DEPTH: usize = 1_024;
pub const MAX_AST_NODES: usize = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyntaxLimits {
    max_query_bytes: usize,
    max_tokens: usize,
    max_nesting_depth: usize,
    max_ast_nodes: usize,
}

impl SyntaxLimits {
    pub fn new(
        max_query_bytes: usize,
        max_tokens: usize,
        max_nesting_depth: usize,
        max_ast_nodes: usize,
    ) -> Result<Self, LimitsError> {
        for (name, value, maximum) in [
            ("max_query_bytes", max_query_bytes, MAX_QUERY_BYTES),
            ("max_tokens", max_tokens, MAX_TOKENS),
            ("max_nesting_depth", max_nesting_depth, MAX_NESTING_DEPTH),
            ("max_ast_nodes", max_ast_nodes, MAX_AST_NODES),
        ] {
            if value == 0 || value > maximum {
                return Err(LimitsError {
                    name,
                    maximum,
                    actual: value,
                });
            }
        }
        Ok(Self {
            max_query_bytes,
            max_tokens,
            max_nesting_depth,
            max_ast_nodes,
        })
    }

    #[must_use]
    pub const fn max_query_bytes(self) -> usize {
        self.max_query_bytes
    }

    #[must_use]
    pub const fn max_tokens(self) -> usize {
        self.max_tokens
    }

    #[must_use]
    pub const fn max_nesting_depth(self) -> usize {
        self.max_nesting_depth
    }

    #[must_use]
    pub const fn max_ast_nodes(self) -> usize {
        self.max_ast_nodes
    }
}

impl Default for SyntaxLimits {
    fn default() -> Self {
        Self {
            max_query_bytes: 1024 * 1024,
            max_tokens: 250_000,
            max_nesting_depth: 256,
            max_ast_nodes: 250_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LimitsError {
    name: &'static str,
    maximum: usize,
    actual: usize,
}

impl Display for LimitsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} must be between 1 and {}, got {}",
            self.name, self.maximum, self.actual
        )
    }
}

impl Error for LimitsError {}
