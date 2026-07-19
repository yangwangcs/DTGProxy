use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtocolError {
    code: &'static str,
    offset: usize,
    message: String,
}

impl ProtocolError {
    #[must_use]
    pub(crate) fn new(code: &'static str, offset: usize, message: impl Into<String>) -> Self {
        Self {
            code,
            offset,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }
}

impl Display for ProtocolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at byte {}: {}",
            self.code, self.offset, self.message
        )
    }
}

impl Error for ProtocolError {}
