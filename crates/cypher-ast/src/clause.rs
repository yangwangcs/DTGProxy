#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TextSpan {
    start: usize,
    end: usize,
}

impl TextSpan {
    #[must_use]
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    #[must_use]
    pub const fn start(self) -> usize {
        self.start
    }

    #[must_use]
    pub const fn end(self) -> usize {
        self.end
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClauseKind {
    Match,
    OptionalMatch,
    Where,
    Filter,
    With,
    Let,
    Return,
    Finish,
    Unwind,
    For,
    Union { all: bool },
    Next,
    When,
    OrderBy,
    Skip,
    Offset,
    Limit,
    Create,
    Merge,
    Set,
    Remove,
    Delete { detach: bool },
    Foreach,
    Call,
    Yield,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Clause {
    kind: ClauseKind,
    span: TextSpan,
    text: String,
}

impl Clause {
    #[must_use]
    pub fn new(kind: ClauseKind, span: TextSpan, text: impl Into<String>) -> Self {
        Self {
            kind,
            span,
            text: text.into(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ClauseKind {
        self.kind
    }

    #[must_use]
    pub const fn span(&self) -> TextSpan {
        self.span
    }

    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }
}
