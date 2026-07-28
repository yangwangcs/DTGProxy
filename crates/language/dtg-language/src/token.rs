#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TokenKind {
    Word(String),
    Parameter(String),
    Integer(i64),
    String(String),
    Symbol(char),
    ArrowRight,
    ArrowLeft,
    End,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl Token {
    pub(crate) fn is_word(&self, word: &str) -> bool {
        matches!(&self.kind, TokenKind::Word(value) if value.eq_ignore_ascii_case(word))
    }
}
