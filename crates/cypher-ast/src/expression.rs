#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Identifier {
    value: String,
    escaped: bool,
}

impl Identifier {
    #[must_use]
    pub fn new(value: impl Into<String>, escaped: bool) -> Self {
        Self {
            value: value.into(),
            escaped,
        }
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub const fn escaped(&self) -> bool {
        self.escaped
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expression {
    Null,
    Boolean(bool),
    Integer(String),
    Float(String),
    String(String),
    Parameter(String),
    Identifier(Identifier),
}
