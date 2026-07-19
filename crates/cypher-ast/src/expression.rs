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
    List(Vec<Expression>),
    Map(Vec<(Identifier, Expression)>),
    Unary {
        operator: UnaryOperator,
        expression: Box<Expression>,
    },
    Binary {
        operator: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Property {
        value: Box<Expression>,
        property: Identifier,
    },
    Index {
        value: Box<Expression>,
        index: Box<Expression>,
    },
    FunctionCall {
        name: Vec<Identifier>,
        arguments: Vec<Expression>,
    },
}

impl Expression {
    #[must_use]
    pub fn identifier(value: impl Into<String>) -> Self {
        Self::Identifier(Identifier::new(value, false))
    }

    #[must_use]
    pub fn unary(operator: UnaryOperator, expression: Self) -> Self {
        Self::Unary {
            operator,
            expression: Box::new(expression),
        }
    }

    #[must_use]
    pub fn binary(operator: BinaryOperator, left: Self, right: Self) -> Self {
        Self::Binary {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    #[must_use]
    pub fn property(value: Self, property: impl Into<String>) -> Self {
        Self::Property {
            value: Box::new(value),
            property: Identifier::new(property, false),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    Or,
    Xor,
    And,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    RegexMatch,
    In,
    Contains,
    StartsWith,
    EndsWith,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
}
