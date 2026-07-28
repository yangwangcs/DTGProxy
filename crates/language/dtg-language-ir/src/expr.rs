use dtg_kernel::Value;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum LogicalExpr {
    Literal(Value),
    Parameter(String),
    Column(String),
    Property {
        input: Box<Self>,
        name: String,
    },
    Unary {
        operator: UnaryOperator,
        input: Box<Self>,
    },
    Binary {
        left: Box<Self>,
        operator: BinaryOperator,
        right: Box<Self>,
    },
    List(Vec<Self>),
    Map(Vec<(String, Self)>),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum UnaryOperator {
    Not,
    Negate,
    IsNull,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Contains,
}
