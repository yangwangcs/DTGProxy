use temporal_types::GraphValue;

use super::SlotId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarExpr {
    Slot(SlotId),
    Parameter(String),
    Literal(GraphValue),
    List(Vec<ScalarExpr>),
    Map(Vec<(u32, ScalarExpr)>),
    Property {
        value: Box<ScalarExpr>,
        property_id: u32,
    },
    Not(Box<ScalarExpr>),
    Negate(Box<ScalarExpr>),
    Equal(Box<ScalarExpr>, Box<ScalarExpr>),
    NotEqual(Box<ScalarExpr>, Box<ScalarExpr>),
    Less(Box<ScalarExpr>, Box<ScalarExpr>),
    LessEqual(Box<ScalarExpr>, Box<ScalarExpr>),
    Greater(Box<ScalarExpr>, Box<ScalarExpr>),
    GreaterEqual(Box<ScalarExpr>, Box<ScalarExpr>),
    And(Box<ScalarExpr>, Box<ScalarExpr>),
    Or(Box<ScalarExpr>, Box<ScalarExpr>),
    Add(Box<ScalarExpr>, Box<ScalarExpr>),
    Subtract(Box<ScalarExpr>, Box<ScalarExpr>),
    Multiply(Box<ScalarExpr>, Box<ScalarExpr>),
    Divide(Box<ScalarExpr>, Box<ScalarExpr>),
    Function {
        function_id: u32,
        arguments: Vec<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub fn visit_slots(&self, visitor: &mut impl FnMut(SlotId)) {
        match self {
            Self::Slot(slot) => visitor(*slot),
            Self::Parameter(_) | Self::Literal(_) => {}
            Self::List(items) => {
                for item in items {
                    item.visit_slots(visitor);
                }
            }
            Self::Map(items) => {
                for (_, item) in items {
                    item.visit_slots(visitor);
                }
            }
            Self::Property { value, .. } | Self::Not(value) | Self::Negate(value) => {
                value.visit_slots(visitor);
            }
            Self::Equal(left, right)
            | Self::NotEqual(left, right)
            | Self::Less(left, right)
            | Self::LessEqual(left, right)
            | Self::Greater(left, right)
            | Self::GreaterEqual(left, right)
            | Self::And(left, right)
            | Self::Or(left, right)
            | Self::Add(left, right)
            | Self::Subtract(left, right)
            | Self::Multiply(left, right)
            | Self::Divide(left, right) => {
                left.visit_slots(visitor);
                right.visit_slots(visitor);
            }
            Self::Function { arguments, .. } => {
                for argument in arguments {
                    argument.visit_slots(visitor);
                }
            }
        }
    }

    pub fn visit_parameters<'a>(&'a self, visitor: &mut impl FnMut(&'a str)) {
        match self {
            Self::Parameter(name) => visitor(name),
            Self::Slot(_) | Self::Literal(_) => {}
            Self::List(items) => {
                for item in items {
                    item.visit_parameters(visitor);
                }
            }
            Self::Map(items) => {
                for (_, item) in items {
                    item.visit_parameters(visitor);
                }
            }
            Self::Property { value, .. } | Self::Not(value) | Self::Negate(value) => {
                value.visit_parameters(visitor);
            }
            Self::Equal(left, right)
            | Self::NotEqual(left, right)
            | Self::Less(left, right)
            | Self::LessEqual(left, right)
            | Self::Greater(left, right)
            | Self::GreaterEqual(left, right)
            | Self::And(left, right)
            | Self::Or(left, right)
            | Self::Add(left, right)
            | Self::Subtract(left, right)
            | Self::Multiply(left, right)
            | Self::Divide(left, right) => {
                left.visit_parameters(visitor);
                right.visit_parameters(visitor);
            }
            Self::Function { arguments, .. } => {
                for argument in arguments {
                    argument.visit_parameters(visitor);
                }
            }
        }
    }
}
