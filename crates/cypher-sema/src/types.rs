#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CypherType {
    Any,
    Null,
    Boolean,
    Integer,
    Float,
    String,
    Bytes,
    List(Box<CypherType>),
    Map,
    Node,
    Relationship,
    Path,
    Temporal,
    Spatial,
    Vector,
}

impl CypherType {
    #[must_use]
    pub fn unify(left: &Self, right: &Self) -> Self {
        match (left, right) {
            (Self::Null, other) | (other, Self::Null) => other.clone(),
            (Self::Integer, Self::Float) | (Self::Float, Self::Integer) => Self::Float,
            (Self::List(left), Self::List(right)) => Self::List(Box::new(Self::unify(left, right))),
            (left, right) if left == right => left.clone(),
            _ => Self::Any,
        }
    }

    #[must_use]
    pub const fn is_numeric(&self) -> bool {
        matches!(self, Self::Integer | Self::Float | Self::Any | Self::Null)
    }

    #[must_use]
    pub const fn is_boolean(&self) -> bool {
        matches!(self, Self::Boolean | Self::Any | Self::Null)
    }
}
