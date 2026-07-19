use crate::{Expression, Identifier};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pattern {
    paths: Vec<PathPattern>,
}

impl Pattern {
    #[must_use]
    pub const fn new(paths: Vec<PathPattern>) -> Self {
        Self { paths }
    }

    #[must_use]
    pub fn paths(&self) -> &[PathPattern] {
        &self.paths
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathPattern {
    start: NodePattern,
    chains: Vec<RelationshipChain>,
}

impl PathPattern {
    #[must_use]
    pub const fn new(start: NodePattern, chains: Vec<RelationshipChain>) -> Self {
        Self { start, chains }
    }

    #[must_use]
    pub const fn start(&self) -> &NodePattern {
        &self.start
    }

    #[must_use]
    pub fn chains(&self) -> &[RelationshipChain] {
        &self.chains
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePattern {
    variable: Option<Identifier>,
    labels: Vec<Identifier>,
    properties: Option<Expression>,
}

impl NodePattern {
    #[must_use]
    pub const fn new(
        variable: Option<Identifier>,
        labels: Vec<Identifier>,
        properties: Option<Expression>,
    ) -> Self {
        Self {
            variable,
            labels,
            properties,
        }
    }

    #[must_use]
    pub const fn variable(&self) -> Option<&Identifier> {
        self.variable.as_ref()
    }

    #[must_use]
    pub fn labels(&self) -> &[Identifier] {
        &self.labels
    }

    #[must_use]
    pub const fn properties(&self) -> Option<&Expression> {
        self.properties.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipChain {
    relationship: RelationshipPattern,
    node: NodePattern,
}

impl RelationshipChain {
    #[must_use]
    pub const fn new(relationship: RelationshipPattern, node: NodePattern) -> Self {
        Self { relationship, node }
    }

    #[must_use]
    pub const fn relationship(&self) -> &RelationshipPattern {
        &self.relationship
    }

    #[must_use]
    pub const fn node(&self) -> &NodePattern {
        &self.node
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationshipDirection {
    Outgoing,
    Incoming,
    Undirected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PatternLength {
    minimum: Option<u32>,
    maximum: Option<u32>,
}

impl PatternLength {
    #[must_use]
    pub const fn new(minimum: Option<u32>, maximum: Option<u32>) -> Self {
        Self { minimum, maximum }
    }

    #[must_use]
    pub const fn minimum(self) -> Option<u32> {
        self.minimum
    }

    #[must_use]
    pub const fn maximum(self) -> Option<u32> {
        self.maximum
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipPattern {
    variable: Option<Identifier>,
    types: Vec<Identifier>,
    length: Option<PatternLength>,
    properties: Option<Expression>,
    direction: RelationshipDirection,
}

impl RelationshipPattern {
    #[must_use]
    pub const fn new(
        variable: Option<Identifier>,
        types: Vec<Identifier>,
        length: Option<PatternLength>,
        properties: Option<Expression>,
        direction: RelationshipDirection,
    ) -> Self {
        Self {
            variable,
            types,
            length,
            properties,
            direction,
        }
    }

    #[must_use]
    pub const fn variable(&self) -> Option<&Identifier> {
        self.variable.as_ref()
    }

    #[must_use]
    pub fn types(&self) -> &[Identifier] {
        &self.types
    }

    #[must_use]
    pub const fn length(&self) -> Option<PatternLength> {
        self.length
    }

    #[must_use]
    pub const fn properties(&self) -> Option<&Expression> {
        self.properties.as_ref()
    }

    #[must_use]
    pub const fn direction(&self) -> RelationshipDirection {
        self.direction
    }
}
