use crate::Expression;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum TemporalAxis {
    ValidTime,
    SystemTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalMode {
    StateAsOf,
    StateBetween,
    ChangesBetween,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalScope {
    axis: TemporalAxis,
    mode: TemporalMode,
    start: Expression,
    end: Option<Expression>,
}

impl TemporalScope {
    #[must_use]
    pub const fn as_of(axis: TemporalAxis, at: Expression) -> Self {
        Self {
            axis,
            mode: TemporalMode::StateAsOf,
            start: at,
            end: None,
        }
    }

    #[must_use]
    pub const fn between(
        axis: TemporalAxis,
        mode: TemporalMode,
        start: Expression,
        end: Expression,
    ) -> Self {
        Self {
            axis,
            mode,
            start,
            end: Some(end),
        }
    }

    #[must_use]
    pub const fn axis(&self) -> TemporalAxis {
        self.axis
    }

    #[must_use]
    pub const fn mode(&self) -> TemporalMode {
        self.mode
    }

    #[must_use]
    pub const fn start(&self) -> &Expression {
        &self.start
    }

    #[must_use]
    pub const fn end(&self) -> Option<&Expression> {
        self.end.as_ref()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TemporalContext {
    scopes: Vec<TemporalScope>,
}

impl TemporalContext {
    #[must_use]
    pub const fn new(scopes: Vec<TemporalScope>) -> Self {
        Self { scopes }
    }

    #[must_use]
    pub fn scopes(&self) -> &[TemporalScope] {
        &self.scopes
    }

    #[must_use]
    pub fn scope(&self, axis: TemporalAxis) -> Option<&TemporalScope> {
        self.scopes.iter().find(|scope| scope.axis() == axis)
    }
}
