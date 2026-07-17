#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use temporal_ir::{
    DiffOperator, ExpandDirection, GraphScope, PlanError, PointOperator, TemporalPlan,
    TemporalSelector,
};
use temporal_storage::{ElementId, ElementKind, GraphId, PartitionId};
use temporal_types::{TransactionTime, ValidTime};

pub const MAX_QUERY_BYTES: usize = 4_096;
pub const MAX_QUERY_TOKENS: usize = 64;

pub fn parse(input: &str) -> Result<TemporalPlan, ParseError> {
    if input.len() > MAX_QUERY_BYTES {
        return Err(ParseError::QueryTooLong {
            max: MAX_QUERY_BYTES,
            actual: input.len(),
        });
    }
    let tokens = input.split_whitespace().collect::<Vec<_>>();
    if tokens.is_empty() {
        return Err(ParseError::Empty);
    }
    if tokens.len() > MAX_QUERY_TOKENS {
        return Err(ParseError::TooManyTokens {
            max: MAX_QUERY_TOKENS,
            actual: tokens.len(),
        });
    }

    let mut cursor = Cursor::new(&tokens);
    let statement = cursor.take().expect("non-empty token list");
    let plan = if keyword(statement, "VERTEX") || keyword(statement, "EDGE") {
        parse_element(statement, &mut cursor)?
    } else if keyword(statement, "EXPAND") {
        parse_expand(&mut cursor)?
    } else {
        return Err(ParseError::UnsupportedStatement {
            token: statement.to_owned(),
        });
    };
    cursor.finish()?;
    plan.validate().map_err(ParseError::InvalidPlan)?;
    Ok(plan)
}

fn parse_element(statement: &str, cursor: &mut Cursor<'_>) -> Result<TemporalPlan, ParseError> {
    let kind = if keyword(statement, "VERTEX") {
        ElementKind::Vertex
    } else {
        ElementKind::Edge
    };
    let id = ElementId::new(cursor.integer("element id")?);
    let scope = parse_scope(cursor)?;
    if cursor.peek_is("FOR") {
        cursor.expect("FOR")?;
        parse_point(
            cursor,
            scope,
            match kind {
                ElementKind::Vertex => PointOperator::VertexById(id),
                ElementKind::Edge => PointOperator::EdgeById(id),
            },
        )
    } else if cursor.peek_is("DIFF") {
        cursor.expect("DIFF")?;
        parse_diff(cursor, scope, DiffOperator::Element { kind, id })
    } else {
        Err(cursor.unexpected("FOR or DIFF"))
    }
}

fn parse_expand(cursor: &mut Cursor<'_>) -> Result<TemporalPlan, ParseError> {
    let position = cursor.position();
    let direction = match cursor.take() {
        Some(token) if keyword(token, "OUT") => ExpandDirection::Out,
        Some(token) if keyword(token, "IN") => ExpandDirection::In,
        Some(token) if keyword(token, "BOTH") => ExpandDirection::Both,
        actual => {
            return Err(ParseError::UnexpectedToken {
                position,
                expected: "OUT, IN, or BOTH",
                actual: actual.map(str::to_owned),
            });
        }
    };
    cursor.expect("FROM")?;
    let origin = ElementId::new(cursor.integer("element id")?);
    let scope = parse_scope(cursor)?;
    cursor.expect("FOR")?;
    parse_point(cursor, scope, PointOperator::Expand { origin, direction })
}

fn parse_scope(cursor: &mut Cursor<'_>) -> Result<GraphScope, ParseError> {
    cursor.expect("GRAPH")?;
    let graph = GraphId::new(cursor.integer("graph id")?);
    cursor.expect("PARTITION")?;
    let partition = PartitionId::new(cursor.integer("partition id")?);
    Ok(GraphScope::new(graph, partition))
}

fn parse_point(
    cursor: &mut Cursor<'_>,
    scope: GraphScope,
    operator: PointOperator,
) -> Result<TemporalPlan, ParseError> {
    cursor.expect("VALID")?;
    cursor.expect("TIME")?;
    let valid_time = ValidTime::from_micros(cursor.integer("valid time")?);
    let transaction = if cursor.peek_is("CURRENT") {
        cursor.expect("CURRENT")?;
        TemporalSelector::Current
    } else {
        cursor.expect("AS")?;
        cursor.expect("OF")?;
        cursor.expect("TRANSACTION")?;
        cursor.expect("TIME")?;
        TemporalSelector::AsOf(cursor.transaction_time()?)
    };
    cursor.expect("LIMIT")?;
    let limit = cursor.integer("result limit")?;
    Ok(TemporalPlan::point(
        scope,
        operator,
        valid_time,
        transaction,
        limit,
    ))
}

fn parse_diff(
    cursor: &mut Cursor<'_>,
    scope: GraphScope,
    operator: DiffOperator,
) -> Result<TemporalPlan, ParseError> {
    cursor.expect("TRANSACTION")?;
    cursor.expect("TIME")?;
    let from_transaction = cursor.transaction_time()?;
    cursor.expect("TO")?;
    let to_transaction = cursor.transaction_time()?;
    cursor.expect("LIMIT")?;
    let limit = cursor.integer("result limit")?;
    Ok(TemporalPlan::diff(
        scope,
        operator,
        from_transaction,
        to_transaction,
        limit,
    ))
}

fn keyword(actual: &str, expected: &str) -> bool {
    actual.eq_ignore_ascii_case(expected)
}

struct Cursor<'a> {
    tokens: &'a [&'a str],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(tokens: &'a [&'a str]) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    const fn position(&self) -> usize {
        self.position
    }

    fn take(&mut self) -> Option<&'a str> {
        let token = self.tokens.get(self.position).copied();
        if token.is_some() {
            self.position += 1;
        }
        token
    }

    fn peek_is(&self, expected: &str) -> bool {
        self.tokens
            .get(self.position)
            .is_some_and(|actual| keyword(actual, expected))
    }

    fn expect(&mut self, expected: &'static str) -> Result<(), ParseError> {
        let position = self.position;
        let actual = self.take();
        if actual.is_some_and(|actual| keyword(actual, expected)) {
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken {
                position,
                expected,
                actual: actual.map(str::to_owned),
            })
        }
    }

    fn integer<T>(&mut self, field: &'static str) -> Result<T, ParseError>
    where
        T: FromStr,
    {
        let position = self.position;
        let value = self.take().ok_or(ParseError::UnexpectedToken {
            position,
            expected: field,
            actual: None,
        })?;
        value.parse().map_err(|_| ParseError::InvalidInteger {
            position,
            field,
            value: value.to_owned(),
        })
    }

    fn transaction_time(&mut self) -> Result<TransactionTime, ParseError> {
        let position = self.position;
        let value = self.take().ok_or(ParseError::UnexpectedToken {
            position,
            expected: "physical:logical transaction time",
            actual: None,
        })?;
        let parsed = value.split_once(':').and_then(|(physical, logical)| {
            Some((physical.parse::<i64>().ok()?, logical.parse::<u32>().ok()?))
        });
        parsed
            .map(|(physical, logical)| TransactionTime::new(physical, logical))
            .ok_or_else(|| ParseError::InvalidInteger {
                position,
                field: "transaction time",
                value: value.to_owned(),
            })
    }

    fn unexpected(&self, expected: &'static str) -> ParseError {
        ParseError::UnexpectedToken {
            position: self.position,
            expected,
            actual: self
                .tokens
                .get(self.position)
                .map(|token| (*token).to_owned()),
        }
    }

    fn finish(&self) -> Result<(), ParseError> {
        if let Some(token) = self.tokens.get(self.position) {
            Err(ParseError::TrailingToken {
                position: self.position,
                token: (*token).to_owned(),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParseError {
    Empty,
    QueryTooLong {
        max: usize,
        actual: usize,
    },
    TooManyTokens {
        max: usize,
        actual: usize,
    },
    UnsupportedStatement {
        token: String,
    },
    UnexpectedToken {
        position: usize,
        expected: &'static str,
        actual: Option<String>,
    },
    InvalidInteger {
        position: usize,
        field: &'static str,
        value: String,
    },
    TrailingToken {
        position: usize,
        token: String,
    },
    InvalidPlan(PlanError),
}

impl Display for ParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("temporal query is empty"),
            Self::QueryTooLong { max, actual } => {
                write!(formatter, "query is {actual} bytes; maximum is {max}")
            }
            Self::TooManyTokens { max, actual } => {
                write!(formatter, "query has {actual} tokens; maximum is {max}")
            }
            Self::UnsupportedStatement { token } => {
                write!(formatter, "unsupported temporal query statement {token}")
            }
            Self::UnexpectedToken {
                position,
                expected,
                actual,
            } => write!(
                formatter,
                "expected {expected} at token {position}, got {}",
                actual.as_deref().unwrap_or("end of query")
            ),
            Self::InvalidInteger {
                position,
                field,
                value,
            } => write!(
                formatter,
                "invalid {field} integer at token {position}: {value}"
            ),
            Self::TrailingToken { position, token } => {
                write!(formatter, "unexpected trailing token {token} at {position}")
            }
            Self::InvalidPlan(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ParseError {}
