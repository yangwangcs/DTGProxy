use std::collections::BTreeMap;

use crate::{ProtocolError, Value, decode, encode};

const HELLO: u8 = 0x01;
const GOODBYE: u8 = 0x02;
const RESET: u8 = 0x0F;
const RUN: u8 = 0x10;
const BEGIN: u8 = 0x11;
const COMMIT: u8 = 0x12;
const ROLLBACK: u8 = 0x13;
const DISCARD: u8 = 0x2F;
const PULL: u8 = 0x3F;
const ROUTE: u8 = 0x66;
const LOGON: u8 = 0x6A;
const LOGOFF: u8 = 0x6B;
const INTERRUPT: u8 = 0x6E;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientMessage {
    Hello(BTreeMap<String, Value>),
    Logon(BTreeMap<String, Value>),
    Logoff,
    Goodbye,
    Reset,
    Interrupt,
    Run {
        query: String,
        parameters: BTreeMap<String, Value>,
        extra: BTreeMap<String, Value>,
    },
    Pull {
        n: i64,
        query_id: Option<i64>,
    },
    Discard {
        n: i64,
        query_id: Option<i64>,
    },
    Begin(BTreeMap<String, Value>),
    Commit,
    Rollback,
    Route {
        routing: BTreeMap<String, Value>,
        bookmarks: Vec<Value>,
        database: Option<String>,
    },
}

pub fn encode_client_message(message: &ClientMessage) -> Result<Vec<u8>, ProtocolError> {
    let (signature, fields) = match message {
        ClientMessage::Hello(metadata) => (HELLO, vec![Value::Map(metadata.clone())]),
        ClientMessage::Logon(auth) => (LOGON, vec![Value::Map(auth.clone())]),
        ClientMessage::Logoff => (LOGOFF, vec![]),
        ClientMessage::Goodbye => (GOODBYE, vec![]),
        ClientMessage::Reset => (RESET, vec![]),
        ClientMessage::Interrupt => (INTERRUPT, vec![]),
        ClientMessage::Run {
            query,
            parameters,
            extra,
        } => (
            RUN,
            vec![
                Value::String(query.clone()),
                Value::Map(parameters.clone()),
                Value::Map(extra.clone()),
            ],
        ),
        ClientMessage::Pull { n, query_id } => {
            (PULL, vec![Value::Map(stream_metadata(*n, *query_id))])
        }
        ClientMessage::Discard { n, query_id } => {
            (DISCARD, vec![Value::Map(stream_metadata(*n, *query_id))])
        }
        ClientMessage::Begin(extra) => (BEGIN, vec![Value::Map(extra.clone())]),
        ClientMessage::Commit => (COMMIT, vec![]),
        ClientMessage::Rollback => (ROLLBACK, vec![]),
        ClientMessage::Route {
            routing,
            bookmarks,
            database,
        } => (
            ROUTE,
            vec![
                Value::Map(routing.clone()),
                Value::List(bookmarks.clone()),
                database
                    .as_ref()
                    .map_or(Value::Null, |database| Value::String(database.clone())),
            ],
        ),
    };
    encode(&Value::Structure { signature, fields })
}

pub fn decode_client_message(bytes: &[u8]) -> Result<ClientMessage, ProtocolError> {
    let Value::Structure { signature, fields } = decode(bytes)? else {
        return Err(ProtocolError::new(
            "DTG-BOLT-EXPECTED-MESSAGE",
            0,
            "Bolt message must be a PackStream structure",
        ));
    };
    match signature {
        HELLO => Ok(ClientMessage::Hello(one_map(fields, "HELLO")?)),
        LOGON => Ok(ClientMessage::Logon(one_map(fields, "LOGON")?)),
        LOGOFF => {
            zero_fields(&fields, "LOGOFF")?;
            Ok(ClientMessage::Logoff)
        }
        GOODBYE => {
            zero_fields(&fields, "GOODBYE")?;
            Ok(ClientMessage::Goodbye)
        }
        RESET => {
            zero_fields(&fields, "RESET")?;
            Ok(ClientMessage::Reset)
        }
        INTERRUPT => {
            zero_fields(&fields, "INTERRUPT")?;
            Ok(ClientMessage::Interrupt)
        }
        RUN => {
            if fields.len() != 3 {
                return Err(field_count("RUN", 3, fields.len()));
            }
            let mut fields = fields.into_iter();
            let Value::String(query) = fields.next().expect("three fields") else {
                return Err(field_type("RUN query"));
            };
            let Value::Map(parameters) = fields.next().expect("three fields") else {
                return Err(field_type("RUN parameters"));
            };
            let Value::Map(extra) = fields.next().expect("three fields") else {
                return Err(field_type("RUN extra"));
            };
            Ok(ClientMessage::Run {
                query,
                parameters,
                extra,
            })
        }
        PULL | DISCARD => {
            let metadata = one_map(fields, if signature == PULL { "PULL" } else { "DISCARD" })?;
            let (n, query_id) = parse_stream_metadata(&metadata)?;
            if signature == PULL {
                Ok(ClientMessage::Pull { n, query_id })
            } else {
                Ok(ClientMessage::Discard { n, query_id })
            }
        }
        BEGIN => Ok(ClientMessage::Begin(one_map(fields, "BEGIN")?)),
        COMMIT => {
            zero_fields(&fields, "COMMIT")?;
            Ok(ClientMessage::Commit)
        }
        ROLLBACK => {
            zero_fields(&fields, "ROLLBACK")?;
            Ok(ClientMessage::Rollback)
        }
        ROUTE => parse_route(fields),
        _ => Err(ProtocolError::new(
            "DTG-BOLT-UNKNOWN-MESSAGE",
            1,
            format!("unknown Bolt message signature 0x{signature:02x}"),
        )),
    }
}

fn stream_metadata(n: i64, query_id: Option<i64>) -> BTreeMap<String, Value> {
    let mut metadata = BTreeMap::from([("n".into(), Value::Integer(n))]);
    if let Some(query_id) = query_id {
        metadata.insert("qid".into(), Value::Integer(query_id));
    }
    metadata
}

fn parse_stream_metadata(
    metadata: &BTreeMap<String, Value>,
) -> Result<(i64, Option<i64>), ProtocolError> {
    let Some(Value::Integer(n)) = metadata.get("n") else {
        return Err(field_type("stream n"));
    };
    let query_id = match metadata.get("qid") {
        Some(Value::Integer(value)) => Some(*value),
        Some(_) => return Err(field_type("stream qid")),
        None => None,
    };
    Ok((*n, query_id))
}

fn parse_route(fields: Vec<Value>) -> Result<ClientMessage, ProtocolError> {
    if fields.len() != 3 {
        return Err(field_count("ROUTE", 3, fields.len()));
    }
    let mut fields = fields.into_iter();
    let Value::Map(routing) = fields.next().expect("three fields") else {
        return Err(field_type("ROUTE routing"));
    };
    let Value::List(bookmarks) = fields.next().expect("three fields") else {
        return Err(field_type("ROUTE bookmarks"));
    };
    let database = match fields.next().expect("three fields") {
        Value::Null => None,
        Value::String(database) => Some(database),
        _ => return Err(field_type("ROUTE database")),
    };
    Ok(ClientMessage::Route {
        routing,
        bookmarks,
        database,
    })
}

fn one_map(
    fields: Vec<Value>,
    message: &'static str,
) -> Result<BTreeMap<String, Value>, ProtocolError> {
    if fields.len() != 1 {
        return Err(field_count(message, 1, fields.len()));
    }
    let Value::Map(map) = fields.into_iter().next().expect("one field") else {
        return Err(field_type(message));
    };
    Ok(map)
}

fn zero_fields(fields: &[Value], message: &'static str) -> Result<(), ProtocolError> {
    if !fields.is_empty() {
        return Err(field_count(message, 0, fields.len()));
    }
    Ok(())
}

fn field_count(message: &str, expected: usize, actual: usize) -> ProtocolError {
    ProtocolError::new(
        "DTG-BOLT-FIELD-COUNT",
        0,
        format!("{message} requires {expected} fields; got {actual}"),
    )
}

fn field_type(field: &str) -> ProtocolError {
    ProtocolError::new(
        "DTG-BOLT-FIELD-TYPE",
        0,
        format!("{field} has an invalid PackStream type"),
    )
}
