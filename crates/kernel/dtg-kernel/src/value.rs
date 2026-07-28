use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Self>),
    Map(BTreeMap<String, Self>),
}
