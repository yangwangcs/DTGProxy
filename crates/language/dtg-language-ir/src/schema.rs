#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Parameter {
    pub name: String,
    pub data_type: LogicalType,
    pub required: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct RowSchema {
    pub fields: Vec<Field>,
}

impl RowSchema {
    pub const fn empty() -> Self {
        Self { fields: Vec::new() }
    }
}

impl Default for RowSchema {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Field {
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum LogicalType {
    Null,
    Boolean,
    Integer,
    Float,
    Bytes,
    String,
    List(Box<Self>),
    Map,
    Vertex,
    Relationship,
    Any,
}
