use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use cypher_ast::{Expression, NodePattern, Pattern, RelationshipDirection};
use cypher_compiler::{CompiledMutation, CompiledQuery, PropertyTarget};
use query_executor::v2::RuntimeValue;
use temporal_ir::GraphScope;
use temporal_storage::{
    EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, ValidTime};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteContext {
    graph_id: u64,
    schema_version: u64,
    virtual_partitions: u32,
    deterministic_seed: [u8; 32],
    valid: Interval<ValidTime>,
    parameters: BTreeMap<String, RuntimeValue>,
}

impl WriteContext {
    pub fn new(
        graph_id: u64,
        schema_version: u64,
        virtual_partitions: u32,
        deterministic_seed: [u8; 32],
        valid: Interval<ValidTime>,
        parameters: BTreeMap<String, RuntimeValue>,
    ) -> Result<Self, WriteMaterializationError> {
        if graph_id == 0
            || schema_version == 0
            || virtual_partitions == 0
            || deterministic_seed == [0; 32]
        {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-INVALID-WRITE-CONTEXT",
                "write context identities, partition count, and seed must be non-zero",
            ));
        }
        Ok(Self {
            graph_id,
            schema_version,
            virtual_partitions,
            deterministic_seed,
            valid,
            parameters,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializedElementKind {
    Vertex,
    Relationship,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedElement {
    kind: MaterializedElementKind,
    element: ElementRef,
    type_id: u32,
    payload: CanonicalElement,
    source: Option<ElementRef>,
    destination: Option<ElementRef>,
    deleted: bool,
}

impl MaterializedElement {
    #[must_use]
    pub const fn kind(&self) -> MaterializedElementKind {
        self.kind
    }

    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn type_id(&self) -> u32 {
        self.type_id
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
    }

    #[must_use]
    pub const fn source(&self) -> Option<ElementRef> {
        self.source
    }

    #[must_use]
    pub const fn destination(&self) -> Option<ElementRef> {
        self.destination
    }

    #[must_use]
    pub const fn deleted(&self) -> bool {
        self.deleted
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedWrite {
    scope: GraphScope,
    transaction: TemporalTransaction,
}

impl ScopedWrite {
    #[must_use]
    pub const fn scope(&self) -> GraphScope {
        self.scope
    }

    #[must_use]
    pub const fn transaction(&self) -> &TemporalTransaction {
        &self.transaction
    }

    #[must_use]
    pub fn into_parts(self) -> (GraphScope, TemporalTransaction) {
        (self.scope, self.transaction)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedWriteSet {
    bindings: BTreeMap<String, MaterializedElement>,
    scoped_transactions: Vec<ScopedWrite>,
}

impl MaterializedWriteSet {
    #[must_use]
    pub fn binding(&self, variable: &str) -> Option<&MaterializedElement> {
        self.bindings.get(variable)
    }

    #[must_use]
    pub const fn bindings(&self) -> &BTreeMap<String, MaterializedElement> {
        &self.bindings
    }

    #[must_use]
    pub fn scoped_transactions(&self) -> &[ScopedWrite] {
        &self.scoped_transactions
    }

    #[must_use]
    pub fn into_scoped_transactions(self) -> Vec<ScopedWrite> {
        self.scoped_transactions
    }
}

pub fn materialize_write(
    compiled: &CompiledQuery,
    context: &WriteContext,
) -> Result<MaterializedWriteSet, WriteMaterializationError> {
    if compiled.is_read_only() {
        return Err(WriteMaterializationError::new(
            "DTG-CYPHER-READ-ONLY-PLAN",
            "a read-only plan cannot be materialized as a write",
        ));
    }
    let mut materializer = Materializer {
        context,
        bindings: BTreeMap::new(),
        anonymous_sequence: 0,
    };
    for mutation in compiled.mutation_plan().mutations() {
        match mutation {
            CompiledMutation::Create(pattern) => materializer.create(pattern)?,
            CompiledMutation::Merge(_) => {
                return Err(WriteMaterializationError::new(
                    "DTG-CYPHER-MERGE-CONSTRAINT-REQUIRED",
                    "MERGE requires the distributed constraint service; it is never lowered to an unprotected CREATE",
                ));
            }
            CompiledMutation::SetProperty { target, value } => {
                materializer.set_property(target, value)?;
            }
            CompiledMutation::RemoveProperty(target) => materializer.remove_property(target)?,
            CompiledMutation::Delete { variables, detach } => {
                materializer.delete(variables, *detach)?;
            }
        }
    }
    materializer.finish()
}

struct Materializer<'context> {
    context: &'context WriteContext,
    bindings: BTreeMap<String, MaterializedElement>,
    anonymous_sequence: u64,
}

impl Materializer<'_> {
    fn create(&mut self, pattern: &Pattern) -> Result<(), WriteMaterializationError> {
        for path in pattern.paths() {
            let mut source = self.node(path.start())?;
            for chain in path.chains() {
                if chain.relationship().length().is_some() {
                    return Err(WriteMaterializationError::new(
                        "DTG-CYPHER-CREATE-VARIABLE-LENGTH",
                        "CREATE relationships cannot have variable length",
                    ));
                }
                let destination = self.node(chain.node())?;
                let (source_endpoint, destination_endpoint) = match chain.relationship().direction()
                {
                    RelationshipDirection::Outgoing => (source, destination),
                    RelationshipDirection::Incoming => (destination, source),
                    RelationshipDirection::Undirected => {
                        return Err(WriteMaterializationError::new(
                            "DTG-CYPHER-CREATE-UNDIRECTED",
                            "CREATE relationship direction must be explicit",
                        ));
                    }
                };
                let relationship_type = exactly_one_name(
                    chain.relationship().types(),
                    "DTG-CYPHER-CREATE-RELATIONSHIP-TYPE",
                    "CREATE relationship requires exactly one type",
                )?;
                let key = self.binding_key(
                    chain
                        .relationship()
                        .variable()
                        .map(cypher_ast::Identifier::value),
                    "relationship",
                );
                if self.bindings.contains_key(&key) {
                    return Err(WriteMaterializationError::new(
                        "DTG-CYPHER-DUPLICATE-CREATE-VARIABLE",
                        format!("relationship variable {key} is already materialized"),
                    ));
                }
                let id = self.element_id(&key, MaterializedElementKind::Relationship);
                let element = ElementRef::edge(
                    GraphId::new(self.context.graph_id),
                    source_endpoint.partition(),
                    id,
                );
                self.bindings.insert(
                    key,
                    MaterializedElement {
                        kind: MaterializedElementKind::Relationship,
                        element,
                        type_id: schema_id(relationship_type),
                        payload: self.properties(chain.relationship().properties())?,
                        source: Some(source_endpoint),
                        destination: Some(destination_endpoint),
                        deleted: false,
                    },
                );
                source = destination;
            }
        }
        Ok(())
    }

    fn node(&mut self, node: &NodePattern) -> Result<ElementRef, WriteMaterializationError> {
        if let Some(variable) = node.variable()
            && let Some(existing) = self.bindings.get(variable.value())
        {
            if existing.kind != MaterializedElementKind::Vertex || existing.deleted {
                return Err(WriteMaterializationError::new(
                    "DTG-CYPHER-INVALID-CREATE-BINDING",
                    format!("{} is not a live vertex", variable.value()),
                ));
            }
            return Ok(existing.element);
        }
        if node.labels().len() > 1 {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-MULTI-LABEL-STORAGE",
                "the prototype canonical storage record currently accepts one primary label",
            ));
        }
        let key = self.binding_key(node.variable().map(cypher_ast::Identifier::value), "vertex");
        let id = self.element_id(&key, MaterializedElementKind::Vertex);
        let partition = PartitionId::new(self.partition(id));
        let element = ElementRef::vertex(GraphId::new(self.context.graph_id), partition, id);
        self.bindings.insert(
            key,
            MaterializedElement {
                kind: MaterializedElementKind::Vertex,
                element,
                type_id: node
                    .labels()
                    .first()
                    .map_or(0, |label| schema_id(label.value())),
                payload: self.properties(node.properties())?,
                source: None,
                destination: None,
                deleted: false,
            },
        );
        Ok(element)
    }

    fn set_property(
        &mut self,
        target: &PropertyTarget,
        value: &Expression,
    ) -> Result<(), WriteMaterializationError> {
        let property_id = schema_id(target.property());
        let value = self.graph_value(value)?;
        let schema_version = self.context.schema_version;
        let element = self.live_binding_mut(target.variable())?;
        let mut properties = element.payload.properties().clone();
        properties.insert(property_id, value);
        element.payload = CanonicalElement::new(schema_version, properties);
        Ok(())
    }

    fn remove_property(
        &mut self,
        target: &PropertyTarget,
    ) -> Result<(), WriteMaterializationError> {
        let schema_version = self.context.schema_version;
        let element = self.live_binding_mut(target.variable())?;
        let mut properties = element.payload.properties().clone();
        properties.remove(&schema_id(target.property()));
        element.payload = CanonicalElement::new(schema_version, properties);
        Ok(())
    }

    fn delete(
        &mut self,
        variables: &[String],
        detach: bool,
    ) -> Result<(), WriteMaterializationError> {
        for variable in variables {
            let target = self.live_binding(variable)?.clone();
            if target.kind == MaterializedElementKind::Vertex {
                let attached = self
                    .bindings
                    .iter()
                    .filter(|(_, value)| {
                        !value.deleted
                            && value.kind == MaterializedElementKind::Relationship
                            && (value.source == Some(target.element)
                                || value.destination == Some(target.element))
                    })
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>();
                if !detach && !attached.is_empty() {
                    return Err(WriteMaterializationError::new(
                        "DTG-CYPHER-DELETE-CONNECTED-NODE",
                        "DELETE cannot remove a vertex with relationships; use DETACH DELETE",
                    ));
                }
                for relationship in attached {
                    self.bindings
                        .get_mut(&relationship)
                        .expect("attached relationship remains present")
                        .deleted = true;
                }
            }
            self.bindings
                .get_mut(variable)
                .expect("live binding remains present")
                .deleted = true;
        }
        Ok(())
    }

    fn finish(self) -> Result<MaterializedWriteSet, WriteMaterializationError> {
        let mut transactions = BTreeMap::<PartitionId, TemporalTransaction>::new();
        for element in self.bindings.values().filter(|element| !element.deleted) {
            let transaction = transactions.entry(element.element.partition()).or_default();
            match element.kind {
                MaterializedElementKind::Vertex => {
                    let mutation = VertexMutation::put(
                        element.element,
                        LabelId::new(element.type_id),
                        self.context.valid,
                        element.payload.clone(),
                    )
                    .map_err(storage_error)?;
                    *transaction = std::mem::take(transaction).with_vertex(mutation);
                }
                MaterializedElementKind::Relationship => {
                    let mutation = EdgeMutation::put_between(
                        element.element,
                        EdgeTypeId::new(element.type_id),
                        element.source.expect("relationship has source"),
                        element.destination.expect("relationship has destination"),
                        self.context.valid,
                        element.payload.clone(),
                    )
                    .map_err(storage_error)?;
                    *transaction = std::mem::take(transaction).with_edge(mutation);
                }
            }
        }
        if transactions.is_empty() {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-EMPTY-WRITE-SET",
                "the write query produced no persistent mutations",
            ));
        }
        let graph = GraphId::new(self.context.graph_id);
        Ok(MaterializedWriteSet {
            bindings: self.bindings,
            scoped_transactions: transactions
                .into_iter()
                .map(|(partition, transaction)| ScopedWrite {
                    scope: GraphScope::new(graph, partition),
                    transaction,
                })
                .collect(),
        })
    }

    fn properties(
        &self,
        expression: Option<&Expression>,
    ) -> Result<CanonicalElement, WriteMaterializationError> {
        let mut properties = BTreeMap::new();
        if let Some(expression) = expression {
            let Expression::Map(items) = expression else {
                return Err(WriteMaterializationError::new(
                    "DTG-CYPHER-CREATE-PROPERTIES",
                    "CREATE properties must be a map",
                ));
            };
            for (name, value) in items {
                let property_id = schema_id(name.value());
                if properties
                    .insert(property_id, self.graph_value(value)?)
                    .is_some()
                {
                    return Err(WriteMaterializationError::new(
                        "DTG-CYPHER-PROPERTY-HASH-COLLISION",
                        format!("property {} collides in the canonical schema", name.value()),
                    ));
                }
            }
        }
        Ok(CanonicalElement::new(
            self.context.schema_version,
            properties,
        ))
    }

    fn graph_value(
        &self,
        expression: &Expression,
    ) -> Result<GraphValue, WriteMaterializationError> {
        match expression {
            Expression::Null => Ok(GraphValue::Null),
            Expression::Boolean(value) => Ok(GraphValue::Boolean(*value)),
            Expression::Integer(value) => value
                .parse()
                .map(GraphValue::Integer)
                .map_err(|_| WriteMaterializationError::new("DTG-CYPHER-INTEGER-OVERFLOW", value)),
            Expression::Float(value) => value
                .parse::<f64>()
                .map(f64::to_bits)
                .map(GraphValue::FloatBits)
                .map_err(|_| WriteMaterializationError::new("DTG-CYPHER-INVALID-FLOAT", value)),
            Expression::String(value) => Ok(GraphValue::String(value.clone())),
            Expression::Parameter(name) => self
                .context
                .parameters
                .get(name)
                .ok_or_else(|| {
                    WriteMaterializationError::new(
                        "DTG-CYPHER-MISSING-PARAMETER",
                        format!("missing parameter ${name}"),
                    )
                })
                .and_then(runtime_graph_value),
            Expression::List(values) => values
                .iter()
                .map(|value| self.graph_value(value))
                .collect::<Result<Vec<_>, _>>()
                .map(GraphValue::List),
            _ => Err(WriteMaterializationError::new(
                "DTG-CYPHER-WRITE-EXPRESSION-UNSUPPORTED",
                "write property expressions currently support literals, lists, and parameters",
            )),
        }
    }

    fn live_binding(
        &self,
        variable: &str,
    ) -> Result<&MaterializedElement, WriteMaterializationError> {
        self.bindings
            .get(variable)
            .filter(|element| !element.deleted)
            .ok_or_else(|| {
                WriteMaterializationError::new(
                    "DTG-CYPHER-WRITE-BINDING-NOT-IN-OVERLAY",
                    format!("variable {variable} is not present in the transaction overlay"),
                )
            })
    }

    fn live_binding_mut(
        &mut self,
        variable: &str,
    ) -> Result<&mut MaterializedElement, WriteMaterializationError> {
        self.bindings
            .get_mut(variable)
            .filter(|element| !element.deleted)
            .ok_or_else(|| {
                WriteMaterializationError::new(
                    "DTG-CYPHER-WRITE-BINDING-NOT-IN-OVERLAY",
                    format!("variable {variable} is not present in the transaction overlay"),
                )
            })
    }

    fn binding_key(&mut self, name: Option<&str>, prefix: &str) -> String {
        name.map(str::to_owned).unwrap_or_else(|| {
            let sequence = self.anonymous_sequence;
            self.anonymous_sequence = self.anonymous_sequence.saturating_add(1);
            format!("__{prefix}_{sequence}")
        })
    }

    fn element_id(&self, key: &str, kind: MaterializedElementKind) -> ElementId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"DTGProxy/CypherWriteElement/V1");
        hasher.update(&self.context.deterministic_seed);
        hasher.update(&self.context.graph_id.to_be_bytes());
        hasher.update(&[match kind {
            MaterializedElementKind::Vertex => 1,
            MaterializedElementKind::Relationship => 2,
        }]);
        hasher.update(key.as_bytes());
        let digest = hasher.finalize();
        ElementId::new(u128::from_be_bytes(
            digest.as_bytes()[..16]
                .try_into()
                .expect("digest has sixteen bytes"),
        ))
    }

    fn partition(&self, id: ElementId) -> u32 {
        u32::try_from(id.value() % u128::from(self.context.virtual_partitions))
            .expect("partition remainder fits u32")
    }
}

fn exactly_one_name<'a>(
    names: &'a [cypher_ast::Identifier],
    code: &'static str,
    message: &'static str,
) -> Result<&'a str, WriteMaterializationError> {
    if names.len() != 1 {
        return Err(WriteMaterializationError::new(code, message));
    }
    Ok(names[0].value())
}

fn runtime_graph_value(value: &RuntimeValue) -> Result<GraphValue, WriteMaterializationError> {
    match value {
        RuntimeValue::Null => Ok(GraphValue::Null),
        RuntimeValue::Boolean(value) => Ok(GraphValue::Boolean(*value)),
        RuntimeValue::Integer(value) => Ok(GraphValue::Integer(*value)),
        RuntimeValue::FloatBits(value) => Ok(GraphValue::FloatBits(*value)),
        RuntimeValue::String(value) => Ok(GraphValue::String(value.clone())),
        RuntimeValue::Bytes(value) => Ok(GraphValue::Bytes(value.clone())),
        RuntimeValue::TimestampMicros(value) => Ok(GraphValue::TimestampMicros(*value)),
        RuntimeValue::List(values) => values
            .iter()
            .map(runtime_graph_value)
            .collect::<Result<Vec<_>, _>>()
            .map(GraphValue::List),
        RuntimeValue::Map(_) | RuntimeValue::Node(_) | RuntimeValue::Relationship(_) => {
            Err(WriteMaterializationError::new(
                "DTG-CYPHER-INVALID-PROPERTY-VALUE",
                "maps and graph entities cannot be stored as a canonical scalar property",
            ))
        }
    }
}

#[must_use]
pub fn schema_id(value: &str) -> u32 {
    let digest = blake3::hash(value.as_bytes());
    u32::from_be_bytes(
        digest.as_bytes()[..4]
            .try_into()
            .expect("digest has four bytes"),
    )
}

fn storage_error(error: temporal_storage::TemporalStoreError) -> WriteMaterializationError {
    WriteMaterializationError::new("DTG-CYPHER-VERSION-REWRITE", error.to_string())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteMaterializationError {
    code: &'static str,
    message: String,
}

impl WriteMaterializationError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl Display for WriteMaterializationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for WriteMaterializationError {}
