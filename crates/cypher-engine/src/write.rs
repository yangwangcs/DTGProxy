use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use cypher_ast::{Expression, NodePattern, Pattern, RelationshipDirection};
use cypher_compiler::{CompiledMutation, CompiledQuery, MutationPlan, PropertyTarget};
use query_executor::RuntimeValue;
use query_executor::{EdgeRecord, VertexRecord};
use temporal_ir::GraphScope;
use temporal_storage::{
    EdgeMutation, EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, LabelId, PartitionId,
    TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, ValidTime};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteContext {
    graph_id: u64,
    schema_version: u64,
    virtual_partitions: u32,
    deterministic_seed: [u8; 32],
    nested_seed: [u8; 32],
    valid: Interval<ValidTime>,
    parameters: BTreeMap<String, RuntimeValue>,
    existing_bindings: BTreeMap<String, RuntimeValue>,
    resolved_merge_keys: BTreeSet<[u8; 32]>,
    has_input_row: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteSubqueryInput {
    clause_start: usize,
    rows: Vec<WriteSubqueryRow>,
}

impl WriteSubqueryInput {
    #[must_use]
    pub fn new(clause_start: usize, rows: Vec<WriteSubqueryRow>) -> Self {
        Self { clause_start, rows }
    }

    #[must_use]
    pub const fn clause_start(&self) -> usize {
        self.clause_start
    }

    #[must_use]
    pub fn rows(&self) -> &[WriteSubqueryRow] {
        &self.rows
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteSubqueryRow {
    bindings: BTreeMap<String, RuntimeValue>,
    nested: Vec<WriteSubqueryInput>,
}

impl WriteSubqueryRow {
    #[must_use]
    pub fn new(bindings: BTreeMap<String, RuntimeValue>, nested: Vec<WriteSubqueryInput>) -> Self {
        Self { bindings, nested }
    }

    #[must_use]
    pub const fn bindings(&self) -> &BTreeMap<String, RuntimeValue> {
        &self.bindings
    }

    #[must_use]
    pub fn nested(&self) -> &[WriteSubqueryInput] {
        &self.nested
    }
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
            nested_seed: deterministic_seed,
            valid,
            parameters,
            existing_bindings: BTreeMap::new(),
            resolved_merge_keys: BTreeSet::new(),
            has_input_row: true,
        })
    }

    pub fn with_existing_bindings(
        mut self,
        bindings: BTreeMap<String, RuntimeValue>,
    ) -> Result<Self, WriteMaterializationError> {
        if bindings.keys().any(|name| name.is_empty()) {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-INVALID-EXISTING-BINDING",
                "existing write bindings must have non-empty variable names",
            ));
        }
        self.existing_bindings = bindings;
        Ok(self)
    }

    #[must_use]
    pub const fn with_nested_seed(mut self, seed: [u8; 32]) -> Self {
        self.nested_seed = seed;
        self
    }

    #[must_use]
    pub fn with_resolved_merge_keys(mut self, keys: BTreeSet<[u8; 32]>) -> Self {
        self.resolved_merge_keys = keys;
        self
    }

    #[must_use]
    pub const fn without_input_row(mut self) -> Self {
        self.has_input_row = false;
        self
    }

    fn take_existing_bindings(&self) -> BTreeMap<String, RuntimeValue> {
        self.existing_bindings.clone()
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

    #[must_use]
    pub fn runtime_value(&self) -> RuntimeValue {
        match self.kind {
            MaterializedElementKind::Vertex => RuntimeValue::Node(VertexRecord::new(
                self.element,
                (self.type_id != 0).then_some(LabelId::new(self.type_id)),
                self.payload.clone(),
            )),
            MaterializedElementKind::Relationship => {
                RuntimeValue::Relationship(EdgeRecord::from_endpoints(
                    self.element,
                    EdgeTypeId::new(self.type_id),
                    self.source.expect("relationship has source"),
                    self.destination.expect("relationship has destination"),
                    self.payload.clone(),
                ))
            }
        }
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
pub struct MergeConstraint {
    key: [u8; 32],
    owner: ElementRef,
    binding_names: BTreeSet<String>,
}

impl MergeConstraint {
    #[must_use]
    pub const fn key(&self) -> [u8; 32] {
        self.key
    }

    #[must_use]
    pub const fn owner(&self) -> ElementRef {
        self.owner
    }

    #[must_use]
    pub const fn binding_names(&self) -> &BTreeSet<String> {
        &self.binding_names
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedWriteSet {
    bindings: BTreeMap<String, MaterializedElement>,
    scope_values: BTreeMap<String, RuntimeValue>,
    overlay_elements: Vec<MaterializedElement>,
    scoped_transactions: Vec<ScopedWrite>,
    merge_constraints: Vec<MergeConstraint>,
}

impl MaterializedWriteSet {
    fn empty() -> Self {
        Self {
            bindings: BTreeMap::new(),
            scope_values: BTreeMap::new(),
            overlay_elements: Vec::new(),
            scoped_transactions: Vec::new(),
            merge_constraints: Vec::new(),
        }
    }

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
    pub fn merge_constraints(&self) -> &[MergeConstraint] {
        &self.merge_constraints
    }

    #[must_use]
    pub fn overlay_elements(&self) -> &[MaterializedElement] {
        &self.overlay_elements
    }

    #[must_use]
    pub fn into_scoped_transactions(self) -> Vec<ScopedWrite> {
        self.scoped_transactions
    }

    #[must_use]
    pub fn overlay_values(&self) -> Vec<RuntimeValue> {
        self.overlay_elements
            .iter()
            .filter(|element| !element.deleted)
            .map(MaterializedElement::runtime_value)
            .collect()
    }
}

pub fn materialize_write(
    compiled: &CompiledQuery,
    context: &WriteContext,
) -> Result<MaterializedWriteSet, WriteMaterializationError> {
    materialize_write_with_subquery_inputs(compiled, context, &[])
}

pub fn materialize_write_with_subquery_inputs(
    compiled: &CompiledQuery,
    context: &WriteContext,
    subquery_inputs: &[WriteSubqueryInput],
) -> Result<MaterializedWriteSet, WriteMaterializationError> {
    if compiled.is_read_only() {
        return Err(WriteMaterializationError::new(
            "DTG-CYPHER-READ-ONLY-PLAN",
            "a read-only plan cannot be materialized as a write",
        ));
    }
    if !context.has_input_row {
        return Ok(MaterializedWriteSet::empty());
    }
    let mut materializer = Materializer {
        context,
        bindings: BTreeMap::new(),
        scope_values: BTreeMap::new(),
        touched: BTreeSet::new(),
        anonymous_sequence: 0,
        merge_key: None,
        merge_element_position: 0,
        merge_constraints: Vec::new(),
        nested_overlay_elements: Vec::new(),
        nested_scoped_transactions: Vec::new(),
        subquery_inputs,
    };
    for (name, value) in context.take_existing_bindings() {
        materializer.seed(name, value)?;
    }
    finish_materializers(materializer.apply_mutation_plan(compiled.mutation_plan())?)
}

pub fn probe_merge_constraints(
    compiled: &CompiledQuery,
    context: &WriteContext,
) -> Result<MaterializedWriteSet, WriteMaterializationError> {
    probe_merge_constraints_with_subquery_inputs(compiled, context, &[])
}

pub fn probe_merge_constraints_with_subquery_inputs(
    compiled: &CompiledQuery,
    context: &WriteContext,
    subquery_inputs: &[WriteSubqueryInput],
) -> Result<MaterializedWriteSet, WriteMaterializationError> {
    if !context.has_input_row {
        return Ok(MaterializedWriteSet::empty());
    }
    let mut materializer = Materializer {
        context,
        bindings: BTreeMap::new(),
        scope_values: BTreeMap::new(),
        touched: BTreeSet::new(),
        anonymous_sequence: 0,
        merge_key: None,
        merge_element_position: 0,
        merge_constraints: Vec::new(),
        nested_overlay_elements: Vec::new(),
        nested_scoped_transactions: Vec::new(),
        subquery_inputs,
    };
    for (name, value) in context.take_existing_bindings() {
        materializer.seed(name, value)?;
    }
    for mutation in compiled.mutation_plan().mutations() {
        match mutation {
            CompiledMutation::Merge(pattern) => materializer.merge(pattern)?,
            CompiledMutation::Subquery(subquery) => {
                materializer.probe_subquery(subquery)?;
            }
            _ => {}
        }
    }
    materializer.finish()
}

#[derive(Clone)]
struct Materializer<'input> {
    context: &'input WriteContext,
    bindings: BTreeMap<String, MaterializedElement>,
    scope_values: BTreeMap<String, RuntimeValue>,
    touched: BTreeSet<String>,
    anonymous_sequence: u64,
    merge_key: Option<[u8; 32]>,
    merge_element_position: u64,
    merge_constraints: Vec<MergeConstraint>,
    nested_overlay_elements: Vec<MaterializedElement>,
    nested_scoped_transactions: Vec<ScopedWrite>,
    subquery_inputs: &'input [WriteSubqueryInput],
}

impl Materializer<'_> {
    fn apply_mutation_plan(
        self,
        plan: &MutationPlan,
    ) -> Result<Vec<Self>, WriteMaterializationError> {
        let mut rows = vec![self];
        for mutation in plan.mutations() {
            let mut next = Vec::new();
            for row in rows {
                next.extend(row.apply_mutation(mutation)?);
            }
            rows = next;
        }
        Ok(rows)
    }

    fn apply_mutation(
        mut self,
        mutation: &CompiledMutation,
    ) -> Result<Vec<Self>, WriteMaterializationError> {
        match mutation {
            CompiledMutation::Create(pattern) => self.create(pattern)?,
            CompiledMutation::Merge(pattern) => self.merge(pattern)?,
            CompiledMutation::SetProperty { target, value } => {
                self.set_property(target, value)?;
            }
            CompiledMutation::RemoveProperty(target) => self.remove_property(target)?,
            CompiledMutation::Delete { variables, detach } => {
                self.delete(variables, *detach)?;
            }
            CompiledMutation::Subquery(subquery) => return self.subquery(subquery),
        }
        Ok(vec![self])
    }

    fn subquery(
        self,
        subquery: &cypher_compiler::CompiledSubqueryMutation,
    ) -> Result<Vec<Self>, WriteMaterializationError> {
        let imports = self.subquery_imports(subquery.imports())?;
        let rows = self.subquery_rows(subquery)?;
        let exports_rows = !subquery.export_expressions().is_empty();
        let mut parent = self;
        let mut expanded = Vec::new();
        for (row_index, row) in rows.iter().enumerate() {
            let existing_bindings = merge_child_bindings(&imports, row.bindings())?;
            let mut child_context = parent.context.clone();
            let child_seed = child_write_seed(
                parent.context.nested_seed,
                subquery.clause_start(),
                row_index,
            );
            child_context.deterministic_seed = child_seed;
            child_context.nested_seed = child_seed;
            child_context.existing_bindings = existing_bindings;
            child_context.has_input_row = true;
            let mut child = Materializer {
                context: &child_context,
                bindings: BTreeMap::new(),
                scope_values: BTreeMap::new(),
                touched: BTreeSet::new(),
                anonymous_sequence: 0,
                merge_key: None,
                merge_element_position: 0,
                merge_constraints: Vec::new(),
                nested_overlay_elements: Vec::new(),
                nested_scoped_transactions: Vec::new(),
                subquery_inputs: row.nested(),
            };
            for (name, value) in child_context.take_existing_bindings() {
                child.seed(name, value)?;
            }
            let child_rows = child.apply_mutation_plan(subquery.mutation_plan())?;
            for child in child_rows {
                let exports = subquery
                    .export_expressions()
                    .iter()
                    .map(|export| {
                        Ok((
                            export.name().to_owned(),
                            child.export_value(export.expression())?,
                        ))
                    })
                    .collect::<Result<Vec<_>, WriteMaterializationError>>()?;
                let child = child.finish()?;
                if exports_rows {
                    let mut output = parent.clone();
                    for (name, value) in exports {
                        output.seed(name, value)?;
                    }
                    output.absorb_child(child);
                    expanded.push(output);
                } else {
                    parent.absorb_child(child);
                }
            }
        }
        if exports_rows {
            Ok(expanded)
        } else {
            Ok(vec![parent])
        }
    }

    fn absorb_child(&mut self, child: MaterializedWriteSet) {
        self.nested_overlay_elements.extend(child.overlay_elements);
        self.nested_scoped_transactions
            .extend(child.scoped_transactions);
        self.merge_constraints.extend(child.merge_constraints);
    }

    fn probe_subquery(
        &mut self,
        subquery: &cypher_compiler::CompiledSubqueryMutation,
    ) -> Result<(), WriteMaterializationError> {
        let imports = self.subquery_imports(subquery.imports())?;
        let mut child_context = self.context.clone();
        let child_seed = child_write_seed(self.context.nested_seed, subquery.clause_start(), 0);
        child_context.deterministic_seed = child_seed;
        child_context.nested_seed = child_seed;
        child_context.existing_bindings = imports;
        let mut child = Materializer {
            context: &child_context,
            bindings: BTreeMap::new(),
            scope_values: BTreeMap::new(),
            touched: BTreeSet::new(),
            anonymous_sequence: 0,
            merge_key: None,
            merge_element_position: 0,
            merge_constraints: Vec::new(),
            nested_overlay_elements: Vec::new(),
            nested_scoped_transactions: Vec::new(),
            subquery_inputs: &[],
        };
        for (name, value) in child_context.take_existing_bindings() {
            child.seed(name, value)?;
        }
        for mutation in subquery.mutation_plan().mutations() {
            match mutation {
                CompiledMutation::Merge(pattern) => child.merge(pattern)?,
                CompiledMutation::Subquery(nested) => child.probe_subquery(nested)?,
                _ => {}
            }
        }
        let child = child.finish()?;
        self.merge_constraints.extend(child.merge_constraints);
        Ok(())
    }

    fn subquery_imports(
        &self,
        imports: &[String],
    ) -> Result<BTreeMap<String, RuntimeValue>, WriteMaterializationError> {
        imports
            .iter()
            .map(|name| {
                self.scope_values
                    .get(name)
                    .cloned()
                    .map(|value| (name.clone(), value))
                    .ok_or_else(|| {
                        WriteMaterializationError::new(
                            "DTG-CYPHER-SUBQUERY-IMPORT-MISSING",
                            format!("write subquery import {name} has no outer-row value"),
                        )
                    })
            })
            .collect()
    }

    fn subquery_rows(
        &self,
        subquery: &cypher_compiler::CompiledSubqueryMutation,
    ) -> Result<Vec<WriteSubqueryRow>, WriteMaterializationError> {
        let mut matching = self
            .subquery_inputs
            .iter()
            .filter(|input| input.clause_start() == subquery.clause_start());
        if let Some(input) = matching.next() {
            if matching.next().is_some() {
                return Err(WriteMaterializationError::new(
                    "DTG-CYPHER-DUPLICATE-SUBQUERY-INPUT",
                    "one structured write subquery received multiple input descriptors",
                ));
            }
            return Ok(input.rows().to_vec());
        }
        if subquery.read_prefix_plan().is_some() {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-WRITE-SUBQUERY-INPUT-MISSING",
                "a row-producing write subquery prefix was not executed",
            ));
        }
        Ok(vec![WriteSubqueryRow::new(BTreeMap::new(), Vec::new())])
    }

    fn merge(&mut self, pattern: &Pattern) -> Result<(), WriteMaterializationError> {
        let merge_key = self.merge_constraint_key(pattern)?;
        if self.context.resolved_merge_keys.contains(&merge_key) {
            return self.validate_resolved_pattern(pattern);
        }
        let all_bound = pattern.paths().iter().all(|path| {
            path.start()
                .variable()
                .is_some_and(|variable| self.bindings.contains_key(variable.value()))
                && path.chains().iter().all(|chain| {
                    chain
                        .relationship()
                        .variable()
                        .is_some_and(|variable| self.bindings.contains_key(variable.value()))
                        && chain
                            .node()
                            .variable()
                            .is_some_and(|variable| self.bindings.contains_key(variable.value()))
                })
        });
        if all_bound {
            return Ok(());
        }
        let existing = self.bindings.keys().cloned().collect::<BTreeSet<_>>();
        self.merge_key = Some(merge_key);
        self.merge_element_position = 0;
        let result = self.create(pattern);
        self.merge_key = None;
        self.merge_element_position = 0;
        result?;
        let binding_names = self
            .bindings
            .keys()
            .filter(|name| !existing.contains(*name))
            .cloned()
            .collect::<BTreeSet<_>>();
        let owner = binding_names
            .iter()
            .filter_map(|name| self.bindings.get(name))
            .filter(|element| !element.deleted())
            .map(MaterializedElement::element)
            .min()
            .ok_or_else(|| {
                WriteMaterializationError::new(
                    "DTG-CYPHER-MERGE-OWNER-MISSING",
                    "MERGE created no canonical element to own its constraint claim",
                )
            })?;
        self.merge_constraints.push(MergeConstraint {
            key: merge_key,
            owner,
            binding_names,
        });
        Ok(())
    }

    fn validate_resolved_pattern(
        &mut self,
        pattern: &Pattern,
    ) -> Result<(), WriteMaterializationError> {
        for path in pattern.paths() {
            let mut source = self.resolved_node(path.start())?;
            for chain in path.chains() {
                let destination = self.resolved_node(chain.node())?;
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
                let key = self.binding_key(
                    chain
                        .relationship()
                        .variable()
                        .map(cypher_ast::Identifier::value),
                    "relationship",
                );
                let relationship = self.resolved_binding(&key)?;
                if relationship.kind != MaterializedElementKind::Relationship
                    || relationship.source != Some(source_endpoint)
                    || relationship.destination != Some(destination_endpoint)
                {
                    return Err(WriteMaterializationError::new(
                        "DTG-CYPHER-RESOLVED-MERGE-BINDING-CONFLICT",
                        format!(
                            "resolved MERGE relationship binding {key} conflicts with its pattern"
                        ),
                    ));
                }
                source = destination;
            }
        }
        Ok(())
    }

    fn resolved_node(
        &mut self,
        node: &NodePattern,
    ) -> Result<ElementRef, WriteMaterializationError> {
        let key = self.binding_key(node.variable().map(cypher_ast::Identifier::value), "vertex");
        let element = self.resolved_binding(&key)?;
        if element.kind != MaterializedElementKind::Vertex {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-RESOLVED-MERGE-BINDING-CONFLICT",
                format!("resolved MERGE node binding {key} is not a vertex"),
            ));
        }
        Ok(element.element)
    }

    fn resolved_binding(
        &self,
        key: &str,
    ) -> Result<&MaterializedElement, WriteMaterializationError> {
        self.bindings
            .get(key)
            .filter(|element| !element.deleted)
            .ok_or_else(|| {
                WriteMaterializationError::new(
                    "DTG-CYPHER-RESOLVED-MERGE-BINDING-MISSING",
                    format!("resolved MERGE binding {key} is missing"),
                )
            })
    }

    fn merge_constraint_key(
        &self,
        pattern: &Pattern,
    ) -> Result<[u8; 32], WriteMaterializationError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"DTGProxy/MergeConstraint/Latest");
        hasher.update(&self.context.graph_id.to_be_bytes());
        hasher.update(&self.context.schema_version.to_be_bytes());
        hash_count(&mut hasher, pattern.paths().len());
        for path in pattern.paths() {
            hasher.update(&[1]);
            self.hash_merge_node(&mut hasher, path.start())?;
            for chain in path.chains() {
                hasher.update(&[2]);
                hasher.update(&[match chain.relationship().direction() {
                    RelationshipDirection::Outgoing => 1,
                    RelationshipDirection::Incoming => 2,
                    RelationshipDirection::Undirected => 3,
                }]);
                let mut types = chain
                    .relationship()
                    .types()
                    .iter()
                    .map(|name| schema_id(name.value()))
                    .collect::<Vec<_>>();
                types.sort_unstable();
                hash_count(&mut hasher, types.len());
                for type_id in types {
                    hasher.update(&type_id.to_be_bytes());
                }
                let properties = self.properties(chain.relationship().properties())?;
                let encoded = properties.encode().map_err(|error| {
                    WriteMaterializationError::new(
                        "DTG-CYPHER-MERGE-CONSTRAINT-ENCODE",
                        error.to_string(),
                    )
                })?;
                hash_bytes(&mut hasher, &encoded);
                self.hash_merge_node(&mut hasher, chain.node())?;
            }
        }
        Ok(*hasher.finalize().as_bytes())
    }

    fn hash_merge_node(
        &self,
        hasher: &mut blake3::Hasher,
        node: &NodePattern,
    ) -> Result<(), WriteMaterializationError> {
        if let Some(variable) = node.variable()
            && let Some(binding) = self.bindings.get(variable.value())
        {
            if binding.kind != MaterializedElementKind::Vertex || binding.deleted {
                return Err(WriteMaterializationError::new(
                    "DTG-CYPHER-INVALID-CREATE-BINDING",
                    format!("{} is not a live vertex", variable.value()),
                ));
            }
            hasher.update(&[1]);
            hash_element_ref(hasher, binding.element);
        } else {
            hasher.update(&[0]);
        }
        let mut labels = node
            .labels()
            .iter()
            .map(|label| schema_id(label.value()))
            .collect::<Vec<_>>();
        labels.sort_unstable();
        hash_count(hasher, labels.len());
        for label in labels {
            hasher.update(&label.to_be_bytes());
        }
        let properties = self.properties(node.properties())?;
        let encoded = properties.encode().map_err(|error| {
            WriteMaterializationError::new("DTG-CYPHER-MERGE-CONSTRAINT-ENCODE", error.to_string())
        })?;
        hash_bytes(hasher, &encoded);
        Ok(())
    }

    fn seed(&mut self, name: String, value: RuntimeValue) -> Result<(), WriteMaterializationError> {
        if self
            .scope_values
            .insert(name.clone(), value.clone())
            .is_some()
        {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-DUPLICATE-EXISTING-BINDING",
                format!("variable {name} is bound more than once"),
            ));
        }
        let element = match value {
            RuntimeValue::Node(node) => Some(MaterializedElement {
                kind: MaterializedElementKind::Vertex,
                element: node.element(),
                type_id: node.label().map_or(0, |label| label.value()),
                payload: node.payload().clone(),
                source: None,
                destination: None,
                deleted: false,
            }),
            RuntimeValue::Relationship(edge) => Some(MaterializedElement {
                kind: MaterializedElementKind::Relationship,
                element: edge.element(),
                type_id: edge.edge_type().value(),
                payload: edge.payload().clone(),
                source: Some(edge.source_ref()),
                destination: Some(edge.destination_ref()),
                deleted: false,
            }),
            _ => None,
        };
        if let Some(element) = element {
            self.bindings.insert(name, element);
        }
        Ok(())
    }

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
                let structural_position = self.next_merge_element_position();
                if self.bindings.contains_key(&key) {
                    return Err(WriteMaterializationError::new(
                        "DTG-CYPHER-DUPLICATE-CREATE-VARIABLE",
                        format!("relationship variable {key} is already materialized"),
                    ));
                }
                let id = self.element_id(
                    &key,
                    MaterializedElementKind::Relationship,
                    structural_position,
                );
                let element = ElementRef::edge(
                    GraphId::new(self.context.graph_id),
                    source_endpoint.partition(),
                    id,
                );
                self.bindings.insert(
                    key.clone(),
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
                self.refresh_scope_value(&key);
                self.touched.insert(key);
                source = destination;
            }
        }
        Ok(())
    }

    fn node(&mut self, node: &NodePattern) -> Result<ElementRef, WriteMaterializationError> {
        let structural_position = self.next_merge_element_position();
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
        let id = self.element_id(&key, MaterializedElementKind::Vertex, structural_position);
        let partition = PartitionId::new(self.partition(id));
        let element = ElementRef::vertex(GraphId::new(self.context.graph_id), partition, id);
        self.bindings.insert(
            key.clone(),
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
        self.refresh_scope_value(&key);
        self.touched.insert(key.clone());
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
        self.touched.insert(target.variable().to_owned());
        self.refresh_scope_value(target.variable());
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
        self.touched.insert(target.variable().to_owned());
        self.refresh_scope_value(target.variable());
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
                    self.touched.insert(relationship);
                }
            }
            self.bindings
                .get_mut(variable)
                .expect("live binding remains present")
                .deleted = true;
            self.touched.insert(variable.clone());
            self.scope_values
                .insert(variable.clone(), RuntimeValue::Null);
        }
        Ok(())
    }

    fn refresh_scope_value(&mut self, name: &str) {
        if let Some(element) = self.bindings.get(name) {
            self.scope_values
                .insert(name.to_owned(), element.runtime_value());
        }
    }

    fn finish(mut self) -> Result<MaterializedWriteSet, WriteMaterializationError> {
        self.merge_constraints.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.owner.cmp(&right.owner))
                .then_with(|| left.binding_names.cmp(&right.binding_names))
        });
        if self
            .merge_constraints
            .windows(2)
            .any(|pair| pair[0].key == pair[1].key && pair[0].owner != pair[1].owner)
        {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-MERGE-CONSTRAINT-OWNER-CONFLICT",
                "one statement assigns different owners to the same MERGE constraint",
            ));
        }
        self.merge_constraints.dedup();
        let mut transactions = BTreeMap::<PartitionId, TemporalTransaction>::new();
        let mut persisted = BTreeMap::<ElementRef, &MaterializedElement>::new();
        for (_, element) in self
            .bindings
            .iter()
            .filter(|(name, _)| self.touched.contains(*name))
        {
            if let Some(existing) = persisted.insert(element.element, element) {
                if existing != element {
                    return Err(WriteMaterializationError::new(
                        "DTG-CYPHER-MERGE-ALIAS-CONFLICT",
                        "aliases of one MERGE element contain conflicting materialized values",
                    ));
                }
                continue;
            }
            let transaction = transactions.entry(element.element.partition()).or_default();
            match element.kind {
                MaterializedElementKind::Vertex => {
                    let mutation = if element.deleted {
                        VertexMutation::delete(
                            element.element,
                            LabelId::new(element.type_id),
                            self.context.valid,
                        )
                    } else {
                        VertexMutation::put(
                            element.element,
                            LabelId::new(element.type_id),
                            self.context.valid,
                            element.payload.clone(),
                        )
                    }
                    .map_err(storage_error)?;
                    *transaction = std::mem::take(transaction).with_vertex(mutation);
                }
                MaterializedElementKind::Relationship => {
                    let mutation = if element.deleted {
                        EdgeMutation::delete_between(
                            element.element,
                            EdgeTypeId::new(element.type_id),
                            element.source.expect("relationship has source"),
                            element.destination.expect("relationship has destination"),
                            self.context.valid,
                        )
                    } else {
                        EdgeMutation::put_between(
                            element.element,
                            EdgeTypeId::new(element.type_id),
                            element.source.expect("relationship has source"),
                            element.destination.expect("relationship has destination"),
                            self.context.valid,
                            element.payload.clone(),
                        )
                    }
                    .map_err(storage_error)?;
                    *transaction = std::mem::take(transaction).with_edge(mutation);
                }
            }
        }
        if transactions.is_empty()
            && self.nested_scoped_transactions.is_empty()
            && self.touched.is_empty()
        {
            return Ok(MaterializedWriteSet {
                bindings: self.bindings,
                scope_values: self.scope_values,
                overlay_elements: Vec::new(),
                scoped_transactions: Vec::new(),
                merge_constraints: self.merge_constraints,
            });
        }
        if transactions.is_empty() && self.nested_scoped_transactions.is_empty() {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-EMPTY-WRITE-SET",
                "the write query produced no persistent mutations",
            ));
        }
        let graph = GraphId::new(self.context.graph_id);
        let mut overlay_elements = self.nested_overlay_elements;
        overlay_elements.extend(persisted.into_values().cloned());
        let mut scoped_transactions = self.nested_scoped_transactions;
        scoped_transactions.extend(transactions.into_iter().map(|(partition, transaction)| {
            ScopedWrite {
                scope: GraphScope::new(graph, partition),
                transaction,
            }
        }));
        Ok(MaterializedWriteSet {
            bindings: self.bindings,
            scope_values: self.scope_values,
            overlay_elements,
            scoped_transactions,
            merge_constraints: self.merge_constraints,
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
            Expression::Identifier(name) => self
                .scope_values
                .get(name.value())
                .ok_or_else(|| {
                    WriteMaterializationError::new(
                        "DTG-CYPHER-WRITE-BINDING-MISSING",
                        format!("write expression binding {} is unavailable", name.value()),
                    )
                })
                .and_then(runtime_graph_value),
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

    fn export_value(
        &self,
        expression: &Expression,
    ) -> Result<RuntimeValue, WriteMaterializationError> {
        match expression {
            Expression::Identifier(name) => {
                self.scope_values.get(name.value()).cloned().ok_or_else(|| {
                    WriteMaterializationError::new(
                        "DTG-CYPHER-SUBQUERY-EXPORT-MISSING",
                        format!(
                            "write subquery export expression {} has no materialized value",
                            name.value()
                        ),
                    )
                })
            }
            Expression::Parameter(name) => {
                self.context.parameters.get(name).cloned().ok_or_else(|| {
                    WriteMaterializationError::new(
                        "DTG-CYPHER-MISSING-PARAMETER",
                        format!("missing parameter ${name}"),
                    )
                })
            }
            Expression::Property { value, property } => {
                let Expression::Identifier(name) = value.as_ref() else {
                    return Err(WriteMaterializationError::new(
                        "DTG-CYPHER-SUBQUERY-EXPORT-EXPRESSION",
                        "write subquery property exports require a bound graph variable",
                    ));
                };
                let element = self.live_binding(name.value())?;
                Ok(element
                    .payload()
                    .property(schema_id(property.value()))
                    .cloned()
                    .map(RuntimeValue::from)
                    .unwrap_or(RuntimeValue::Null))
            }
            _ => self.graph_value(expression).map(RuntimeValue::from),
        }
    }

    fn live_binding(
        &self,
        variable: &str,
    ) -> Result<&MaterializedElement, WriteMaterializationError> {
        if !self.scope_values.contains_key(variable) {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-WRITE-BINDING-NOT-IN-SCOPE",
                format!("variable {variable} is outside the current write scope"),
            ));
        }
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
        if !self.scope_values.contains_key(variable) {
            return Err(WriteMaterializationError::new(
                "DTG-CYPHER-WRITE-BINDING-NOT-IN-SCOPE",
                format!("variable {variable} is outside the current write scope"),
            ));
        }
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

    fn next_merge_element_position(&mut self) -> Option<u64> {
        self.merge_key.map(|_| {
            let position = self.merge_element_position;
            self.merge_element_position = self.merge_element_position.saturating_add(1);
            position
        })
    }

    fn element_id(
        &self,
        key: &str,
        kind: MaterializedElementKind,
        structural_position: Option<u64>,
    ) -> ElementId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"DTGProxy/CypherWriteElement/V1");
        if let Some(merge_key) = self.merge_key {
            hasher.update(&merge_key);
            hasher.update(
                &structural_position
                    .expect("MERGE elements have a structural position")
                    .to_be_bytes(),
            );
        } else {
            hasher.update(&self.context.deterministic_seed);
            hasher.update(key.as_bytes());
        }
        hasher.update(&self.context.graph_id.to_be_bytes());
        hasher.update(&[match kind {
            MaterializedElementKind::Vertex => 1,
            MaterializedElementKind::Relationship => 2,
        }]);
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

fn finish_materializers(
    rows: Vec<Materializer<'_>>,
) -> Result<MaterializedWriteSet, WriteMaterializationError> {
    if rows.is_empty() {
        return Ok(MaterializedWriteSet::empty());
    }
    let mut bindings = BTreeMap::new();
    let mut scope_values = BTreeMap::new();
    let mut overlay_elements = BTreeMap::new();
    let mut scoped_transactions = BTreeMap::<(u64, u32), (GraphScope, TemporalTransaction)>::new();
    let mut merge_constraints = Vec::new();
    for row in rows {
        let row = row.finish()?;
        bindings.extend(row.bindings);
        scope_values.extend(row.scope_values);
        for element in row.overlay_elements {
            overlay_elements.insert(element.element(), element);
        }
        for scoped in row.scoped_transactions {
            let (scope, transaction) = scoped.into_parts();
            let key = (scope.graph().value(), scope.partition().value());
            match scoped_transactions.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((scope, transaction));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry
                        .get_mut()
                        .1
                        .merge_overlay(transaction)
                        .map_err(storage_error)?;
                }
            }
        }
        merge_constraints.extend(row.merge_constraints);
    }
    merge_constraints.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| left.owner.cmp(&right.owner))
            .then_with(|| left.binding_names.cmp(&right.binding_names))
    });
    if merge_constraints
        .windows(2)
        .any(|pair| pair[0].key == pair[1].key && pair[0].owner != pair[1].owner)
    {
        return Err(WriteMaterializationError::new(
            "DTG-CYPHER-MERGE-CONSTRAINT-OWNER-CONFLICT",
            "one statement assigns different owners to the same MERGE constraint",
        ));
    }
    merge_constraints.dedup();
    Ok(MaterializedWriteSet {
        bindings,
        scope_values,
        overlay_elements: overlay_elements.into_values().collect(),
        scoped_transactions: scoped_transactions
            .into_values()
            .map(|(scope, transaction)| ScopedWrite { scope, transaction })
            .collect(),
        merge_constraints,
    })
}

fn merge_child_bindings(
    imports: &BTreeMap<String, RuntimeValue>,
    row: &BTreeMap<String, RuntimeValue>,
) -> Result<BTreeMap<String, RuntimeValue>, WriteMaterializationError> {
    let mut bindings = imports.clone();
    for (name, value) in row {
        if let Some(imported) = bindings.get(name) {
            if imported != value {
                return Err(WriteMaterializationError::new(
                    "DTG-CYPHER-SUBQUERY-IMPORT-MISMATCH",
                    format!("write subquery row changed imported binding {name}"),
                ));
            }
            continue;
        }
        bindings.insert(name.clone(), value.clone());
    }
    Ok(bindings)
}

fn child_write_seed(parent: [u8; 32], clause_start: usize, row_index: usize) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/CypherWriteSubquery/Latest");
    hasher.update(&parent);
    hasher.update(&clause_start.to_be_bytes());
    hasher.update(&row_index.to_be_bytes());
    *hasher.finalize().as_bytes()
}

fn hash_count(hasher: &mut blake3::Hasher, count: usize) {
    hasher.update(
        &u64::try_from(count)
            .expect("collection length fits u64")
            .to_be_bytes(),
    );
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hash_count(hasher, bytes.len());
    hasher.update(bytes);
}

fn hash_element_ref(hasher: &mut blake3::Hasher, element: ElementRef) {
    hasher.update(&element.graph().value().to_be_bytes());
    hasher.update(&element.partition().value().to_be_bytes());
    hasher.update(&[match element.kind() {
        ElementKind::Vertex => 1,
        ElementKind::Edge => 2,
    }]);
    hasher.update(&element.id().value().to_be_bytes());
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
