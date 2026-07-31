use std::collections::{BTreeMap, BTreeSet};

use dtg_language_ir::{
    AggregateKind, BinaryOperator, Field, JoinKind, LogicalExpr, LogicalType, RowSchema,
};
use dtg_storage::{PushdownOperation, ShardId};

use crate::{
    AggregateOperator, BatchOperator, BuiltInSpillPolicy, CancellationToken, ColumnBatch,
    ExecutableAccess, ExecutableFragment, ExecutableOperatorKind, ExecutablePlan, Expression,
    FilterOperator, HashJoinOperator, LimitOperator, Operator, OverlayOperator, ProjectOperator,
    ProjectionExpr, QueryBudget, QueryContext, QueryError, QueryFuture, QueryOverlay, QueryStorage,
    ReadOperation, SortOperator, SpillConfig, SpillMergeOperator, StorageSourceOperator,
    UnwindOperator,
};

pub struct QueryRuntime {
    batch_size: u32,
    spill: RuntimeSpill,
}

enum RuntimeSpill {
    BuiltIn(BuiltInSpillPolicy),
    Injected(SpillConfig),
}

enum FragmentSources<'a> {
    Local(&'a BTreeMap<u32, (&'a ExecutableFragment, QueryStorage)>),
    Materialized(&'a BTreeMap<u32, (&'a ExecutableFragment, Vec<ColumnBatch>)>),
}

impl QueryRuntime {
    pub fn new(batch_size: u32) -> Self {
        Self {
            batch_size,
            spill: RuntimeSpill::BuiltIn(BuiltInSpillPolicy::default()),
        }
    }

    pub fn with_spill(mut self, spill: SpillConfig) -> Self {
        self.spill = RuntimeSpill::Injected(spill);
        self
    }

    pub fn with_builtin_spill(mut self, spill: BuiltInSpillPolicy) -> Self {
        self.spill = RuntimeSpill::BuiltIn(spill);
        self
    }

    fn spill_config(&self) -> Result<SpillConfig, QueryError> {
        match &self.spill {
            RuntimeSpill::BuiltIn(policy) => policy.create_config(),
            RuntimeSpill::Injected(config) => Ok(config.clone()),
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn execute(
        &self,
        plan: &ExecutablePlan,
        storage: BTreeMap<ShardId, QueryStorage>,
        snapshot: &crate::SnapshotGuard,
        budget: QueryBudget,
        cancellation: CancellationToken,
        overlay: Option<QueryOverlay>,
    ) -> Result<QueryStream, QueryError> {
        if self.batch_size == 0 {
            return Err(QueryError::InvalidPlan(
                "query runtime batch size must be nonzero".into(),
            ));
        }
        let mut fragment_storage = BTreeMap::new();
        for fragment in plan.fragments() {
            snapshot.validate(fragment.fence())?;
            let shard_id = fragment.fence().shard_id();
            let shard_storage = storage
                .get(&shard_id)
                .ok_or(QueryError::MissingStorage(shard_id))?;
            fragment_storage.insert(fragment.id(), (fragment, shard_storage.clone()));
        }
        let definitions = plan
            .operators()
            .iter()
            .map(|operator| (operator.id(), operator.kind().clone()))
            .collect::<BTreeMap<_, _>>();
        let mut visiting = BTreeSet::new();
        let mut built = BTreeSet::new();
        let sources = FragmentSources::Local(&fragment_storage);
        let mut root = self.build_operator(
            plan.root_operator(),
            &definitions,
            &sources,
            &mut visiting,
            &mut built,
        )?;
        if built.len() != definitions.len() {
            return Err(QueryError::InvalidPlan(
                "physical operator DAG contains unreachable operators".into(),
            ));
        }
        if !plan.result_schema().fields.is_empty() && root.schema() != plan.result_schema() {
            return Err(QueryError::InvalidPlan(
                "physical root schema does not match declared result schema".into(),
            ));
        }
        if let Some(overlay) = overlay {
            root = Box::new(OverlayOperator::new(
                root,
                overlay,
                plan.fragments()[0].fence().valid_at(),
            ));
        }
        Ok(QueryStream::from_operator(root, budget, cancellation))
    }

    pub async fn execute_materialized(
        &self,
        plan: &ExecutablePlan,
        fragment_batches: BTreeMap<u32, Vec<ColumnBatch>>,
        budget: QueryBudget,
        cancellation: CancellationToken,
    ) -> Result<QueryStream, QueryError> {
        if self.batch_size == 0 {
            return Err(QueryError::InvalidPlan(
                "query runtime batch size must be nonzero".into(),
            ));
        }
        let expected = plan
            .fragments()
            .iter()
            .map(ExecutableFragment::id)
            .collect::<BTreeSet<_>>();
        let provided = fragment_batches.keys().copied().collect::<BTreeSet<_>>();
        if let Some(fragment_id) = expected.difference(&provided).next() {
            return Err(QueryError::InvalidPlan(format!(
                "materialized input is missing fragment {fragment_id}"
            )));
        }
        if let Some(fragment_id) = provided.difference(&expected).next() {
            return Err(QueryError::InvalidPlan(format!(
                "materialized input contains extra fragment {fragment_id}"
            )));
        }
        validate_materialized_fragment_owners(plan)?;
        for (fragment_id, batches) in &fragment_batches {
            if let Some(schema) = batches.first().map(ColumnBatch::schema) {
                let [field] = schema.fields.as_slice() else {
                    return Err(QueryError::InvalidPlan(format!(
                        "materialized fragment {fragment_id} must have a one-column schema"
                    )));
                };
                if field.name.is_empty() {
                    return Err(QueryError::InvalidPlan(format!(
                        "materialized fragment {fragment_id} source name must be nonempty"
                    )));
                }
                if batches.iter().any(|batch| batch.schema() != schema) {
                    return Err(QueryError::InvalidPlan(format!(
                        "materialized fragment {fragment_id} batch schemas must be identical"
                    )));
                }
            }
        }

        let plan_fragments = plan
            .fragments()
            .iter()
            .map(|fragment| (fragment.id(), fragment))
            .collect::<BTreeMap<_, _>>();
        let fragments = fragment_batches
            .into_iter()
            .map(|(fragment_id, batches)| {
                (
                    fragment_id,
                    (
                        *plan_fragments
                            .get(&fragment_id)
                            .expect("fragment identity coverage was validated"),
                        batches,
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let definitions = plan
            .operators()
            .iter()
            .map(|operator| (operator.id(), operator.kind().clone()))
            .collect::<BTreeMap<_, _>>();
        let mut visiting = BTreeSet::new();
        let mut built = BTreeSet::new();
        let sources = FragmentSources::Materialized(&fragments);
        let root = self.build_operator(
            plan.root_operator(),
            &definitions,
            &sources,
            &mut visiting,
            &mut built,
        )?;
        if built.len() != definitions.len() {
            return Err(QueryError::InvalidPlan(
                "physical operator DAG contains unreachable operators".into(),
            ));
        }
        if !plan.result_schema().fields.is_empty() && root.schema() != plan.result_schema() {
            return Err(QueryError::InvalidPlan(
                "physical root schema does not match declared result schema".into(),
            ));
        }
        Ok(QueryStream::from_operator(root, budget, cancellation))
    }

    fn build_operator(
        &self,
        id: u32,
        definitions: &BTreeMap<u32, ExecutableOperatorKind>,
        sources: &FragmentSources<'_>,
        visiting: &mut BTreeSet<u32>,
        built: &mut BTreeSet<u32>,
    ) -> Result<Box<dyn Operator>, QueryError> {
        if visiting.contains(&id) {
            return Err(QueryError::InvalidPlan(
                "physical operator DAG contains a cycle".into(),
            ));
        }
        if built.contains(&id) {
            return Err(QueryError::InvalidPlan(
                "physical operator DAG contains a shared input".into(),
            ));
        }
        let kind = definitions.get(&id).cloned().ok_or_else(|| {
            QueryError::InvalidPlan(format!("physical operator input {id} is absent"))
        })?;
        visiting.insert(id);
        let operator: Box<dyn Operator> = match kind {
            ExecutableOperatorKind::Source {
                logical_node,
                fragments,
                output,
            } => self.build_source(logical_node, &fragments, &output, sources)?,
            ExecutableOperatorKind::Filter { input, predicate } => {
                let input = self.build_operator(input, definitions, sources, visiting, built)?;
                validate_expression(&predicate, input.schema())?;
                Box::new(FilterOperator::new(input, predicate))
            }
            ExecutableOperatorKind::Project { input, projections } => {
                let input = self.build_operator(input, definitions, sources, visiting, built)?;
                if projections.is_empty() {
                    return Err(QueryError::InvalidPlan(
                        "physical projection must contain at least one expression".into(),
                    ));
                }
                let mut aliases = BTreeSet::new();
                let mut lowered = Vec::with_capacity(projections.len());
                for projection in projections {
                    if projection.alias().is_empty()
                        || !aliases.insert(projection.alias().to_owned())
                    {
                        return Err(QueryError::InvalidPlan(
                            "physical projection aliases must be nonempty and unique".into(),
                        ));
                    }
                    validate_expression(projection.expression(), input.schema())?;
                    lowered.push(ProjectionExpr::new(
                        projection.alias(),
                        LogicalType::Any,
                        true,
                        projection.expression().clone(),
                    ));
                }
                Box::new(ProjectOperator::new(input, lowered))
            }
            ExecutableOperatorKind::Join {
                left,
                right,
                kind,
                predicate,
            } => {
                if kind != JoinKind::Inner {
                    return Err(QueryError::Unsupported(
                        "query runtime currently supports only inner hash joins".into(),
                    ));
                }
                let left = self.build_operator(left, definitions, sources, visiting, built)?;
                let right = self.build_operator(right, definitions, sources, visiting, built)?;
                let predicate = predicate.ok_or_else(|| {
                    QueryError::Unsupported("inner hash join requires an equality predicate".into())
                })?;
                let (left_key, right_key) = join_keys(&predicate, left.schema(), right.schema())?;
                Box::new(HashJoinOperator::with_batch_size(
                    left,
                    right,
                    left_key,
                    right_key,
                    self.batch_size as usize,
                )?)
            }
            ExecutableOperatorKind::Aggregate {
                input,
                groups,
                aggregates,
            } => {
                let input = self.build_operator(input, definitions, sources, visiting, built)?;
                let [aggregate] = aggregates.as_slice() else {
                    return Err(QueryError::Unsupported(
                        "query runtime currently supports one aggregate function".into(),
                    ));
                };
                if aggregate.function != AggregateKind::Count
                    || aggregate.distinct
                    || aggregate.argument.is_some()
                {
                    return Err(QueryError::Unsupported(
                        "query runtime currently supports non-distinct COUNT(*)".into(),
                    ));
                }
                match groups.as_slice() {
                    [] => Box::new(AggregateOperator::count_all(input, aggregate.alias.clone())),
                    [group] => {
                        validate_expression(group.expression(), input.schema())?;
                        let grouped = ProjectOperator::new(
                            input,
                            vec![ProjectionExpr::new(
                                group.alias(),
                                LogicalType::Any,
                                true,
                                group.expression().clone(),
                            )],
                        );
                        Box::new(AggregateOperator::count_by(
                            Box::new(grouped),
                            0,
                            aggregate.alias.clone(),
                        ))
                    }
                    _ => {
                        return Err(QueryError::Unsupported(
                            "query runtime currently supports at most one aggregate group".into(),
                        ));
                    }
                }
            }
            ExecutableOperatorKind::Sort { input, keys } => {
                let input = self.build_operator(input, definitions, sources, visiting, built)?;
                let [key] = keys.as_slice() else {
                    return Err(QueryError::Unsupported(
                        "query runtime currently supports one sort key".into(),
                    ));
                };
                let column = expression_column(&key.expression).ok_or_else(|| {
                    QueryError::Unsupported(
                        "query runtime sort key must be a projected column".into(),
                    )
                })?;
                let column = required_column(input.schema(), column)?;
                Box::new(SortOperator::new(input, column, key.direction))
            }
            ExecutableOperatorKind::Limit { input, skip, limit } => {
                let input = self.build_operator(input, definitions, sources, visiting, built)?;
                let skip = usize::try_from(skip)
                    .map_err(|_| QueryError::InvalidPlan("LIMIT skip exceeds usize".into()))?;
                let limit = match limit {
                    Some(limit) => usize::try_from(limit)
                        .map_err(|_| QueryError::InvalidPlan("LIMIT count exceeds usize".into()))?,
                    None => usize::MAX,
                };
                Box::new(LimitOperator::new(input, skip, limit))
            }
            ExecutableOperatorKind::Unwind {
                input,
                expression,
                alias,
            } => {
                let input = self.build_operator(input, definitions, sources, visiting, built)?;
                validate_expression(&expression, input.schema())?;
                Box::new(UnwindOperator::new(
                    input,
                    expression,
                    alias,
                    self.batch_size as usize,
                )?)
            }
        };
        visiting.remove(&id);
        built.insert(id);
        Ok(operator)
    }

    fn build_source(
        &self,
        logical_node: u32,
        fragments: &[u32],
        output: &str,
        fragment_sources: &FragmentSources<'_>,
    ) -> Result<Box<dyn Operator>, QueryError> {
        if fragments.is_empty() || output.is_empty() {
            return Err(QueryError::InvalidPlan(
                "physical source fragments and output must be nonempty".into(),
            ));
        }
        let mut referenced = BTreeSet::new();
        for fragment_id in fragments {
            if !referenced.insert(*fragment_id) {
                return Err(QueryError::InvalidPlan(format!(
                    "physical source contains duplicate fragment {fragment_id}"
                )));
            }
        }
        let mut sources = Vec::with_capacity(fragments.len());
        for fragment_id in fragments {
            let source: Box<dyn Operator> = match fragment_sources {
                FragmentSources::Local(fragment_storage) => {
                    let (fragment, shard_storage) =
                        fragment_storage.get(fragment_id).ok_or_else(|| {
                            QueryError::InvalidPlan(format!(
                                "physical source references missing fragment {fragment_id}"
                            ))
                        })?;
                    let access = fragment.access(logical_node).cloned().ok_or_else(|| {
                        QueryError::InvalidPlan(format!(
                            "fragment {fragment_id} is missing logical source {logical_node}"
                        ))
                    })?;
                    Box::new(StorageSourceOperator::new(
                        access,
                        fragment.fence().clone(),
                        shard_storage.clone(),
                        self.batch_size,
                    )?)
                }
                FragmentSources::Materialized(fragment_batches) => {
                    let (fragment, batches) =
                        fragment_batches.get(fragment_id).ok_or_else(|| {
                            QueryError::InvalidPlan(format!(
                                "physical source references missing fragment {fragment_id}"
                            ))
                        })?;
                    let access = fragment.access(logical_node).ok_or_else(|| {
                        QueryError::InvalidPlan(format!(
                            "fragment {fragment_id} is missing logical source {logical_node}"
                        ))
                    })?;
                    let batches = if batches.is_empty() {
                        vec![ColumnBatch::empty(materialized_access_schema(access))]
                    } else {
                        batches.clone()
                    };
                    Box::new(BatchOperator::new(batches))
                }
            };
            let storage_field = source
                .schema()
                .fields
                .first()
                .ok_or_else(|| {
                    QueryError::InvalidPlan("storage source schema is unexpectedly empty".into())
                })?
                .name
                .clone();
            sources.push(Box::new(ProjectOperator::new(
                source,
                vec![ProjectionExpr::new(
                    output,
                    LogicalType::Any,
                    false,
                    Expression::new(LogicalExpr::Column(storage_field)),
                )],
            )) as Box<dyn Operator>);
        }
        if sources.len() == 1 {
            Ok(sources.pop().expect("length checked"))
        } else {
            Ok(Box::new(SpillMergeOperator::new(
                sources,
                0,
                true,
                self.batch_size as usize,
                self.spill_config()?,
            )?))
        }
    }
}

fn validate_materialized_fragment_owners(plan: &ExecutablePlan) -> Result<(), QueryError> {
    let fragments = plan
        .fragments()
        .iter()
        .map(|fragment| (fragment.id(), fragment))
        .collect::<BTreeMap<_, _>>();
    for fragment in plan.fragments() {
        if fragment.accesses().len() != 1 {
            return Err(QueryError::InvalidPlan(format!(
                "materialized fragment {} contains {} storage accesses; exactly one is required",
                fragment.id(),
                fragment.accesses().len()
            )));
        }
    }

    let mut owners = BTreeMap::new();
    for operator in plan.operators() {
        let ExecutableOperatorKind::Source {
            logical_node,
            fragments: source_fragments,
            ..
        } = operator.kind()
        else {
            continue;
        };
        for fragment_id in source_fragments {
            let fragment = fragments.get(fragment_id).ok_or_else(|| {
                QueryError::InvalidPlan(format!(
                    "physical source references missing fragment {fragment_id}"
                ))
            })?;
            if fragment.access(*logical_node).is_none() {
                return Err(QueryError::InvalidPlan(format!(
                    "fragment {fragment_id} is missing logical source {logical_node}"
                )));
            }
            if let Some((owner_operator, owner_logical_node)) =
                owners.insert(*fragment_id, (operator.id(), *logical_node))
            {
                if owner_operator == operator.id() && owner_logical_node == *logical_node {
                    return Err(QueryError::InvalidPlan(format!(
                        "physical source contains duplicate fragment {fragment_id}"
                    )));
                }
                return Err(QueryError::InvalidPlan(format!(
                    "materialized fragment {fragment_id} has multiple source owners: operator {owner_operator} logical node {owner_logical_node} and operator {} logical node {logical_node}",
                    operator.id()
                )));
            }
        }
    }
    Ok(())
}

fn materialized_access_schema(access: &ExecutableAccess) -> RowSchema {
    let vertex = match access {
        ExecutableAccess::Logical(read) => matches!(
            read.operation(),
            ReadOperation::VertexPoint(_) | ReadOperation::VertexScan
        ),
        ExecutableAccess::Pushdown { request, .. } => matches!(
            request.operation(),
            PushdownOperation::Vertex(_) | PushdownOperation::VertexScan(_)
        ),
    };
    RowSchema {
        fields: vec![Field {
            name: if vertex { "vertex" } else { "relationship" }.into(),
            data_type: LogicalType::Any,
            nullable: false,
        }],
    }
}

fn validate_expression(expression: &Expression, schema: &RowSchema) -> Result<(), QueryError> {
    fn validate(logical: &LogicalExpr, schema: &RowSchema) -> Result<(), QueryError> {
        match logical {
            LogicalExpr::Literal(_) => Ok(()),
            LogicalExpr::Parameter(name) => Err(QueryError::Unsupported(format!(
                "unbound parameter in executable expression: {name}"
            ))),
            LogicalExpr::Column(name) => required_column(schema, name).map(|_| ()),
            LogicalExpr::Property { input, .. } | LogicalExpr::Unary { input, .. } => {
                validate(input, schema)
            }
            LogicalExpr::Binary { left, right, .. } => {
                validate(left, schema)?;
                validate(right, schema)
            }
            LogicalExpr::List(values) => {
                values.iter().try_for_each(|value| validate(value, schema))
            }
            LogicalExpr::Map(values) => values
                .iter()
                .try_for_each(|(_, value)| validate(value, schema)),
        }
    }
    validate(expression.logical(), schema)
}

fn expression_column(expression: &Expression) -> Option<&str> {
    match expression.logical() {
        LogicalExpr::Column(name) => Some(name),
        _ => None,
    }
}

fn required_column(schema: &RowSchema, name: &str) -> Result<usize, QueryError> {
    let mut matches = schema
        .fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name == name)
        .map(|(index, _)| index);
    let Some(index) = matches.next() else {
        return Err(QueryError::InvalidPlan(format!(
            "physical expression references missing column {name}"
        )));
    };
    if matches.next().is_some() {
        return Err(QueryError::InvalidPlan(format!(
            "physical expression references ambiguous column {name}"
        )));
    }
    Ok(index)
}

fn join_keys(
    predicate: &Expression,
    left: &RowSchema,
    right: &RowSchema,
) -> Result<(usize, usize), QueryError> {
    let LogicalExpr::Binary {
        left: predicate_left,
        operator: BinaryOperator::Equal,
        right: predicate_right,
    } = predicate.logical()
    else {
        return Err(QueryError::Unsupported(
            "query runtime hash join requires a column equality predicate".into(),
        ));
    };
    let (LogicalExpr::Column(predicate_left), LogicalExpr::Column(predicate_right)) =
        (predicate_left.as_ref(), predicate_right.as_ref())
    else {
        return Err(QueryError::Unsupported(
            "query runtime hash join requires column equality keys".into(),
        ));
    };
    match (
        required_column(left, predicate_left),
        required_column(right, predicate_right),
    ) {
        (Ok(left_key), Ok(right_key)) => Ok((left_key, right_key)),
        _ => Ok((
            required_column(left, predicate_right)?,
            required_column(right, predicate_left)?,
        )),
    }
}

pub struct QueryStream {
    operator: Box<dyn Operator>,
    context: QueryContext,
    schema: RowSchema,
}

impl QueryStream {
    pub fn from_operator(
        operator: Box<dyn Operator>,
        budget: QueryBudget,
        cancellation: CancellationToken,
    ) -> Self {
        let schema = operator.schema().clone();
        Self {
            operator,
            context: QueryContext::new(budget, cancellation),
            schema,
        }
    }

    pub fn next_batch(&mut self) -> QueryFuture<'_, Option<ColumnBatch>> {
        Box::pin(async move {
            self.context.checkpoint()?;
            let batch = self.operator.next_batch(&mut self.context).await?;
            if let Some(batch) = &batch {
                self.context.charge_rows(batch.row_count() as u64)?;
                self.context.charge_memory(batch.estimated_bytes())?;
            }
            Ok(batch)
        })
    }

    pub fn collect(&mut self) -> QueryFuture<'_, ColumnBatch> {
        Box::pin(async move {
            let mut rows = Vec::new();
            while let Some(batch) = self.next_batch().await? {
                self.context.checkpoint()?;
                if batch.schema() != &self.schema {
                    return Err(QueryError::InvalidBatch(
                        "query stream schema changed between batches".into(),
                    ));
                }
                rows.extend(batch.rows());
            }
            ColumnBatch::from_rows(self.schema.clone(), rows)
        })
    }
}
