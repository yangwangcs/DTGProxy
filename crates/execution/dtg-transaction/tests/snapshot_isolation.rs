use dtg_transaction::{
    BackendGeneration, BaseGraphSnapshot, EdgeId, EdgeVersion, EntityIdentity, IntervalWrite,
    LogicalMutation, PlacementEpoch, Properties, ShardId, ShardSnapshotFence, SnapshotToken,
    TransactionId, TransactionOverlay, TransactionTime, TxnError, ValidInterval, Value, Version,
    VertexId, VertexTombstone, VertexVersion, detect_conflict,
};

fn transaction_time(value: i64) -> TransactionTime {
    TransactionTime::new(value).unwrap()
}

fn interval_write(start: i64, end: i64, transaction_time: TransactionTime) -> IntervalWrite {
    IntervalWrite::new(
        EntityIdentity::Vertex(VertexId::new(7).unwrap()),
        ValidInterval::new(start, end).unwrap(),
        transaction_time,
    )
}

#[test]
fn overlapping_post_snapshot_write_conflicts() {
    let committed = interval_write(10, 30, transaction_time(50));
    let staged = interval_write(20, 40, transaction_time(40));
    assert_eq!(
        detect_conflict(&staged, &[committed]),
        Err(TxnError::WriteConflict)
    );
}

#[test]
fn disjoint_valid_time_corrections_can_commit() {
    let committed = interval_write(10, 20, transaction_time(50));
    let staged = interval_write(20, 30, transaction_time(40));
    assert_eq!(detect_conflict(&staged, &[committed]), Ok(()));
}

#[test]
fn tombstones_conflict_with_the_complete_identity() {
    let committed = IntervalWrite::tombstone(
        EntityIdentity::Vertex(VertexId::new(7).unwrap()),
        transaction_time(50),
    );
    let staged = interval_write(100, 200, transaction_time(40));

    assert_eq!(
        detect_conflict(&staged, &[committed]),
        Err(TxnError::WriteConflict)
    );
}

#[test]
fn commits_at_or_before_start_time_do_not_conflict() {
    let at_start = interval_write(10, 30, transaction_time(40));
    let before_start = interval_write(10, 30, transaction_time(39));
    let staged = interval_write(20, 40, transaction_time(40));

    assert_eq!(detect_conflict(&staged, &[at_start, before_start]), Ok(()));
}

fn snapshot_fence(closed_time: i64) -> ShardSnapshotFence {
    ShardSnapshotFence {
        placement_epoch: PlacementEpoch::new(3).unwrap(),
        backend_generation: BackendGeneration::new(5).unwrap(),
        applied_index: 11,
        closed_time: transaction_time(closed_time),
    }
}

#[test]
fn snapshot_rejects_incomplete_and_duplicate_fences() {
    let transaction_id = TransactionId::new(9).unwrap();
    let empty = SnapshotToken::new(
        transaction_id,
        transaction_time(40),
        Version::new(2),
        Vec::new(),
    );
    assert_eq!(empty, Err(TxnError::IncompleteSnapshot));

    let shard_id = ShardId::new(7).unwrap();
    let duplicate = SnapshotToken::new(
        transaction_id,
        transaction_time(40),
        Version::new(2),
        vec![
            (shard_id, snapshot_fence(50)),
            (shard_id, snapshot_fence(50)),
        ],
    );
    assert_eq!(duplicate, Err(TxnError::DuplicateShardFence));

    let inconsistent = SnapshotToken::new(
        transaction_id,
        transaction_time(40),
        Version::new(2),
        vec![(shard_id, snapshot_fence(39))],
    );
    assert_eq!(inconsistent, Err(TxnError::InconsistentSnapshot));
}

#[test]
fn snapshot_rejects_stale_epoch_and_generation() {
    let shard_id = ShardId::new(7).unwrap();
    let token = SnapshotToken::new(
        TransactionId::new(9).unwrap(),
        transaction_time(40),
        Version::new(2),
        vec![(shard_id, snapshot_fence(50))],
    )
    .unwrap();

    assert_eq!(
        token.validate_shard(
            shard_id,
            PlacementEpoch::new(4).unwrap(),
            BackendGeneration::new(5).unwrap(),
            11,
        ),
        Err(TxnError::StalePlacementEpoch)
    );
    assert_eq!(
        token.validate_shard(
            shard_id,
            PlacementEpoch::new(3).unwrap(),
            BackendGeneration::new(6).unwrap(),
            11,
        ),
        Err(TxnError::StaleBackendGeneration)
    );
    assert_eq!(
        token.validate_shard(
            shard_id,
            PlacementEpoch::new(3).unwrap(),
            BackendGeneration::new(5).unwrap(),
            10,
        ),
        Err(TxnError::AppliedIndexUnavailable)
    );
}

#[test]
fn snapshot_read_time_is_pinned_to_the_published_start() {
    let token = SnapshotToken::new(
        TransactionId::new(9).unwrap(),
        transaction_time(40),
        Version::new(2),
        vec![
            (ShardId::new(7).unwrap(), snapshot_fence(50)),
            (ShardId::new(8).unwrap(), snapshot_fence(45)),
        ],
    )
    .unwrap();

    assert_eq!(token.validate_read_time(transaction_time(40)), Ok(()));
    assert_eq!(
        token.validate_read_time(transaction_time(45)),
        Err(TxnError::InconsistentSnapshot)
    );
}

fn vertex(id: u128, version: u64, start: i64, end: i64, label: &str) -> VertexVersion {
    let mut properties = Properties::new();
    properties.insert("label".into(), Value::String(label.into()));
    VertexVersion::new(
        VertexId::new(id).unwrap(),
        Version::new(version),
        ValidInterval::new(start, end).unwrap(),
        transaction_time(40),
        properties,
    )
    .unwrap()
}

fn edge(id: u128, source: u128, target: u128) -> EdgeVersion {
    edge_interval(id, source, target, 0, 100)
}

fn edge_interval(id: u128, source: u128, target: u128, start: i64, end: i64) -> EdgeVersion {
    EdgeVersion::new(
        EdgeId::new(id).unwrap(),
        VertexId::new(source).unwrap(),
        VertexId::new(target).unwrap(),
        "knows",
        Version::new(1),
        ValidInterval::new(start, end).unwrap(),
        transaction_time(40),
        Properties::new(),
    )
    .unwrap()
}

#[test]
fn overlay_reads_its_own_writes() {
    let base_vertex = vertex(1, 1, 0, 100, "base");
    let mut overlay = TransactionOverlay::new();
    overlay
        .stage(LogicalMutation::PutVertex(vertex(1, 2, 0, 100, "overlay")))
        .unwrap();

    let visible = overlay
        .get_vertex(Some(&base_vertex), VertexId::new(1).unwrap(), 50)
        .unwrap();
    assert_eq!(
        visible.properties().get("label"),
        Some(&Value::String("overlay".into()))
    );

    let mut deleted = TransactionOverlay::new();
    deleted
        .stage(LogicalMutation::DeleteVertex(VertexTombstone::new(
            VertexId::new(1).unwrap(),
            Version::new(2),
            transaction_time(40),
        )))
        .unwrap();
    assert_eq!(
        deleted.get_vertex(Some(&base_vertex), base_vertex.id(), 50),
        None
    );

    let mut edge_overlay = TransactionOverlay::new();
    edge_overlay
        .stage(LogicalMutation::PutEdge(edge(9, 1, 2)))
        .unwrap();
    assert_eq!(
        edge_overlay
            .get_edge(None, EdgeId::new(9).unwrap(), 50)
            .unwrap()
            .source(),
        VertexId::new(1).unwrap()
    );
}

#[test]
fn overlay_validates_references_against_the_complete_staged_graph() {
    let mut overlay = TransactionOverlay::new();
    overlay
        .stage(LogicalMutation::PutEdge(edge(9, 1, 2)))
        .unwrap();
    overlay
        .stage(LogicalMutation::PutVertex(vertex(2, 1, 0, 100, "target")))
        .unwrap();
    overlay
        .stage(LogicalMutation::PutVertex(vertex(1, 1, 0, 100, "source")))
        .unwrap();

    assert_eq!(overlay.validate(&BaseGraphSnapshot::default()), Ok(()));
}

#[test]
fn overlay_rejects_missing_references_and_overlapping_identity_versions() {
    let mut missing_endpoint = TransactionOverlay::new();
    missing_endpoint
        .stage(LogicalMutation::PutEdge(edge(9, 1, 2)))
        .unwrap();
    assert_eq!(
        missing_endpoint.validate(&BaseGraphSnapshot::default()),
        Err(TxnError::ReferentialIntegrity)
    );

    let mut duplicate_identity = TransactionOverlay::new();
    duplicate_identity
        .stage(LogicalMutation::PutVertex(vertex(1, 1, 0, 60, "first")))
        .unwrap();
    duplicate_identity
        .stage(LogicalMutation::PutVertex(vertex(1, 2, 50, 100, "second")))
        .unwrap();
    assert_eq!(
        duplicate_identity.validate(&BaseGraphSnapshot::default()),
        Err(TxnError::DuplicateIdentity)
    );
}

#[test]
fn base_and_staged_histories_allow_disjoint_intervals_per_identity() {
    let base = BaseGraphSnapshot::new(
        vec![
            vertex(1, 1, 0, 50, "source-first"),
            vertex(1, 2, 50, 100, "source-second"),
            vertex(2, 1, 0, 100, "target"),
        ],
        Vec::new(),
    );
    assert!(base.is_ok());

    let mut staged = TransactionOverlay::new();
    staged
        .stage(LogicalMutation::PutVertex(vertex(
            1,
            1,
            0,
            50,
            "source-first",
        )))
        .unwrap();
    staged
        .stage(LogicalMutation::PutVertex(vertex(
            1,
            2,
            50,
            100,
            "source-second",
        )))
        .unwrap();
    staged
        .stage(LogicalMutation::PutVertex(vertex(2, 1, 0, 100, "target")))
        .unwrap();
    staged
        .stage(LogicalMutation::PutEdge(edge_interval(9, 1, 2, 0, 100)))
        .unwrap();
    assert_eq!(staged.validate(&BaseGraphSnapshot::default()), Ok(()));
}

#[test]
fn edge_endpoints_must_cover_the_complete_edge_interval() {
    let base = BaseGraphSnapshot::new(
        vec![
            vertex(1, 1, 0, 50, "short-source"),
            vertex(2, 1, 0, 100, "target"),
        ],
        Vec::new(),
    )
    .unwrap();
    let mut overlay = TransactionOverlay::new();
    overlay
        .stage(LogicalMutation::PutEdge(edge_interval(9, 1, 2, 0, 100)))
        .unwrap();

    assert_eq!(overlay.validate(&base), Err(TxnError::ReferentialIntegrity));
}
