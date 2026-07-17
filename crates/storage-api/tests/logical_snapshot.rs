use storage_api::{
    KeyValue, Keyspace, LogicalKey, LogicalSnapshotAccumulator, LogicalSnapshotChunkV1,
    LogicalSnapshotError, LogicalSnapshotExportRequest, LogicalSnapshotHeaderV1,
    LogicalSnapshotManifestV1, MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES,
    MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES,
};

#[test]
fn canonical_chunks_build_and_verify_one_content_manifest() {
    let header = LogicalSnapshotHeaderV1::new(77, 12);
    let first = LogicalSnapshotChunkV1::new(
        77,
        0,
        vec![
            entry(Keyspace::Identity, b"a"),
            entry(Keyspace::Current, b"a"),
        ],
    )
    .unwrap();
    let second = LogicalSnapshotChunkV1::new(
        77,
        1,
        vec![entry(Keyspace::History, b"a"), entry(Keyspace::Txn, b"z")],
    )
    .unwrap();
    let mut source = LogicalSnapshotAccumulator::new(header.clone());
    source.observe(&first).unwrap();
    source.observe(&second).unwrap();
    let manifest = source.complete();
    assert_eq!(manifest.header(), &header);
    assert_eq!(manifest.total_chunks(), 2);
    assert_eq!(manifest.total_entries(), 4);

    let mut target = LogicalSnapshotAccumulator::new(header);
    target.observe(&first).unwrap();
    target.observe(&second).unwrap();
    target.verify(&manifest).unwrap();
}

#[test]
fn wrong_order_identity_ordinal_digest_and_manifest_fail_closed() {
    assert!(matches!(
        LogicalSnapshotChunkV1::new(
            1,
            0,
            vec![
                entry(Keyspace::Current, b"b"),
                entry(Keyspace::Current, b"a")
            ]
        ),
        Err(LogicalSnapshotError::EntriesNotStrictlyOrdered)
    ));
    let chunk = LogicalSnapshotChunkV1::new(1, 0, vec![entry(Keyspace::Current, b"a")]).unwrap();
    assert!(matches!(
        LogicalSnapshotChunkV1::from_parts(1, 0, chunk.entries().to_vec(), [9; 32]),
        Err(LogicalSnapshotError::ChunkDigestMismatch { .. })
    ));
    let mut accumulator = LogicalSnapshotAccumulator::new(LogicalSnapshotHeaderV1::new(1, 0));
    let wrong_ordinal =
        LogicalSnapshotChunkV1::new(1, 2, vec![entry(Keyspace::Current, b"a")]).unwrap();
    assert!(matches!(
        accumulator.observe(&wrong_ordinal),
        Err(LogicalSnapshotError::ChunkOrdinalMismatch { .. })
    ));
    let wrong_identity =
        LogicalSnapshotChunkV1::new(2, 0, vec![entry(Keyspace::Current, b"a")]).unwrap();
    assert!(matches!(
        accumulator.observe(&wrong_identity),
        Err(LogicalSnapshotError::SnapshotIdMismatch { .. })
    ));

    accumulator.observe(&chunk).unwrap();
    let actual = accumulator.clone().complete();
    let bad_manifest = LogicalSnapshotManifestV1::from_parts(
        actual.header().clone(),
        actual.total_chunks(),
        actual.total_entries(),
        [0; 32],
    )
    .unwrap();
    assert!(matches!(
        accumulator.verify(&bad_manifest),
        Err(LogicalSnapshotError::ManifestMismatch)
    ));
}

#[test]
fn export_request_bounds_are_explicit() {
    assert!(LogicalSnapshotExportRequest::new(1, 1).is_ok());
    assert!(
        LogicalSnapshotExportRequest::new(
            MAX_LOGICAL_SNAPSHOT_CHUNK_ENTRIES,
            MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES
        )
        .is_ok()
    );
    assert!(LogicalSnapshotExportRequest::new(0, 1).is_err());
    assert!(LogicalSnapshotExportRequest::new(1, MAX_LOGICAL_SNAPSHOT_CHUNK_BYTES + 1).is_err());
}

fn entry(keyspace: Keyspace, key: &[u8]) -> KeyValue {
    KeyValue::new(
        LogicalKey::in_keyspace(keyspace, key.to_vec()),
        key.to_vec(),
    )
}
