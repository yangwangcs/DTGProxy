use cluster_protocol::{
    CLUSTER_PROTOCOL_VERSION, CommandPayload, CommonRequestContext, MAX_COMMAND_BYTES,
    MAX_SNAPSHOT_CHUNK_BYTES, ProtocolError, ShardRequestContext, SnapshotChunkEnvelope,
    StatusReason,
};

fn common_context() -> CommonRequestContext {
    CommonRequestContext::new(
        CLUSTER_PROTOCOL_VERSION,
        [0x11; 16],
        [0x22; 16],
        1_800_000_000_000,
    )
    .expect("valid request context")
}

#[test]
fn common_context_requires_version_ids_and_deadline() {
    let valid = common_context();
    assert_eq!(valid.protocol_version(), CLUSTER_PROTOCOL_VERSION);
    assert_eq!(valid.cluster_id(), &[0x11; 16]);
    assert_eq!(valid.request_id(), &[0x22; 16]);
    assert_eq!(valid.deadline_unix_ms(), 1_800_000_000_000);
    assert_eq!(valid.ensure_active_at(1_799_999_999_999), Ok(()));
    assert_eq!(
        valid.ensure_active_at(1_800_000_000_000),
        Err(ProtocolError::DeadlineExpired {
            deadline_unix_ms: 1_800_000_000_000,
            now_unix_ms: 1_800_000_000_000,
        })
    );

    assert_eq!(
        CommonRequestContext::new(2, [0x11; 16], [0x22; 16], 1),
        Err(ProtocolError::UnsupportedVersion { actual: 2 })
    );
    assert_eq!(
        CommonRequestContext::new(CLUSTER_PROTOCOL_VERSION, [0; 16], [0x22; 16], 1),
        Err(ProtocolError::ZeroClusterId)
    );
    assert_eq!(
        CommonRequestContext::new(CLUSTER_PROTOCOL_VERSION, [0x11; 16], [0; 16], 1),
        Err(ProtocolError::ZeroRequestId)
    );
    assert_eq!(
        CommonRequestContext::new(CLUSTER_PROTOCOL_VERSION, [0x11; 16], [0x22; 16], 0),
        Err(ProtocolError::ZeroDeadline)
    );
}

#[test]
fn shard_context_rejects_zero_graph_shard_and_epoch() {
    let common = common_context();
    let valid = ShardRequestContext::new(common.clone(), 7, 9, 11).expect("valid Shard context");
    assert_eq!(valid.common(), &common);
    assert_eq!(valid.graph_id(), 7);
    assert_eq!(valid.shard_id(), 9);
    assert_eq!(valid.placement_epoch(), 11);

    assert_eq!(
        ShardRequestContext::new(common.clone(), 0, 9, 11),
        Err(ProtocolError::ZeroGraphId)
    );
    assert_eq!(
        ShardRequestContext::new(common.clone(), 7, 0, 11),
        Err(ProtocolError::ZeroShardId)
    );
    assert_eq!(
        ShardRequestContext::new(common, 7, 9, 0),
        Err(ProtocolError::ZeroPlacementEpoch)
    );
}

#[test]
fn command_and_snapshot_payloads_are_bounded_before_allocation_contracts() {
    assert!(CommandPayload::try_from(vec![1]).is_ok());
    assert_eq!(
        CommandPayload::try_from(Vec::new()),
        Err(ProtocolError::EmptyCommand)
    );
    assert_eq!(
        CommandPayload::try_from(vec![0; MAX_COMMAND_BYTES + 1]),
        Err(ProtocolError::CommandTooLarge {
            actual: MAX_COMMAND_BYTES + 1,
            maximum: MAX_COMMAND_BYTES,
        })
    );

    let shard = ShardRequestContext::new(common_context(), 7, 9, 11).unwrap();
    let chunk = SnapshotChunkEnvelope::new(shard.clone(), [0x33; 16], 0, vec![4, 5, 6])
        .expect("valid chunk");
    assert_eq!(chunk.payload(), &[4, 5, 6]);
    assert_eq!(chunk.ordinal(), 0);
    assert!(chunk.verify_checksum());

    assert_eq!(
        SnapshotChunkEnvelope::new(shard.clone(), [0; 16], 0, vec![1]),
        Err(ProtocolError::ZeroMigrationId)
    );
    assert_eq!(
        SnapshotChunkEnvelope::new(shard.clone(), [0x33; 16], 0, Vec::new()),
        Err(ProtocolError::EmptySnapshotChunk)
    );
    assert_eq!(
        SnapshotChunkEnvelope::new(shard, [0x33; 16], 0, vec![0; MAX_SNAPSHOT_CHUNK_BYTES + 1],),
        Err(ProtocolError::SnapshotChunkTooLarge {
            actual: MAX_SNAPSHOT_CHUNK_BYTES + 1,
            maximum: MAX_SNAPSHOT_CHUNK_BYTES,
        })
    );
}

#[test]
fn protobuf_round_trip_revalidates_instead_of_trusting_wire_data() {
    let validated = common_context();
    let wire: cluster_protocol::proto::RequestContext = validated.clone().into();
    assert_eq!(CommonRequestContext::try_from(wire).unwrap(), validated);

    let invalid = cluster_protocol::proto::RequestContext {
        protocol_version: CLUSTER_PROTOCOL_VERSION,
        cluster_id: vec![0x11; 15],
        request_id: vec![0x22; 16],
        deadline_unix_ms: 1,
    };
    assert_eq!(
        CommonRequestContext::try_from(invalid),
        Err(ProtocolError::InvalidClusterIdLength { actual: 15 })
    );
}

#[test]
fn snapshot_wire_round_trip_rechecks_checksum_and_chunk_kind() {
    let shard = ShardRequestContext::new(common_context(), 7, 9, 11).unwrap();
    let validated = SnapshotChunkEnvelope::new(shard, [0x33; 16], 4, vec![7, 8, 9]).unwrap();
    let wire: cluster_protocol::proto::SnapshotChunk = validated.clone().into();
    assert_eq!(
        SnapshotChunkEnvelope::try_from(wire.clone()).unwrap(),
        validated
    );

    let mut corrupt = wire.clone();
    corrupt.checksum ^= 1;
    assert_eq!(
        SnapshotChunkEnvelope::try_from(corrupt),
        Err(ProtocolError::SnapshotChecksumMismatch)
    );

    let mut terminal = wire;
    terminal.terminal = true;
    assert_eq!(
        SnapshotChunkEnvelope::try_from(terminal),
        Err(ProtocolError::TerminalChunkIsNotData)
    );
}

#[test]
fn status_reasons_reject_unknown_and_unspecified_values() {
    assert_eq!(StatusReason::try_from(1), Ok(StatusReason::NotLeader));
    assert_eq!(StatusReason::try_from(2), Ok(StatusReason::StaleEpoch));
    assert_eq!(
        StatusReason::try_from(0),
        Err(ProtocolError::UnspecifiedStatusReason)
    );
    assert_eq!(
        StatusReason::try_from(999),
        Err(ProtocolError::UnknownStatusReason { actual: 999 })
    );
}
