use data_node::{ChunkAppendOutcome, MigrationChunk, MigrationStorageError, SnapshotInbox};

#[test]
fn chunk_transfer_resumes_after_restart_and_terminal_digest_publishes_completion() {
    let temporary = tempfile::tempdir().unwrap();
    let migration_id = [0x51; 16];
    let payloads = [b"snapshot-".as_slice(), b"archive".as_slice()];
    let archive = payloads.concat();
    let digest = *blake3::hash(&archive).as_bytes();
    let inbox = SnapshotInbox::open(temporary.path()).unwrap();
    assert!(matches!(
        inbox
            .append(
                MigrationChunk::new(migration_id, 0, payloads[0].to_vec(), false, None).unwrap()
            )
            .unwrap(),
        ChunkAppendOutcome::Stored { next_ordinal: 1 }
    ));
    drop(inbox);

    let inbox = SnapshotInbox::open(temporary.path()).unwrap();
    assert!(matches!(
        inbox
            .append(
                MigrationChunk::new(migration_id, 0, payloads[0].to_vec(), false, None).unwrap()
            )
            .unwrap(),
        ChunkAppendOutcome::Duplicate { next_ordinal: 1 }
    ));
    let completed = inbox
        .append(
            MigrationChunk::new(migration_id, 1, payloads[1].to_vec(), true, Some(digest)).unwrap(),
        )
        .unwrap();
    let ChunkAppendOutcome::Completed {
        archive_path,
        content_digest,
        duplicate,
    } = completed
    else {
        panic!("terminal chunk did not complete transfer")
    };
    assert!(!duplicate);
    assert_eq!(content_digest, digest);
    assert_eq!(std::fs::read(archive_path).unwrap(), archive);

    assert!(matches!(
        inbox
            .append(
                MigrationChunk::new(migration_id, 1, payloads[1].to_vec(), true, Some(digest))
                    .unwrap()
            )
            .unwrap(),
        ChunkAppendOutcome::Completed {
            duplicate: true,
            ..
        }
    ));
}

#[test]
fn chunk_transfer_rejects_gaps_payload_conflicts_and_wrong_terminal_digest() {
    let temporary = tempfile::tempdir().unwrap();
    let inbox = SnapshotInbox::open(temporary.path()).unwrap();
    assert!(matches!(
        inbox.append(MigrationChunk::new([0x61; 16], 1, b"gap".to_vec(), false, None).unwrap()),
        Err(MigrationStorageError::UnexpectedChunkOrdinal {
            expected: 0,
            actual: 1
        })
    ));
    inbox
        .append(MigrationChunk::new([0x61; 16], 0, b"first".to_vec(), false, None).unwrap())
        .unwrap();
    assert!(matches!(
        inbox.append(MigrationChunk::new([0x61; 16], 0, b"changed".to_vec(), false, None).unwrap()),
        Err(MigrationStorageError::ChunkReplayConflict { ordinal: 0 })
    ));
    assert!(matches!(
        inbox.append(
            MigrationChunk::new([0x61; 16], 1, b"last".to_vec(), true, Some([0x99; 32])).unwrap()
        ),
        Err(MigrationStorageError::ContentDigestMismatch)
    ));
}
