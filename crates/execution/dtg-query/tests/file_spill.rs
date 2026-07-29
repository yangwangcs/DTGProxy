use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dtg_language_ir::{Field, LogicalType, RowSchema};
use dtg_query::{ColumnBatch, FileSpillLimits, FileSpillStore, QueryValue, SpillStore};

fn temp_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("dtg-query-{name}-{}-{nonce}", std::process::id()))
}

fn schema() -> RowSchema {
    RowSchema {
        fields: vec![Field {
            name: "value".into(),
            data_type: LogicalType::String,
            nullable: false,
        }],
    }
}

#[test]
fn file_spill_rejects_corruption_and_cross_query_handles() {
    let root = temp_root("checksum-isolation");
    let first = FileSpillStore::create(&root, FileSpillLimits::default()).unwrap();
    let second = FileSpillStore::create(&root, FileSpillLimits::default()).unwrap();
    let handle = first
        .write_run(&schema(), vec![vec![QueryValue::String("alpha".into())]])
        .unwrap();

    assert!(second.read_row(handle, 0).is_err());
    let run = fs::read_dir(first.namespace_path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|extension| extension == "run"))
        .unwrap();
    let mut bytes = fs::read(&run).unwrap();
    let last = bytes.last_mut().unwrap();
    *last ^= 0xff;
    fs::write(&run, bytes).unwrap();

    let error = first.read_row(handle, 0).unwrap_err();
    assert!(error.to_string().contains("checksum"));

    drop(first);
    drop(second);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn file_spill_cleans_drop_namespaces_and_reclaims_crash_orphans() {
    let root = temp_root("cleanup");
    let store = FileSpillStore::create(&root, FileSpillLimits::default()).unwrap();
    let namespace = store.namespace_path().to_owned();
    store
        .write_run(&schema(), vec![vec![QueryValue::String("alpha".into())]])
        .unwrap();
    assert!(namespace.exists());
    drop(store);
    assert!(!namespace.exists());

    let crashed = FileSpillStore::create(&root, FileSpillLimits::default()).unwrap();
    let orphan = crashed.namespace_path().to_owned();
    crashed
        .write_run(&schema(), vec![vec![QueryValue::String("orphan".into())]])
        .unwrap();
    std::mem::forget(crashed);
    FileSpillStore::reclaim_orphans(&root, Duration::ZERO).unwrap();
    assert!(!orphan.exists());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn file_spill_enforces_record_bounds_before_creating_a_run() {
    let root = temp_root("bounds");
    let store = FileSpillStore::create(&root, FileSpillLimits::new(4, 8, 64).unwrap()).unwrap();

    assert!(
        store
            .write_run(
                &schema(),
                vec![vec![QueryValue::String("larger-than-eight".into())]],
            )
            .is_err()
    );
    assert_eq!(
        fs::read_dir(store.namespace_path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "run"))
            .count(),
        0
    );

    drop(store);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn file_spill_reservation_includes_nested_wire_overhead_before_io() {
    let root = temp_root("reservation");
    let store = FileSpillStore::create(&root, FileSpillLimits::new(4, 1024, 150).unwrap()).unwrap();
    let schema = RowSchema {
        fields: vec![Field {
            name: "values".into(),
            data_type: LogicalType::List(Box::new(LogicalType::String)),
            nullable: false,
        }],
    };
    let batch = ColumnBatch::from_rows(
        schema.clone(),
        vec![vec![QueryValue::List(
            (0..10).map(|_| QueryValue::String(String::new())).collect(),
        )]],
    )
    .unwrap();

    assert!(store.reserved_write_bytes(&batch).is_err());

    drop(store);
    let _ = fs::remove_dir_all(root);
}
