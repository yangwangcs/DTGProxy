use storage_api::{
    BackendFamily, MAPPING_SPI_VERSION, MappingCapabilities, MappingCompatibilityError,
    MappingDescriptorV1, MappingRequirement, RequiredMappingCapability,
};

fn production_capabilities() -> MappingCapabilities {
    MappingCapabilities {
        atomic_batch_lifecycle: true,
        deterministic_mapping: true,
        idempotent_replay: true,
        canonical_multi_get: true,
        canonical_ordered_scan: true,
        durable_applied_index: true,
        durability: storage_api::Durability::Synchronous,
        snapshot: storage_api::SnapshotCapability::PhysicalCheckpoint,
        canonical_export: true,
        canonical_restore: true,
        native_temporal_layout: true,
        predicate_pushdown: false,
        adjacency_pushdown: false,
        change_feed: false,
    }
}

#[test]
fn current_mapping_descriptor_accepts_a_hot_pluggable_backend() {
    let descriptor = MappingDescriptorV1::new(
        "rocksdb-canonical",
        "1.0.0",
        BackendFamily::KeyValue,
        [7; 32],
        production_capabilities(),
    )
    .unwrap();

    descriptor
        .validate(MappingRequirement::HotPluggableReplica)
        .unwrap();
    assert_eq!(descriptor.spi_version(), MAPPING_SPI_VERSION);
    assert_eq!(descriptor.name(), "rocksdb-canonical");
    assert_eq!(descriptor.version(), "1.0.0");
    assert_eq!(descriptor.family(), BackendFamily::KeyValue);
    assert_eq!(descriptor.schema_fingerprint(), [7; 32]);
    assert_eq!(descriptor.capabilities(), production_capabilities());
}

#[test]
fn mapping_descriptor_rejects_invalid_identity_before_capability_checks() {
    assert_eq!(
        MappingDescriptorV1::new(
            "RocksDB",
            "1.0.0",
            BackendFamily::KeyValue,
            [7; 32],
            production_capabilities(),
        )
        .unwrap_err(),
        MappingCompatibilityError::InvalidName {
            name: "RocksDB".into(),
        }
    );
    assert_eq!(
        MappingDescriptorV1::new(
            "rocksdb-canonical",
            "",
            BackendFamily::KeyValue,
            [7; 32],
            production_capabilities(),
        )
        .unwrap_err(),
        MappingCompatibilityError::InvalidVersion
    );
    assert_eq!(
        MappingDescriptorV1::new(
            "rocksdb-canonical",
            "1.0.0",
            BackendFamily::KeyValue,
            [0; 32],
            production_capabilities(),
        )
        .unwrap_err(),
        MappingCompatibilityError::ZeroSchemaFingerprint
    );
}

#[test]
fn mapping_descriptor_rejects_unknown_spi_and_the_first_missing_capability() {
    let future = MappingDescriptorV1::with_spi_version(
        MAPPING_SPI_VERSION + 1,
        "future-mapping",
        "2.0.0",
        BackendFamily::Sql,
        [9; 32],
        production_capabilities(),
    )
    .unwrap();
    assert_eq!(
        future.validate(MappingRequirement::ManagedReplica),
        Err(MappingCompatibilityError::SpiVersionMismatch {
            expected: MAPPING_SPI_VERSION,
            actual: MAPPING_SPI_VERSION + 1,
        })
    );

    let mut capabilities = production_capabilities();
    capabilities.deterministic_mapping = false;
    let descriptor = MappingDescriptorV1::new(
        "nondeterministic",
        "1.0.0",
        BackendFamily::PropertyGraph,
        [11; 32],
        capabilities,
    )
    .unwrap();
    assert_eq!(
        descriptor.validate(MappingRequirement::ManagedReplica),
        Err(MappingCompatibilityError::MissingCapability {
            mapping: "nondeterministic".into(),
            capability: RequiredMappingCapability::DeterministicMapping,
        })
    );

    let mut capabilities = production_capabilities();
    capabilities.idempotent_replay = false;
    let descriptor = MappingDescriptorV1::new(
        "non-idempotent",
        "1.0.0",
        BackendFamily::PropertyGraph,
        [12; 32],
        capabilities,
    )
    .unwrap();
    assert_eq!(
        descriptor.validate(MappingRequirement::ManagedReplica),
        Err(MappingCompatibilityError::MissingCapability {
            mapping: "non-idempotent".into(),
            capability: RequiredMappingCapability::IdempotentReplay,
        })
    );
}

#[test]
fn hot_pluggable_mapping_requires_export_and_restore() {
    let mut capabilities = production_capabilities();
    capabilities.canonical_export = false;
    let descriptor = MappingDescriptorV1::new(
        "no-export",
        "1.0.0",
        BackendFamily::Sql,
        [13; 32],
        capabilities,
    )
    .unwrap();
    assert_eq!(
        descriptor.validate(MappingRequirement::HotPluggableReplica),
        Err(MappingCompatibilityError::MissingCapability {
            mapping: "no-export".into(),
            capability: RequiredMappingCapability::CanonicalExport,
        })
    );

    let mut capabilities = production_capabilities();
    capabilities.canonical_restore = false;
    let descriptor = MappingDescriptorV1::new(
        "no-restore",
        "1.0.0",
        BackendFamily::Sql,
        [15; 32],
        capabilities,
    )
    .unwrap();
    assert_eq!(
        descriptor.validate(MappingRequirement::HotPluggableReplica),
        Err(MappingCompatibilityError::MissingCapability {
            mapping: "no-restore".into(),
            capability: RequiredMappingCapability::CanonicalRestore,
        })
    );
}
