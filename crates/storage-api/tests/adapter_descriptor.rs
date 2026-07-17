use storage_api::{
    ADAPTER_SPI_VERSION, AdapterCapabilities, AdapterCompatibilityError, AdapterDescriptorV1,
    AdapterRequirement, BackendFamily, Durability, RequiredCapability, SnapshotCapability,
};

fn production_capabilities(snapshot: SnapshotCapability) -> AdapterCapabilities {
    AdapterCapabilities {
        local_atomic_batch: true,
        idempotent_apply: true,
        consistent_multi_get: true,
        ordered_scan: true,
        durable_applied_index: true,
        durability: Durability::Synchronous,
        snapshot,
        predicate_pushdown: false,
        adjacency_pushdown: false,
        change_feed: false,
    }
}

#[test]
fn managed_replica_accepts_physical_or_logical_snapshot_backends() {
    for (name, family, snapshot) in [
        (
            "rocksdb",
            BackendFamily::KeyValue,
            SnapshotCapability::PhysicalCheckpoint,
        ),
        (
            "postgresql",
            BackendFamily::Sql,
            SnapshotCapability::LogicalExport,
        ),
        (
            "neo4j",
            BackendFamily::PropertyGraph,
            SnapshotCapability::LogicalExport,
        ),
    ] {
        let descriptor = AdapterDescriptorV1::new(
            name,
            "contract-test",
            family,
            production_capabilities(snapshot),
        );
        assert_eq!(
            descriptor.validate(AdapterRequirement::ManagedReplica),
            Ok(())
        );
    }
}

#[test]
fn production_validation_fails_on_the_first_missing_safety_capability() {
    let mut capabilities = production_capabilities(SnapshotCapability::LogicalExport);
    capabilities.local_atomic_batch = false;
    let descriptor = AdapterDescriptorV1::new(
        "unsafe-graph",
        "test",
        BackendFamily::PropertyGraph,
        capabilities,
    );
    assert_eq!(
        descriptor.validate(AdapterRequirement::ManagedReplica),
        Err(AdapterCompatibilityError::MissingCapability {
            adapter: "unsafe-graph".to_owned(),
            capability: RequiredCapability::LocalAtomicBatch,
        })
    );
}

#[test]
fn analytical_or_memory_only_backends_cannot_claim_production_compatibility() {
    let mut capabilities = production_capabilities(SnapshotCapability::None);
    capabilities.durable_applied_index = false;
    capabilities.durability = Durability::Volatile;
    let descriptor = AdapterDescriptorV1::new(
        "memgraph-analytical",
        "test",
        BackendFamily::PropertyGraph,
        capabilities,
    );
    assert!(matches!(
        descriptor.validate(AdapterRequirement::ManagedReplica),
        Err(AdapterCompatibilityError::MissingCapability {
            capability: RequiredCapability::DurableAppliedIndex,
            ..
        })
    ));
    assert_eq!(descriptor.validate(AdapterRequirement::Development), Ok(()));
}

#[test]
fn incompatible_spi_version_is_rejected_before_capability_checks() {
    let descriptor = AdapterDescriptorV1::with_spi_version(
        ADAPTER_SPI_VERSION + 1,
        "future-adapter",
        "test",
        BackendFamily::KeyValue,
        production_capabilities(SnapshotCapability::PhysicalCheckpoint),
    );
    assert_eq!(
        descriptor.validate(AdapterRequirement::ManagedReplica),
        Err(AdapterCompatibilityError::SpiVersionMismatch {
            expected: ADAPTER_SPI_VERSION,
            actual: ADAPTER_SPI_VERSION + 1,
        })
    );
}
