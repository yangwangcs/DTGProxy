use analytics_api::GraphModel;
use analytics_ledger::{AnalyticsJobId, GraphProjectionScope, JobSpec, ProjectionLimits};
use temporal_types::{TransactionTime, ValidTime};

#[test]
fn job_spec_exposes_every_immutable_scheduler_fence() {
    let transaction = TransactionTime::new(101, 7);
    let projection = GraphProjectionScope::Interval {
        valid_from: ValidTime::from_micros(11),
        valid_to: ValidTime::from_micros(19),
    };
    let limits = ProjectionLimits::new(23, 29, 31).unwrap();
    let spec = JobSpec::new(
        AnalyticsJobId::new(37).unwrap(),
        41,
        43,
        47,
        53,
        59,
        61,
        transaction,
        projection.clone(),
        "dtg.temporal.intervalComponents",
        "2.0.0",
        "analytics-native",
        "3.0.0",
        vec![67, 71],
        [73; 32],
        limits,
    )
    .unwrap();

    assert_eq!(spec.job_id(), AnalyticsJobId::new(37).unwrap());
    assert_eq!(spec.submission_request_id(), 41);
    assert_eq!(spec.graph_id(), 43);
    assert_eq!(spec.catalog_revision(), 47);
    assert_eq!(spec.topology_epoch(), 53);
    assert_eq!(spec.schema_version(), 59);
    assert_eq!(spec.backend_generation(), 61);
    assert_eq!(spec.transaction_time(), transaction);
    assert_eq!(spec.projection(), &projection);
    assert_eq!(spec.graph_model(), GraphModel::Interval);
    assert_eq!(spec.algorithm(), "dtg.temporal.intervalComponents");
    assert_eq!(spec.algorithm_version(), "2.0.0");
    assert_eq!(spec.provider(), "analytics-native");
    assert_eq!(spec.provider_version(), "3.0.0");
    assert_eq!(spec.parameters(), &[67, 71]);
    assert_eq!(spec.security_fingerprint(), [73; 32]);
    assert_eq!(spec.limits(), limits);
}

#[test]
fn projection_scope_canonically_determines_graph_model() {
    assert_eq!(
        GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(1),
        }
        .graph_model(),
        GraphModel::Snapshot
    );
    assert_eq!(GraphProjectionScope::Event.graph_model(), GraphModel::Event);
    assert_eq!(
        GraphProjectionScope::Delta {
            before: ValidTime::from_micros(1),
            after: ValidTime::from_micros(2),
        }
        .graph_model(),
        GraphModel::Delta
    );
}
