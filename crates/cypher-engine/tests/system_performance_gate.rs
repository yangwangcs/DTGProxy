use cypher_engine::{
    LatencyPercentiles, PairedSystemPerformanceReport, PerformanceScenario, QueryScopedOverhead,
    QueryScopedOverheadLimits, ScaleOutPerformance, SystemPerformanceGateError,
    SystemPerformanceObservation,
};

fn overhead(sent_frames: u64, exchange_bytes: u64, peak_memory: u64) -> QueryScopedOverhead {
    QueryScopedOverhead::observed(
        1,
        1,
        4,
        sent_frames,
        exchange_bytes,
        exchange_bytes,
        0,
        peak_memory,
    )
}

fn identified(
    observation: Result<SystemPerformanceObservation, SystemPerformanceGateError>,
) -> SystemPerformanceObservation {
    observation
        .expect("performance observation")
        .with_result_identity([7; 32], 10)
        .expect("result identity")
}

fn scale_observation(
    node_count: u8,
    throughput_rows_per_second: u64,
    result_digest: [u8; 32],
) -> SystemPerformanceObservation {
    SystemPerformanceObservation::new(
        node_count,
        LatencyPercentiles::from_samples(vec![100]),
        LatencyPercentiles::from_samples(vec![50]),
        throughput_rows_per_second,
        1_000,
        1_000,
        0,
        overhead(0, 0, 1_000),
    )
    .expect("scale-out observation")
    .with_result_identity(result_digest, 10)
    .expect("scale-out result identity")
}

#[test]
fn release_gate_accepts_the_documented_point_lookup_boundaries() {
    let direct = identified(SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![100, 100, 100]),
        LatencyPercentiles::from_samples(vec![100, 100, 100]),
        10_000,
        1_000,
        10_000,
        0,
        overhead(0, 0, 1_000),
    ));
    let proxy = identified(SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![175, 175, 400]),
        LatencyPercentiles::from_samples(vec![100, 100, 350]),
        9_000,
        1_050,
        10_000,
        812,
        overhead(2, 812, 1_050),
    ));
    let report =
        PairedSystemPerformanceReport::new(PerformanceScenario::PointLookup, direct, proxy)
            .expect("paired report");

    report
        .evaluate_release(
            1_050,
            &QueryScopedOverheadLimits::new(1, 1, 4, 2, 812, 812, 0, 1_050),
        )
        .expect("documented point lookup limits");
}

#[test]
fn scale_out_gate_accepts_the_documented_four_and_eight_node_boundaries() {
    ScaleOutPerformance::new(1_000, 2_800, 5_000)
        .expect("scale-out samples")
        .evaluate_release()
        .expect("documented scale-out limits");
}

#[test]
fn release_gate_rejects_single_shard_throughput_below_ninety_percent() {
    let direct = identified(SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![1_000]),
        LatencyPercentiles::from_samples(vec![100]),
        10_000,
        1_000,
        1_000,
        0,
        overhead(0, 0, 1_000),
    ));
    let proxy = identified(SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![1_100]),
        LatencyPercentiles::from_samples(vec![200]),
        8_999,
        1_000,
        1_000,
        0,
        overhead(0, 0, 1_000),
    ));
    let error =
        PairedSystemPerformanceReport::new(PerformanceScenario::SingleShardScan, direct, proxy)
            .expect("paired report")
            .evaluate_release(
                2_000,
                &QueryScopedOverheadLimits::new(1, 1, 4, 0, 0, 0, 0, 2_000),
            )
            .expect_err("throughput below 90% must block release");

    assert!(matches!(
        error,
        SystemPerformanceGateError::ThroughputBelowLimit {
            observed_rows_per_second: 8_999,
            required_rows_per_second: 9_000,
        }
    ));
}

#[test]
fn scale_out_gate_rejects_results_just_below_the_documented_boundaries() {
    let error = ScaleOutPerformance::new(1_000, 2_799, 5_000)
        .expect("scale-out samples")
        .evaluate_release()
        .expect_err("4-node throughput below 2.8x must block release");

    assert!(matches!(
        error,
        SystemPerformanceGateError::ScaleOutBelowLimit {
            nodes: 4,
            observed_rows_per_second: 2_799,
            required_rows_per_second: 2_800,
        }
    ));
}

#[test]
fn scale_out_gate_rejects_semantically_different_results() {
    let one = scale_observation(1, 1_000, [1; 32]);
    let four = scale_observation(4, 2_800, [2; 32]);
    let eight = scale_observation(8, 5_000, [1; 32]);

    let error = ScaleOutPerformance::from_observations(&one, &four, &eight)
        .expect_err("scale-out comparisons require identical results");

    assert!(matches!(error, SystemPerformanceGateError::ResultMismatch));
}

#[test]
fn release_gate_rejects_peak_memory_above_direct_plus_five_percent() {
    let direct = identified(SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![100]),
        LatencyPercentiles::from_samples(vec![50]),
        10_000,
        1_000,
        1_000,
        0,
        overhead(0, 0, 1_000),
    ));
    let proxy = identified(SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![100]),
        LatencyPercentiles::from_samples(vec![50]),
        10_000,
        1_051,
        1_000,
        0,
        overhead(0, 0, 1_051),
    ));
    let error = PairedSystemPerformanceReport::new(PerformanceScenario::PointLookup, direct, proxy)
        .expect("paired report")
        .evaluate_release(
            2_000,
            &QueryScopedOverheadLimits::new(1, 1, 4, 0, 0, 0, 0, 2_000),
        )
        .expect_err("memory above direct plus 5% must block release");

    assert!(matches!(
        error,
        SystemPerformanceGateError::PeakMemoryExceeded {
            observed: 1_051,
            limit: 1_050,
        }
    ));
}

#[test]
fn release_gate_rejects_exchange_bytes_above_frame_and_payload_allowance() {
    let direct = identified(SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![100]),
        LatencyPercentiles::from_samples(vec![50]),
        10_000,
        1_000,
        10_000,
        0,
        overhead(0, 0, 1_000),
    ));
    let proxy = identified(SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![100]),
        LatencyPercentiles::from_samples(vec![50]),
        10_000,
        1_000,
        10_000,
        813,
        overhead(2, 813, 1_000),
    ));
    let error = PairedSystemPerformanceReport::new(PerformanceScenario::PointLookup, direct, proxy)
        .expect("paired report")
        .evaluate_release(
            2_000,
            &QueryScopedOverheadLimits::new(1, 1, 4, 2, 1_000, 1_000, 0, 2_000),
        )
        .expect_err("exchange bytes above frame plus payload allowance must block release");

    assert!(matches!(
        error,
        SystemPerformanceGateError::ExchangeBytesExceeded {
            observed: 813,
            limit: 812,
        }
    ));
}

#[test]
fn paired_system_report_rejects_semantically_different_results() {
    let direct = SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![100]),
        LatencyPercentiles::from_samples(vec![50]),
        10_000,
        1_000,
        1_000,
        0,
        overhead(0, 0, 1_000),
    )
    .expect("direct observation")
    .with_result_identity([1; 32], 10)
    .expect("direct result identity");
    let proxy = SystemPerformanceObservation::new(
        1,
        LatencyPercentiles::from_samples(vec![100]),
        LatencyPercentiles::from_samples(vec![50]),
        10_000,
        1_000,
        1_000,
        0,
        overhead(0, 0, 1_000),
    )
    .expect("proxy observation")
    .with_result_identity([2; 32], 10)
    .expect("proxy result identity");

    let error = PairedSystemPerformanceReport::new(PerformanceScenario::PointLookup, direct, proxy)
        .expect_err("different result digests must block performance comparison");

    assert!(matches!(error, SystemPerformanceGateError::ResultMismatch));
}
