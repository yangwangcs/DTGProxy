use std::sync::Arc;

use gateway_node::{AdmissionController, AdmissionError};

#[test]
fn admission_is_bounded_and_releases_capacity_with_the_request_guard() {
    let admission = AdmissionController::new(2).unwrap();
    let first = admission.try_enter().unwrap();
    let second = admission.try_enter().unwrap();
    assert_eq!(admission.inflight(), 2);
    assert!(matches!(
        admission.try_enter(),
        Err(AdmissionError::Exhausted)
    ));

    drop(first);
    assert_eq!(admission.inflight(), 1);
    let replacement = admission.try_enter().unwrap();
    assert_eq!(admission.inflight(), 2);
    drop((second, replacement));
    assert_eq!(admission.inflight(), 0);
}

#[test]
fn closing_admission_rejects_new_work_without_revoking_inflight_work() {
    let admission = Arc::new(AdmissionController::new(1).unwrap());
    let permit = admission.try_enter().unwrap();
    admission.close();
    assert!(admission.is_closed());
    assert_eq!(admission.inflight(), 1);
    assert!(matches!(admission.try_enter(), Err(AdmissionError::Closed)));
    drop(permit);
    assert_eq!(admission.inflight(), 0);
}
