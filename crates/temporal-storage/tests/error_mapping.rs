use storage_api::AdapterError;
use temporal_storage::TemporalStoreError;

#[test]
fn scan_response_body_exhaustion_preserves_wire_byte_units_and_values() {
    assert_eq!(
        TemporalStoreError::from(AdapterError::ScanResponseByteLimit {
            limit: 64,
            required: 65,
        }),
        TemporalStoreError::ScanResponseByteLimit {
            limit: 64,
            required: 65,
        }
    );
}
