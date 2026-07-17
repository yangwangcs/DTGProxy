use storage_api::{Keyspace, LogicalKey};
use temporal_storage::{
    EdgeIdentity, HistoryEntry, ProjectionRecord, VertexIdentity, decode_graph_key,
};

#[test]
fn fixed_seed_arbitrary_graph_keys_and_records_never_panic() {
    let mut seed = 0x44_54_47_50_u64;
    for _ in 0..10_000 {
        let length = usize::try_from(next(&mut seed) % 257).unwrap();
        let bytes = (0..length)
            .map(|_| u8::try_from(next(&mut seed) & 0xff).unwrap())
            .collect::<Vec<_>>();
        let keyspace = Keyspace::ALL[usize::try_from(next(&mut seed) % 8).unwrap()];
        let key = LogicalKey::in_keyspace(keyspace, bytes.clone());
        let _ = decode_graph_key(&key);
        let _ = VertexIdentity::decode(&bytes);
        let _ = EdgeIdentity::decode(&bytes);
        let _ = ProjectionRecord::decode(&bytes);
        let _ = HistoryEntry::decode(&bytes);
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(3_202_034_522_624_059_733).wrapping_add(1);
    *seed
}
