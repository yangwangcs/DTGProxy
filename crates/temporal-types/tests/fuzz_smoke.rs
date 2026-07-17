use temporal_types::CanonicalElement;

#[test]
fn fixed_seed_arbitrary_canonical_payloads_never_panic() {
    let mut seed = 0x44_54_50_31_u64;
    for _ in 0..10_000 {
        let length = usize::try_from(next(&mut seed) % 257).unwrap();
        let bytes = (0..length)
            .map(|_| u8::try_from(next(&mut seed) & 0xff).unwrap())
            .collect::<Vec<_>>();
        let _ = CanonicalElement::decode(&bytes);
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(2_862_933_555_777_941_757)
        .wrapping_add(3_037_000_493);
    *seed
}
