use temporal_query::parse;

#[test]
fn fixed_seed_arbitrary_ascii_queries_never_panic_or_exceed_parser_bounds() {
    let mut seed = 0x51_75_45_52_59_u64;
    for case in 0..10_000_usize {
        let length = if case % 997 == 0 {
            4_097
        } else {
            usize::try_from(next(&mut seed) % 160).unwrap()
        };
        let mut input = String::with_capacity(length);
        for _ in 0..length {
            let byte = 0x20 + u8::try_from(next(&mut seed) % 95).unwrap();
            input.push(char::from(byte));
        }
        let _ = parse(&input);
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}
