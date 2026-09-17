//! The two hashes the pipeline's own decisions share: FNV-1a over bytes, and the splitmix64
//! finalizer that spreads a 64-bit value over the whole space. `sample` and `dedupe` key
//! on them, and so does a record's trace (see [`crate::trace`]), so each has one
//! definition.

/// FNV-1a, 64-bit: stable across builds and platforms, and cheap next to a store round trip.
#[must_use]
pub const fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    let mut i = 0;
    while i < bytes.len() {
        hash = (hash ^ bytes[i] as u64).wrapping_mul(PRIME);
        i += 1;
    }
    hash
}

/// The splitmix64 finalizer: a bijection on `u64` that spreads nearby inputs (sequential
/// record ids, hashes of similar keys) evenly over the whole space, so a threshold on the
/// output keeps the configured share of any input population. It maps 0 to 0.
#[must_use]
pub const fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
