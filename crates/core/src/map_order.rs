// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The order in which the service visits the members of one request map.

/// The members of a map in the order the service visits them, given in the
/// order the request sends them: the iteration order of a Java `HashMap`
/// filled from the request's map. The service prints the maps of a statement
/// list's echo in this order (measured 2026-11-02 on maps of 2, 3, 4, 6, 12,
/// 13, 24 and 48 members). It checks the members of one
/// `ExpressionAttributeValues` map and of one PartiQL parameter map in this
/// order too (measured 2026-11-12 and 2026-11-19 on maps of 2, 3 and 4
/// members).
///
/// The table has the smallest power of two of buckets at or above
/// `size / 0.75` rounded up. A key sits in the bucket its Java
/// `String.hashCode` picks once the hash's high 16 bits are folded into its
/// low 16 by exclusive or. The buckets come in ascending order, and the keys
/// of one bucket in the order the request sends them.
///
/// The keys of one bucket keep the order `members` yields them in. For a
/// parsed request that is the request's order in every build that includes
/// the server: its crate turns on serde_json's `preserve_order` feature.
pub fn hash_map_order<K: AsRef<str>, V>(members: impl IntoIterator<Item = (K, V)>) -> Vec<(K, V)> {
    let mut members: Vec<(K, V)> = members.into_iter().collect();
    // `size / 0.75` rounded up, in whole numbers.
    let buckets = (members.len() * 4).div_ceil(3).next_power_of_two();
    // The sort is stable, so the keys of one bucket keep their order.
    members.sort_by_cached_key(|(key, _)| bucket(key.as_ref(), buckets));
    members
}

/// The bucket of `key` in a Java `HashMap` table of `buckets` buckets.
fn bucket(key: &str, buckets: usize) -> usize {
    // Java's `String.hashCode`: 31 times the hash plus each UTF-16 unit,
    // wrapping at 32 bits.
    let hash = key.encode_utf16().fold(0_u32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(u32::from(unit))
    });
    (hash ^ (hash >> 16)) as usize & (buckets - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The keys of `members`, given in this order, in the order the service
    /// visits them.
    fn visited<'a>(members: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
        hash_map_order(members.into_iter().map(|key| (key, ())))
            .into_iter()
            .map(|(key, ())| key)
            .collect()
    }

    #[test]
    fn map_members_come_in_the_measured_order() {
        assert_eq!(visited(["c", "a", "p"]), ["p", "a", "c"]);
        assert_eq!(
            visited(["zeta", "alpha", "mid", "beta"]),
            ["zeta", "mid", "beta", "alpha"]
        );
        let keys: Vec<String> = (0..=12).rev().map(|i| format!("k{i}")).collect();
        assert_eq!(
            visited(keys.iter().map(String::as_str)),
            [
                "k0", "k1", "k2", "k3", "k4", "k11", "k5", "k10", "k6", "k7", "k12", "k8", "k9"
            ]
        );
    }

    #[test]
    fn the_table_holds_size_over_three_quarters_rounded_up() {
        // 3 members take 4 buckets and 6 take 8, not 8 and 16.
        assert_eq!(visited(["m0", "m1", "m2"]), ["m1", "m2", "m0"]);
        assert_eq!(
            visited(["k0", "k1", "k2", "k3", "k4", "k5"]),
            ["k3", "k4", "k5", "k0", "k1", "k2"]
        );
    }

    #[test]
    fn keys_that_share_a_bucket_keep_the_order_they_come_in() {
        // q and a share bucket 1 of 4.
        assert_eq!(bucket("q", 4), bucket("a", 4));
        assert_eq!(visited(["q", "a"]), ["q", "a"]);
        assert_eq!(visited(["a", "q"]), ["a", "q"]);
    }

    #[test]
    fn a_key_hashes_over_its_utf16_units() {
        // 0xD83D * 31 + 0xDE00 = 1772899, whose folded low five bits are 24.
        assert_eq!(bucket("\u{1F600}", 32), 24);
        assert_eq!(bucket("a", 1), 0);
    }
}
