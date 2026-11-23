// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The order in which the service visits the members of a `Scan` request's
//! maps, read from the request text and fitted to its hash table.

use std::collections::HashSet;

use serde::Deserialize;
use serde_json::Value;

/// The member names of the body's `ExclusiveStartKey`, `ExpressionAttributeValues`
/// and `ScanFilter` in the order the request wrote them, read from the raw
/// request text in one pass ([`member_orders`]), since the parsed body sorts
/// object members on every build but the MongoDB one. A field is `None` when
/// the body has no such field; every field is `None` when the text does not
/// parse or one of the three is not an object, which the shape fault check
/// refuses first. A name that appears twice keeps its first position, and a
/// field that appears twice is read at its first position.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct MemberOrders {
    pub(crate) key: Option<Vec<String>>,
    pub(crate) values: Option<Vec<String>>,
    pub(crate) filter: Option<Vec<String>>,
}

/// Reads the [`MemberOrders`] of `raw_body`, in time linear in its length.
pub(crate) fn member_orders(raw_body: &[u8]) -> MemberOrders {
    /// The member names of one JSON object, in document order.
    struct MemberNames(Vec<String>);

    impl<'de> Deserialize<'de> for MemberNames {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct NamesVisitor;

            impl<'de> serde::de::Visitor<'de> for NamesVisitor {
                type Value = MemberNames;

                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("an object")
                }

                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> Result<MemberNames, A::Error> {
                    let mut names: Vec<String> = Vec::new();
                    let mut seen: HashSet<String> = HashSet::new();
                    while let Some(name) = map.next_key::<String>()? {
                        map.next_value::<serde::de::IgnoredAny>()?;
                        if seen.insert(name.clone()) {
                            names.push(name);
                        }
                    }
                    Ok(MemberNames(names))
                }
            }

            deserializer.deserialize_map(NamesVisitor)
        }
    }

    /// The top-level object, with only the member names of the three fields
    /// read.
    struct FieldNames;

    impl<'de> serde::de::Visitor<'de> for FieldNames {
        type Value = MemberOrders;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an object")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut orders = MemberOrders::default();
            while let Some(name) = map.next_key::<String>()? {
                let slot = match name.as_str() {
                    "ExclusiveStartKey" => &mut orders.key,
                    "ExpressionAttributeValues" => &mut orders.values,
                    "ScanFilter" => &mut orders.filter,
                    _ => {
                        map.next_value::<serde::de::IgnoredAny>()?;
                        continue;
                    }
                };
                if slot.is_none() {
                    *slot = Some(map.next_value::<MemberNames>()?.0);
                } else {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
            Ok(orders)
        }
    }

    let mut deserializer = serde_json::Deserializer::from_slice(raw_body);
    serde::Deserializer::deserialize_map(&mut deserializer, FieldNames).unwrap_or_default()
}

/// The order in which the service visits the members of an `ExclusiveStartKey`
/// with faults in more than one member, and the entries of
/// `ExpressionAttributeValues` and the `ScanFilter` conditions in order U,
/// fitted to the measured rows: the iteration order of a Java `HashMap` over
/// the names, that is `String.hashCode()` of the name, spread as `h ^ (h >>>
/// 16)`, masked to the table's buckets, the buckets in ascending order and the
/// request's order inside a bucket. The table starts with four buckets and
/// doubles when it holds more than three quarters of its size, and when a
/// name put in the request's order is the ninth in its bucket while the table
/// has fewer than 64 buckets ([`hash_table_mask`]): eight buckets for four to
/// six names spread over the buckets. With four
/// buckets `sk` falls in bucket 0, `a` in 1, `b` in 2, `pk`, `gpk`, `id` and
/// `ts` in 3; with eight, `sk` in 0, `gpk` and `pk` in 3, `gsk` in 6.
///
/// Measured (2026-10-31, 2026-11-04 and 2026-11-05, six tables, every row
/// with a shape fault in one of two faulty members, both request orders): on
/// the `pk`, `sk` table the service names `sk`; on HASH `a`, RANGE `b` it
/// names `a`; on HASH `pk`, RANGE `a` it names `a`; on `ts`, `id` and on `id`,
/// `ts` it names the member first in the request; on HASH `ä`, RANGE `é` it
/// names `ä`; on the GSI `{"gpk": {}, "pk": {"N": "bob"}, "sk": {"S": "x"}}`
/// it names `gpk`; on a GSI with a range key, where the key has four members,
/// it names `gpk` ahead of `gsk` and `pk` ahead of `gsk`, which four buckets
/// would reverse. In `ExpressionAttributeValues` (order U, 2026-11-05) it
/// names `:n` ahead of `:e`, `:b` ahead of `:p` and `:a`, and `:a` ahead of
/// `:e` when `:a` comes first in the request; on 2026-11-11 and 2026-11-23,
/// `:z` ahead of `:u`, `:b` ahead of `:a` and `:n` ahead of `:a` (see
/// `older_form_checks`). The rule is fitted to those rows. The service does
/// not document it. With two value faults and no shape fault the service's
/// own two sends disagreed on 19 of 40 matrix cells, so no order is pinned
/// there. Measured 2026-11-05 as well: a key of five members and one of seven
/// on the same GSI (eight and sixteen buckets), four and seven
/// `ExpressionAttributeValues` entries, two `ScanFilter` conditions, a key
/// name outside the Basic Multilingual Plane (`𝄞`, hashed as its two UTF-16
/// code units) and the request's order inside one bucket for the entries
/// (`:e` ahead of `:a` when `:e` comes first); each as the rule gives.
/// Measured 2026-11-23 (21:49 UTC, each body twice): of eight, nine and ten
/// `ExpressionAttributeValues` entries in bucket 10 of sixteen, `:aa` first
/// and `:dt` last in the request and both empty, the eight name `:aa` and the
/// nine and ten name `:dt`, which the table doubled at the ninth name in one
/// bucket puts in bucket 10 of 32, ahead of the others in 26. A bucket of a
/// table of 64 buckets or more that reaches nine names becomes a tree in Java,
/// which moves one of its names to the front of the bucket; the model keeps
/// the request's order there (not measured).
pub(super) fn fitted_member_order<'a>(
    names_in_request_order: impl Iterator<Item = &'a str>,
) -> Vec<String> {
    let names: Vec<&str> = names_in_request_order.collect();
    let hashes: Vec<u32> = names.iter().map(|name| java_hash(name)).collect();
    let mask = hash_table_mask(&hashes);
    let mut names: Vec<(u32, usize, &str)> = names
        .into_iter()
        .zip(&hashes)
        .enumerate()
        .map(|(position, (name, hash))| (hash & mask, position, name))
        .collect();
    names.sort_unstable();
    names
        .into_iter()
        .map(|(_, _, name)| name.to_owned())
        .collect()
}

/// The bucket mask of a Java `HashMap` that starts with four buckets after
/// names with the spread hashes `hashes` are put in that order: the table
/// doubles when it holds more than three quarters of its size (3 for up to
/// three names, 7 for four to six, 15 for seven to twelve, and so on), and
/// when a put makes one bucket hold nine names while the table has fewer than
/// 64 buckets (`HashMap.treeifyBin`). The count of each bucket is read again
/// after each doubling, so the work grows with the name count times the
/// number of doublings.
pub(super) fn hash_table_mask(hashes: &[u32]) -> u32 {
    let bucket_counts = |hashes: &[u32], buckets: u32| {
        let mut counts = vec![0usize; usize::try_from(buckets).unwrap_or(usize::MAX)];
        for hash in hashes {
            if let Some(count) = usize::try_from(hash & (buckets - 1))
                .ok()
                .and_then(|bucket| counts.get_mut(bucket))
            {
                *count += 1;
            }
        }
        counts
    };
    let mut buckets: u32 = 4;
    let mut counts = bucket_counts(&[], buckets);
    for (put, hash) in hashes.iter().enumerate() {
        let full_bucket = usize::try_from(hash & (buckets - 1))
            .ok()
            .and_then(|bucket| counts.get_mut(bucket))
            .is_some_and(|count| {
                *count += 1;
                *count >= 9
            });
        let mut doubled = false;
        if full_bucket && buckets < 64 {
            buckets *= 2;
            doubled = true;
        }
        if u64::try_from(put + 1).unwrap_or(u64::MAX) > u64::from(buckets) * 3 / 4 {
            buckets = buckets.saturating_mul(2);
            doubled = true;
        }
        if doubled {
            counts = bucket_counts(&hashes[..=put], buckets);
        }
    }
    buckets - 1
}

/// `String.hashCode()` of `name` in Java, over its UTF-16 code units, spread
/// as `h ^ (h >>> 16)` as a `HashMap` spreads it before masking it to its
/// buckets. The arithmetic wraps as Java's `int` does.
pub(super) fn java_hash(name: &str) -> u32 {
    let hash = name.encode_utf16().fold(0u32, |h, unit| {
        h.wrapping_mul(31).wrapping_add(u32::from(unit))
    });
    hash ^ (hash >> 16)
}

/// The names of the object `field` of the body in the fitted order
/// ([`fitted_member_order`]): the request's order when `request_order` names
/// exactly the object's members, else the parsed body's order.
pub(super) fn fitted_order_of(
    body: &Value,
    field: &str,
    request_order: Option<&[String]>,
) -> Vec<String> {
    let Some(members) = body.get(field).and_then(Value::as_object) else {
        return Vec::new();
    };
    let parsed: Vec<&str> = members.keys().map(String::as_str).collect();
    let in_request: Vec<&str> = match request_order {
        Some(names)
            if names.len() == parsed.len()
                && names.iter().all(|name| members.contains_key(name)) =>
        {
            names.iter().map(String::as_str).collect()
        }
        _ => parsed,
    };
    fitted_member_order(in_request.into_iter())
}
