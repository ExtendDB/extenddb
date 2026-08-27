// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Vector-index write metering.
//!
//! Vector indexes bill in their own units, separate from table read and write
//! capacity. Writes replicated into an index are reported as
//! `ConsumedCapacity.VectorIndexes.<indexName>.VectorWriteRequestBytes`.
//!
//! The model here was measured against the live service in us-east-1 on
//! 2026-08-13 across 20 probes, and reproduces every one of them exactly (not
//! approximately). Three blind predictions computed before measuring also
//! matched to the byte. Unlike the search-side figure, which the service
//! reports non-deterministically in one of two modes, the write figure is
//! deterministic: four identical writes reported an identical value at both
//! 1024 and 2048 dimensions.

use std::collections::BTreeSet;

use crate::types::{Item, attribute_value_size};

/// Byte floor for a single vector-index write.
///
/// Measured: an 8-dimension index reported exactly 1024 for a small item,
/// where the unfloored model gives 47.
pub const VECTOR_WRITE_FLOOR_BYTES: f64 = 1024.0;

/// Bytes metered per vector dimension.
///
/// Derived from (8207 - 4111) / (2048 - 1024) = 4.0, i.e. one f32 per
/// component: the index stores the raw vector, so the wire representation is
/// irrelevant. Confirmed by writing the same vector with 1-character and
/// 20-character numbers, which reported the same figure.
pub const BYTES_PER_DIMENSION: f64 = 4.0;

/// Which attributes an index projects, for metering purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectedAttributes<'a> {
    /// Every attribute of the item is projected (`ProjectionType: ALL`).
    All,
    /// Only the table key, the vector attribute, and the search-schema
    /// attributes are projected (`ProjectionType: KEYS_ONLY`).
    ///
    /// Measured: a 2000-byte non-projected attribute did not change the figure
    /// under KEYS_ONLY, where it added exactly its own size under ALL.
    KeysOnly {
        /// Table key attribute names, always projected.
        key_attributes: &'a [&'a str],
        /// Search-schema attribute names, always projected.
        search_schema_attributes: &'a [&'a str],
    },
}

/// Bytes reported for replicating one item image into one vector index.
///
/// `image` is the item as it exists in the index after the write, or the
/// deleted image for a delete. `vector_attribute` contributes only its NAME to
/// the byte count, because its payload is metered by the dimension term
/// instead; this is not an approximation, it is what makes the measured
/// arithmetic come out exactly.
///
/// This is the charge for ONE index entry. A write that moves an entry between
/// search-schema partitions is charged twice; see
/// [`search_schema_partition_moved`].
#[must_use]
pub fn vector_write_request_bytes(
    dimensions: u32,
    image: &Item,
    vector_attribute: &str,
    projected: ProjectedAttributes<'_>,
) -> f64 {
    let projected_names: Option<BTreeSet<&str>> = match projected {
        ProjectedAttributes::All => None,
        ProjectedAttributes::KeysOnly {
            key_attributes,
            search_schema_attributes,
        } => Some(
            key_attributes
                .iter()
                .chain(search_schema_attributes.iter())
                .copied()
                .collect(),
        ),
    };

    // The dimension term is charged unconditionally, so a vectorless image
    // would be mispriced: callers must only pass an image that is actually in
    // the index (the wiring in `vector_write_charges` guarantees this by
    // selecting the image that carries the vector attribute).
    debug_assert!(
        image.contains_key(vector_attribute),
        "vector_write_request_bytes charged for an image without the vector attribute"
    );

    let mut bytes = 0usize;
    for (name, value) in image {
        if name == vector_attribute {
            // Name only: the vector's payload is the dimension term.
            bytes += name.len();
            continue;
        }
        let included = projected_names
            .as_ref()
            .is_none_or(|allowed| allowed.contains(name.as_str()));
        if included {
            bytes += name.len() + attribute_value_size(value);
        }
    }

    let unfloored = f64::from(dimensions) * BYTES_PER_DIMENSION + bytes as f64;
    unfloored.max(VECTOR_WRITE_FLOOR_BYTES)
}

/// Whether a write changes what the index holds, and so is charged at all.
///
/// Measured behaviour, which is NOT what the public documentation describes.
/// The docs say "writes that do not change an indexed attribute do not incur
/// vector write capacity"; the service actually charges whenever the PROJECTED
/// entry changes:
///
/// * setting the vector to a byte-identical value is NOT charged;
/// * changing a non-indexed attribute IS charged under `ALL`, because the
///   projection includes it, and is NOT charged under `KEYS_ONLY`;
/// * an item with no vector attribute is never in the index, so neither
///   writing nor deleting it is charged.
#[must_use]
pub fn projected_entry_changed(
    before: Option<&Item>,
    after: Option<&Item>,
    vector_attribute: &str,
    projected: ProjectedAttributes<'_>,
) -> bool {
    let in_index =
        |image: Option<&Item>| -> bool { image.is_some_and(|i| i.contains_key(vector_attribute)) };
    let (was, is) = (in_index(before), in_index(after));
    if !was && !is {
        // Never in the index: nothing is replicated either way.
        return false;
    }
    if was != is {
        // Entering or leaving the index always replicates.
        return true;
    }
    // Present both sides: compare only what the index actually holds.
    let (Some(b), Some(a)) = (before, after) else {
        return true;
    };
    match projected {
        ProjectedAttributes::All => b != a,
        ProjectedAttributes::KeysOnly {
            key_attributes,
            search_schema_attributes,
        } => key_attributes
            .iter()
            .chain(search_schema_attributes.iter())
            .chain(std::iter::once(&vector_attribute))
            .any(|name| b.get(*name) != a.get(*name)),
    }
}

/// Whether the entry moves between search-schema partitions, which the service
/// charges as a delete plus an insert.
///
/// Measured: changing the search-schema HASH value on a 1024-dimension index
/// reported 8224, exactly twice the 4112 a single write of that image costs.
#[must_use]
pub fn search_schema_partition_moved(
    before: Option<&Item>,
    after: Option<&Item>,
    search_schema_hash_attribute: Option<&str>,
) -> bool {
    let Some(hash_attr) = search_schema_hash_attribute else {
        return false;
    };
    match (before, after) {
        (Some(b), Some(a)) => {
            let (bv, av) = (b.get(hash_attr), a.get(hash_attr));
            bv.is_some() && av.is_some() && bv != av
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AttributeValue;

    fn emb(n: usize) -> AttributeValue {
        AttributeValue::L(
            (0..n)
                .map(|_| AttributeValue::N("0.1".to_owned()))
                .collect(),
        )
    }

    /// Build the exact item shape used by the live probes.
    fn probe_item(pk: &str, dims: usize, blob: Option<usize>) -> Item {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
        item.insert("tenant".to_owned(), AttributeValue::S("t1".to_owned()));
        item.insert("emb".to_owned(), emb(dims));
        if let Some(n) = blob {
            item.insert("blob".to_owned(), AttributeValue::S("x".repeat(n)));
        }
        item
    }

    fn all() -> ProjectedAttributes<'static> {
        ProjectedAttributes::All
    }

    fn keys_only() -> ProjectedAttributes<'static> {
        ProjectedAttributes::KeysOnly {
            key_attributes: &["pk"],
            search_schema_attributes: &["tenant"],
        }
    }

    // Each case below is a figure MEASURED against the live service on
    // 2026-08-13, us-east-1, account 964157134968. They are regression pins on
    // real service behaviour, not on our own arithmetic.

    #[test]
    fn measured_8_dims_small_item_hits_the_floor() {
        let got = vector_write_request_bytes(8, &probe_item("a1", 8, None), "emb", all());
        assert_eq!(got, 1024.0, "measured 1024 (unfloored model gives 47)");
    }

    #[test]
    fn measured_8_dims_with_2000_byte_attribute() {
        let got = vector_write_request_bytes(8, &probe_item("a2", 8, Some(2000)), "emb", all());
        assert_eq!(got, 2051.0, "measured 2051");
    }

    #[test]
    fn measured_1024_dims_small_item() {
        let got = vector_write_request_bytes(1024, &probe_item("b1", 1024, None), "emb", all());
        assert_eq!(got, 4111.0, "measured 4111");
    }

    #[test]
    fn measured_2048_dims_small_item() {
        let got = vector_write_request_bytes(2048, &probe_item("b1", 2048, None), "emb", all());
        assert_eq!(got, 8207.0, "measured 8207");
    }

    #[test]
    fn measured_1024_dims_with_2000_byte_attribute() {
        let got =
            vector_write_request_bytes(1024, &probe_item("b2", 1024, Some(2000)), "emb", all());
        assert_eq!(got, 6115.0, "measured 6115");
    }

    #[test]
    fn measured_2048_dims_with_2000_byte_attribute() {
        let got =
            vector_write_request_bytes(2048, &probe_item("b2", 2048, Some(2000)), "emb", all());
        assert_eq!(got, 10211.0, "measured 10211");
    }

    #[test]
    fn measured_delete_image_longer_key() {
        let got = vector_write_request_bytes(1024, &probe_item("del1", 1024, None), "emb", all());
        assert_eq!(got, 4113.0, "measured 4113 on delete of pk=del1");
    }

    #[test]
    fn measured_delete_image_longer_key_with_blob() {
        let got =
            vector_write_request_bytes(1024, &probe_item("del2", 1024, Some(2000)), "emb", all());
        assert_eq!(got, 6117.0, "measured 6117");
    }

    /// Under KEYS_ONLY the unprojected 2000-byte attribute must not be counted,
    /// giving the same figure as the item without it. Measured 4111 for both.
    #[test]
    fn measured_keys_only_ignores_unprojected_attribute() {
        let with_blob = vector_write_request_bytes(
            1024,
            &probe_item("k1", 1024, Some(2000)),
            "emb",
            keys_only(),
        );
        assert_eq!(with_blob, 4111.0, "measured 4111 under KEYS_ONLY");
        let without =
            vector_write_request_bytes(1024, &probe_item("k1", 1024, None), "emb", keys_only());
        assert_eq!(with_blob, without, "projection must make these identical");
    }

    /// The same item under ALL does count it, which is what makes the previous
    /// test discriminating rather than a tautology.
    #[test]
    fn projection_all_counts_what_keys_only_ignores() {
        let item = probe_item("k1", 1024, Some(2000));
        let a = vector_write_request_bytes(1024, &item, "emb", all());
        let k = vector_write_request_bytes(1024, &item, "emb", keys_only());
        assert_eq!(a, 6115.0, "same shape measured 6115 under ALL");
        assert_eq!(k, 4111.0);
        assert!(a > k);
    }

    /// Blind prediction from the live run: pk="pred-g" plus three small
    /// attributes measured 4127.
    #[test]
    fn measured_blind_prediction_case() {
        let mut item = probe_item("pred-g", 1024, None);
        item.insert("a".to_owned(), AttributeValue::S("1".to_owned()));
        item.insert("bb".to_owned(), AttributeValue::S("22".to_owned()));
        item.insert("ccc".to_owned(), AttributeValue::S("333".to_owned()));
        let got = vector_write_request_bytes(1024, &item, "emb", all());
        assert_eq!(got, 4127.0, "predicted and measured 4127");
    }

    #[test]
    fn identical_rewrite_is_not_charged() {
        let item = probe_item("a1", 8, None);
        assert!(!projected_entry_changed(
            Some(&item),
            Some(&item),
            "emb",
            all()
        ));
    }

    #[test]
    fn item_without_vector_is_never_charged() {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("a3".to_owned()));
        assert!(!projected_entry_changed(None, Some(&item), "emb", all()));
        assert!(!projected_entry_changed(Some(&item), None, "emb", all()));
    }

    #[test]
    fn entering_and_leaving_the_index_is_charged() {
        let with_vec = probe_item("a1", 8, None);
        let mut without = Item::new();
        without.insert("pk".to_owned(), AttributeValue::S("a1".to_owned()));
        assert!(projected_entry_changed(None, Some(&with_vec), "emb", all()));
        assert!(projected_entry_changed(Some(&with_vec), None, "emb", all()));
        assert!(projected_entry_changed(
            Some(&without),
            Some(&with_vec),
            "emb",
            all()
        ));
    }

    /// Measured divergence from the public documentation: a non-indexed
    /// attribute change IS charged under ALL and is NOT under KEYS_ONLY.
    #[test]
    fn non_indexed_attribute_change_follows_the_projection() {
        let before = probe_item("mv1", 8, None);
        let mut after = before.clone();
        after.insert("label".to_owned(), AttributeValue::S("zz".to_owned()));
        assert!(
            projected_entry_changed(Some(&before), Some(&after), "emb", all()),
            "ALL projects it, so it is charged"
        );
        assert!(
            !projected_entry_changed(Some(&before), Some(&after), "emb", keys_only()),
            "KEYS_ONLY does not project it, so it is not charged"
        );
    }

    #[test]
    fn search_schema_move_detected_only_on_a_real_change() {
        let before = probe_item("mv1", 8, None);
        let mut after = before.clone();
        after.insert("tenant".to_owned(), AttributeValue::S("t9".to_owned()));
        assert!(search_schema_partition_moved(
            Some(&before),
            Some(&after),
            Some("tenant")
        ));
        assert!(!search_schema_partition_moved(
            Some(&before),
            Some(&before),
            Some("tenant")
        ));
        assert!(
            !search_schema_partition_moved(Some(&before), Some(&after), None),
            "an index with no search-schema HASH cannot move"
        );
    }

    /// Measured: the search-schema change at 1024 dimensions reported 8224,
    /// exactly twice the 4112 one write of that image costs.
    #[test]
    fn measured_search_schema_move_is_exactly_double() {
        let single = vector_write_request_bytes(1024, &probe_item("mv1", 1024, None), "emb", all());
        assert_eq!(single, 4112.0);
        assert_eq!(single * 2.0, 8224.0, "measured 8224 for the move");
    }

    /// The 1024-byte floor, asserted at the boundary so an off-by-one in the
    /// `max` direction cannot hide. Arithmetic pin, not a service measurement:
    /// probe_item("b", 250, blob) carries 14 fixed non-vector bytes (pk 2+1,
    /// tenant 6+2, emb name 3), so 250 dims = 1000 + 14 = 1014 unfloored, and
    /// a blob adds its name (4) plus its length.
    #[test]
    fn the_floor_binds_below_and_releases_above_1024() {
        // Below the floor: 1014 unfloored, reported as 1024.
        let below = vector_write_request_bytes(250, &probe_item("b", 250, None), "emb", all());
        assert_eq!(below, 1024.0);

        // Exactly at the boundary: 1014 + 4 + 6 = 1024 unfloored, still 1024.
        let at = vector_write_request_bytes(250, &probe_item("b", 250, Some(6)), "emb", all());
        assert_eq!(at, 1024.0);

        // One byte over: 1025, the floor no longer binds.
        let over = vector_write_request_bytes(250, &probe_item("b", 250, Some(7)), "emb", all());
        assert_eq!(over, 1025.0);
    }
}
