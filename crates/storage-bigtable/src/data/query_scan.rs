// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query and Scan over a Bigtable data table (base table, GSI shadow table, or
//! LSI shadow table).

use std::collections::BTreeMap;

use extenddb_core::expression::{
    CompareOp, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition,
};
use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::{
    Chain, Filter, Interleave,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    ReadRowsRequest, RowFilter, RowRange, RowSet,
};

use crate::data::client::{BigtableClient, is_reverse_scan_unsupported_err, map_bigtable_err};
use crate::data::encoding::row_key;
use crate::data::item_ops::{FAMILY_DATA, FAMILY_META, ItemOps, decode_snapshot_cells};

/// Extract the `[pk_tag:1][pk_len:4][pk_bytes]` slice from an encoded row key
/// (base-table `[S/N/B][len][bytes]` or shadow-table `[TAG_PK_DIGEST][32][sha256]`).
fn pk_prefix_slice(encoded_row_key: &[u8]) -> Result<&[u8], StorageError> {
    if encoded_row_key.len() < 5 {
        return Err(StorageError::Internal(
            "malformed row key (shorter than 5-byte PK header)".into(),
        ));
    }
    let pk_len = u32::from_be_bytes(
        encoded_row_key[1..5]
            .try_into()
            .map_err(|_| StorageError::Internal("malformed PK length".into()))?,
    ) as usize;
    let end = 5usize.saturating_add(pk_len);
    if encoded_row_key.len() < end {
        return Err(StorageError::Internal(
            "malformed row key (truncated PK bytes)".into(),
        ));
    }
    Ok(&encoded_row_key[..end])
}

/// Compute the parallel-scan segment `[0, total_segments)` for an encoded row key
/// (base table or GSI/LSI shadow table) by hashing its partition-key prefix
/// with CRC32.
///
/// # Errors
///
/// Returns [`StorageError::Internal`] if `encoded_row_key` has a malformed PK header.
pub fn pk_segment_from_encoded_key(
    encoded_row_key: &[u8],
    total_segments: u64,
) -> Result<u64, StorageError> {
    if total_segments <= 1 {
        return Ok(0);
    }
    let prefix = pk_prefix_slice(encoded_row_key)?;
    let hash = u64::from(crc32fast::hash(prefix));
    Ok(hash % total_segments)
}

/// Compute whether `key` belongs to `segment` of `total_segments` for
/// `key_schema` on a base table (`is_index == false`) or shadow index table
/// (`is_index == true`).
///
/// # Errors
///
/// Returns [`StorageError`] if `key_schema` has no `HASH` key or key encoding fails.
pub fn key_belongs_to_segment(
    key: &Item,
    key_schema: &[KeySchemaElement],
    is_index: bool,
    segment: i64,
    total_segments: i64,
) -> Result<bool, StorageError> {
    let total = u64::try_from(total_segments.max(1)).unwrap_or(1);
    if total <= 1 {
        return Ok(true);
    }
    let mut hash_vals: Vec<&AttributeValue> = Vec::with_capacity(4);
    for ks in key_schema.iter().filter(|k| k.key_type == KeyType::Hash) {
        let Some(v) = key.get(&ks.attribute_name) else {
            return Ok(true);
        };
        hash_vals.push(v);
    }
    if hash_vals.is_empty() {
        return Err(StorageError::Validation("key schema missing HASH".into()));
    }
    let pk_prefix = if is_index {
        crate::gsi::encode_gsi_pk_prefix(&hash_vals)?
    } else {
        row_key::pk_range_start(hash_vals[0])?
    };
    let seg = u64::try_from(segment.max(0)).unwrap_or(0) % total;
    Ok(pk_segment_from_encoded_key(&pk_prefix, total)? == seg)
}

/// Row filter for `Query` and `Scan`: emits the latest cell per attribute in
/// family `d` PLUS any active `m:intent:*` or `m:chunk` staging marker in
/// family `m`, while still filtering out pure `m:v`-only tombstones server-side
/// so deleted rows never consume `rows_limit`.
fn query_scan_row_filter() -> RowFilter {
    RowFilter {
        filter: Some(Filter::Interleave(Interleave {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::Chain(Chain {
                        filters: vec![
                            RowFilter {
                                filter: Some(Filter::FamilyNameRegexFilter(format!(
                                    "^{FAMILY_DATA}$"
                                ))),
                            },
                            RowFilter {
                                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                            },
                        ],
                    })),
                },
                RowFilter {
                    filter: Some(Filter::Chain(Chain {
                        filters: vec![
                            RowFilter {
                                filter: Some(Filter::FamilyNameRegexFilter(format!(
                                    "^{FAMILY_META}$"
                                ))),
                            },
                            RowFilter {
                                filter: Some(Filter::ColumnQualifierRegexFilter(
                                    b"^(intent:.*|chunk)$".to_vec(),
                                )),
                            },
                            RowFilter {
                                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                            },
                        ],
                    })),
                },
            ],
        })),
    }
}

fn extract_lek(item: &Item, key_info: &TableKeyInfo, is_index: bool) -> Item {
    let mut key_attrs: Item = BTreeMap::new();
    for ks in &key_info.key_schema {
        if let Some(v) = item.get(&ks.attribute_name) {
            key_attrs.insert(ks.attribute_name.clone(), v.clone());
        }
    }
    if is_index {
        for ks in &key_info.base_key_schema {
            if let Some(v) = item.get(&ks.attribute_name) {
                key_attrs.insert(ks.attribute_name.clone(), v.clone());
            }
        }
    }
    key_attrs
}

type QueryRangeBounds = (Vec<u8>, Vec<u8>, Vec<u8>, bool);

fn check_defined_scalar_type_msg(
    key_info: &TableKeyInfo,
    attr_name: &str,
    val: &AttributeValue,
    invalid_msg: &str,
) -> Result<(), StorageError> {
    if let Some(ad) = key_info
        .attribute_definitions
        .iter()
        .find(|a| a.attribute_name == attr_name)
    {
        let ok = matches!(
            (&ad.attribute_type, val),
            (ScalarAttributeType::S, AttributeValue::S(_))
                | (ScalarAttributeType::N, AttributeValue::N(_))
                | (ScalarAttributeType::B, AttributeValue::B(_))
        );
        if !ok {
            return Err(StorageError::Validation(invalid_msg.into()));
        }
    }
    Ok(())
}

/// Validate and encode an `ExclusiveStartKey` for base-table or index
/// (`GSI`/`LSI`) `Query`/`Scan`, enforcing required key presence, absence of
/// extraneous attributes, and declared scalar types before encoding the row key.
fn encode_exclusive_start_key(
    esk: &Item,
    key_info: &TableKeyInfo,
    is_index: bool,
    is_scan: bool,
) -> Result<Vec<u8>, StorageError> {
    let invalid_msg = if !is_index && is_scan {
        "The provided starting key is invalid: The provided key element does not match the schema"
    } else {
        "The provided starting key is invalid"
    };

    let mut required_names: Vec<&str> = Vec::with_capacity(
        key_info.key_schema.len()
            + if is_index {
                key_info.base_key_schema.len()
            } else {
                0
            },
    );
    for ks in &key_info.key_schema {
        if !required_names.contains(&ks.attribute_name.as_str()) {
            required_names.push(ks.attribute_name.as_str());
        }
    }
    if is_index {
        for ks in &key_info.base_key_schema {
            if !required_names.contains(&ks.attribute_name.as_str()) {
                required_names.push(ks.attribute_name.as_str());
            }
        }
    }

    for &attr in &required_names {
        let Some(v) = esk.get(attr) else {
            return Err(StorageError::Validation(invalid_msg.into()));
        };
        check_defined_scalar_type_msg(key_info, attr, v, invalid_msg)?;
    }
    if esk.len() != required_names.len() {
        return Err(StorageError::Validation(invalid_msg.into()));
    }

    if is_index {
        crate::gsi::shadow_row_key_for_item(esk, &key_info.key_schema, &key_info.base_key_schema)?
            .ok_or_else(|| StorageError::Validation(invalid_msg.into()))
    } else {
        row_key::encode_key(esk, &key_info.key_schema)
            .map_err(|_| StorageError::Validation(invalid_msg.into()))
    }
}

fn resolve_expr_value_with_maps<'b>(
    expr: &Expr,
    maps: &'b ExpressionMaps,
) -> Result<&'b AttributeValue, StorageError> {
    match expr {
        Expr::Placeholder(name) => maps
            .resolve_value(name)
            .map_err(|e| StorageError::Validation(format!("placeholder resolve: {e}"))),
        _ => Err(StorageError::Validation(
            "key condition values must be placeholders".into(),
        )),
    }
}

fn path_to_raw_name(path: &[PathElement]) -> Result<&str, StorageError> {
    if path.len() != 1 {
        return Err(StorageError::Validation(
            "key condition path must be a single attribute".into(),
        ));
    }
    match &path[0] {
        PathElement::Attribute(s) => Ok(s.as_str()),
        PathElement::Index(_) => Err(StorageError::Validation(
            "key condition path must be an attribute name, not an index".into(),
        )),
    }
}

fn resolve_path_attr(path: &[PathElement], maps: &ExpressionMaps) -> Result<String, StorageError> {
    let raw = path_to_raw_name(path)?;
    if let Some(stripped) = raw.strip_prefix('#') {
        maps.names
            .get(stripped)
            .or_else(|| maps.names.get(raw))
            .cloned()
            .ok_or_else(|| {
                StorageError::Validation(format!(
                    "Invalid KeyConditionExpression: An expression attribute name used in the document path is not defined; attribute name: #{stripped}"
                ))
            })
    } else {
        Ok(raw.to_owned())
    }
}

/// Build `(pk_prefix, start, end, end_open)` over a base table partition
/// (`pk_value` + optional terminal `sk_condition`).
///
/// Base-table sort keys are terminal (`[pk_prefix][sk_tag][sk_bytes]`), so exact
/// equality, `<=`, and `BETWEEN` use closed `EndKeyClosed(exact)` bounds
/// (avoiding prefix bleed across `"a"` vs `"ab"`), `>` starts at
/// `next_valid_row_key_after(&exact)`, and `begins_with` / partition bounds use
/// `binary_prefix_upper_bound`.
pub(crate) fn build_base_range_for_query(
    pk_value: &AttributeValue,
    sk_condition: Option<&SortKeyCondition>,
    maps: &ExpressionMaps,
) -> Result<QueryRangeBounds, StorageError> {
    let pk_prefix = row_key::pk_range_start(pk_value)?;
    let pk_upper = row_key::pk_range_end_exclusive(pk_value)?;

    let (start, end, end_open) = match sk_condition {
        None => (pk_prefix.clone(), pk_upper, true),
        Some(SortKeyCondition::Compare { path, op, value }) => {
            let _ = resolve_path_attr(path, maps)?;
            let sk_val = resolve_expr_value_with_maps(value, maps)?;
            let (sk_tag, sk_bytes) = row_key::sk_tag_and_bytes(sk_val)?;
            let mut exact = pk_prefix.clone();
            exact.push(sk_tag);
            exact.extend_from_slice(&sk_bytes);

            let mut type_start = pk_prefix.clone();
            type_start.push(sk_tag);
            let mut type_upper = pk_prefix.clone();
            type_upper.push(sk_tag + 1);

            match op {
                CompareOp::Eq => (exact.clone(), exact, false),
                CompareOp::Gt => {
                    let gt_start = row_key::next_valid_row_key_after(&exact)
                        .unwrap_or_else(|| type_upper.clone());
                    (gt_start, type_upper, true)
                }
                CompareOp::Ge => (exact, type_upper, true),
                CompareOp::Lt => (type_start, exact, true),
                CompareOp::Le => (type_start, exact, false),
                CompareOp::Ne => {
                    return Err(StorageError::Validation(
                        "Ne not valid in KeyConditionExpression for SK".into(),
                    ));
                }
            }
        }
        Some(SortKeyCondition::Between { path, low, high }) => {
            let _ = resolve_path_attr(path, maps)?;
            let low_val = resolve_expr_value_with_maps(low, maps)?;
            let high_val = resolve_expr_value_with_maps(high, maps)?;
            let make_bound = |v: &AttributeValue| -> Result<Vec<u8>, StorageError> {
                let (tag, bytes) = row_key::sk_tag_and_bytes(v)?;
                let mut out = pk_prefix.clone();
                out.push(tag);
                out.extend_from_slice(&bytes);
                Ok(out)
            };
            let lo = make_bound(low_val)?;
            let hi = make_bound(high_val)?;
            if lo > hi {
                (lo, hi, true)
            } else {
                (lo, hi, false)
            }
        }
        Some(SortKeyCondition::BeginsWith { path, prefix }) => {
            let _ = resolve_path_attr(path, maps)?;
            let pref_val = resolve_expr_value_with_maps(prefix, maps)?;
            let (tag, bytes) = match pref_val {
                AttributeValue::S(s) => (0x53u8, s.as_bytes()),
                AttributeValue::B(b) => (0x42, b.as_slice()),
                _ => {
                    return Err(StorageError::Validation(
                        "begins_with prefix must be S/B".into(),
                    ));
                }
            };
            let mut bw_start = pk_prefix.clone();
            bw_start.push(tag);
            bw_start.extend_from_slice(bytes);
            let bw_upper = if let Some(u) = row_key::binary_prefix_upper_bound(bytes) {
                let mut end = pk_prefix.clone();
                end.push(tag);
                end.extend_from_slice(&u);
                end
            } else {
                let mut end = pk_prefix.clone();
                end.push(tag + 1);
                end
            };
            (bw_start, bw_upper, true)
        }
    };
    Ok((pk_prefix, start, end, end_open))
}

fn resolve_index_hash_equalities<'b>(
    index_key_schema: &[KeySchemaElement],
    kc_work: &KeyCondition,
    maps: &'b ExpressionMaps,
) -> Result<Vec<(String, &'b AttributeValue)>, StorageError> {
    let hash_attrs: Vec<&str> = index_key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Hash)
        .map(|k| k.attribute_name.as_str())
        .collect();
    if hash_attrs.is_empty() {
        return Err(StorageError::Validation("index missing HASH key".into()));
    }
    if hash_attrs.len() == 1 && kc_work.extra_pk_conditions.is_empty() {
        let _ = resolve_path_attr(&kc_work.pk_path, maps)?;
        return Ok(vec![(
            hash_attrs[0].to_owned(),
            resolve_expr_value_with_maps(&kc_work.pk_value, maps)?,
        )]);
    }
    let mut by_name: BTreeMap<String, &Expr> = BTreeMap::new();
    by_name.insert(
        resolve_path_attr(&kc_work.pk_path, maps)?,
        &kc_work.pk_value,
    );
    for (p, v) in &kc_work.extra_pk_conditions {
        by_name.insert(resolve_path_attr(p, maps)?, v);
    }
    let mut ordered = Vec::with_capacity(hash_attrs.len());
    for &h in &hash_attrs {
        let expr = by_name.get(h).copied().ok_or_else(|| {
            StorageError::Validation(format!(
                "HASH key attributes {h} must have equality conditions specified in the query"
            ))
        })?;
        ordered.push((h.to_owned(), resolve_expr_value_with_maps(expr, maps)?));
    }
    Ok(ordered)
}

type EncodedHashAttr = (String, (u8, Vec<u8>));

/// Build `(pk_prefix, start, end, end_open)` over an index shadow table (`GSI`
/// or `LSI`) supporting `1..=4` HASH and `0..=4` RANGE key attributes.
// Justification: composite HASH/RANGE validation, contiguous prefix slot assembly, and terminal SK operator bounds run in one pass.
#[allow(clippy::too_many_lines)]
pub(crate) fn build_gsi_range_for_query(
    index_key_schema: &[KeySchemaElement],
    kc: &KeyCondition,
    maps: &ExpressionMaps,
) -> Result<(QueryRangeBounds, Vec<EncodedHashAttr>), StorageError> {
    enum RangeClause<'b> {
        Eq(&'b Expr),
        Cond(&'b SortKeyCondition),
    }

    let hash_attrs: Vec<&str> = index_key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Hash)
        .map(|k| k.attribute_name.as_str())
        .collect();
    if hash_attrs.is_empty() {
        return Err(StorageError::Validation("index missing HASH key".into()));
    }
    let range_attrs: Vec<&str> = index_key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Range)
        .map(|k| k.attribute_name.as_str())
        .collect();

    let owned_kc;
    let kc_work: &KeyCondition = if hash_attrs.len() > 1 || range_attrs.len() > 1 {
        let mut tmp = kc.clone();
        tmp.resolve_multipart(&hash_attrs, &range_attrs, &maps.names)
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        owned_kc = tmp;
        &owned_kc
    } else {
        kc
    };

    let hash_pairs = resolve_index_hash_equalities(index_key_schema, kc_work, maps)?;
    let hash_vals: Vec<&AttributeValue> = hash_pairs.iter().map(|(_, v)| *v).collect();
    let mut expected_hash_encoded = Vec::with_capacity(hash_pairs.len());
    for (attr, val) in hash_pairs {
        expected_hash_encoded.push((attr, row_key::sk_tag_and_bytes(val)?));
    }
    let pk_prefix = crate::gsi::encode_gsi_pk_prefix(&hash_vals)?;

    if kc_work.sk_condition.is_none() && kc_work.extra_sk_conditions.is_empty() {
        let upper = row_key::binary_prefix_upper_bound(&pk_prefix)
            .ok_or_else(|| StorageError::Internal("all-0xFF index PK prefix".into()))?;
        return Ok((
            (pk_prefix.clone(), pk_prefix, upper, true),
            expected_hash_encoded,
        ));
    }

    if range_attrs.is_empty() {
        return Err(StorageError::Validation(
            "sort key condition provided on HASH-only index".into(),
        ));
    }

    let mut slots: Vec<Option<RangeClause<'_>>> = (0..range_attrs.len()).map(|_| None).collect();
    for (p, v) in &kc_work.extra_sk_conditions {
        let attr = resolve_path_attr(p, maps)?;
        let pos = range_attrs
            .iter()
            .position(|&r| r == attr)
            .ok_or_else(|| StorageError::Validation(format!("unknown index RANGE key {attr}")))?;
        slots[pos] = Some(RangeClause::Eq(v));
    }
    if let Some(sk_cond) = kc_work.sk_condition.as_ref() {
        let cond_path = sk_cond.path();
        let pos = if range_attrs.len() == 1 && kc_work.extra_sk_conditions.is_empty() {
            let _ = resolve_path_attr(cond_path, maps)?;
            0
        } else {
            let attr = resolve_path_attr(cond_path, maps)?;
            range_attrs.iter().position(|&r| r == attr).ok_or_else(|| {
                StorageError::Validation(format!("unknown index RANGE key {attr}"))
            })?
        };
        slots[pos] = Some(RangeClause::Cond(sk_cond));
    }

    let mut active: Vec<RangeClause<'_>> = Vec::with_capacity(range_attrs.len());
    let mut saw_gap = false;
    for slot in slots {
        match slot {
            Some(c) => {
                if saw_gap {
                    return Err(StorageError::Validation(
                        "index RANGE key conditions must form a contiguous leading prefix".into(),
                    ));
                }
                active.push(c);
            }
            None => {
                saw_gap = true;
            }
        }
    }

    let m = active.len();
    let mut prefix = pk_prefix.clone();
    for clause in active.iter().take(m.saturating_sub(1)) {
        let eq_expr = match clause {
            RangeClause::Eq(e) => *e,
            RangeClause::Cond(SortKeyCondition::Compare {
                op: CompareOp::Eq,
                value,
                ..
            }) => value,
            RangeClause::Cond(_) => {
                return Err(StorageError::Validation(
                    "non-terminal index RANGE key condition must be equality".into(),
                ));
            }
        };
        let val = resolve_expr_value_with_maps(eq_expr, maps)?;
        let (tag, bytes) = row_key::sk_tag_and_bytes(val)?;
        prefix.push(tag);
        prefix.extend_from_slice(&crate::gsi::escape_bytes_terminated(&bytes));
    }

    let encode_exact_terminated = |base_prefix: &[u8],
                                   val: &AttributeValue|
     -> Result<(u8, Vec<u8>, Vec<u8>), StorageError> {
        let (tag, bytes) = row_key::sk_tag_and_bytes(val)?;
        let mut exact = base_prefix.to_vec();
        exact.push(tag);
        exact.extend_from_slice(&crate::gsi::escape_bytes_terminated(&bytes));
        let exact_upper = row_key::binary_prefix_upper_bound(&exact)
            .ok_or_else(|| StorageError::Internal("all-0xFF escaped index SK".into()))?;
        Ok((tag, exact, exact_upper))
    };

    let bounds = match &active[m - 1] {
        RangeClause::Eq(expr) => {
            let val = resolve_expr_value_with_maps(expr, maps)?;
            let (_, exact, exact_upper) = encode_exact_terminated(&prefix, val)?;
            (pk_prefix, exact, exact_upper, true)
        }
        RangeClause::Cond(SortKeyCondition::Compare { op, value, .. }) => {
            let val = resolve_expr_value_with_maps(value, maps)?;
            let (tag, exact, exact_upper) = encode_exact_terminated(&prefix, val)?;
            let mut type_start = prefix.clone();
            type_start.push(tag);
            let mut type_upper = prefix;
            type_upper.push(tag + 1);
            match op {
                CompareOp::Eq => (pk_prefix, exact, exact_upper, true),
                CompareOp::Gt => (pk_prefix, exact_upper, type_upper, true),
                CompareOp::Ge => (pk_prefix, exact, type_upper, true),
                CompareOp::Lt => (pk_prefix, type_start, exact, true),
                CompareOp::Le => (pk_prefix, type_start, exact_upper, true),
                CompareOp::Ne => {
                    return Err(StorageError::Validation(
                        "Ne not valid in KeyConditionExpression for SK".into(),
                    ));
                }
            }
        }
        RangeClause::Cond(SortKeyCondition::Between { low, high, .. }) => {
            let low_val = resolve_expr_value_with_maps(low, maps)?;
            let high_val = resolve_expr_value_with_maps(high, maps)?;
            let (_, lo_exact, _) = encode_exact_terminated(&prefix, low_val)?;
            let (_, hi_exact, hi_exact_upper) = encode_exact_terminated(&prefix, high_val)?;
            if lo_exact > hi_exact {
                (pk_prefix, lo_exact, hi_exact, true)
            } else {
                (pk_prefix, lo_exact, hi_exact_upper, true)
            }
        }
        RangeClause::Cond(SortKeyCondition::BeginsWith {
            prefix: pref_expr, ..
        }) => {
            let pref_val = resolve_expr_value_with_maps(pref_expr, maps)?;
            let (tag, bytes) = match pref_val {
                AttributeValue::S(s) => (0x53u8, s.as_bytes()),
                AttributeValue::B(b) => (0x42, b.as_slice()),
                _ => {
                    return Err(StorageError::Validation(
                        "begins_with prefix must be S/B".into(),
                    ));
                }
            };
            let mut bw_start = prefix.clone();
            bw_start.push(tag);
            bw_start.extend_from_slice(&crate::gsi::escape_bytes(bytes));
            let bw_upper = if let Some(u) = row_key::binary_prefix_upper_bound(bytes) {
                let mut end = prefix;
                end.push(tag);
                end.extend_from_slice(&crate::gsi::escape_bytes(&u));
                end
            } else {
                let mut end = prefix;
                end.push(tag + 1);
                end
            };
            (pk_prefix, bw_start, bw_upper, true)
        }
    };
    Ok((bounds, expected_hash_encoded))
}

fn item_matches_expected_hashes(item: &Item, expected_hashes: &[(String, (u8, Vec<u8>))]) -> bool {
    expected_hashes.iter().all(|(attr, expected)| {
        item.get(attr)
            .and_then(|v| row_key::sk_tag_and_bytes(v).ok())
            .as_ref()
            == Some(expected)
    })
}

pub struct QueryScan<'a> {
    client: &'a BigtableClient,
    data_table_short: String,
    full_table_name: String,
    engine: Option<&'a crate::engine::BigtableEngine>,
}

impl<'a> QueryScan<'a> {
    #[must_use]
    pub fn new(client: &'a BigtableClient, data_table_short: &str) -> Self {
        Self {
            client,
            data_table_short: data_table_short.to_owned(),
            full_table_name: client.full_table_name(data_table_short),
            engine: None,
        }
    }

    #[must_use]
    pub(crate) fn with_engine(
        engine: &'a crate::engine::BigtableEngine,
        data_table_short: &str,
    ) -> Self {
        let client = engine.data_client();
        Self {
            client,
            data_table_short: data_table_short.to_owned(),
            full_table_name: client.full_table_name(data_table_short),
            engine: Some(engine),
        }
    }

    async fn resolve_row_cells(
        &self,
        is_index: bool,
        raw_key: &[u8],
        cells: Vec<crate::data::client::RowCell>,
    ) -> Result<Option<Item>, StorageError> {
        let mut snap = decode_snapshot_cells(cells)?;
        if !snap.in_progress_chunk && snap.intents.is_empty() {
            return Ok(snap.item);
        }
        if let Some(engine) = self.engine {
            if is_index {
                return engine
                    .resolve_scanned_shadow_row(&self.data_table_short, raw_key, snap)
                    .await;
            }
            return engine
                .read_row_resolving_committed_intents(&self.data_table_short, raw_key)
                .await;
        }

        // `query_scan_row_filter()` intentionally omits `m:v` so pure `m:v`
        // tombstones are filtered out server-side without consuming `rows_limit`;
        // reload the full `(d, m)` row snapshot here so `snap.version` reflects
        // the live `m:v` cell before matching coordinator versions.
        let ops = ItemOps::new(self.client, &self.data_table_short, 10);
        snap = ops.get_row_snapshot_by_raw_key(raw_key).await?;
        if is_index {
            for attempt in 0..8u64 {
                if !snap.in_progress_chunk {
                    return Ok(snap.item);
                }
                tokio::time::sleep(std::time::Duration::from_millis(2 << attempt.min(4))).await;
                snap = ops.get_row_snapshot_by_raw_key(raw_key).await?;
            }
            if snap.in_progress_chunk {
                return Err(StorageError::Transient(
                    "concurrent multi-chunk shadow write still in progress".into(),
                ));
            }
            return Ok(snap.item);
        }

        let coord =
            crate::transact::TxnCoordinator::new(self.client, std::time::Duration::from_mins(1));
        for attempt in 0..8u64 {
            for (tid, _) in &snap.intents {
                if let Some(st) = coord.get_state(tid).await?
                    && st.state == "COMMITTED"
                    && let Some(muts) = st.mutations
                {
                    for pm in muts {
                        if pm.participant.data_table == self.data_table_short
                            && pm.participant.row_key.as_slice() == raw_key
                            && ((pm.expected_version == snap.version
                                && (snap.in_progress_chunk
                                    || pm.row_existed == snap.item.is_some()))
                                || (pm.new_version.is_some() && pm.new_version == snap.version))
                        {
                            return Ok(match pm.payload {
                                crate::transact::TxnOpPayload::Put { item } => Some(item),
                                crate::transact::TxnOpPayload::Delete => None,
                            });
                        }
                    }
                }
            }
            if !snap.in_progress_chunk {
                return Ok(snap.item);
            }
            tokio::time::sleep(std::time::Duration::from_millis(2 << attempt.min(4))).await;
            snap = ops.get_row_snapshot_by_raw_key(raw_key).await?;
        }
        if snap.in_progress_chunk {
            return Err(StorageError::Transient(
                "concurrent multi-chunk write still in progress".into(),
            ));
        }
        Ok(snap.item)
    }

    /// Run a `Query` against the base table (`is_index == false`) or an index
    /// shadow table (`is_index == true`, covering both GSIs and LSIs, even when
    /// an index's key schema matches the base table's key schema).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] on invalid key conditions, malformed
    /// `ExclusiveStartKey`, or `ReadRows` / intent-resolution RPC failures.
    // Justification: Query combines range-bound construction, ExclusiveStartKey validation, reverse-scan fallback, and cooperative intent resolution.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn query(
        &self,
        key_info: &TableKeyInfo,
        is_index: bool,
        kc: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        let ((query_pk_prefix, mut start, mut end, mut end_open), expected_hashes) = if is_index {
            build_gsi_range_for_query(&key_info.key_schema, kc, maps)?
        } else {
            let _pk_name = resolve_path_attr(&kc.pk_path, maps)?;
            let pk_value = resolve_expr_value_with_maps(&kc.pk_value, maps)?;
            (
                build_base_range_for_query(pk_value, kc.sk_condition.as_ref(), maps)?,
                Vec::new(),
            )
        };

        if let Some(esk) = exclusive_start_key {
            let resume = encode_exclusive_start_key(esk, key_info, is_index, false)?;
            if pk_prefix_slice(&resume)? != query_pk_prefix.as_slice() {
                let msg = if kc.sk_condition.is_some() || !kc.extra_sk_conditions.is_empty() {
                    "The query can return at most one row and cannot be restarted"
                } else {
                    "The provided starting key is outside query boundaries based on provided conditions"
                };
                return Err(StorageError::Validation(msg.into()));
            }
            if forward {
                let Some(s) = row_key::next_valid_row_key_after(&resume) else {
                    return Ok((Vec::new(), None));
                };
                start = s.max(start);
            } else if (!end_open && resume <= end) || (end_open && resume < end) {
                end = resume;
                end_open = true;
            }
        }

        let make_row_set = |s: Vec<u8>, e: Vec<u8>, open: bool| RowSet {
            row_keys: vec![],
            row_ranges: vec![RowRange {
                start_key: Some(StartKey::StartKeyClosed(s)),
                end_key: Some(if open {
                    EndKey::EndKeyOpen(e)
                } else {
                    EndKey::EndKeyClosed(e)
                }),
            }],
        };

        let target_plus_one = limit
            .filter(|&l| l > 0)
            .map(|l| usize::try_from(l).unwrap_or(usize::MAX).saturating_add(1));

        let mut data = self.client.data();
        let mut items: Vec<Item> = Vec::new();
        let mut cur_start = start;
        let mut cur_end = end;
        let mut cur_end_open = end_open;

        if !forward && self.client.emulator_host.is_some() {
            let Some((s, e, open)) =
                row_key::clamp_range_to_max_row_key(cur_start, cur_end, cur_end_open)
            else {
                return Ok((Vec::new(), None));
            };
            let req = ReadRowsRequest {
                table_name: self.full_table_name.clone(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                rows: Some(make_row_set(s, e, open)),
                rows_limit: 0,
                filter: Some(query_scan_row_filter()),
                reversed: false,
                ..ReadRowsRequest::default()
            };
            let resp = data
                .read_rows(req)
                .await
                .map_err(|e| map_bigtable_err("Query ReadRows", e))?;
            let mut forward_items = Vec::with_capacity(resp.len());
            for (raw_key, cells) in resp {
                if let Some(item) = self.resolve_row_cells(is_index, &raw_key, cells).await?
                    && (!is_index || item_matches_expected_hashes(&item, &expected_hashes))
                {
                    forward_items.push(item);
                }
            }
            forward_items.reverse();
            items = forward_items;
        } else {
            while let Some((s, e, open)) = row_key::clamp_range_to_max_row_key(
                cur_start.clone(),
                cur_end.clone(),
                cur_end_open,
            ) {
                let page_limit = target_plus_one.map_or(0, |want| {
                    i64::try_from(want.saturating_sub(items.len()).max(1)).unwrap_or(i64::MAX)
                });
                let req = ReadRowsRequest {
                    table_name: self.full_table_name.clone(),
                    app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                    rows: Some(make_row_set(s.clone(), e.clone(), open)),
                    rows_limit: page_limit,
                    filter: Some(query_scan_row_filter()),
                    reversed: !forward,
                    ..ReadRowsRequest::default()
                };
                let resp = match data.read_rows(req).await {
                    Ok(r) => r,
                    Err(err) if !forward && is_reverse_scan_unsupported_err(&err) => {
                        let fallback_req = ReadRowsRequest {
                            table_name: self.full_table_name.clone(),
                            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                            rows: Some(make_row_set(s, e, open)),
                            rows_limit: 0,
                            filter: Some(query_scan_row_filter()),
                            reversed: false,
                            ..ReadRowsRequest::default()
                        };
                        let fb_resp = data
                            .read_rows(fallback_req)
                            .await
                            .map_err(|e2| map_bigtable_err("Query ReadRows fallback", e2))?;
                        let mut tail = Vec::with_capacity(fb_resp.len());
                        for (raw_key, cells) in fb_resp {
                            if let Some(item) =
                                self.resolve_row_cells(is_index, &raw_key, cells).await?
                                && (!is_index
                                    || item_matches_expected_hashes(&item, &expected_hashes))
                            {
                                tail.push(item);
                            }
                        }
                        tail.reverse();
                        items.extend(tail);
                        break;
                    }
                    Err(err) => return Err(map_bigtable_err("Query ReadRows", err)),
                };

                let batch_count = resp.len();
                if batch_count == 0 {
                    break;
                }
                let mut last_raw_key: Option<Vec<u8>> = None;
                for (raw_key, cells) in resp {
                    last_raw_key = Some(raw_key.clone());
                    if let Some(item) = self.resolve_row_cells(is_index, &raw_key, cells).await?
                        && (!is_index || item_matches_expected_hashes(&item, &expected_hashes))
                    {
                        items.push(item);
                        if let Some(want) = target_plus_one
                            && items.len() >= want
                        {
                            break;
                        }
                    }
                }
                if page_limit == 0 {
                    break;
                }
                if let Some(want) = target_plus_one
                    && items.len() >= want
                {
                    break;
                }
                if i64::try_from(batch_count).unwrap_or(i64::MAX) < page_limit {
                    break;
                }
                let Some(last_k) = last_raw_key else {
                    break;
                };
                if forward {
                    let Some(next_s) = row_key::next_valid_row_key_after(&last_k) else {
                        break;
                    };
                    cur_start = next_s;
                } else {
                    cur_end = last_k;
                    cur_end_open = true;
                }
            }
        }

        let last_evaluated = match limit {
            Some(l) if l > 0 && items.len() > usize::try_from(l).unwrap_or(usize::MAX) => {
                items.truncate(usize::try_from(l).unwrap_or(usize::MAX));
                items.last().map(|it| extract_lek(it, key_info, is_index))
            }
            _ => None,
        };
        Ok((items, last_evaluated))
    }

    /// Run a `Scan` against the base table (`is_index == false`) or an index
    /// shadow table (`is_index == true`, covering both GSIs and LSIs).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] on malformed `ExclusiveStartKey` or `ReadRows`
    /// / intent-resolution RPC failures.
    pub async fn scan(
        &self,
        key_info: &TableKeyInfo,
        is_index: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        let mut cursor_start = match exclusive_start_key {
            Some(esk) => {
                let k = encode_exclusive_start_key(esk, key_info, is_index, true)?;
                let Some(next_k) = row_key::next_valid_row_key_after(&k) else {
                    return Ok((Vec::new(), None));
                };
                next_k
            }
            None => Vec::new(),
        };

        let total = u64::try_from(total_segments.unwrap_or(1).max(1)).unwrap_or(1);
        let seg = u64::try_from(segment.unwrap_or(0).max(0)).unwrap_or(0) % total;
        let mut data = self.client.data();

        let target_plus_one = limit
            .filter(|&l| l > 0)
            .map(|l| usize::try_from(l).unwrap_or(usize::MAX).saturating_add(1));

        let mut items: Vec<Item> = Vec::new();
        loop {
            let req_limit = scan_page_limit(total, limit, target_plus_one, items.len());
            let req = ReadRowsRequest {
                table_name: self.full_table_name.clone(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                rows: Some(RowSet {
                    row_keys: vec![],
                    row_ranges: vec![RowRange {
                        start_key: Some(StartKey::StartKeyClosed(cursor_start.clone())),
                        end_key: None,
                    }],
                }),
                rows_limit: req_limit,
                filter: Some(query_scan_row_filter()),
                ..ReadRowsRequest::default()
            };
            let resp = data
                .read_rows(req)
                .await
                .map_err(|e| map_bigtable_err("Scan ReadRows", e))?;
            let batch_count = resp.len();
            if batch_count == 0 {
                break;
            }
            let mut last_raw_key: Option<Vec<u8>> = None;
            for (raw_key, cells) in resp {
                last_raw_key = Some(raw_key.clone());
                if total > 1 && pk_segment_from_encoded_key(&raw_key, total)? != seg {
                    continue;
                }
                if let Some(item) = self.resolve_row_cells(is_index, &raw_key, cells).await? {
                    items.push(item);
                    if target_plus_one.is_some_and(|want| items.len() >= want) {
                        break;
                    }
                }
            }
            if req_limit == 0
                || target_plus_one.is_some_and(|want| items.len() >= want)
                || i64::try_from(batch_count).unwrap_or(i64::MAX) < req_limit
            {
                break;
            }
            let Some(next_k) = last_raw_key
                .as_deref()
                .and_then(row_key::next_valid_row_key_after)
            else {
                break;
            };
            cursor_start = next_k;
        }

        let last_evaluated = match limit {
            Some(l) if l > 0 && items.len() > usize::try_from(l).unwrap_or(usize::MAX) => {
                items.truncate(usize::try_from(l).unwrap_or(usize::MAX));
                items.last().map(|it| extract_lek(it, key_info, is_index))
            }
            _ => None,
        };
        Ok((items, last_evaluated))
    }
}

fn scan_page_limit(
    total: u64,
    limit: Option<i64>,
    target_plus_one: Option<usize>,
    have: usize,
) -> i64 {
    if total == 1 {
        return target_plus_one.map_or(0, |want| {
            i64::try_from(want.saturating_sub(have).max(1)).unwrap_or(i64::MAX)
        });
    }
    match limit {
        Some(l) if l > 0 => {
            (l.saturating_mul(i64::try_from(total).unwrap_or(i64::MAX))).clamp(64, 1024)
        }
        _ => 512,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::AttributeDefinition;

    #[test]
    fn pk_segment_matches_key_belongs_to_segment() {
        let schema = vec![
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".into(),
                key_type: KeyType::Range,
            },
        ];
        for i in 0..20 {
            let mut item = BTreeMap::new();
            item.insert("pk".to_string(), AttributeValue::S(format!("user#{i}")));
            item.insert("sk".to_string(), AttributeValue::S("s1".into()));
            let encoded = row_key::encode_key(&item, &schema).unwrap();
            let seg = pk_segment_from_encoded_key(&encoded, 4).unwrap();
            assert!(key_belongs_to_segment(&item, &schema, false, seg as i64, 4).unwrap());
            for other in 0..4 {
                if other != seg as i64 {
                    assert!(!key_belongs_to_segment(&item, &schema, false, other, 4).unwrap());
                }
            }

            let shadow_encoded = crate::gsi::shadow_row_key_for_item(&item, &schema, &schema)
                .unwrap()
                .unwrap();
            let shadow_seg = pk_segment_from_encoded_key(&shadow_encoded, 4).unwrap();
            assert!(key_belongs_to_segment(&item, &schema, true, shadow_seg as i64, 4).unwrap());
        }
    }

    #[test]
    fn gsi_same_key_schema_as_base_uses_shadow_bounds() {
        use extenddb_core::expression::{parse_key_condition, tokenize};

        let same_schema = vec![
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".into(),
                key_type: KeyType::Range,
            },
        ];
        let mut item = BTreeMap::new();
        item.insert("pk".into(), AttributeValue::S("a".into()));
        item.insert("sk".into(), AttributeValue::N("5".into()));
        let shadow_rk = crate::gsi::shadow_row_key_for_item(&item, &same_schema, &same_schema)
            .unwrap()
            .unwrap();

        let kc = parse_key_condition(&tokenize("pk = :p AND sk = :s").unwrap()).unwrap();
        let mut maps = ExpressionMaps::default();
        maps.values
            .insert("p".into(), AttributeValue::S("a".into()));
        maps.values
            .insert("s".into(), AttributeValue::N("5".into()));
        let ((_, start, end, open), expected_hashes) =
            build_gsi_range_for_query(&same_schema, &kc, &maps).unwrap();
        assert!(open);
        assert!(shadow_rk.as_slice() >= start.as_slice());
        assert!(shadow_rk.as_slice() < end.as_slice());
        assert!(item_matches_expected_hashes(&item, &expected_hashes));
    }

    #[test]
    fn base_table_sort_key_bounds_prevent_prefix_bleed_across_all_operators() {
        use extenddb_core::expression::{parse_key_condition, tokenize};

        let schema = vec![
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".into(),
                key_type: KeyType::Range,
            },
        ];
        let mk_row = |pk: &str, sk: &[u8]| -> Vec<u8> {
            let mut it = BTreeMap::new();
            it.insert("pk".into(), AttributeValue::S(pk.into()));
            it.insert("sk".into(), AttributeValue::B(sk.to_vec()));
            row_key::encode_key(&it, &schema).unwrap()
        };

        let candidates: [(&str, Vec<u8>); 8] = [
            ("prev_pk", mk_row("p0", b"Z")),
            ("lt_a", mk_row("p1", b"@")),
            ("eq_a", mk_row("p1", b"A")),
            ("a_nul", mk_row("p1", b"A\x00")),
            ("ab", mk_row("p1", b"AB")),
            (
                "a_ff",
                mk_row("p1", &{
                    let mut v = b"A".to_vec();
                    v.extend_from_slice(&[0xFF; 24]);
                    v
                }),
            ),
            ("b", mk_row("p1", b"B")),
            ("next_pk", mk_row("p2", b"0")),
        ];

        let eval = |expr_str: &str, vals: &[(&str, &[u8])]| -> Vec<&str> {
            let kc = parse_key_condition(&tokenize(expr_str).unwrap()).unwrap();
            let mut maps = ExpressionMaps::default();
            maps.values
                .insert("p".into(), AttributeValue::S("p1".into()));
            for (k, v) in vals {
                maps.values
                    .insert((*k).into(), AttributeValue::B(v.to_vec()));
            }
            let pk_val = maps.values.get("p").unwrap();
            let (pk_pfx, s, e, open) =
                build_base_range_for_query(pk_val, kc.sk_condition.as_ref(), &maps).unwrap();
            assert_eq!(pk_pfx, row_key::pk_range_start(pk_val).unwrap());
            let (s, e, open) = row_key::clamp_range_to_max_row_key(s, e, open).unwrap();
            candidates
                .iter()
                .filter(|(_, rk)| {
                    rk.as_slice() >= s.as_slice()
                        && if open {
                            rk.as_slice() < e.as_slice()
                        } else {
                            rk.as_slice() <= e.as_slice()
                        }
                })
                .map(|(name, _)| *name)
                .collect()
        };

        assert_eq!(
            eval("pk = :p", &[]),
            vec!["lt_a", "eq_a", "a_nul", "ab", "a_ff", "b"]
        );
        assert_eq!(eval("pk = :p AND sk = :v", &[("v", b"A")]), vec!["eq_a"]);
        assert_eq!(eval("pk = :p AND sk < :v", &[("v", b"A")]), vec!["lt_a"]);
        assert_eq!(
            eval("pk = :p AND sk <= :v", &[("v", b"A")]),
            vec!["lt_a", "eq_a"]
        );
        assert_eq!(
            eval("pk = :p AND sk > :v", &[("v", b"A")]),
            vec!["a_nul", "ab", "a_ff", "b"]
        );
        assert_eq!(
            eval("pk = :p AND sk >= :v", &[("v", b"A")]),
            vec!["eq_a", "a_nul", "ab", "a_ff", "b"]
        );
        assert_eq!(
            eval(
                "pk = :p AND sk BETWEEN :lo AND :hi",
                &[("lo", b"@"), ("hi", b"A")]
            ),
            vec!["lt_a", "eq_a"]
        );
        assert_eq!(
            eval("pk = :p AND begins_with(sk, :pfx)", &[("pfx", b"A")]),
            vec!["eq_a", "a_nul", "ab", "a_ff"]
        );
        let overlong_sk = vec![b'B'; row_key::MAX_ROW_KEY_SIZE + 64];
        assert_eq!(
            eval("pk = :p AND sk < :v", &[("v", &overlong_sk)]),
            vec!["lt_a", "eq_a", "a_nul", "ab", "a_ff", "b"]
        );
    }

    #[test]
    fn multipart_gsi_query_bounds_and_segment_hashing() {
        use extenddb_core::expression::{parse_key_condition, tokenize};

        let base_ks = vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }];
        let gsi_ks = vec![
            KeySchemaElement {
                attribute_name: "h0".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "h1".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "r0".into(),
                key_type: KeyType::Range,
            },
            KeySchemaElement {
                attribute_name: "r1".into(),
                key_type: KeyType::Range,
            },
        ];

        let mk_item_and_key = |h0: &str, h1: &str, r0: &[u8], r1: &[u8], pk: &str| {
            let mut it = BTreeMap::new();
            it.insert("pk".into(), AttributeValue::S(pk.into()));
            it.insert("h0".into(), AttributeValue::S(h0.into()));
            it.insert("h1".into(), AttributeValue::S(h1.into()));
            it.insert("r0".into(), AttributeValue::B(r0.to_vec()));
            it.insert("r1".into(), AttributeValue::B(r1.to_vec()));
            let key = crate::gsi::shadow_row_key_for_item(&it, &gsi_ks, &base_ks)
                .unwrap()
                .unwrap();
            (it, key)
        };

        for i in 0..16 {
            let (it, enc) = mk_item_and_key(&format!("t#{i}"), "us", b"a", b"1", "p1");
            let seg = pk_segment_from_encoded_key(&enc, 4).unwrap();
            assert!(key_belongs_to_segment(&it, &gsi_ks, true, seg as i64, 4).unwrap());
        }

        let candidates = [
            ("other_h1", mk_item_and_key("p", "z", b"a", b"1", "base1").1),
            ("r0_lt", mk_item_and_key("p", "q", b"a", b"9", "base1").1),
            ("r1_lt", mk_item_and_key("p", "q", b"b", b"\x00", "base1").1),
            (
                "r1_eq_1",
                mk_item_and_key("p", "q", b"b", b"\x00\xff", "base1").1,
            ),
            (
                "r1_eq_2",
                mk_item_and_key("p", "q", b"b", b"\x00\xff", "base2").1,
            ),
            (
                "r1_prefix_ext",
                mk_item_and_key("p", "q", b"b", b"\x00\xff\x00", "base1").1,
            ),
            ("r1_gt", mk_item_and_key("p", "q", b"b", b"\x01", "base1").1),
            (
                "r1_all_ff",
                mk_item_and_key("p", "q", b"b", &[0xFF; 24], "base1").1,
            ),
            ("r0_gt", mk_item_and_key("p", "q", b"c", b"\x00", "base1").1),
        ];

        let in_half_open = |k: &[u8], s: &[u8], e: &[u8]| k >= s && k < e;

        let kc = parse_key_condition(
            &tokenize("h1 = :h1 AND r0 = :r0 AND h0 = :h0 AND r1 = :r1").unwrap(),
        )
        .unwrap();
        let mut maps = ExpressionMaps::default();
        maps.values
            .insert("h0".into(), AttributeValue::S("p".into()));
        maps.values
            .insert("h1".into(), AttributeValue::S("q".into()));
        maps.values
            .insert("r0".into(), AttributeValue::B(b"b".to_vec()));
        maps.values
            .insert("r1".into(), AttributeValue::B(b"\x00\xff".to_vec()));
        let ((_, s, e, open), _) = build_gsi_range_for_query(&gsi_ks, &kc, &maps).unwrap();
        assert!(open);
        let matched: Vec<&str> = candidates
            .iter()
            .filter(|(_, k)| in_half_open(k, &s, &e))
            .map(|(n, _)| *n)
            .collect();
        assert_eq!(matched, vec!["r1_eq_1", "r1_eq_2"]);

        let kc_bw = parse_key_condition(
            &tokenize("h0 = :h0 AND h1 = :h1 AND r0 = :r0 AND begins_with(r1, :pfx)").unwrap(),
        )
        .unwrap();
        maps.values
            .insert("pfx".into(), AttributeValue::B(b"\x00\xff".to_vec()));
        let ((_, s, e, open), _) = build_gsi_range_for_query(&gsi_ks, &kc_bw, &maps).unwrap();
        assert!(open);
        let matched_bw: Vec<&str> = candidates
            .iter()
            .filter(|(_, k)| in_half_open(k, &s, &e))
            .map(|(n, _)| *n)
            .collect();
        assert_eq!(matched_bw, vec!["r1_eq_1", "r1_eq_2", "r1_prefix_ext"]);

        maps.values
            .insert("pfx".into(), AttributeValue::B(vec![0xFF; 24]));
        let ((_, s_ff, e_ff, open_ff), _) =
            build_gsi_range_for_query(&gsi_ks, &kc_bw, &maps).unwrap();
        assert!(open_ff);
        let matched_ff: Vec<&str> = candidates
            .iter()
            .filter(|(_, k)| in_half_open(k, &s_ff, &e_ff))
            .map(|(n, _)| *n)
            .collect();
        assert_eq!(matched_ff, vec!["r1_all_ff"]);
    }

    #[test]
    fn encode_exclusive_start_key_rejects_missing_extra_and_wrong_type() {
        let base_info = TableKeyInfo {
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".into(),
                    key_type: KeyType::Range,
                },
            ],
            attribute_definitions: vec![
                AttributeDefinition {
                    attribute_name: "pk".into(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".into(),
                    attribute_type: ScalarAttributeType::N,
                },
                AttributeDefinition {
                    attribute_name: "gsi_pk".into(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "lsi_sk".into(),
                    attribute_type: ScalarAttributeType::S,
                },
            ],
            ..TableKeyInfo::default()
        };

        let mut valid_base = BTreeMap::new();
        valid_base.insert("pk".into(), AttributeValue::S("p1".into()));
        valid_base.insert("sk".into(), AttributeValue::N("10".into()));
        assert!(encode_exclusive_start_key(&valid_base, &base_info, false, true).is_ok());

        let mut extra_base = valid_base.clone();
        extra_base.insert("extra".into(), AttributeValue::S("x".into()));
        let err = encode_exclusive_start_key(&extra_base, &base_info, false, true).unwrap_err();
        assert!(matches!(
            err,
            StorageError::Validation(ref m)
                if m == "The provided starting key is invalid: The provided key element does not match the schema"
        ));

        let mut wrong_type_base = valid_base.clone();
        wrong_type_base.insert("sk".into(), AttributeValue::S("10".into()));
        let err_q =
            encode_exclusive_start_key(&wrong_type_base, &base_info, false, false).unwrap_err();
        assert!(matches!(
            err_q,
            StorageError::Validation(ref m) if m == "The provided starting key is invalid"
        ));

        // LSI ExclusiveStartKey requires `(pk, sk, lsi_sk)` with no extras.
        let lsi_info = TableKeyInfo {
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "lsi_sk".into(),
                    key_type: KeyType::Range,
                },
            ],
            base_key_schema: base_info.key_schema.clone(),
            attribute_definitions: base_info.attribute_definitions.clone(),
            ..TableKeyInfo::default()
        };
        let mut valid_lsi = valid_base.clone();
        valid_lsi.insert("lsi_sk".into(), AttributeValue::S("l1".into()));
        assert_eq!(extract_lek(&valid_lsi, &lsi_info, true).len(), 3);
        assert!(encode_exclusive_start_key(&valid_lsi, &lsi_info, true, false).is_ok());

        let gsi_info = TableKeyInfo {
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".into(),
                key_type: KeyType::Hash,
            }],
            base_key_schema: base_info.key_schema.clone(),
            attribute_definitions: base_info.attribute_definitions.clone(),
            ..TableKeyInfo::default()
        };
        let mut valid_gsi = valid_base;
        valid_gsi.insert("gsi_pk".into(), AttributeValue::S("g1".into()));
        assert!(encode_exclusive_start_key(&valid_gsi, &gsi_info, true, true).is_ok());

        let mut extra_gsi = valid_gsi;
        extra_gsi.insert("bogus".into(), AttributeValue::S("x".into()));
        let err_gsi = encode_exclusive_start_key(&extra_gsi, &gsi_info, true, true).unwrap_err();
        assert!(matches!(
            err_gsi,
            StorageError::Validation(ref m) if m == "The provided starting key is invalid"
        ));
    }
}
