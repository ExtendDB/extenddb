// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query and Scan over a BigTable data table (base, GSI shadow, and sparse LSI).

use std::collections::BTreeMap;

use extenddb_core::expression::{
    Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition,
};
use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::{
    Chain, Condition, Filter,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    ReadRowsRequest, RowFilter, RowRange, RowSet,
};

use crate::data::client::{
    BigtableClient, exact_re2_bytes, is_reverse_scan_unsupported_err, map_bigtable_err,
};
use crate::data::encoding::{cell, row_key};
use crate::data::item_ops::FAMILY_DATA;

/// Extract the `[pk_tag:1][pk_len:4][pk_bytes]` slice from an encoded row key.
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
/// (base table or GSI shadow table) by hashing its partition-key prefix
/// (`[pk_tag:1][pk_len:4][pk_bytes]`) with CRC32.
pub fn pk_segment_from_encoded_key(
    encoded_row_key: &[u8],
    total_segments: u64,
) -> Result<u64, StorageError> {
    if total_segments <= 1 {
        return Ok(0);
    }
    let prefix = pk_prefix_slice(encoded_row_key)?;
    let hash = crc32fast::hash(prefix) as u64;
    Ok(hash % total_segments)
}

/// Compute whether `key` belongs to `segment` of `total_segments` for `key_schema`.
pub fn key_belongs_to_segment(
    key: &Item,
    key_schema: &[KeySchemaElement],
    segment: i64,
    total_segments: i64,
) -> Result<bool, StorageError> {
    let total = total_segments.max(1) as u64;
    if total <= 1 {
        return Ok(true);
    }
    let pk_name = key_schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .map(|k| &k.attribute_name)
        .ok_or_else(|| StorageError::Validation("key schema missing HASH".into()))?;
    let Some(pk_val) = key.get(pk_name) else {
        return Ok(true);
    };
    let pk_prefix = row_key::pk_range_start(pk_val)?;
    let seg = (segment.max(0) as u64) % total;
    Ok(pk_segment_from_encoded_key(&pk_prefix, total)? == seg)
}

fn data_family_latest_filter() -> RowFilter {
    RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{FAMILY_DATA}$"))),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
            ],
        })),
    }
}

/// Server-side sparse filter returning latest `d:*` cells iff `d:<lsi_sk_name>` exists.
fn build_lsi_sparse_filter(lsi_sk_name: &str) -> RowFilter {
    let predicate = RowFilter {
        filter: Some(Filter::Chain(Chain {
            filters: vec![
                RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(format!("^{FAMILY_DATA}$"))),
                },
                RowFilter {
                    filter: Some(Filter::ColumnQualifierRegexFilter(exact_re2_bytes(
                        lsi_sk_name.as_bytes(),
                    ))),
                },
                RowFilter {
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                },
            ],
        })),
    };
    RowFilter {
        filter: Some(Filter::Condition(Box::new(Condition {
            predicate_filter: Some(Box::new(predicate)),
            true_filter: Some(Box::new(data_family_latest_filter())),
            false_filter: Some(Box::new(RowFilter {
                filter: Some(Filter::BlockAllFilter(true)),
            })),
        }))),
    }
}

/// Lexicographic sort encoding `[tag, bytes...]` for an optional sort-key attribute.
fn encode_sk_sort_bytes(val: Option<&AttributeValue>) -> Vec<u8> {
    let Some(av) = val else {
        return Vec::new();
    };
    match row_key::sk_tag_and_bytes(av) {
        Ok((tag, bytes)) => {
            let mut out = Vec::with_capacity(1 + bytes.len());
            out.push(tag);
            out.extend_from_slice(&bytes);
            out
        }
        Err(_) => Vec::new(),
    }
}

fn effective_base_schema(key_info: &TableKeyInfo) -> &[KeySchemaElement] {
    if key_info.base_key_schema.is_empty() {
        &key_info.key_schema
    } else {
        &key_info.base_key_schema
    }
}

fn extract_lek(item: &Item, key_info: &TableKeyInfo, is_gsi: bool) -> Item {
    let mut key_attrs: Item = BTreeMap::new();
    for ks in &key_info.key_schema {
        if let Some(v) = item.get(&ks.attribute_name) {
            key_attrs.insert(ks.attribute_name.clone(), v.clone());
        }
    }
    if is_gsi {
        for ks in &key_info.base_key_schema {
            if let Some(v) = item.get(&ks.attribute_name) {
                key_attrs.insert(ks.attribute_name.clone(), v.clone());
            }
        }
    }
    key_attrs
}

fn extract_lsi_lek(item: &Item, base_key_info: &TableKeyInfo, lsi_sk_name: &str) -> Item {
    let mut lek: Item = BTreeMap::new();
    for ks in effective_base_schema(base_key_info) {
        if let Some(v) = item.get(&ks.attribute_name) {
            lek.insert(ks.attribute_name.clone(), v.clone());
        }
    }
    if let Some(v) = item.get(lsi_sk_name) {
        lek.insert(lsi_sk_name.to_owned(), v.clone());
    }
    lek
}

type LsiStartCursor = (Vec<u8>, Vec<u8>, Vec<u8>);

fn check_defined_scalar_type(
    base_key_info: &TableKeyInfo,
    attr_name: &str,
    val: &AttributeValue,
) -> Result<(), StorageError> {
    if let Some(ad) = base_key_info
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
            return Err(StorageError::Validation(
                "The provided starting key is invalid".into(),
            ));
        }
    }
    Ok(())
}

/// Validate and decode an LSI `ExclusiveStartKey` into
/// `(pk_prefix, lsi_sk_sort_bytes, base_sk_sort_bytes)` (`BUG-13`).
fn parse_lsi_exclusive_start_key(
    esk: &Item,
    base_key_info: &TableKeyInfo,
    lsi_sk_name: &str,
) -> Result<LsiStartCursor, StorageError> {
    let base_schema = effective_base_schema(base_key_info);
    for ks in base_schema {
        let Some(v) = esk.get(&ks.attribute_name) else {
            return Err(StorageError::Validation(
                "The provided starting key is invalid".into(),
            ));
        };
        check_defined_scalar_type(base_key_info, &ks.attribute_name, v)?;
    }

    let base_encoded = row_key::encode_key(esk, base_schema)
        .map_err(|_| StorageError::Validation("The provided starting key is invalid".into()))?;
    let pk_prefix = pk_prefix_slice(&base_encoded)?.to_vec();

    let Some(lsi_val) = esk.get(lsi_sk_name) else {
        return Err(StorageError::Validation(
            "The provided starting key is invalid".into(),
        ));
    };
    check_defined_scalar_type(base_key_info, lsi_sk_name, lsi_val)?;
    let (lsi_tag, lsi_bytes) = row_key::sk_tag_and_bytes(lsi_val)
        .map_err(|_| StorageError::Validation("The provided starting key is invalid".into()))?;
    let mut lsi_sort = Vec::with_capacity(1 + lsi_bytes.len());
    lsi_sort.push(lsi_tag);
    lsi_sort.extend_from_slice(&lsi_bytes);

    let base_sk_name = base_schema
        .iter()
        .find(|k| k.key_type == KeyType::Range)
        .map(|k| k.attribute_name.as_str());
    let base_sort = base_sk_name
        .and_then(|n| esk.get(n))
        .map(|v| encode_sk_sort_bytes(Some(v)))
        .unwrap_or_default();

    let expected_attr_count = if base_schema.iter().any(|k| k.attribute_name == lsi_sk_name) {
        base_schema.len()
    } else {
        base_schema.len() + 1
    };
    if esk.len() != expected_attr_count {
        return Err(StorageError::Validation(
            "The provided starting key is invalid".into(),
        ));
    }

    Ok((pk_prefix, lsi_sort, base_sort))
}

pub struct QueryScan<'a> {
    client: &'a BigtableClient,
    full_table_name: String,
}

impl<'a> QueryScan<'a> {
    pub fn new(client: &'a BigtableClient, data_table_short: &str) -> Self {
        Self {
            client,
            full_table_name: client.full_table_name(data_table_short),
        }
    }

    fn resolve_expr_value<'b>(
        &self,
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

    fn path_to_name(&self, path: &[PathElement]) -> Result<String, StorageError> {
        if path.len() != 1 {
            return Err(StorageError::Validation(
                "key condition path must be a single attribute".into(),
            ));
        }
        match &path[0] {
            PathElement::Attribute(s) => Ok(s.clone()),
            PathElement::Index(_) => Err(StorageError::Validation(
                "key condition path must be an attribute name, not an index".into(),
            )),
        }
    }

    fn build_range_for_query(
        &self,
        key_info: &TableKeyInfo,
        pk_value: AttributeValue,
        sk_condition: Option<&SortKeyCondition>,
        maps: &ExpressionMaps,
    ) -> Result<(Vec<u8>, Vec<u8>, bool), StorageError> {
        let is_gsi =
            !key_info.base_key_schema.is_empty() && key_info.base_key_schema != key_info.key_schema;
        let pk_prefix = row_key::pk_range_start(&pk_value)?;
        let pk_upper = row_key::pk_range_end_inclusive(&pk_value)?;

        let (start, end, end_open) = match sk_condition {
            None => (pk_prefix.clone(), pk_upper, false),
            Some(SortKeyCondition::Compare { op, value, .. }) => {
                let sk_val = self.resolve_expr_value(value, maps)?.clone();
                let (sk_tag, sk_bytes) = row_key::sk_tag_and_bytes(&sk_val)?;
                let mut exact = pk_prefix.clone();
                exact.push(sk_tag);
                if is_gsi {
                    let escaped = crate::gsi::escape_bytes_terminated(&sk_bytes);
                    exact.extend_from_slice(&escaped);
                } else {
                    exact.extend_from_slice(&sk_bytes);
                }
                let mut exact_plus = exact.clone();
                row_key::append_sk_upper_trailer(&mut exact_plus);
                use extenddb_core::expression::CompareOp;
                match op {
                    CompareOp::Eq => (exact.clone(), exact_plus, false),
                    CompareOp::Gt => (exact_plus, pk_upper, false),
                    CompareOp::Ge => (exact, pk_upper, false),
                    CompareOp::Lt => (pk_prefix, exact, true),
                    CompareOp::Le => (pk_prefix, exact_plus, false),
                    CompareOp::Ne => {
                        return Err(StorageError::Validation(
                            "Ne not valid in KeyConditionExpression for SK".into(),
                        ));
                    }
                }
            }
            Some(SortKeyCondition::Between { low, high, .. }) => {
                let low_val = self.resolve_expr_value(low, maps)?.clone();
                let high_val = self.resolve_expr_value(high, maps)?.clone();
                let make_bound = |v: &AttributeValue| -> Result<Vec<u8>, StorageError> {
                    let (tag, bytes) = row_key::sk_tag_and_bytes(v)?;
                    let mut out = pk_prefix.clone();
                    out.push(tag);
                    if is_gsi {
                        let escaped = crate::gsi::escape_bytes_terminated(&bytes);
                        out.extend_from_slice(&escaped);
                    } else {
                        out.extend_from_slice(&bytes);
                    }
                    Ok(out)
                };
                let mut hi = make_bound(&high_val)?;
                row_key::append_sk_upper_trailer(&mut hi);
                (make_bound(&low_val)?, hi, false)
            }
            Some(SortKeyCondition::BeginsWith { prefix, .. }) => {
                let prefix_val = self.resolve_expr_value(prefix, maps)?.clone();
                let (tag, bytes) = match &prefix_val {
                    AttributeValue::S(s) => (0x53u8, s.as_bytes().to_vec()),
                    AttributeValue::B(b) => (0x42, b.clone()),
                    _ => {
                        return Err(StorageError::Validation(
                            "begins_with prefix must be S/B".into(),
                        ));
                    }
                };
                let mut start = pk_prefix.clone();
                start.push(tag);
                if is_gsi {
                    let escaped = crate::gsi::escape_bytes(&bytes);
                    start.extend_from_slice(&escaped);
                } else {
                    start.extend_from_slice(&bytes);
                }
                let mut end = start.clone();
                row_key::append_sk_upper_trailer(&mut end);
                (start, end, false)
            }
        };
        Ok((start, end, end_open))
    }

    fn cells_to_item(
        cells: Vec<bigtable_rs::bigtable::RowCell>,
    ) -> Result<Option<Item>, StorageError> {
        let mut item: Item = BTreeMap::new();
        for c in cells {
            if c.family_name == FAMILY_DATA {
                let attr = String::from_utf8(c.qualifier)
                    .map_err(|e| StorageError::Internal(format!("decode qualifier: {e}")))?;
                if let std::collections::btree_map::Entry::Vacant(e) = item.entry(attr) {
                    e.insert(cell::decode(&c.value)?);
                }
            }
        }
        if item.is_empty() {
            Ok(None)
        } else {
            Ok(Some(item))
        }
    }

    /// Run a Query against the base table or a GSI shadow table.
    pub async fn query(
        &self,
        key_info: &TableKeyInfo,
        kc: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        let is_gsi =
            !key_info.base_key_schema.is_empty() && key_info.base_key_schema != key_info.key_schema;
        let _pk_name = self.path_to_name(&kc.pk_path)?;
        let pk_value = self.resolve_expr_value(&kc.pk_value, maps)?.clone();

        let (mut start, mut end, mut end_open) =
            self.build_range_for_query(key_info, pk_value, kc.sk_condition.as_ref(), maps)?;

        if let Some(esk) = exclusive_start_key {
            let resume = if is_gsi {
                crate::gsi::shadow_row_key_for_item(
                    esk,
                    &key_info.key_schema,
                    &key_info.base_key_schema,
                )?
                .unwrap_or_else(|| {
                    row_key::encode_key(esk, &key_info.key_schema).unwrap_or_default()
                })
            } else {
                row_key::encode_key(esk, &key_info.key_schema)?
            };
            if forward {
                let mut s = resume;
                s.push(0x00);
                start = s.max(start);
            } else if !end_open && resume <= end || end_open && resume < end {
                end = resume;
                end_open = true;
            }
        }

        if start > end || (end_open && start == end) {
            return Ok((Vec::new(), None));
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

        let fetch_limit = match limit {
            Some(l) if l > 0 => l.saturating_add(1),
            _ => 0,
        };

        let mut data = self.client.data();
        let use_client_reverse_fallback;
        let resp = if !forward && self.client.emulator_host.is_some() {
            // `cbtemulator` does not implement `ReadRowsRequest.reversed = true`;
            // scan the partition forward and reverse in memory.
            use_client_reverse_fallback = true;
            let req = ReadRowsRequest {
                table_name: self.full_table_name.clone(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                rows: Some(make_row_set(start, end, end_open)),
                rows_limit: 0,
                filter: Some(data_family_latest_filter()),
                reversed: false,
                ..ReadRowsRequest::default()
            };
            data.read_rows(req)
                .await
                .map_err(|e| map_bigtable_err("Query ReadRows", e))?
        } else {
            let req = ReadRowsRequest {
                table_name: self.full_table_name.clone(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                rows: Some(make_row_set(start.clone(), end.clone(), end_open)),
                rows_limit: fetch_limit,
                filter: Some(data_family_latest_filter()),
                reversed: !forward,
                ..ReadRowsRequest::default()
            };
            match data.read_rows(req).await {
                Ok(r) => {
                    use_client_reverse_fallback = false;
                    r
                }
                Err(e) if !forward && is_reverse_scan_unsupported_err(&e) => {
                    use_client_reverse_fallback = true;
                    let fallback_req = ReadRowsRequest {
                        table_name: self.full_table_name.clone(),
                        app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                        rows: Some(make_row_set(start, end, end_open)),
                        rows_limit: 0,
                        filter: Some(data_family_latest_filter()),
                        reversed: false,
                        ..ReadRowsRequest::default()
                    };
                    data.read_rows(fallback_req)
                        .await
                        .map_err(|e2| map_bigtable_err("Query ReadRows fallback", e2))?
                }
                Err(e) => return Err(map_bigtable_err("Query ReadRows", e)),
            }
        };

        let mut items: Vec<Item> = Vec::with_capacity(resp.len());
        for (_key, cells) in resp {
            if let Some(item) = Self::cells_to_item(cells)? {
                items.push(item);
            }
        }
        if use_client_reverse_fallback {
            items.reverse();
        }

        let last_evaluated = match limit {
            Some(l) if l > 0 && items.len() > l as usize => {
                items.truncate(l as usize);
                items.last().map(|it| extract_lek(it, key_info, is_gsi))
            }
            _ => None,
        };
        Ok((items, last_evaluated))
    }

    /// Run a Query against a Local Secondary Index (`BUG-13`).
    #[allow(clippy::too_many_arguments)]
    pub async fn query_lsi(
        &self,
        base_key_info: &TableKeyInfo,
        lsi_key_schema: &[KeySchemaElement],
        kc: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        let _pk_name = self.path_to_name(&kc.pk_path)?;
        let pk_value = self.resolve_expr_value(&kc.pk_value, maps)?.clone();

        let lsi_sk_name = lsi_key_schema
            .iter()
            .find(|k| k.key_type == KeyType::Range)
            .map(|k| k.attribute_name.clone())
            .ok_or_else(|| StorageError::Validation("LSI missing RANGE key".into()))?;
        let base_schema = effective_base_schema(base_key_info);
        let base_sk_name = base_schema
            .iter()
            .find(|k| k.key_type == KeyType::Range)
            .map(|k| k.attribute_name.clone());

        let pk_start = row_key::pk_range_start(&pk_value)?;
        let pk_end = row_key::pk_range_end_inclusive(&pk_value)?;

        let parsed_esk = if let Some(esk) = exclusive_start_key {
            let (esk_pk_prefix, esk_lsi, esk_base) =
                parse_lsi_exclusive_start_key(esk, base_key_info, &lsi_sk_name)?;
            if esk_pk_prefix != pk_start {
                return Err(StorageError::Validation(
                    "The provided starting key is outside query boundaries".into(),
                ));
            }
            Some((esk_lsi, esk_base))
        } else {
            None
        };

        let lsi_cond = build_lsi_sparse_filter(&lsi_sk_name);
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(StartKey::StartKeyClosed(pk_start)),
                    end_key: Some(EndKey::EndKeyClosed(pk_end)),
                }],
            }),
            rows_limit: 0,
            filter: Some(lsi_cond),
            ..ReadRowsRequest::default()
        };
        let resp = self
            .client
            .data()
            .read_rows(req)
            .await
            .map_err(|e| map_bigtable_err("LSI Query ReadRows", e))?;

        let mut items: Vec<Item> = Vec::with_capacity(resp.len());
        for (_raw_key, cells) in resp {
            if let Some(item) = Self::cells_to_item(cells)?
                && item.contains_key(&lsi_sk_name)
            {
                items.push(item);
            }
        }

        items.sort_by(|a, b| {
            let a_lsi = encode_sk_sort_bytes(a.get(&lsi_sk_name));
            let b_lsi = encode_sk_sort_bytes(b.get(&lsi_sk_name));
            a_lsi.cmp(&b_lsi).then_with(|| {
                let a_base = encode_sk_sort_bytes(base_sk_name.as_deref().and_then(|n| a.get(n)));
                let b_base = encode_sk_sort_bytes(base_sk_name.as_deref().and_then(|n| b.get(n)));
                a_base.cmp(&b_base)
            })
        });

        if let Some(sk_cond) = &kc.sk_condition {
            items = filter_items_by_sk_condition(items, &lsi_sk_name, sk_cond, maps, self)?;
        }

        if !forward {
            items.reverse();
        }

        if let Some((esk_lsi, esk_base)) = parsed_esk {
            items.retain(|it| {
                let item_lsi = encode_sk_sort_bytes(it.get(&lsi_sk_name));
                let item_base =
                    encode_sk_sort_bytes(base_sk_name.as_deref().and_then(|n| it.get(n)));
                if forward {
                    (item_lsi.as_slice(), item_base.as_slice())
                        > (esk_lsi.as_slice(), esk_base.as_slice())
                } else {
                    (item_lsi.as_slice(), item_base.as_slice())
                        < (esk_lsi.as_slice(), esk_base.as_slice())
                }
            });
        }

        let last_evaluated = match limit {
            Some(l) if l > 0 && items.len() > l as usize => {
                let last = items[l as usize - 1].clone();
                items.truncate(l as usize);
                Some(extract_lsi_lek(&last, base_key_info, &lsi_sk_name))
            }
            _ => None,
        };

        Ok((items, last_evaluated))
    }

    /// Run a Scan against a Local Secondary Index (`BUG-13`; `RFC-0003` §7.1, §7.2, §7.4).
    ///
    /// Streams partitions forward, filtering out sparse base items missing
    /// `lsi_sk_name` *before* applying `limit`, sorting each partition by
    /// `(enc_lsi_sk, enc_base_sk)`, honoring parallel scan (`segment` /
    /// `total_segments`), resuming cleanly from `ExclusiveStartKey`
    /// `(pk, base_sk, lsi_sk)` across intra-partition and inter-partition
    /// boundaries (even if the boundary item was deleted), and emitting
    /// `LastEvaluatedKey` with `(pk, base_sk, lsi_sk)` only when strictly more
    /// LSI items remain.
    pub async fn scan_lsi(
        &self,
        base_key_info: &TableKeyInfo,
        lsi_key_schema: &[KeySchemaElement],
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        let base_schema = effective_base_schema(base_key_info);
        if !base_schema.iter().any(|k| k.key_type == KeyType::Hash) {
            return Err(StorageError::Validation(
                "base table missing HASH key".into(),
            ));
        }
        let base_sk_name = base_schema
            .iter()
            .find(|k| k.key_type == KeyType::Range)
            .map(|k| k.attribute_name.clone());
        let lsi_sk_name = lsi_key_schema
            .iter()
            .find(|k| k.key_type == KeyType::Range)
            .map(|k| k.attribute_name.clone())
            .ok_or_else(|| StorageError::Validation("LSI missing RANGE key".into()))?;

        let (mut cursor_start, esk_resume): (Vec<u8>, Option<LsiStartCursor>) =
            if let Some(esk) = exclusive_start_key {
                let (pk_start, esk_lsi_enc, esk_base_enc) =
                    parse_lsi_exclusive_start_key(esk, base_key_info, &lsi_sk_name)?;
                (
                    pk_start.clone(),
                    Some((pk_start, esk_lsi_enc, esk_base_enc)),
                )
            } else {
                (Vec::new(), None)
            };

        let total = total_segments.unwrap_or(1).max(1) as u64;
        let seg = segment.unwrap_or(0).max(0) as u64 % total;
        let target_plus_one = limit.filter(|&l| l > 0).map(|l| l as usize + 1);
        let page_batch: i64 = match limit {
            Some(l) if l > 0 => (l.saturating_mul(total as i64).saturating_mul(4)).clamp(64, 1024),
            _ => 512,
        };

        let lsi_filter = build_lsi_sparse_filter(&lsi_sk_name);
        let mut data = self.client.data();
        let mut collected: Vec<Item> = Vec::new();
        let mut current_pk_prefix: Option<Vec<u8>> = None;
        let mut current_partition_items: Vec<Item> = Vec::new();

        let flush_partition = |pk_prefix: &[u8],
                               mut part_items: Vec<Item>,
                               out: &mut Vec<Item>|
         -> Result<(), StorageError> {
            if pk_segment_from_encoded_key(pk_prefix, total)? != seg {
                return Ok(());
            }
            part_items.sort_by(|a, b| {
                let a_lsi = encode_sk_sort_bytes(a.get(&lsi_sk_name));
                let b_lsi = encode_sk_sort_bytes(b.get(&lsi_sk_name));
                a_lsi.cmp(&b_lsi).then_with(|| {
                    let a_base =
                        encode_sk_sort_bytes(base_sk_name.as_deref().and_then(|n| a.get(n)));
                    let b_base =
                        encode_sk_sort_bytes(base_sk_name.as_deref().and_then(|n| b.get(n)));
                    a_base.cmp(&b_base)
                })
            });
            for it in part_items {
                if let Some((ref esk_pk_pfx, ref esk_lsi_enc, ref esk_base_enc)) = esk_resume {
                    if pk_prefix < esk_pk_pfx.as_slice() {
                        continue;
                    }
                    if pk_prefix == esk_pk_pfx.as_slice() {
                        let item_lsi_enc = encode_sk_sort_bytes(it.get(&lsi_sk_name));
                        let item_base_enc =
                            encode_sk_sort_bytes(base_sk_name.as_deref().and_then(|n| it.get(n)));
                        if (item_lsi_enc.as_slice(), item_base_enc.as_slice())
                            <= (esk_lsi_enc.as_slice(), esk_base_enc.as_slice())
                        {
                            continue;
                        }
                    }
                }
                out.push(it);
                if let Some(want) = target_plus_one
                    && out.len() >= want
                {
                    break;
                }
            }
            Ok(())
        };

        loop {
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
                rows_limit: page_batch,
                filter: Some(lsi_filter.clone()),
                ..ReadRowsRequest::default()
            };
            let resp = data
                .read_rows(req)
                .await
                .map_err(|e| map_bigtable_err("LSI Scan ReadRows", e))?;
            let batch_count = resp.len();
            if batch_count == 0 {
                break;
            }

            let mut last_raw_key: Option<Vec<u8>> = None;
            let mut enough_after_flush = false;
            for (raw_key, cells) in resp {
                last_raw_key = Some(raw_key.clone());
                let row_pk_prefix = pk_prefix_slice(&raw_key)?.to_vec();
                if let Some(ref cur_pfx) = current_pk_prefix
                    && cur_pfx != &row_pk_prefix
                {
                    let finished = std::mem::take(&mut current_partition_items);
                    flush_partition(cur_pfx, finished, &mut collected)?;
                    if let Some(want) = target_plus_one
                        && collected.len() >= want
                    {
                        current_pk_prefix = None;
                        enough_after_flush = true;
                        break;
                    }
                }
                current_pk_prefix = Some(row_pk_prefix);
                if let Some(item) = Self::cells_to_item(cells)?
                    && item.contains_key(&lsi_sk_name)
                {
                    current_partition_items.push(item);
                }
            }
            if enough_after_flush {
                break;
            }
            if (batch_count as i64) < page_batch {
                break;
            }
            if let Some(mut k) = last_raw_key {
                k.push(0x00);
                cursor_start = k;
            } else {
                break;
            }
        }

        if let Some(cur_pfx) = current_pk_prefix.take() {
            flush_partition(&cur_pfx, current_partition_items, &mut collected)?;
        }

        let last_evaluated = match limit {
            Some(l) if l > 0 && collected.len() > l as usize => {
                collected.truncate(l as usize);
                collected
                    .last()
                    .map(|it| extract_lsi_lek(it, base_key_info, &lsi_sk_name))
            }
            _ => None,
        };
        Ok((collected, last_evaluated))
    }

    /// Run a Scan against the base table or a GSI shadow table.
    pub async fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        let is_gsi =
            !key_info.base_key_schema.is_empty() && key_info.base_key_schema != key_info.key_schema;
        let mut cursor_start = match exclusive_start_key {
            Some(esk) => {
                let mut k = if is_gsi {
                    crate::gsi::shadow_row_key_for_item(
                        esk,
                        &key_info.key_schema,
                        &key_info.base_key_schema,
                    )?
                    .unwrap_or_else(|| {
                        row_key::encode_key(esk, &key_info.key_schema).unwrap_or_default()
                    })
                } else {
                    row_key::encode_key(esk, &key_info.key_schema)?
                };
                k.push(0x00);
                k
            }
            None => Vec::new(),
        };

        let total = total_segments.unwrap_or(1).max(1) as u64;
        let seg = segment.unwrap_or(0).max(0) as u64 % total;
        let mut data = self.client.data();

        if total == 1 {
            let fetch_limit = match limit {
                Some(l) if l > 0 => l.saturating_add(1),
                _ => 0,
            };
            let req = ReadRowsRequest {
                table_name: self.full_table_name.clone(),
                app_profile_id: self.client.app_profile_id.clone().unwrap_or_default(),
                rows: Some(RowSet {
                    row_keys: vec![],
                    row_ranges: vec![RowRange {
                        start_key: Some(StartKey::StartKeyClosed(cursor_start)),
                        end_key: None,
                    }],
                }),
                rows_limit: fetch_limit,
                filter: Some(data_family_latest_filter()),
                ..ReadRowsRequest::default()
            };
            let resp = data
                .read_rows(req)
                .await
                .map_err(|e| map_bigtable_err("Scan ReadRows", e))?;
            let mut items = Vec::with_capacity(resp.len());
            for (_raw_key, cells) in resp {
                if let Some(item) = Self::cells_to_item(cells)? {
                    items.push(item);
                }
            }
            let last_evaluated = match limit {
                Some(l) if l > 0 && items.len() > l as usize => {
                    items.truncate(l as usize);
                    items.last().map(|it| extract_lek(it, key_info, is_gsi))
                }
                _ => None,
            };
            return Ok((items, last_evaluated));
        }

        // Parallel scan (`total > 1`): page forward until we collect `limit + 1`
        // items belonging to `seg` or reach the end of the table.
        let target_plus_one = limit.filter(|&l| l > 0).map(|l| l as usize + 1);
        let page_batch: i64 = match limit {
            Some(l) if l > 0 => (l.saturating_mul(total as i64)).clamp(64, 1024),
            _ => 512,
        };
        let mut items: Vec<Item> = Vec::new();
        loop {
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
                rows_limit: page_batch,
                filter: Some(data_family_latest_filter()),
                ..ReadRowsRequest::default()
            };
            let resp = data
                .read_rows(req)
                .await
                .map_err(|e| map_bigtable_err("Scan ReadRows segment", e))?;
            let batch_count = resp.len();
            if batch_count == 0 {
                break;
            }
            let mut last_raw_key: Option<Vec<u8>> = None;
            for (raw_key, cells) in resp {
                last_raw_key = Some(raw_key.clone());
                if pk_segment_from_encoded_key(&raw_key, total)? != seg {
                    continue;
                }
                if let Some(item) = Self::cells_to_item(cells)? {
                    items.push(item);
                    if let Some(want) = target_plus_one
                        && items.len() >= want
                    {
                        break;
                    }
                }
            }
            if let Some(want) = target_plus_one
                && items.len() >= want
            {
                break;
            }
            if (batch_count as i64) < page_batch {
                break;
            }
            if let Some(mut k) = last_raw_key {
                k.push(0x00);
                cursor_start = k;
            } else {
                break;
            }
        }

        let last_evaluated = match limit {
            Some(l) if l > 0 && items.len() > l as usize => {
                items.truncate(l as usize);
                items.last().map(|it| extract_lek(it, key_info, is_gsi))
            }
            _ => None,
        };
        Ok((items, last_evaluated))
    }
}

/// Filter items by a `KeyCondition`'s sort-key clause.
fn filter_items_by_sk_condition(
    items: Vec<Item>,
    sk_name: &str,
    cond: &SortKeyCondition,
    maps: &ExpressionMaps,
    qs: &QueryScan<'_>,
) -> Result<Vec<Item>, StorageError> {
    let encode = |v: &AttributeValue| -> Result<Vec<u8>, StorageError> {
        row_key::sk_tag_and_bytes(v).map(|(_, b)| b)
    };
    match cond {
        SortKeyCondition::Compare { op, value, .. } => {
            let target = qs.resolve_expr_value(value, maps)?.clone();
            let target_enc = encode(&target)?;
            use extenddb_core::expression::CompareOp;
            type KeepPredicate = Box<dyn Fn(&[u8]) -> bool>;
            let keep: KeepPredicate = match op {
                CompareOp::Eq => Box::new(move |enc: &[u8]| enc == target_enc.as_slice()),
                CompareOp::Lt => Box::new(move |enc: &[u8]| enc < target_enc.as_slice()),
                CompareOp::Le => Box::new(move |enc: &[u8]| enc <= target_enc.as_slice()),
                CompareOp::Gt => Box::new(move |enc: &[u8]| enc > target_enc.as_slice()),
                CompareOp::Ge => Box::new(move |enc: &[u8]| enc >= target_enc.as_slice()),
                CompareOp::Ne => {
                    return Err(StorageError::Validation(
                        "Ne not valid in KeyConditionExpression for SK".into(),
                    ));
                }
            };
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                if let Some(v) = it.get(sk_name) {
                    let enc = encode(v)?;
                    if keep(&enc) {
                        out.push(it);
                    }
                }
            }
            Ok(out)
        }
        SortKeyCondition::Between { low, high, .. } => {
            let lo = encode(&qs.resolve_expr_value(low, maps)?.clone())?;
            let hi = encode(&qs.resolve_expr_value(high, maps)?.clone())?;
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                if let Some(v) = it.get(sk_name) {
                    let enc = encode(v)?;
                    if enc.as_slice() >= lo.as_slice() && enc.as_slice() <= hi.as_slice() {
                        out.push(it);
                    }
                }
            }
            Ok(out)
        }
        SortKeyCondition::BeginsWith { prefix, .. } => {
            let pref_val = qs.resolve_expr_value(prefix, maps)?.clone();
            let pref = match &pref_val {
                AttributeValue::S(s) => s.as_bytes().to_vec(),
                AttributeValue::B(b) => b.clone(),
                _ => {
                    return Err(StorageError::Validation(
                        "begins_with prefix must be S/B".into(),
                    ));
                }
            };
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                if let Some(v) = it.get(sk_name) {
                    let val_bytes = match v {
                        AttributeValue::S(s) => s.as_bytes().to_vec(),
                        AttributeValue::B(b) => b.clone(),
                        _ => continue,
                    };
                    if val_bytes.starts_with(&pref) {
                        out.push(it);
                    }
                }
            }
            Ok(out)
        }
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
            assert!(key_belongs_to_segment(&item, &schema, seg as i64, 4).unwrap());
            for other in 0..4 {
                if other != seg as i64 {
                    assert!(!key_belongs_to_segment(&item, &schema, other, 4).unwrap());
                }
            }
        }
    }

    #[test]
    fn extract_lsi_lek_and_esk_validation() {
        let base_schema = vec![
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".into(),
                key_type: KeyType::Range,
            },
        ];
        let mut engine_rewritten_info = TableKeyInfo {
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
            base_key_schema: base_schema,
            attribute_definitions: vec![
                AttributeDefinition {
                    attribute_name: "pk".into(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".into(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "lsi_sk".into(),
                    attribute_type: ScalarAttributeType::N,
                },
            ],
            ..TableKeyInfo::default()
        };
        let mut item = BTreeMap::new();
        item.insert("pk".to_string(), AttributeValue::S("p1".into()));
        item.insert("sk".to_string(), AttributeValue::S("s9".into()));
        item.insert("lsi_sk".to_string(), AttributeValue::N("42".into()));
        item.insert("extra".to_string(), AttributeValue::S("ignored".into()));

        let lek = extract_lsi_lek(&item, &engine_rewritten_info, "lsi_sk");
        assert_eq!(lek.len(), 3);
        assert!(parse_lsi_exclusive_start_key(&lek, &engine_rewritten_info, "lsi_sk").is_ok());

        // Missing base_sk must fail validation
        let mut missing_bsk = lek.clone();
        missing_bsk.remove("sk");
        assert!(
            parse_lsi_exclusive_start_key(&missing_bsk, &engine_rewritten_info, "lsi_sk").is_err()
        );

        // Wrong scalar type on lsi_sk must fail validation
        let mut wrong_type = lek.clone();
        wrong_type.insert("lsi_sk".into(), AttributeValue::S("not_num".into()));
        assert!(
            parse_lsi_exclusive_start_key(&wrong_type, &engine_rewritten_info, "lsi_sk").is_err()
        );

        // Extra extraneous attribute must fail validation
        let mut extra_attr = lek;
        extra_attr.insert("bogus".into(), AttributeValue::S("x".into()));
        assert!(
            parse_lsi_exclusive_start_key(&extra_attr, &engine_rewritten_info, "lsi_sk").is_err()
        );

        // Also works when base_key_schema is empty (direct storage caller)
        engine_rewritten_info.key_schema = engine_rewritten_info.base_key_schema.clone();
        engine_rewritten_info.base_key_schema.clear();
        let lek2 = extract_lsi_lek(&item, &engine_rewritten_info, "lsi_sk");
        assert!(parse_lsi_exclusive_start_key(&lek2, &engine_rewritten_info, "lsi_sk").is_ok());
    }
}
