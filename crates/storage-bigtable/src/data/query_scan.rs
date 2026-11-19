// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query and Scan over a BigTable data table.

use std::collections::BTreeMap;

use extenddb_core::expression::{
    Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition, evaluate_condition,
};
use extenddb_core::types::{AttributeValue, Item, KeySchemaElement, KeyType, TableKeyInfo};
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
    let hash = crc32fast::hash(&encoded_row_key[..end]) as u64;
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

    /// Run a Query against a Local Secondary Index.
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

        let pk_start = row_key::pk_range_start(&pk_value)?;
        let pk_end = row_key::pk_range_end_inclusive(&pk_value)?;

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
        let lsi_cond = RowFilter {
            filter: Some(Filter::Condition(Box::new(Condition {
                predicate_filter: Some(Box::new(predicate)),
                true_filter: Some(Box::new(data_family_latest_filter())),
                false_filter: Some(Box::new(RowFilter {
                    filter: Some(Filter::BlockAllFilter(true)),
                })),
            }))),
        };

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
            let av = a.get(&lsi_sk_name);
            let bv = b.get(&lsi_sk_name);
            let aenc = av
                .and_then(|v| row_key::sk_tag_and_bytes(v).ok())
                .map(|(_, b)| b)
                .unwrap_or_default();
            let benc = bv
                .and_then(|v| row_key::sk_tag_and_bytes(v).ok())
                .map(|(_, b)| b)
                .unwrap_or_default();
            aenc.cmp(&benc)
        });

        if let Some(sk_cond) = &kc.sk_condition {
            items = filter_items_by_sk_condition(items, &lsi_sk_name, sk_cond, maps, self)?;
        }

        if !forward {
            items.reverse();
        }

        if let Some(esk) = exclusive_start_key {
            let base_sk_name = base_key_info
                .key_schema
                .iter()
                .find(|k| k.key_type == KeyType::Range)
                .map(|k| k.attribute_name.clone());
            let esk_lsi = esk.get(&lsi_sk_name);
            let esk_base_sk = base_sk_name.as_ref().and_then(|n| esk.get(n));
            let cut = items.iter().position(|it| {
                let item_lsi = it.get(&lsi_sk_name);
                let item_base_sk = base_sk_name.as_ref().and_then(|n| it.get(n));
                item_lsi == esk_lsi && item_base_sk == esk_base_sk
            });
            if let Some(p) = cut {
                items.drain(..=p);
            }
        }

        let last_evaluated = match limit {
            Some(l) if l > 0 && items.len() > l as usize => {
                let last = items[l as usize - 1].clone();
                items.truncate(l as usize);
                let mut lek: Item = BTreeMap::new();
                for ks in &base_key_info.key_schema {
                    if let Some(v) = last.get(&ks.attribute_name) {
                        lek.insert(ks.attribute_name.clone(), v.clone());
                    }
                }
                if let Some(v) = last.get(&lsi_sk_name) {
                    lek.insert(lsi_sk_name.clone(), v.clone());
                }
                Some(lek)
            }
            _ => None,
        };

        Ok((items, last_evaluated))
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

/// Apply a `FilterExpression` to a list of items, returning only those that match.
pub fn apply_filter(
    items: Vec<Item>,
    filter: Option<&Expr>,
    maps: &ExpressionMaps,
) -> Result<Vec<Item>, StorageError> {
    let Some(f) = filter else {
        return Ok(items);
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match evaluate_condition(f, &item, maps) {
            Ok(true) => out.push(item),
            Ok(false) => {}
            Err(e) => {
                return Err(StorageError::Validation(format!(
                    "filter expression evaluation: {e}"
                )));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
