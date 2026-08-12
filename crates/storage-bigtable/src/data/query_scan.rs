//! Query and Scan over a BigTable data table.

use std::collections::BTreeMap;

use extenddb_core::expression::{
    Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition, evaluate_condition,
};
use extenddb_core::types::{AttributeValue, Item, KeySchemaElement, KeyType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    ReadRowsRequest, RowFilter, RowRange, RowSet,
};

use crate::data::client::BigtableClient;
use crate::data::encoding::{cell, row_key};
use crate::data::item_ops::FAMILY_DATA;

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

    /// Resolve an Expr that should be a placeholder reference to its underlying
    /// AttributeValue. KeyCondition pk/sk values are always placeholders per
    /// the DDB spec.
    fn resolve_expr_value<'b>(
        &self,
        expr: &Expr,
        maps: &'b ExpressionMaps,
    ) -> Result<&'b AttributeValue, StorageError> {
        match expr {
            Expr::Placeholder(name) => maps.resolve_value(name).map_err(|e| {
                StorageError::Validation(format!("placeholder resolve: {e}"))
            }),
            _ => Err(StorageError::Validation(
                "key condition values must be placeholders".into(),
            )),
        }
    }

    /// Translate a Vec<PathElement> to the single attribute name we expect.
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

    /// Build a single-item Item map containing the partition key, so the
    /// row-key encoder can compute its prefix.
    fn _pk_item(
        &self,
        key_info: &TableKeyInfo,
        pk_value: AttributeValue,
    ) -> Result<Item, StorageError> {
        let pk_name = key_info
            .key_schema
            .iter()
            .find(|k| k.key_type == extenddb_core::types::KeyType::Hash)
            .map(|k| k.attribute_name.clone())
            .ok_or_else(|| StorageError::Validation("table has no HASH key".into()))?;
        let mut m = BTreeMap::new();
        m.insert(pk_name, pk_value);
        Ok(m)
    }

    /// Compute the inclusive lower / upper bounds of the row-range to scan
    /// for a Query. The third element is `true` if the upper bound should be
    /// treated as *exclusive* (BigTable EndKeyOpen) — needed for strict `Lt`
    /// where the user explicitly excluded the boundary value.
    fn build_range_for_query(
        &self,
        _key_info: &TableKeyInfo,
        pk_value: AttributeValue,
        sk_condition: Option<&SortKeyCondition>,
        maps: &ExpressionMaps,
    ) -> Result<(Vec<u8>, Vec<u8>, bool), StorageError> {
        let pk_prefix = row_key::pk_range_start(&pk_value)?;
        let pk_upper = row_key::pk_range_end_inclusive(&pk_value)?;

        let (start, end, end_open) = match sk_condition {
            None => (pk_prefix.clone(), pk_upper, false),
            Some(SortKeyCondition::Compare { op, value, .. }) => {
                let sk_val = self.resolve_expr_value(value, maps)?.clone();
                let (sk_tag, sk_bytes) = row_key::sk_tag_and_bytes(&sk_val)?;
                let mut exact = pk_prefix.clone();
                exact.push(sk_tag);
                exact.extend_from_slice(&sk_bytes);
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
                    out.extend_from_slice(&bytes);
                    Ok(out)
                };
                let mut hi = make_bound(&high_val)?;
                row_key::append_sk_upper_trailer(&mut hi);
                (make_bound(&low_val)?, hi, false)
            }
            Some(SortKeyCondition::BeginsWith { prefix, .. }) => {
                let prefix_val = self.resolve_expr_value(prefix, maps)?.clone();
                // begins_with on N keys is undefined in DDB (and would be
                // surprising given our lex encoding); restrict to S/B.
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
                start.extend_from_slice(&bytes);
                let mut end = start.clone();
                row_key::append_sk_upper_trailer(&mut end);
                (start, end, false)
            }
        };
        Ok((start, end, end_open))
    }

    /// Read a single row's cells into an Item map (or None if absent).
    fn cells_to_item(
        cells: Vec<bigtable_rs::bigtable::RowCell>,
    ) -> Result<Option<Item>, StorageError> {
        let mut item: Item = BTreeMap::new();
        for c in cells {
            if c.family_name == FAMILY_DATA {
                let attr = String::from_utf8(c.qualifier)
                    .map_err(|e| StorageError::Internal(format!("decode qualifier: {e}")))?;
                item.insert(attr, cell::decode(&c.value)?);
            }
        }
        if item.is_empty() {
            Ok(None)
        } else {
            Ok(Some(item))
        }
    }

    /// Run a Query against the base table.
    pub async fn query(
        &self,
        key_info: &TableKeyInfo,
        kc: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        let pk_name = self.path_to_name(&kc.pk_path)?;
        let _ = pk_name; // pk_name validation is enough; we use pk_value directly
        let pk_value = self.resolve_expr_value(&kc.pk_value, maps)?.clone();

        let (mut start, mut end, mut end_open) = self.build_range_for_query(
            key_info,
            pk_value.clone(),
            kc.sk_condition.as_ref(),
            maps,
        )?;

        // ExclusiveStartKey narrows the range to skip past the row the caller
        // last returned. For forward, this means start = resume + 0x00 (smallest
        // byte sequence strictly greater than resume). For reverse, the client
        // post-processes a forward scan — so we need to narrow the END of the
        // forward scan to just-below resume, i.e. EndKeyOpen(resume).
        if let Some(esk) = exclusive_start_key {
            let resume = row_key::encode_key(esk, &key_info.key_schema)?;
            if forward {
                let mut s = resume.clone();
                s.push(0x00);
                start = s.max(start);
            } else {
                end = resume;
                end_open = true;
            }
        }

        let mut data = self.client.data();
        // bigtable gRPC reversed is supported, but we keep the client-side
        // reversal for compatibility with the POC logic for now, or we can use the reversed flag?
        // Wait, CheckAndMutateRow doesn't have reversed, but ReadRows does.
        // ReadRowsRequest has `reversed: bool`.
        // Let's check if the tonic-generated ReadRowsRequest has `reversed`.
        // Actually, we can just do server-side reverse!
        // But the POC says: "bigtable_rs 0.3 doesn't expose the v2 `reversed` flag — we always
        // forward-scan and reverse in the client."
        // Since we are using tonic-generated gRPC code directly, we DO have the `reversed` flag in `ReadRowsRequest`.
        // However, for simplicity and risk reduction, let's keep the client-side reverse for now as it is proven in the POC,
        // and we can optimize it later if we want.
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(StartKey::StartKeyClosed(start)),
                    end_key: Some(if end_open {
                        EndKey::EndKeyOpen(end)
                    } else {
                        EndKey::EndKeyClosed(end)
                    }),
                }],
            }),
            rows_limit: limit.unwrap_or(0),
            filter: Some(RowFilter {
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            reversed: !forward,
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| StorageError::Internal(format!("Query ReadRows: {e}")))?;

        let mut items: Vec<Item> = Vec::with_capacity(resp.len());
        for (_key, cells) in resp {
            if let Some(item) = Self::cells_to_item(cells)? {
                items.push(item);
            }
        }

        let last_evaluated = match limit {
            Some(l) if items.len() == l as usize => items.last().cloned(),
            _ => None,
        };
        Ok((items, last_evaluated))
    }

    /// Run a Query against a Local Secondary Index.
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
        let pk_name = self.path_to_name(&kc.pk_path)?;
        let _ = pk_name;
        let pk_value = self.resolve_expr_value(&kc.pk_value, maps)?.clone();

        let lsi_sk_name = lsi_key_schema
            .iter()
            .find(|k| k.key_type == KeyType::Range)
            .map(|k| k.attribute_name.clone())
            .ok_or_else(|| StorageError::Validation("LSI missing RANGE key".into()))?;

        let pk_start = row_key::pk_range_start(&pk_value)?;
        let pk_end = row_key::pk_range_end_inclusive(&pk_value)?;

        use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::{Chain, Condition};

        let predicate = RowFilter {
            filter: Some(Filter::ColumnQualifierRegexFilter(
                format!("^{}$", lsi_sk_name).into_bytes(),
            )),
        };
        let lsi_cond = RowFilter {
            filter: Some(Filter::Condition(Box::new(Condition {
                predicate_filter: Some(Box::new(predicate)),
                true_filter: Some(Box::new(RowFilter {
                    filter: Some(Filter::PassAllFilter(true)),
                })),
                false_filter: Some(Box::new(RowFilter {
                    filter: Some(Filter::BlockAllFilter(true)),
                })),
            }))),
        };
        let lsi_filter = RowFilter {
            filter: Some(Filter::Chain(Chain {
                filters: vec![
                    lsi_cond,
                    RowFilter {
                        filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                    },
                ],
            })),
        };

        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(StartKey::StartKeyClosed(pk_start)),
                    end_key: Some(EndKey::EndKeyClosed(pk_end)),
                }],
            }),
            rows_limit: 0,
            filter: Some(lsi_filter),
            ..ReadRowsRequest::default()
        };
        let resp = self
            .client
            .data()
            .read_rows(req)
            .await
            .map_err(|e| StorageError::Internal(format!("LSI Query ReadRows: {e}")))?;

        let mut items: Vec<Item> = Vec::with_capacity(resp.len());
        for (_raw_key, cells) in resp {
            if let Some(item) = Self::cells_to_item(cells)? {
                if item.contains_key(&lsi_sk_name) {
                    items.push(item);
                }
            }
        }

        // Sort by LSI sort key.
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

        // SK condition on the LSI sort key.
        if let Some(sk_cond) = &kc.sk_condition {
            items = filter_items_by_sk_condition(items, &lsi_sk_name, sk_cond, maps, self)?;
        }

        if !forward {
            items.reverse();
        }

        // ExclusiveStartKey: skip up to and including the row matching ESK.
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
            Some(l) if items.len() > l as usize => {
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
            Some(l) if items.len() == l as usize => items.last().map(|last| {
                let mut lek: Item = BTreeMap::new();
                for ks in &base_key_info.key_schema {
                    if let Some(v) = last.get(&ks.attribute_name) {
                        lek.insert(ks.attribute_name.clone(), v.clone());
                    }
                }
                if let Some(v) = last.get(&lsi_sk_name) {
                    lek.insert(lsi_sk_name.clone(), v.clone());
                }
                lek
            }),
            _ => None,
        };

        Ok((items, last_evaluated))
    }

    /// Run a Scan against the base table.
    pub async fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
    ) -> Result<(Vec<Item>, Option<Item>), StorageError> {
        let start_key = match exclusive_start_key {
            Some(esk) => {
                let mut k = row_key::encode_key(esk, &key_info.key_schema)?;
                k.push(0x00);
                k
            }
            None => Vec::new(),
        };

        let mut data = self.client.data();
        let req = ReadRowsRequest {
            table_name: self.full_table_name.clone(),
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(StartKey::StartKeyClosed(start_key)),
                    end_key: None,
                }],
            }),
            rows_limit: limit.unwrap_or(0),
            filter: Some(RowFilter {
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| StorageError::Internal(format!("Scan ReadRows: {e}")))?;

        let total = total_segments.unwrap_or(1).max(1) as u64;
        let seg = segment.unwrap_or(0).max(0) as u64 % total;

        let mut items: Vec<Item> = Vec::with_capacity(resp.len());
        let mut last_key: Option<Vec<u8>> = None;
        for (raw_key, cells) in resp {
            if total > 1 && hash_segment(&raw_key, total) != seg {
                continue;
            }
            if let Some(item) = Self::cells_to_item(cells)? {
                items.push(item);
                last_key = Some(raw_key);
            }
        }

        let last_evaluated = match limit {
            Some(l) if items.len() == l as usize => {
                let _ = last_key;
                items.last().map(|item| {
                    let mut key_attrs: Item = BTreeMap::new();
                    for ks in &key_info.key_schema {
                        if let Some(v) = item.get(&ks.attribute_name) {
                            key_attrs.insert(ks.attribute_name.clone(), v.clone());
                        }
                    }
                    key_attrs
                })
            }
            _ => None,
        };
        Ok((items, last_evaluated))
    }
}

fn hash_segment(key: &[u8], total: u64) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in key {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h % total
}

/// Filter items by a KeyCondition's sort-key clause.
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
            let keep: Box<dyn Fn(&[u8]) -> bool> = match op {
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

/// Apply a FilterExpression to a list of items, returning only those that match.
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
