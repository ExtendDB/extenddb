// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared post-read processing for Query and Scan operations.
//!
//! Both operations read from storage in chunks and apply the same filter,
//! project, and page-budget pipeline to each item as it arrives, stopping the
//! read when the page is full. This module holds that shared logic.

use std::collections::{HashMap, HashSet};

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionMaps, PathElement, Projection, evaluate_condition, validate_unused_attributes,
};
use extenddb_core::types::{
    IndexInfo, Item, KeySchemaElement, Select, extract_key, item_size_bytes,
};
use extenddb_storage::{BoxedFuture, QueryResult};

use crate::create_table::storage_err_to_dynamo;
use crate::index_helpers::apply_index_projection;

/// Reject `ExpressionAttributeNames` entries the projection never references.
///
/// Mirrors the unused-attribute check Query and Scan run via
/// `validate_unused_attributes`, narrowed to read handlers that accept only a
/// projection (`GetItem`, `BatchGetItem`). `names` is the raw request map with
/// keys still carrying their `#` prefix. The caller passes this only for a
/// user-supplied `ProjectionExpression`. Desugared `AttributesToGet` uses
/// synthetic placeholders, so it is not checked here; duplicate
/// `AttributesToGet` entries are currently accepted (a divergence from
/// Amazon DynamoDB, tracked separately).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when a declared name is unused.
pub fn validate_projection_unused_names(
    names: Option<&HashMap<String, String>>,
    projection: &[Vec<PathElement>],
) -> Result<(), DynamoDbError> {
    let Some(names) = names.filter(|m| !m.is_empty()) else {
        return Ok(());
    };
    let mut used_names: HashSet<String> = HashSet::new();
    for path in projection {
        for el in path {
            if let PathElement::Attribute(name) = el
                && let Some(stripped) = name.strip_prefix('#')
            {
                used_names.insert(stripped.to_owned());
            }
        }
    }
    validate_unused_attributes(
        names,
        &HashMap::new(),
        &[],
        &[],
        &used_names,
        &HashSet::new(),
    )
}

/// Result of the post-read processing pipeline.
#[derive(Debug)]
pub struct PostReadResult {
    pub items: Option<Vec<Item>>,
    pub count: i64,
    pub scanned_count: i64,
    pub last_evaluated_key: Option<Item>,
}

/// The `DynamoDB` page budget: a Query or Scan evaluates at most this many bytes
/// of items before it returns a page.
pub const PAGE_BYTE_BUDGET: usize = 1_048_576;

/// Items requested from storage on the first chunk of a page.
const FIRST_CHUNK: i64 = 128;
/// Bounds on later chunk sizes. The lower bound keeps round trips reasonable
/// for tiny items; the upper bound caps the memory one chunk can hold when
/// the items after the estimate are far larger than the items before it
/// (1,024 items of the 400 KB maximum is 400 MB, against a whole table
/// before this change).
const MIN_CHUNK: i64 = 32;
const MAX_CHUNK: i64 = 1024;

/// The inputs to the post-read pipeline that stay fixed across the chunks of
/// one page.
pub struct PostRead<'a> {
    pub filter: Option<&'a Expr>,
    pub projection: Option<&'a Projection>,
    pub maps: &'a ExpressionMaps,
    /// Key schema used for `LastEvaluatedKey` and for the chunk cursor: the
    /// base key, plus the index key on an index read.
    pub lek_key_schema: &'a [KeySchemaElement],
    pub select: Option<&'a Select>,
    pub index_proj: Option<&'a IndexInfo>,
    pub base_key_schema: &'a [KeySchemaElement],
}

/// A page of a Query or Scan, plus the bytes that were evaluated to build it
/// (the basis for `ConsumedCapacity`).
#[derive(Debug)]
pub struct PageReadResult {
    pub post: PostReadResult,
    pub evaluated_bytes: usize,
}

/// Outcome of offering one item to the accumulator.
enum Pushed {
    Evaluated,
    /// The item was not evaluated: it would take the page past its budget.
    PageFull,
}

/// Applies `FilterExpression`, `ProjectionExpression`, `Select`, and the page
/// byte budget to items one at a time, so the caller can stop reading from
/// storage as soon as the page is full.
struct PostReadAccumulator<'a> {
    post: &'a PostRead<'a>,
    is_count: bool,
    scanned_count: i64,
    filtered_count: i64,
    evaluated_bytes: usize,
    result_items: Vec<Item>,
    last_processed_key: Option<Item>,
}

impl<'a> PostReadAccumulator<'a> {
    fn new(post: &'a PostRead<'a>) -> Self {
        Self {
            post,
            is_count: matches!(post.select, Some(Select::Count)),
            scanned_count: 0,
            filtered_count: 0,
            evaluated_bytes: 0,
            result_items: Vec::new(),
            last_processed_key: None,
        }
    }

    /// Evaluate `item` unless doing so would exceed the page budget.
    ///
    /// The first item of a page is always evaluated, so a page always makes
    /// progress. The budget applies to evaluated bytes whether or not the
    /// item passes the filter: a page whose items all fail the filter still
    /// ends at the budget, with `Count` 0 and a `LastEvaluatedKey`, as the
    /// service does.
    fn push(&mut self, item: &Item) -> Result<Pushed, DynamoDbError> {
        let item_bytes = item_size_bytes(item);
        if self.scanned_count > 0 && self.evaluated_bytes + item_bytes > PAGE_BYTE_BUDGET {
            return Ok(Pushed::PageFull);
        }
        self.evaluated_bytes += item_bytes;
        self.scanned_count += 1;
        self.last_processed_key = Some(extract_key(item, self.post.lek_key_schema));

        if let Some(filter_expr) = self.post.filter {
            let passed = evaluate_condition(filter_expr, item, self.post.maps).map_err(|e| {
                let msg = e.to_string();
                if msg.starts_with("Invalid ") {
                    DynamoDbError::ValidationException(msg)
                } else {
                    DynamoDbError::ValidationException(format!("Invalid FilterExpression: {msg}"))
                }
            })?;
            if !passed {
                return Ok(Pushed::Evaluated);
            }
        }

        self.filtered_count += 1;

        if !self.is_count {
            let projected = if let Some(proj) = self.post.projection {
                proj.apply(item)
            } else if let Some(idx) = self.post.index_proj {
                apply_index_projection(item, idx, self.post.base_key_schema)
            } else {
                item.clone()
            };
            self.result_items.push(projected);
        }
        Ok(Pushed::Evaluated)
    }

    /// Items to request next so the remaining budget fills in one round trip,
    /// estimated from the average size of the items evaluated so far.
    fn next_chunk(&self) -> i64 {
        if self.scanned_count == 0 {
            return FIRST_CHUNK;
        }
        let avg = (self.evaluated_bytes / usize::try_from(self.scanned_count).unwrap_or(1)).max(1);
        let remaining = PAGE_BYTE_BUDGET.saturating_sub(self.evaluated_bytes);
        // One extra so the read that fills the budget also learns whether
        // more data exists, and an eighth of slack for size variance.
        let want = remaining / avg + 1;
        i64::try_from(want + want / 8)
            .unwrap_or(MAX_CHUNK)
            .clamp(MIN_CHUNK, MAX_CHUNK)
    }

    /// Close the page. `more` says whether data may remain after the last
    /// evaluated item; when it does, that item's key is the `LastEvaluatedKey`.
    fn finish(self, more: bool) -> PageReadResult {
        let count = if self.is_count {
            self.filtered_count
        } else {
            i64::try_from(self.result_items.len()).unwrap_or(i64::MAX)
        };
        PageReadResult {
            post: PostReadResult {
                items: if self.is_count {
                    None
                } else {
                    Some(self.result_items)
                },
                count,
                scanned_count: self.scanned_count,
                last_evaluated_key: if more { self.last_processed_key } else { None },
            },
            evaluated_bytes: self.evaluated_bytes,
        }
    }
}

/// Read one page of a Query or Scan from storage in chunks.
///
/// `fetch(cursor, count)` performs the storage read for up to `count` items
/// after `cursor` (a full `LastEvaluatedKey`-shaped key, or `None` for the
/// start) and returns the items with the storage layer's own
/// `LastEvaluatedKey`, which is `Some` when more items exist past the chunk.
///
/// The page ends when the next item would exceed [`PAGE_BYTE_BUDGET`], when
/// `limit` items have been evaluated, or when storage is exhausted. Storage is
/// asked for at most one chunk beyond what the page needs, so the memory and
/// time of a page depend on the page, not on the size of the table or
/// partition behind it.
///
/// # Errors
///
/// Returns the storage error mapped through `storage_err_to_dynamo`, a
/// `ValidationException` from `FilterExpression` evaluation, or an
/// `InternalServerError` when storage returns a page that does not advance
/// the cursor (which would otherwise loop).
pub async fn read_page<'s, F>(
    limit: Option<i64>,
    exclusive_start_key: Option<&Item>,
    post: &PostRead<'_>,
    mut fetch: F,
) -> Result<PageReadResult, DynamoDbError>
where
    F: FnMut(Option<&Item>, i64) -> BoxedFuture<'s, QueryResult>,
{
    let mut acc = PostReadAccumulator::new(post);
    let mut cursor: Option<Item> = exclusive_start_key.cloned();

    loop {
        let want = match limit {
            Some(l) => acc.next_chunk().min(l - acc.scanned_count).max(1),
            None => acc.next_chunk(),
        };
        let (items, storage_lek) = fetch(cursor.as_ref(), want)
            .await
            .map_err(storage_err_to_dynamo)?;

        for item in &items {
            // A backend that returns more than it was asked for must not push
            // the page past Limit.
            if limit.is_some_and(|l| acc.scanned_count >= l) {
                return Ok(acc.finish(true));
            }
            match acc.push(item)? {
                Pushed::Evaluated => {}
                Pushed::PageFull => return Ok(acc.finish(true)),
            }
        }

        if limit.is_some_and(|l| acc.scanned_count >= l) {
            return Ok(acc.finish(storage_lek.is_some()));
        }
        // The budget is spent to the byte: the next item, whatever its size,
        // would not fit, so there is nothing to fetch.
        if acc.evaluated_bytes >= PAGE_BYTE_BUDGET {
            return Ok(acc.finish(storage_lek.is_some()));
        }
        if storage_lek.is_none() {
            return Ok(acc.finish(false));
        }
        // Every fetched item was evaluated and storage has more: continue
        // from the last one, addressed the way a client would address it. A
        // page that does not move the cursor (empty, or the same key again)
        // would loop forever, so it is an error rather than a retry.
        let next = items
            .last()
            .map(|item| extract_key(item, post.lek_key_schema));
        if next.is_none() || next == cursor {
            return Err(DynamoDbError::InternalServerError(
                "storage returned a page that did not advance the read cursor".to_owned(),
            ));
        }
        cursor = next;
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use extenddb_core::limits::LimitsConfig;
    use extenddb_core::types::{AttributeValue, KeyType};

    use super::*;
    use crate::expression_helpers::parse_optional_filter;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn pk_schema() -> Vec<KeySchemaElement> {
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }]
    }

    /// `n` items with zero-padded keys and a payload of `payload_bytes`.
    fn items(n: usize, payload_bytes: usize) -> Vec<Item> {
        (0..n)
            .map(|i| {
                let mut item = Item::new();
                item.insert("pk".to_owned(), AttributeValue::S(format!("{i:08}")));
                item.insert(
                    "payload".to_owned(),
                    AttributeValue::S("x".repeat(payload_bytes)),
                );
                item
            })
            .collect()
    }

    /// An in-memory store that answers `limit + exclusive_start_key` reads
    /// the way the backends do (fetch one past the limit to detect more) and
    /// records every read it was asked for.
    /// Every read the store answered: the cursor's pk and the count asked for.
    type Reads = Rc<RefCell<Vec<(Option<String>, i64)>>>;

    struct Store {
        items: Vec<Item>,
        reads: Reads,
    }

    impl Store {
        fn fetch(&self, cursor: Option<&Item>, count: i64) -> BoxedFuture<'static, QueryResult> {
            let cursor_pk = cursor.and_then(|k| match k.get("pk") {
                Some(AttributeValue::S(s)) => Some(s.clone()),
                _ => None,
            });
            self.reads.borrow_mut().push((cursor_pk.clone(), count));
            let start = cursor_pk.map_or(0, |c| {
                self.items
                    .iter()
                    .position(|it| matches!(it.get("pk"), Some(AttributeValue::S(s)) if *s > c))
                    .unwrap_or(self.items.len())
            });
            let want = usize::try_from(count).unwrap_or(0);
            let slice = &self.items[start..(start + want + 1).min(self.items.len())];
            let has_more = slice.len() > want;
            let page: Vec<Item> = slice.iter().take(want).cloned().collect();
            let lek = if has_more {
                page.last().map(|it| extract_key(it, &pk_schema()))
            } else {
                None
            };
            Box::pin(async move { Ok((page, lek)) })
        }
    }

    fn run(
        store: &Store,
        limit: Option<i64>,
        start: Option<&Item>,
        filter: Option<&Expr>,
    ) -> PageReadResult {
        let schema = pk_schema();
        let maps = ExpressionMaps::default();
        let post = PostRead {
            filter,
            projection: None,
            maps: &maps,
            lek_key_schema: &schema,
            select: None,
            index_proj: None,
            base_key_schema: &schema,
        };
        block_on(read_page(limit, start, &post, |cursor, count| {
            store.fetch(cursor, count)
        }))
        .unwrap()
    }

    fn store(n: usize, payload_bytes: usize) -> Store {
        Store {
            items: items(n, payload_bytes),
            reads: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn pk_of(key: &Item) -> &str {
        match key.get("pk") {
            Some(AttributeValue::S(s)) => s,
            other => panic!("unexpected key {other:?}"),
        }
    }

    #[test]
    fn unlimited_read_of_a_large_table_fetches_only_what_the_page_needs() {
        // 100,000 items of ~4 KB: the page holds about 256 of them.
        let s = store(100_000, 4_000);
        let page = run(&s, None, None, None);
        let items = page.post.items.unwrap();
        assert!(page.evaluated_bytes <= PAGE_BYTE_BUDGET);
        assert!(items.len() >= 250 && items.len() <= 262, "{}", items.len());
        assert_eq!(
            pk_of(page.post.last_evaluated_key.as_ref().unwrap()),
            pk_of(items.last().unwrap())
        );
        let reads = s.reads.borrow();
        let fetched: i64 = reads.iter().map(|(_, n)| n).sum();
        assert!(reads.len() <= 3, "reads: {reads:?}");
        assert!(fetched < 1_000, "fetched {fetched} items for a 1 MB page");
    }

    #[test]
    fn page_ends_at_the_budget_inside_a_chunk_and_names_the_last_evaluated_item() {
        // 300 KB items: three fit, the fourth would exceed 1 MB.
        let s = store(10, 300_000);
        let page = run(&s, None, None, None);
        assert_eq!(page.post.scanned_count, 3);
        assert_eq!(page.post.count, 3);
        assert_eq!(
            pk_of(page.post.last_evaluated_key.as_ref().unwrap()),
            "00000002"
        );
        // Resuming from that key yields the next three.
        let next = run(&s, None, page.post.last_evaluated_key.as_ref(), None);
        let next_items = next.post.items.unwrap();
        assert_eq!(pk_of(&next_items[0]), "00000003");
        assert_eq!(next_items.len(), 3);
    }

    #[test]
    fn a_page_whose_items_all_fail_the_filter_ends_at_the_budget_with_a_key() {
        let s = store(1_000, 4_000);
        let filter =
            parse_optional_filter(Some("payload = :nomatch"), &LimitsConfig::default()).unwrap();
        let mut values = HashMap::new();
        values.insert(":nomatch".to_owned(), AttributeValue::S("never".to_owned()));
        let maps = crate::expression_helpers::build_expression_maps(None, Some(&values));
        let schema = pk_schema();
        let post = PostRead {
            filter: filter.as_ref(),
            projection: None,
            maps: &maps,
            lek_key_schema: &schema,
            select: None,
            index_proj: None,
            base_key_schema: &schema,
        };
        let page = block_on(read_page(None, None, &post, |c, n| s.fetch(c, n))).unwrap();
        assert_eq!(page.post.count, 0);
        assert!(page.post.items.unwrap().is_empty());
        assert!(page.evaluated_bytes <= PAGE_BYTE_BUDGET);
        assert!(page.post.scanned_count < 1_000, "scanned the whole table");
        assert!(page.post.last_evaluated_key.is_some());
    }

    #[test]
    fn limit_pages_end_at_the_limit_and_report_more_only_when_more_exists() {
        let s = store(20, 10);
        let p1 = run(&s, Some(7), None, None);
        assert_eq!(p1.post.count, 7);
        assert_eq!(
            pk_of(p1.post.last_evaluated_key.as_ref().unwrap()),
            "00000006"
        );
        let p2 = run(&s, Some(7), p1.post.last_evaluated_key.as_ref(), None);
        assert_eq!(
            pk_of(p2.post.last_evaluated_key.as_ref().unwrap()),
            "00000013"
        );
        let p3 = run(&s, Some(7), p2.post.last_evaluated_key.as_ref(), None);
        assert_eq!(p3.post.count, 6);
        assert!(p3.post.last_evaluated_key.is_none());
        // Exactly the limit: storage reports nothing past it, so no key.
        let exact = run(&s, Some(20), None, None);
        assert_eq!(exact.post.count, 20);
        assert!(exact.post.last_evaluated_key.is_none());
        // Storage was never asked for more than the limit.
        assert!(s.reads.borrow().iter().all(|(_, n)| *n <= 20));
    }

    #[test]
    fn small_items_are_read_in_growing_chunks_until_the_budget_fills() {
        // 100 byte items: about 9,000 fit in a page.
        let s = store(50_000, 80);
        let page = run(&s, None, None, None);
        assert!(page.evaluated_bytes <= PAGE_BYTE_BUDGET);
        assert!(page.evaluated_bytes > PAGE_BYTE_BUDGET - 200);
        let reads = s.reads.borrow();
        assert_eq!(reads[0].1, FIRST_CHUNK);
        assert!(
            reads[1..]
                .iter()
                .all(|(_, n)| (MIN_CHUNK..=MAX_CHUNK).contains(n))
        );
        assert!(reads.len() < 40, "{} reads for one page", reads.len());
        // Each read after the first starts where the previous one ended.
        assert!(reads[1].0.is_some());
    }

    #[test]
    fn exhausting_storage_returns_no_key_and_evaluates_everything() {
        let s = store(300, 100);
        let page = run(&s, None, None, None);
        assert_eq!(page.post.count, 300);
        assert_eq!(page.post.scanned_count, 300);
        assert!(page.post.last_evaluated_key.is_none());
        assert_eq!(
            page.evaluated_bytes,
            items(300, 100).iter().map(item_size_bytes).sum::<usize>()
        );
    }

    #[test]
    fn count_select_reports_matches_without_items() {
        let s = store(50, 100);
        let schema = pk_schema();
        let maps = ExpressionMaps::default();
        let select = Select::Count;
        let post = PostRead {
            filter: None,
            projection: None,
            maps: &maps,
            lek_key_schema: &schema,
            select: Some(&select),
            index_proj: None,
            base_key_schema: &schema,
        };
        let page = block_on(read_page(None, None, &post, |c, n| s.fetch(c, n))).unwrap();
        assert!(page.post.items.is_none());
        assert_eq!(page.post.count, 50);
        assert_eq!(page.post.scanned_count, 50);
    }

    /// A storage page that does not move the cursor is an error, never a loop.
    fn run_with_fetch<F>(fetch: F) -> Result<PageReadResult, DynamoDbError>
    where
        F: FnMut(Option<&Item>, i64) -> BoxedFuture<'static, QueryResult>,
    {
        let schema = pk_schema();
        let maps = ExpressionMaps::default();
        let post = PostRead {
            filter: None,
            projection: None,
            maps: &maps,
            lek_key_schema: &schema,
            select: None,
            index_proj: None,
            base_key_schema: &schema,
        };
        block_on(read_page(None, None, &post, fetch))
    }

    #[test]
    fn empty_page_that_claims_more_is_an_error_not_a_loop() {
        let calls = Rc::new(RefCell::new(0));
        let c = calls.clone();
        let err = run_with_fetch(move |_, _| {
            *c.borrow_mut() += 1;
            let key = items(1, 1)[0].clone();
            Box::pin(async move { Ok((Vec::new(), Some(key))) })
        });
        assert!(matches!(err, Err(DynamoDbError::InternalServerError(_))));
        assert_eq!(*calls.borrow(), 1);
    }

    #[test]
    fn repeated_page_is_an_error_not_a_loop() {
        let calls = Rc::new(RefCell::new(0));
        let c = calls.clone();
        let err = run_with_fetch(move |_, _| {
            *c.borrow_mut() += 1;
            let page = items(3, 10);
            let key = extract_key(&page[2], &pk_schema());
            Box::pin(async move { Ok((page, Some(key))) })
        });
        assert!(matches!(err, Err(DynamoDbError::InternalServerError(_))));
        // The first page advances from None; the second repeats it and stops.
        assert_eq!(*calls.borrow(), 2);
    }

    /// A backend that ignores the requested count cannot push a page past Limit.
    #[test]
    fn over_returning_backend_cannot_exceed_limit() {
        let schema = pk_schema();
        let maps = ExpressionMaps::default();
        let post = PostRead {
            filter: None,
            projection: None,
            maps: &maps,
            lek_key_schema: &schema,
            select: None,
            index_proj: None,
            base_key_schema: &schema,
        };
        let page = block_on(read_page(Some(3), None, &post, |_, _| {
            let page = items(10, 10);
            let key = extract_key(&page[9], &pk_schema());
            Box::pin(async move { Ok((page, Some(key))) })
        }))
        .unwrap();
        assert_eq!(page.post.count, 3);
        assert_eq!(page.post.scanned_count, 3);
        assert_eq!(
            pk_of(page.post.last_evaluated_key.as_ref().unwrap()),
            "00000002"
        );
    }
}
