// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The read of a SELECT's disjuncts one after another, a page at a time.

use serde::Deserialize;
use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::SelectStatement;
use extenddb_core::partiql::plan::{KeyNames, KeyRead};
use extenddb_core::types::{Item, item_size_bytes};

use super::read_plan::Step;
use super::request::{Paging, key_condition, query_request, render_filter};
use super::token_mismatch;
use super::visits::{Cursor, Ending, Read, Walk};
use crate::partiql::SingleRequest;
use crate::partiql::expr_bridge::ExprBridge;
use crate::{DispatchMetrics, DispatchResult, OperationContext};

/// The size of a page of a read of disjuncts one after another: the 1 MB of
/// a Query's page.
const PAGE_BYTES: u64 = crate::read_helpers::PAGE_BYTE_BUDGET as u64;

/// Read a page of the disjuncts `steps`, numbered in read order: all of them
/// for the first page, else those a NextToken says the last page left, in
/// the order the visit rules give (see the `visits` module). Measured
/// 2026-11-02:
///
/// * a disjunct that names one item takes one unit of `Limit`, whether the
///   item exists or not (`pk IN ['a', 'zz']` with `Limit` 1 gives [a] and a
///   NextToken, then an empty page with none);
/// * any other disjunct is a Query with the units left; a Query that stops
///   at the limit ends the page, and a later page goes on inside it; a
///   Query that runs out leaves the units it did not evaluate to the next
///   disjunct in the same page (`(pk = 'b' AND sk = 'x') OR (pk = 'a' AND
///   sk > 'a')` with `Limit` 1 gives [a/x], [a/y], then [b/x]);
/// * once the last disjunct is read there is no NextToken, so a read of one
///   item never has one;
/// * a page stops before a whole item that would take it past 1 MB, and the
///   item is left for a later page (four items of about 380 KB, `pk IN
///   ['a', 'b', 'c', 'd']` without `Limit`, give [a, b] with a NextToken,
///   then [c, d]).
///
/// The disjuncts of a page share one budget of [`PAGE_BYTES`], counted on
/// the items read as a Query's page counts them, before the projection and
/// the filter. A Query reads until the item that takes the page past it,
/// keeps that item, and ends the page there with that item's key as the
/// LastEvaluatedKey; the next page goes on inside that Query (measured
/// 2026-11-03: with partitions a and b of two items of about 380 KB, `pk IN
/// ['a', 'b']` gives [a/1, a/2, b/1] with the key b/1 and a NextToken, then
/// [b/2]; with partitions a, b and c of one such item, `pk IN ['a', 'b',
/// 'c']` gives all three in the first page). A Query that ends its own page
/// at 1 MB stops before that item (see `read_helpers::read_page`), so the
/// page reads that item with one more Query.
///
/// A page visits at most 10 disjuncts (see `visits::PAGE_VISITS`), and the
/// rules above apply within that cap (measured 2026-11-03: `pk IN [...]`
/// with 25 values gives 10, 10 and 5 items with `Limit` 15 or 11, and 7, 7,
/// 7 and 4 with `Limit` 7; 11 partitions of 3 items give 30 items and then
/// 3). A disjunct held back for the refusal it gets when it is read takes a
/// visit and no unit of `Limit`, and is visited again by every later page
/// that reaches it. A request whose page visits only disjuncts held back
/// gets their refusal ([`StepTarget::refusal`]) instead of the page, also
/// when other disjuncts are left, so once only disjuncts held back are
/// left, the next request gets it (measured 2026-11-03: `pk IN [v000..v010,
/// <2049 bytes that sort after v005>]` gives v000 to v008, then v009 and
/// v010, then the refusal; `(pk IN [a00..a09] AND sk = a) OR pk IN
/// [b00..b10]` is refused on the first request; measured 2026-11-02: `(pk =
/// 'a' AND sk = a) OR pk = 'b' OR pk = 'c'` with `Limit` 1 gives [b/x],
/// then [c/x], then the refusal).
///
/// A page that holds `Limit` items has a LastEvaluatedKey, the key of the
/// last item it returned in visit order (see [`StepTarget::last_key`]),
/// with or without a NextToken; a page that ends inside a Query at the byte
/// budget has the key of the item it ended on; any other page has none (see
/// [`page_key`]), so a page the cap ends with fewer items than `Limit` has
/// none (measured 2026-11-03: `pk IN [...]` with 11 values gives 10 items
/// with a NextToken and no LastEvaluatedKey; with 21 values and `Limit` 10
/// the second page, which visits v016 to v020 and then v010 to v014, has
/// the key v014).
pub(super) async fn read_steps(
    ctx: &OperationContext,
    stmt: &SelectStatement,
    req: &SingleRequest,
    target: &StepTarget<'_>,
    steps: &[Step<'_, '_>],
    paging: Paging<'_>,
) -> Result<StepsPage, DynamoDbError> {
    let cursor = match paging.start {
        None => Cursor::first(steps.len()),
        Some(token) => Cursor::from_value(token, steps.len())?,
    };
    let held_back: Vec<bool> = steps.iter().map(|step| step.held_back).collect();
    let limit = paging.limit.and_then(|limit| usize::try_from(limit).ok());
    let mut walk = Walk::new(cursor, &held_back, limit);
    let reader = Reader {
        ctx,
        stmt,
        req,
        keys: target.keys,
        descending: paging.descending,
        filtered: target.filtered,
    };
    let mut page = PageParts::default();
    let mut budget_key = None;
    while let Some(visit) = walk.next_read() {
        let current = &steps[visit.position];
        let read = if current.point.is_some() {
            reader.read_item(current.read, &mut page).await?
        } else {
            let start = visit.start.as_ref();
            match reader
                .read_query(current.read, start, walk.units(), &mut page)
                .await?
            {
                QueryEnd::Ran(units) => Read::Done(units),
                QueryEnd::RanPastBudget => Read::DoneAndEnd,
                QueryEnd::AtLimit(key) => Read::Stopped(key),
                QueryEnd::AtBudget(key) => {
                    budget_key = Some(key.clone());
                    Read::Stopped(key)
                }
            }
        };
        walk.record(visit.position, read);
    }
    let next = match walk.finish() {
        Ending::Refused => {
            return Err(target.refusal.cloned().unwrap_or_else(token_mismatch));
        }
        Ending::Last => None,
        Ending::More(cursor) => Some(cursor.into_value()),
    };
    let last_key = budget_key.or_else(|| page_key(&page.items, &target.last_key, paging.limit));
    let mut body = Map::new();
    body.insert("Items".to_owned(), Value::Array(page.items));
    if let Some(capacity) = page.capacity {
        body.insert("ConsumedCapacity".to_owned(), capacity);
    }
    Ok(StepsPage {
        page: DispatchResult {
            body: Value::Object(body),
            metrics: page.metrics,
        },
        next,
        last_key,
    })
}

/// How a Query of a disjunct stands against the page's byte budget.
enum BudgetEnd {
    /// The page stays within its budget with every item the Query read.
    Within(DispatchResult),
    /// The page passes its budget inside the Query: the read up to and
    /// including the item that takes it past.
    Past(DispatchResult),
}

/// How a page's Query of a disjunct ended.
enum QueryEnd {
    /// The Query read the disjunct to its end, evaluating this many items.
    Ran(usize),
    /// The Query read the disjunct to its end and took the page past its
    /// byte budget on its last item.
    RanPastBudget,
    /// The page ends inside the Query at `Limit`, on the item with this key.
    AtLimit(Value),
    /// The page ends inside the Query at the byte budget, on the item with
    /// this key.
    AtBudget(Value),
}

/// What a read of disjuncts one after another reads besides the disjuncts.
pub(super) struct StepTarget<'t> {
    /// The key names of the table or index read.
    pub(super) keys: KeyNames<'t>,
    /// The attributes of a page's LastEvaluatedKey: the key of what is read,
    /// and for an index the table's key after it, as a Query of an index
    /// names them.
    pub(super) last_key: Vec<&'t str>,
    /// Whether each Query carries its disjunct's filter. A read of an index
    /// that reaches back filters the base-table items instead (see
    /// `base_join`).
    pub(super) filtered: bool,
    /// The refusal of the disjuncts held back (see
    /// `read_plan::Checked::deferred`), which a request whose page visits
    /// only them gets.
    pub(super) refusal: Option<&'t DynamoDbError>,
}

/// What one read of a disjunct needs besides the disjunct and its paging.
struct Reader<'r> {
    ctx: &'r OperationContext,
    stmt: &'r SelectStatement,
    req: &'r SingleRequest,
    keys: KeyNames<'r>,
    /// Whether a Query reads its items in descending order.
    descending: bool,
    /// Whether a Query carries its disjunct's filter (see
    /// [`StepTarget::filtered`]).
    filtered: bool,
}

impl Reader<'_> {
    /// Read the one item the disjunct `read` names into `page`, unless the
    /// item would take the page past its byte budget.
    async fn read_item(
        &self,
        read: &KeyRead<'_>,
        page: &mut PageParts,
    ) -> Result<Read, DynamoDbError> {
        let result = self.query(read, Paging::default(), true).await?;
        if page.bytes() > 0
            && page.bytes().saturating_add(result.metrics.returned_bytes) > PAGE_BYTES
        {
            return Ok(Read::Unread);
        }
        page.add(result, read);
        Ok(Read::Done(1))
    }

    /// Read the Query of the disjunct `read` from `start` into `page`, with
    /// `units` of `Limit` left.
    async fn read_query(
        &self,
        read: &KeyRead<'_>,
        start: Option<&Value>,
        units: Option<usize>,
        page: &mut PageParts,
    ) -> Result<QueryEnd, DynamoDbError> {
        let paging = Paging {
            start,
            limit: units.and_then(|u| i64::try_from(u).ok()),
            ..Paging::default()
        };
        let result = self.query(read, paging, true).await?;
        let evaluated = scanned_count(&result);
        match self.budget_end(read, result, start, page.bytes()).await? {
            BudgetEnd::Within(result) => {
                let limited = units.is_some_and(|u| evaluated >= u);
                match page.add(result, read).0 {
                    None => Ok(QueryEnd::Ran(evaluated)),
                    Some(key) if limited => Ok(QueryEnd::AtLimit(key)),
                    Some(key) => {
                        // The Query ended its own page at 1 MB, before the
                        // item that takes this page past it: the page keeps
                        // that item.
                        let paging = Paging {
                            start: Some(&key),
                            limit: Some(1),
                            ..Paging::default()
                        };
                        let result = self.query(read, paging, true).await?;
                        Ok(match page.add(result, read) {
                            (Some(key), _) => QueryEnd::AtBudget(key),
                            (None, more) => QueryEnd::Ran(evaluated + more),
                        })
                    }
                }
            }
            BudgetEnd::Past(result) => Ok(match page.add(result, read).0 {
                Some(key) => QueryEnd::AtBudget(key),
                None => QueryEnd::RanPastBudget,
            }),
        }
    }

    /// Dispatch the Query of one disjunct, with its filter when `filtered`
    /// and the read carries filters.
    async fn query(
        &self,
        read: &KeyRead<'_>,
        paging: Paging<'_>,
        filtered: bool,
    ) -> Result<DispatchResult, DynamoDbError> {
        let mut bridge = ExprBridge::new(&self.req.parameters);
        let kce = key_condition(&mut bridge, self.keys, read.partition, read.sort.as_ref())?;
        let filter = if filtered && self.filtered {
            render_filter(&mut bridge, &read.filter)?
        } else {
            None
        };
        let paging = Paging {
            descending: self.descending,
            ..paging
        };
        query_request(self.ctx, self.stmt, self.req, paging, &bridge, kce, filter).await
    }

    /// Set `result`, the Query of `read` from `start`, against a page that
    /// has read `spent` bytes. When the page passes its budget inside it,
    /// the Query is read again with the number of items up to and including
    /// the one that takes the page past as its `Limit`.
    async fn budget_end(
        &self,
        read: &KeyRead<'_>,
        result: DispatchResult,
        start: Option<&Value>,
        spent: u64,
    ) -> Result<BudgetEnd, DynamoDbError> {
        if spent == 0 || spent.saturating_add(result.metrics.returned_bytes) <= PAGE_BYTES {
            return Ok(BudgetEnd::Within(result));
        }
        let evaluated = scanned_count(&result);
        let sizes = self
            .evaluated_sizes(read, &result, start, evaluated)
            .await?;
        let take = items_to_budget(spent, &sizes).max(1);
        if take == evaluated && result.body.get("LastEvaluatedKey").is_some() {
            return Ok(BudgetEnd::Past(result));
        }
        let paging = Paging {
            start,
            limit: i64::try_from(take).ok(),
            ..Paging::default()
        };
        Ok(BudgetEnd::Past(self.query(read, paging, true).await?))
    }

    /// The sizes of the `evaluated` items a Query of `read` from `start`
    /// read for `result`, in the order it read them. When the disjunct has a
    /// filter, the items are read again without it, since the filter drops
    /// some of them from `result`.
    async fn evaluated_sizes(
        &self,
        read: &KeyRead<'_>,
        result: &DispatchResult,
        start: Option<&Value>,
        evaluated: usize,
    ) -> Result<Vec<u64>, DynamoDbError> {
        let unfiltered;
        let source = if read.filter.is_empty() || !self.filtered {
            result
        } else {
            let paging = Paging {
                start,
                limit: i64::try_from(evaluated.max(1)).ok(),
                ..Paging::default()
            };
            unfiltered = self.query(read, paging, false).await?;
            &unfiltered
        };
        let items = source.body.get("Items").and_then(Value::as_array);
        Ok(items.map_or_else(Vec::new, |items| items.iter().map(wire_item_size).collect()))
    }
}

/// The items, consumed capacity and metrics of the reads a page is made of.
#[derive(Default)]
struct PageParts {
    items: Vec<Value>,
    capacity: Option<Value>,
    metrics: DispatchMetrics,
}

impl PageParts {
    /// The bytes the page has read, as a Query's page counts them.
    fn bytes(&self) -> u64 {
        self.metrics.returned_bytes
    }

    /// Add one read of the disjunct `read` to the page, its items unless the
    /// disjunct returns none of them (see `KeyRead::returns_nothing`).
    /// Returns the read's LastEvaluatedKey and how many items it evaluated.
    fn add(&mut self, result: DispatchResult, read: &KeyRead<'_>) -> (Option<Value>, usize) {
        let evaluated = scanned_count(&result);
        self.metrics.read_capacity_units += result.metrics.read_capacity_units;
        self.metrics.returned_bytes += result.metrics.returned_bytes;
        let Value::Object(mut body) = result.body else {
            return (None, evaluated);
        };
        if let Some(Value::Array(found)) = body.remove("Items")
            && !read.returns_nothing()
        {
            self.metrics.returned_item_count += result.metrics.returned_item_count;
            self.items.extend(found);
        }
        add_capacity(&mut self.capacity, body.remove("ConsumedCapacity"));
        (body.remove("LastEvaluatedKey"), evaluated)
    }
}

/// How many items a read evaluated (its `ScannedCount`).
fn scanned_count(result: &DispatchResult) -> usize {
    result
        .body
        .get("ScannedCount")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0)
}

/// The size of a wire-form item, as a Query's page counts it.
fn wire_item_size(item: &Value) -> u64 {
    Item::deserialize(item).map_or(0, |item| item_size_bytes(&item) as u64)
}

/// How many of the items of `sizes` a page that has read `spent` bytes
/// takes: up to and including the one that takes it past [`PAGE_BYTES`],
/// or all of them.
fn items_to_budget(spent: u64, sizes: &[u64]) -> usize {
    let mut total = spent;
    for (index, size) in sizes.iter().enumerate() {
        total = total.saturating_add(*size);
        if total > PAGE_BYTES {
            return index + 1;
        }
    }
    sizes.len()
}

/// A page of a read of disjuncts one after another.
pub(super) struct StepsPage {
    pub(super) page: DispatchResult,
    /// Where the next page goes on, when this one leaves disjuncts (see
    /// [`Cursor`]).
    pub(super) next: Option<Value>,
    /// The page's LastEvaluatedKey.
    pub(super) last_key: Option<Value>,
}

/// The LastEvaluatedKey of a page of a read of disjuncts one after another
/// that does not end at the byte budget: the attributes `names` of the last
/// item when the page holds `limit` items, else none. Measured 2026-11-02:
/// with `Limit` 1, `pk IN ['a', 'b']` gives [a] with key a and a NextToken,
/// then [b] with key b and no NextToken; `pk IN ['a', 'zz']` gives [a] with
/// key a, then an empty page with none; `pk IN ['a', 'b', 'c']` with `Limit`
/// 2 gives [a, b] with key b, then [c] with none; a projection without the
/// key attributes has the key too; and a page read without `Limit` has
/// none, the one before a refused disjunct too.
fn page_key(items: &[Value], names: &[&str], limit: Option<i64>) -> Option<Value> {
    let full = limit
        .and_then(|limit| usize::try_from(limit).ok())
        .is_some_and(|limit| limit > 0 && items.len() == limit);
    if !full {
        return None;
    }
    let last = items.last()?;
    let mut key = Map::new();
    for name in names {
        key.insert((*name).to_owned(), last.get(*name)?.clone());
    }
    Some(Value::Object(key))
}

/// Add one read's consumed capacity to a running total, number by number.
fn add_capacity(total: &mut Option<Value>, more: Option<Value>) {
    let Some(more) = more else {
        return;
    };
    match total {
        Some(sum) => add_numbers(sum, more),
        None => *total = Some(more),
    }
}

fn add_numbers(sum: &mut Value, more: Value) {
    match (sum, more) {
        (Value::Object(sum), Value::Object(more)) => {
            for (name, value) in more {
                match sum.get_mut(&name) {
                    Some(existing) => add_numbers(existing, value),
                    None => {
                        sum.insert(name, value);
                    }
                }
            }
        }
        (sum, Value::Number(more)) if sum.is_number() => {
            let total = sum.as_f64().unwrap_or(0.0) + more.as_f64().unwrap_or(0.0);
            *sum = Value::from(total);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests;
