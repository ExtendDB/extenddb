// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Per-table capacity totals for `TransactGetItems` and `TransactWriteItems`,
//! in the order Amazon DynamoDB lists them.

use extenddb_core::types::TableKeyInfo;

/// Per-table capacity totals, listed oldest table first.
///
/// Amazon DynamoDB documents no order for the `ConsumedCapacity` entries of a
/// transaction. Measured on 2026-10-23 with three tables created on
/// 2026-10-22, 2026-10-23 11:01 and 2026-10-23 14:45 UTC, where the
/// newest table's name sorts first and the oldest table's name sorts last:
/// `TransactGetItems` and `TransactWriteItems` listed the tables oldest first
/// for the request orders [newest, middle, oldest], [middle, newest, oldest]
/// and [oldest, newest, middle]. The order follows neither the request nor
/// the names; it follows the tables' creation time. An earlier measurement
/// the same day (16 transactions over three tables created in name order) is
/// consistent with it.
///
/// ExtendDB orders the entries the same way, by the `creation_date_time` the
/// catalog carries on [`TableKeyInfo`], oldest first; two tables with the same
/// stored creation time are listed in name order so the array is deterministic
/// on every backend. The precision of the stored value sets how close two
/// creates have to be to tie: nanoseconds on SQLite, microseconds on
/// PostgreSQL, milliseconds on MongoDB and Cassandra. Before this change the
/// handlers aggregated into a `HashMap`, so the order changed from one process
/// to the next and differed between backends
/// (docs/differences-from-dynamodb.md, Capacity and Throttling).
#[derive(Default)]
pub struct PerTableCapacity(Vec<Entry>);

struct Entry {
    table: String,
    created: f64,
    units: f64,
}

impl PerTableCapacity {
    /// Add `units` to the table `key_info` describes.
    pub fn add(&mut self, key_info: &TableKeyInfo, units: f64) {
        match self.0.iter_mut().find(|e| e.table == key_info.table_name) {
            Some(e) => e.units += units,
            None => self.0.push(Entry {
                table: key_info.table_name.clone(),
                created: key_info.creation_date_time,
                units,
            }),
        }
    }

    /// The totals, oldest table first; ties on creation time in name order.
    pub fn ordered(&self) -> Vec<(&str, f64)> {
        let mut entries: Vec<&Entry> = self.0.iter().collect();
        entries.sort_by(|a, b| {
            a.created
                .total_cmp(&b.created)
                .then_with(|| a.table.cmp(&b.table))
        });
        entries
            .into_iter()
            .map(|e| (e.table.as_str(), e.units))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, created: f64) -> TableKeyInfo {
        TableKeyInfo {
            table_name: name.to_owned(),
            creation_date_time: created,
            ..Default::default()
        }
    }

    #[test]
    fn orders_by_creation_time_not_by_request_or_name() {
        // Names sort opposite to creation order, and the request names the
        // newest table first: the service's order is oldest first.
        let (c, b, a) = (table("c", 1.0), table("b", 2.0), table("a", 3.0));
        let mut totals = PerTableCapacity::default();
        totals.add(&a, 2.0);
        totals.add(&c, 2.0);
        totals.add(&b, 2.0);
        totals.add(&a, 4.0);
        assert_eq!(
            totals.ordered(),
            vec![("c", 2.0), ("b", 2.0), ("a", 6.0)],
            "oldest table first, with the repeated table summed once"
        );
    }

    #[test]
    fn ties_on_creation_time_fall_back_to_name_order() {
        let mut totals = PerTableCapacity::default();
        totals.add(&table("zeta", 5.0), 1.0);
        totals.add(&table("alpha", 5.0), 1.0);
        totals.add(&table("mid", 5.0), 1.0);
        assert_eq!(
            totals.ordered(),
            vec![("alpha", 1.0), ("mid", 1.0), ("zeta", 1.0)]
        );
    }

    #[test]
    fn unknown_creation_time_sorts_before_known() {
        // A source that carries no creation time reports 0.0 (TableKeyInfo
        // doc); it lists first rather than failing the sort.
        let mut totals = PerTableCapacity::default();
        totals.add(&table("known", 10.0), 1.0);
        totals.add(&table("unknown", 0.0), 1.0);
        assert_eq!(totals.ordered(), vec![("unknown", 1.0), ("known", 1.0)]);
    }
}
