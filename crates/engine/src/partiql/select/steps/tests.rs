// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the read of a SELECT's disjuncts one after another.

use super::*;

#[test]
fn a_full_page_names_the_key_of_its_last_item() {
    let item = |pk: &str, sk: &str| serde_json::json!({ "pk": { "S": pk }, "sk": { "S": sk }, "n": { "N": "1" } });
    let key = |pk: &str, sk: &str| serde_json::json!({ "pk": { "S": pk }, "sk": { "S": sk } });
    let two = [item("a", "x"), item("b", "x")];
    let keys = ["pk", "sk"];
    assert_eq!(page_key(&two, &keys, Some(2)), Some(key("b", "x")));
    assert_eq!(page_key(&two[..1], &keys, Some(1)), Some(key("a", "x")));
    // A page with fewer items than the limit, an empty one, and one read
    // without a limit have none.
    assert_eq!(page_key(&two[..1], &keys, Some(2)), None);
    assert_eq!(page_key(&[], &keys, Some(1)), None);
    assert_eq!(page_key(&two, &keys, None), None);
    // A table keyed on the partition key alone.
    assert_eq!(
        page_key(&two[..1], &["pk"], Some(1)),
        Some(serde_json::json!({ "pk": { "S": "a" } }))
    );
    // An index keyed on n, read with the table's key after its own.
    assert_eq!(
        page_key(&two[..1], &["n", "pk", "sk"], Some(1)),
        Some(serde_json::json!({ "n": { "N": "1" }, "pk": { "S": "a" }, "sk": { "S": "x" } }))
    );
}

#[test]
fn a_page_takes_items_up_to_the_one_that_passes_its_budget() {
    let item = 380 * 1024;
    // Two items read, the next one passes 1 MB and is kept.
    assert_eq!(items_to_budget(2 * item, &[item, item]), 1);
    // One item read, the second of the next two passes 1 MB.
    assert_eq!(items_to_budget(item, &[item, item, item]), 2);
    // Exactly 1 MB does not pass it.
    assert_eq!(items_to_budget(PAGE_BYTES - 10, &[10, 1]), 2);
    // A page that stays within its budget takes them all.
    assert_eq!(items_to_budget(10, &[10, 10]), 2);
    assert_eq!(items_to_budget(10, &[]), 0);
}

#[test]
fn a_wire_item_is_sized_as_a_page_counts_it() {
    let item = serde_json::json!({
        "pk": { "S": "a" }, "n": { "N": "12" }, "b": { "B": "AAEC" }, "t": { "BOOL": true }
    });
    // pk 2 + 1, n 1 + 2, b 1 + 3 bytes, t 1 + 1.
    assert_eq!(wire_item_size(&item), 12);
    assert_eq!(wire_item_size(&serde_json::json!("not an item")), 0);
}

#[test]
fn consumed_capacity_adds_up_number_by_number() {
    let mut total = None;
    let read = serde_json::json!({
        "TableName": "t", "CapacityUnits": 0.5, "Table": { "CapacityUnits": 0.5 }
    });
    add_capacity(&mut total, Some(read.clone()));
    add_capacity(&mut total, None);
    add_capacity(&mut total, Some(read));
    assert_eq!(
        total,
        Some(serde_json::json!({
            "TableName": "t", "CapacityUnits": 1.0, "Table": { "CapacityUnits": 1.0 }
        }))
    );
}
