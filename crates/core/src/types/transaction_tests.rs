use super::*;

#[test]
fn item_response_missing_serializes_as_empty_object() {
    let resp = ItemResponse { item: None };
    let json = serde_json::to_value(&resp).unwrap();
    // Real DynamoDB returns {} (no Item key) for missing items.
    assert_eq!(json, serde_json::json!({}));
}

#[test]
fn item_response_present_serializes_item() {
    let mut item = Item::new();
    item.insert("pk".to_owned(), AttributeValue::S("val".to_owned()));
    let resp = ItemResponse { item: Some(item) };
    let json = serde_json::to_value(&resp).unwrap();
    assert_eq!(json["Item"]["pk"]["S"], "val");
}

fn codes(reasons: &[CancellationReason]) -> Vec<&str> {
    reasons.iter().map(|r| r.code.as_str()).collect()
}

#[test]
fn no_key_fault_leaves_the_reasons_untouched() {
    let reasons = vec![
        CancellationReason::condition_check_failed_with_item(None),
        CancellationReason::validation_error("cannot apply"),
        CancellationReason::none(),
    ];
    let out = CancellationReason::resolve_key_faults(reasons).unwrap();
    assert_eq!(
        codes(&out),
        ["ConditionalCheckFailed", "ValidationError", "None"]
    );
}

// The kinds of fault a transaction member can carry into the resolver:
// two internal key faults (a wrong type supplied in the request, an empty
// index key) and the plain reasons beside them (a wrong type an Update
// writes from the stored item, a failed condition). Each test below puts
// one pair in both member orders; the expected answers are the measured
// ones (2026-10-30 rows A2, B1, C1, D1, E1, F1, F2 and their reversals;
// 2026-10-31 section 1, a Put item's empty key against a wrong type in
// the request, which the first in request order decides).
const COMPUTED: &str =
    "The update expression attempted to update the secondary index key to unsupported type";

fn type_fault() -> CancellationReason {
    CancellationReason::key_type_mismatch("type")
}

fn empty_fault() -> CancellationReason {
    CancellationReason::index_key_empty_abort("empty key")
}

fn computed_fault() -> CancellationReason {
    CancellationReason::validation_error(COMPUTED)
}

fn failed_condition() -> CancellationReason {
    CancellationReason::condition_check_failed_with_item(None)
}

/// `[first, second]` and `[second, first]`, resolved.
fn both_orders(
    first: fn() -> CancellationReason,
    second: fn() -> CancellationReason,
) -> [Result<Vec<CancellationReason>, String>; 2] {
    [
        CancellationReason::resolve_key_faults(vec![first(), second()]),
        CancellationReason::resolve_key_faults(vec![second(), first()]),
    ]
}

/// Assert a cancellation with `ValidationError` carrying the type fault's
/// message at `at` and `None`, with no message, everywhere else.
fn assert_type_fault_at(out: &Result<Vec<CancellationReason>, String>, at: usize) {
    let out = out.as_ref().expect("a cancellation");
    for (i, reason) in out.iter().enumerate() {
        if i == at {
            assert_eq!(reason.code, "ValidationError");
            assert_eq!(reason.message.as_deref(), Some("type"));
        } else {
            assert_eq!(reason.code, "None", "position {i}");
            assert_eq!(reason.message, None, "position {i}");
        }
    }
}

#[test]
fn a_wrong_type_and_an_empty_key_answer_whichever_comes_first() {
    let [type_first, empty_first] = both_orders(type_fault, empty_fault);
    assert_type_fault_at(&type_first, 0);
    assert_eq!(empty_first.unwrap_err(), "empty key");
}

#[test]
fn a_wrong_type_in_the_request_masks_a_computed_wrong_type_in_either_order() {
    let [type_first, computed_first] = both_orders(type_fault, computed_fault);
    assert_type_fault_at(&type_first, 0);
    assert_type_fault_at(&computed_first, 1);
}

#[test]
fn a_wrong_type_in_the_request_masks_a_failed_condition_in_either_order() {
    let [type_first, condition_first] = both_orders(type_fault, failed_condition);
    assert_type_fault_at(&type_first, 0);
    assert_type_fault_at(&condition_first, 1);
}

#[test]
fn an_empty_key_outranks_a_computed_wrong_type_in_either_order() {
    for out in both_orders(empty_fault, computed_fault) {
        assert_eq!(out.unwrap_err(), "empty key");
    }
}

#[test]
fn an_empty_key_outranks_a_failed_condition_in_either_order() {
    for out in both_orders(empty_fault, failed_condition) {
        assert_eq!(out.unwrap_err(), "empty key");
    }
}

#[test]
fn a_computed_wrong_type_and_a_failed_condition_keep_their_own_outcomes() {
    let [computed_first, condition_first] = both_orders(computed_fault, failed_condition);
    let computed_first = computed_first.unwrap();
    assert_eq!(
        codes(&computed_first),
        ["ValidationError", "ConditionalCheckFailed"]
    );
    assert_eq!(computed_first[0].message.as_deref(), Some(COMPUTED));
    let condition_first = condition_first.unwrap();
    assert_eq!(
        codes(&condition_first),
        ["ConditionalCheckFailed", "ValidationError"]
    );
    assert_eq!(condition_first[1].message.as_deref(), Some(COMPUTED));
}

#[test]
fn only_the_first_of_two_wrong_types_is_reported() {
    for (first, second) in [("second", "third"), ("third", "second")] {
        let reasons = vec![
            CancellationReason::none(),
            CancellationReason::key_type_mismatch(first),
            CancellationReason::key_type_mismatch(second),
        ];
        let out = CancellationReason::resolve_key_faults(reasons).unwrap();
        assert_eq!(codes(&out), ["None", "ValidationError", "None"]);
        assert_eq!(out[1].message.as_deref(), Some(first));
        assert_eq!(out[2].message, None);
    }
}

#[test]
fn the_first_of_two_empty_keys_answers() {
    for (first, second) in [
        ("put wording", "update wording"),
        ("update wording", "put wording"),
    ] {
        let reasons = vec![
            failed_condition(),
            CancellationReason::index_key_empty_abort(first),
            CancellationReason::index_key_empty_abort(second),
        ];
        assert_eq!(
            CancellationReason::resolve_key_faults(reasons).unwrap_err(),
            first
        );
    }
}

#[test]
fn the_first_internal_fault_decides_among_plain_reasons() {
    let empty_before_type = vec![
        failed_condition(),
        empty_fault(),
        type_fault(),
        CancellationReason::validation_error("cannot apply"),
    ];
    assert_eq!(
        CancellationReason::resolve_key_faults(empty_before_type).unwrap_err(),
        "empty key"
    );
    let type_before_empty = vec![
        failed_condition(),
        type_fault(),
        empty_fault(),
        CancellationReason::validation_error("cannot apply"),
    ];
    assert_type_fault_at(
        &CancellationReason::resolve_key_faults(type_before_empty),
        1,
    );
}
