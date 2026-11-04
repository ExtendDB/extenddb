// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Mirrors evaluating a parsed expression against an item, which every storage
// backend does, for example crates/storage-sqlite/src/data/query.rs and
// update_item.rs call evaluate_condition and apply_update_validated. The first
// byte selects a condition or an update; the generated maps and item stand in
// for ExpressionAttributeNames, ExpressionAttributeValues and the stored item;
// the remaining bytes are the expression string. The expression is bounded by
// max_expression_length_bytes the way the server bounds a request expression.
//
// Every generated number, in the item or in the values map, goes through
// extenddb_core::validation::number::validate_and_normalize_number. A number
// it refuses is dropped, and a number it keeps is in its normalized form.

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

use extenddb_core::expression::{
    apply_update_validated, evaluate_condition, parse_condition_with_depth_limit,
    parse_update_from, tokenize_for, tokenize_with_limit, ExpressionKind,
};
use extenddb_core::limits::LimitsConfig;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let is_update = bool::arbitrary(&mut u).unwrap_or(false);
    let maps = extenddb_fuzz::arb_expression_maps(&mut u);
    let mut item = extenddb_fuzz::arb_item(&mut u);
    let rest = u.take_rest();
    let Ok(expr) = std::str::from_utf8(rest) else {
        return;
    };
    let limits = LimitsConfig::default();
    if expr.len() > limits.max_expression_length_bytes {
        return;
    }
    let max_tokens = limits.max_expression_tokens;
    if is_update {
        if let Ok(tokens) = tokenize_for(expr, max_tokens, ExpressionKind::Update) {
            if let Ok(actions) = parse_update_from(&tokens, expr) {
                // Empty vector-index and attribute-definition slices: a table
                // without vector indexes, which is the common case and a valid
                // call (see the Cassandra update path).
                let _ = apply_update_validated(&actions, &mut item, &maps, &[], &[]);
            }
        }
    } else if let Ok(tokens) = tokenize_with_limit(expr, max_tokens) {
        if let Ok(ast) = parse_condition_with_depth_limit(&tokens, limits.max_expression_depth) {
            let _ = evaluate_condition(&ast, &item, &maps);
        }
    }
});
