// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Mirrors the engine expression pipeline in
// crates/engine/src/expression_helpers.rs, which the server runs on
// ConditionExpression, KeyConditionExpression, ProjectionExpression and
// UpdateExpression: a byte-size check against max_expression_length_bytes, then
// tokenize with max_expression_tokens, then parse with max_expression_depth.
// The first input byte selects the parser; the rest is the expression string.
// The limits are LimitsConfig::default(), the server's defaults. The reserved
// word check is left out so the parsers are reached on every token stream the
// tokenizer can emit.
//
// The authorization leading-keys check in crates/server/src/leading_keys.rs
// parses a KeyConditionExpression with its own sequence of calls and no
// byte-size check. This target does not run that sequence.

#![no_main]

use libfuzzer_sys::fuzz_target;

use extenddb_core::expression::{
    parse_condition_with_depth_limit, parse_key_condition, parse_projection, parse_update_from,
    tokenize_for, tokenize_with_limit, ExpressionKind,
};
use extenddb_core::limits::LimitsConfig;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let which = data[0] % 4;
    let Ok(expr) = std::str::from_utf8(&data[1..]) else {
        return;
    };
    let limits = LimitsConfig::default();
    // The engine rejects an over-size expression before tokenizing.
    if expr.len() > limits.max_expression_length_bytes {
        return;
    }
    let max_tokens = limits.max_expression_tokens;
    match which {
        0 => {
            if let Ok(tokens) = tokenize_with_limit(expr, max_tokens) {
                let _ = parse_condition_with_depth_limit(&tokens, limits.max_expression_depth);
            }
        }
        1 => {
            if let Ok(tokens) = tokenize_for(expr, max_tokens, ExpressionKind::KeyCondition) {
                let _ = parse_key_condition(&tokens);
            }
        }
        2 => {
            if let Ok(tokens) = tokenize_for(expr, max_tokens, ExpressionKind::Projection) {
                let _ = parse_projection(&tokens);
            }
        }
        _ => {
            if let Ok(tokens) = tokenize_for(expr, max_tokens, ExpressionKind::Update) {
                let _ = parse_update_from(&tokens, expr);
            }
        }
    }
});
