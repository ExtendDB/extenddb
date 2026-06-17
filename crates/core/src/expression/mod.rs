// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Expression tokenizer, parser, and evaluator for Virtual `DynamoDB`.
//!
//! Supports `ConditionExpression`, `UpdateExpression`, and expression attribute
//! name/value resolution. This module is pure synchronous Rust — no async, no I/O.
//!
//! Architecture: `input string → tokenizer → tokens → parser → AST → evaluator → result`

mod ast;
mod evaluator;
mod key_condition;
mod kind;
mod parser;
mod parser_common;
mod projection;
mod reserved_words;
mod resolver;
mod tokenizer;
mod update_evaluator;
mod update_parser;

pub use ast::{ArithOp, CompareOp, Expr, PathElement, UpdateAction};
pub use evaluator::evaluate_condition;
pub use key_condition::{KeyCondition, SortKeyCondition, parse_key_condition};
pub use kind::ExpressionKind;
pub use parser::{parse_condition, parse_condition_with_depth_limit};
pub use projection::{apply_projection, parse_projection};
pub use reserved_words::validate_no_reserved_words;
pub use resolver::{
    ExpressionMaps, collect_key_condition_refs, collect_value_placeholders, resolve_element_name,
    resolve_name_ref, resolve_path, validate_begins_with_operands, validate_expression_param_usage,
    validate_unused_attributes,
};
pub use tokenizer::{Token, tokenize, tokenize_for, tokenize_with_limit};
pub use update_evaluator::apply_update;
pub use update_parser::{parse_update, parse_update_from};
