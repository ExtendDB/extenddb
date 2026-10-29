// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 2, `resource_tag_condition_keys`: rewrite stored IAM policies
//! from `dynamodb:ResourceTag/<key>` to `aws:ResourceTag/<key>`.
//!
//! The live service evaluates resource tags only as `aws:ResourceTag/<key>`
//! and leaves `dynamodb:ResourceTag/<key>` absent, so ExtendDB no longer
//! resolves the `dynamodb:` form. Before the evaluator change both forms
//! resolved from the same table tags, so replacing the first with the second
//! keeps every policy's decision.
//!
//! The pure transform, [`rewrite_policy_document`], defines this migration and
//! is frozen here: this file's checksum is pinned once the migration is
//! released, and the file is excluded from rustfmt so formatting cannot
//! change it. The body reaches the stored policies through
//! `ManagementStore` on every backend, with the driver in
//! `crate::policy_migration`.

use serde_json::Value;

use async_trait::async_trait;

use super::code::{CodeBody, Done, HasManagementStore};
use super::status::{CatalogStatus, MigrationError};
use crate::management_store::OpError;
use crate::policy_migration::{MigrationReport, NeedsAttention, migrate_resource_tag_condition_keys};

/// The condition-key prefix the live service does not populate.
const DYNAMODB_PREFIX: &str = "dynamodb:ResourceTag/";
/// The condition-key prefix the live service evaluates resource tags as.
const AWS_PREFIX: &str = "aws:ResourceTag/";

/// ASCII-case-insensitive prefix strip, returning the untouched remainder.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &s[prefix.len()..])
}

/// Outcome of rewriting one policy document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewriteOutcome {
    /// The rewritten document. Equal to the input when nothing changed or a
    /// collision was found.
    pub document: Value,
    /// Number of rewrites applied (condition-key renames plus policy-variable
    /// prefix rewrites). Zero when nothing changed or on a collision.
    pub rewrites: usize,
    /// A `Condition` operator block held both `aws:ResourceTag/k` and a
    /// `dynamodb:ResourceTag/k` that would rewrite onto the same JSON key.
    /// The two cannot share one JSON object, so the document is left unchanged
    /// and the caller reports it for manual attention.
    pub collision: bool,
}

impl RewriteOutcome {
    /// Whether the document was rewritten (changed and no collision).
    #[must_use]
    pub fn changed(&self) -> bool {
        self.rewrites > 0 && !self.collision
    }
}

/// Rewrite every `dynamodb:ResourceTag/<key>` condition key and
/// `${dynamodb:ResourceTag/...}` policy variable (any ASCII case) in a policy
/// document to the `aws:ResourceTag/` form, inside `Condition` blocks only.
///
/// Idempotent: a document with no `dynamodb:ResourceTag` reference is returned
/// unchanged with `rewrites == 0`. On a collision the input is returned
/// unchanged with `collision == true`.
#[must_use]
pub fn rewrite_policy_document(document: &Value) -> RewriteOutcome {
    let mut doc = document.clone();
    let mut rewrites = 0usize;
    let mut collision = false;

    // Statement may be a single object or an array of objects.
    match doc.get_mut("Statement") {
        Some(Value::Array(statements)) => {
            for stmt in statements.iter_mut() {
                rewrite_statement(stmt, &mut rewrites, &mut collision);
            }
        }
        Some(stmt @ Value::Object(_)) => {
            rewrite_statement(stmt, &mut rewrites, &mut collision);
        }
        _ => {}
    }

    if collision {
        // Leave the stored policy exactly as it was.
        return RewriteOutcome {
            document: document.clone(),
            rewrites: 0,
            collision: true,
        };
    }

    RewriteOutcome {
        document: if rewrites > 0 { doc } else { document.clone() },
        rewrites,
        collision: false,
    }
}

/// Rewrite the `Condition` block of one statement in place.
fn rewrite_statement(stmt: &mut Value, rewrites: &mut usize, collision: &mut bool) {
    let Some(condition) = stmt.get_mut("Condition").and_then(Value::as_object_mut) else {
        return;
    };
    for (_operator, block) in condition.iter_mut() {
        let Some(map) = block.as_object_mut() else {
            continue;
        };
        rewrite_operator_block(map, rewrites, collision);
    }
}

/// Rewrite one operator block: its condition keys (object keys) and the policy
/// variables in its values.
fn rewrite_operator_block(
    block: &mut serde_json::Map<String, Value>,
    rewrites: &mut usize,
    collision: &mut bool,
) {
    // First rewrite the variables inside the values, keyed by the current
    // (pre-rename) condition key, so a value rewrite is not lost by the rename.
    for value in block.values_mut() {
        rewrite_variables_in_json(value, rewrites);
    }

    // Collect the condition-key renames.
    let mut renames: Vec<(String, String)> = Vec::new();
    for key in block.keys() {
        if let Some(rest) = strip_prefix_ci(key, DYNAMODB_PREFIX) {
            renames.push((key.clone(), format!("{AWS_PREFIX}{rest}")));
        }
    }
    if renames.is_empty() {
        return;
    }
    // A rename onto a key the block already has (an aws:ResourceTag key, or
    // an earlier rename) is a duplicate when the two values are equal: both
    // prefixes resolve from the same table tags, so the two conditions are
    // the same condition and the dynamodb: one is dropped. Different values
    // cannot share one JSON key, so the block is a collision.
    let mut moves: Vec<(String, String)> = Vec::new();
    let mut drops: Vec<String> = Vec::new();
    for (old_key, new_key) in &renames {
        let value = &block[old_key];
        let existing = block.get(new_key).or_else(|| {
            moves
                .iter()
                .find(|(_, n)| n == new_key)
                .map(|(o, _)| &block[o])
        });
        match existing {
            Some(v) if v == value => drops.push(old_key.clone()),
            Some(_) => {
                *collision = true;
                return;
            }
            None => moves.push((old_key.clone(), new_key.clone())),
        }
    }
    for old_key in drops {
        block.remove(&old_key);
        *rewrites += 1;
    }
    for (old_key, new_key) in moves {
        if let Some(value) = block.remove(&old_key) {
            block.insert(new_key, value);
            *rewrites += 1;
        }
    }
}

/// Rewrite `${dynamodb:ResourceTag/...}` policy variables inside a value, which
/// is a string or an array of strings.
fn rewrite_variables_in_json(value: &mut Value, rewrites: &mut usize) {
    match value {
        Value::String(s) => {
            if let Some(new) = rewrite_variables_in_str(s, rewrites) {
                *s = new;
            }
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                rewrite_variables_in_json(item, rewrites);
            }
        }
        _ => {}
    }
}

/// Rewrite the prefix of every `${dynamodb:ResourceTag/...}` variable in `s`,
/// leaving all other text (including a literal `dynamodb:ResourceTag/...` not
/// wrapped in `${...}`) untouched. Returns `None` when nothing changed.
fn rewrite_variables_in_str(s: &str, rewrites: &mut usize) -> Option<String> {
    if !s.contains("${") {
        return None;
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    let mut changed = false;
    while let Some(open) = rest.find("${") {
        out.push_str(&rest[..open + 2]);
        let after_open = &rest[open + 2..];
        let Some(close) = after_open.find('}') else {
            // No closing brace: copy the remainder and stop.
            out.push_str(after_open);
            rest = "";
            break;
        };
        let inner = &after_open[..close];
        if let Some(tag_rest) = strip_prefix_ci(inner, DYNAMODB_PREFIX) {
            out.push_str(AWS_PREFIX);
            out.push_str(tag_rest);
            *rewrites += 1;
            changed = true;
        } else {
            out.push_str(inner);
        }
        out.push('}');
        rest = &after_open[close + 1..];
    }
    out.push_str(rest);
    changed.then_some(out)
}

/// This file, whose SHA-256 each backend's checksum test pins.
pub const SOURCE: &str = include_str!("m0002_resource_tag_condition_keys.rs");

/// Recorded as this migration's checksum in every ledger. Changes only with a
/// new migration.
pub const FINGERPRINT: &str = "0002_resource_tag_condition_keys/1";

/// Migration 2's body on every backend.
pub struct ResourceTagConditionKeys;

#[async_trait]
impl<H: HasManagementStore + Send + Sync + 'static> CodeBody<H> for ResourceTagConditionKeys {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// The policies whose two resource-tag forms name the same key with
    /// different values in one operator block. They cannot be rewritten, and
    /// leaving them in the `dynamodb:` form can turn a Deny off.
    async fn detect(&self, stores: &H) -> Result<Vec<String>, MigrationError> {
        let report = migrate_resource_tag_condition_keys(stores.management_store(), false).await;
        refuse_lookup_errors(&report)?;
        Ok(report.needs_attention.iter().map(collision_line).collect())
    }

    async fn run(&self, stores: &H) -> Result<Done, MigrationError> {
        let report = migrate_resource_tag_condition_keys(stores.management_store(), true).await;
        refuse_lookup_errors(&report)?;
        if !report.needs_attention.is_empty() {
            return Err(MigrationError::Status(CatalogStatus::NeedsAttention {
                items: report.needs_attention.iter().map(collision_line).collect(),
            }));
        }
        let notes = report
            .rewritten
            .iter()
            .map(|r| {
                format!(
                    "rewrote account {} {} policy {} ({} key(s))",
                    r.account_id, r.principal, r.policy_name, r.rewrites
                )
            })
            .collect();
        Ok(Done { notes })
    }
}

/// A pass that could not read or write a policy must not be recorded: the
/// next run finishes it. The error carries the first failure's database
/// error code, so the runner runs the body again after a lock timeout.
fn refuse_lookup_errors(report: &MigrationReport) -> Result<(), MigrationError> {
    if report.errors == 0 {
        return Ok(());
    }
    let failed = format!(
        "resource-tag rewrite: {} policy lookup(s) or write(s) failed",
        report.errors
    );
    Err(match &report.first_error {
        Some(OpError::Database { message, sqlstate }) => MigrationError::Database {
            message: format!("{failed}, the first with {message}"),
            sqlstate: sqlstate.clone(),
        },
        _ => MigrationError::Store(failed),
    })
}

fn collision_line(n: &NeedsAttention) -> String {
    format!(
        "account {} {} policy {} names aws:ResourceTag/<key> and dynamodb:ResourceTag/<key> \
         for the same key with different values in one condition operator; remove one of the \
         two forms",
        n.account_id, n.principal, n.policy_name
    )
}
