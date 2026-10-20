// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers and module wiring for condition-evaluation unit tests.

use super::*;
use crate::policy::context::ConditionContext;
use std::collections::HashMap;

/// Simple test context that maps keys to values.
struct TestContext {
    values: HashMap<String, Vec<String>>,
    unresolvable: std::collections::HashSet<String>,
}

impl TestContext {
    fn new() -> Self {
        Self {
            values: HashMap::new(),
            unresolvable: std::collections::HashSet::new(),
        }
    }

    fn with(mut self, key: &str, values: Vec<&str>) -> Self {
        self.values.insert(
            key.to_owned(),
            values.into_iter().map(ToOwned::to_owned).collect(),
        );
        self
    }

    /// Mark `key` as service-populated but unresolvable (case 3).
    fn unresolvable(mut self, key: &str) -> Self {
        self.unresolvable.insert(key.to_owned());
        self
    }
}

impl ConditionContext for TestContext {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        self.values
            .get(key)
            .map(|v| v.iter().map(|s| s.as_str()).collect())
    }

    fn is_multivalued_key(&self, key: &str) -> bool {
        // Mirror the real RequestContext classification so tests exercise the
        // production arity rule.
        matches!(key, "dynamodb:LeadingKeys" | "dynamodb:Attributes")
    }

    fn is_unresolvable_key(&self, key: &str) -> bool {
        self.unresolvable.contains(key)
    }
}

fn cond(op: ConditionOperator, key: &str, values: Vec<&str>) -> Condition {
    Condition {
        operator: op,
        key: key.to_owned(),
        values: values.into_iter().map(ToOwned::to_owned).collect(),
    }
}

mod multivalued;
mod operators;
mod three_case;
