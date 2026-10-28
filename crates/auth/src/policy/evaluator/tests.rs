// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers and module wiring for policy-evaluation unit tests.

use super::*;
use crate::policy::context::ConditionContext;
use crate::policy::document::PolicyDocument;
use std::collections::HashMap;

/// Simple test context.
struct Ctx {
    values: HashMap<String, Vec<String>>,
    unresolvable: std::collections::HashSet<String>,
}

impl Ctx {
    fn empty() -> Self {
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

impl ConditionContext for Ctx {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        self.values
            .get(key)
            .map(|v| v.iter().map(|s| s.as_str()).collect())
    }

    fn is_multivalued_key(&self, key: &str) -> bool {
        matches!(key, "dynamodb:LeadingKeys" | "dynamodb:Attributes")
    }

    fn is_unresolvable_key(&self, key: &str) -> bool {
        self.unresolvable.contains(key)
    }
}

fn parse(json: &str) -> PolicyDocument {
    PolicyDocument::from_json(json).unwrap()
}

mod case_three;
mod combined;
mod condition_variables;
mod condition_variables_arn;
mod condition_variables_operators;
mod core;
mod resource_variables;
mod resource_variables_fields;
mod variable_values;
