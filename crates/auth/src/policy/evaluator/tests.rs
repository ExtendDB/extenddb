// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers and module wiring for policy-evaluation unit tests.

use super::*;
use crate::policy::context::ConditionContext;
use crate::policy::document::PolicyDocument;
use std::collections::HashMap;

/// Simple test context.
struct Ctx(HashMap<String, Vec<String>>);

impl Ctx {
    fn empty() -> Self {
        Self(HashMap::new())
    }

    fn with(mut self, key: &str, values: Vec<&str>) -> Self {
        self.0.insert(
            key.to_owned(),
            values.into_iter().map(ToOwned::to_owned).collect(),
        );
        self
    }
}

impl ConditionContext for Ctx {
    fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
        self.0
            .get(key)
            .map(|v| v.iter().map(|s| s.as_str()).collect())
    }

    fn is_multivalued_key(&self, key: &str) -> bool {
        matches!(key, "dynamodb:LeadingKeys" | "dynamodb:Attributes")
    }
}

fn parse(json: &str) -> PolicyDocument {
    PolicyDocument::from_json(json).unwrap()
}

mod combined;
mod core;
