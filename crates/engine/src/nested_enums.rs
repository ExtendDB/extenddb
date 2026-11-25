// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `ProjectionType` of each index and the `StreamViewType` of a
//! `CreateTable` or `UpdateTable` body, which [`crate::validate_enum_fields`]
//! does not reach.
//!
//! The typed read refuses a value outside its enum with a
//! `SerializationException`. The service answers a `ValidationException`
//! naming each such member, in its own order, once the body is read
//! (measured 2026-11-13, 2026-11-14 and 2026-11-17, us-east-1). So
//! [`EnumFaults`] takes the faults out of the body before the read.

use serde_json::Value;

use extenddb_core::error::{DynamoDbError, validation_errors};

/// The values of `ProjectionType`, in the service's order.
const PROJECTION_TYPES: &[&str] = &["ALL", "INCLUDE", "KEYS_ONLY"];

/// The values of `StreamViewType`, in the service's order.
const STREAM_VIEW_TYPES: &[&str] = &["OLD_IMAGE", "KEYS_ONLY", "NEW_AND_OLD_IMAGES", "NEW_IMAGE"];

/// Each member outside its set: the value sent, the member's path and the
/// constraint it fails.
#[derive(Default)]
pub(crate) struct EnumFaults(Vec<(String, String, String)>);

impl EnumFaults {
    pub(crate) fn create_table(body: &mut Value) -> Self {
        let mut faults = Self::default();
        faults.projections(body, "GlobalSecondaryIndexes", false);
        faults.stream_view_type(body);
        faults.projections(body, "LocalSecondaryIndexes", false);
        faults
    }

    pub(crate) fn update_table(body: &mut Value) -> Self {
        let mut faults = Self::default();
        faults.projections(body, "GlobalSecondaryIndexUpdates", true);
        faults.stream_view_type(body);
        faults
    }

    pub(crate) fn check(&self) -> Result<(), DynamoDbError> {
        if self.0.is_empty() {
            return Ok(());
        }
        let faults: Vec<(&str, &str, &str)> = self
            .0
            .iter()
            .map(|(value, path, constraint)| (value.as_str(), path.as_str(), constraint.as_str()))
            .collect();
        let message = validation_errors(&faults);
        Err(DynamoDbError::ValidationException(message))
    }

    /// The `Projection.ProjectionType` of each element of the list `member`,
    /// or of the `Create` object of each element when `create` is set. The
    /// service names a member in lower camel case.
    fn projections(&mut self, body: &mut Value, member: &str, create: bool) {
        let (pointer, segment) = if create {
            ("/Create/Projection/ProjectionType", ".create")
        } else {
            ("/Projection/ProjectionType", "")
        };
        let path = format!("{}{}", member[..1].to_ascii_lowercase(), &member[1..]);
        let elements = body.get_mut(member).and_then(Value::as_array_mut);
        for (i, element) in elements.into_iter().flatten().enumerate() {
            let at = format!("{path}.{}.member{segment}.projection.projectionType", i + 1);
            self.one_of(element.pointer_mut(pointer), at, PROJECTION_TYPES);
        }
    }

    fn stream_view_type(&mut self, body: &mut Value) {
        self.one_of(
            body.pointer_mut("/StreamSpecification/StreamViewType"),
            "streamSpecification.streamViewType".to_owned(),
            STREAM_VIEW_TYPES,
        );
    }

    /// A string `value` outside `valid`. The member then holds the first
    /// value of `valid`, so that the read goes on past it.
    fn one_of(&mut self, value: Option<&mut Value>, path: String, valid: &[&str]) {
        if let Some(value) = value
            && let Some(sent) = value.as_str()
            && !valid.contains(&sent)
        {
            let constraint = crate::enum_constraint(valid);
            self.0.push((sent.to_owned(), path, constraint));
            *value = Value::from(valid[0]);
        }
    }
}

// The row test takes its context from `partiql::test_context`, so it runs
// only with the `partiql` feature, which is on by default.
#[cfg(all(test, feature = "partiql"))]
#[path = "nested_enums_tests.rs"]
mod tests;
