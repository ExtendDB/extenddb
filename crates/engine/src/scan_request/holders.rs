// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The values of a `Scan` body the service checks after the request-time
//! checks: the handler holds them back from the typed deserialization and
//! reports them at their step.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{EMPTY_ATTRIBUTE_VALUE, Item, MULTI_ATTRIBUTE_VALUE};
use serde_json::Value;

use super::CheckOrder;
use super::member_order::fitted_order_of;
use crate::query_key_values::{
    Fault, SELECT, SetFault, first_fault, is_shape_fault, validate_names,
};

/// The service's prefix on a fault in an `ExclusiveStartKey` value it cannot
/// read as one type (measured 2026-10-31 and 2026-11-05: `{"pk": {}, "sk":
/// {"S": "x"}}` answers `The provided starting key is invalid: Supplied
/// AttributeValue is empty, must contain exactly one of the supported
/// datatypes`), and on an empty `SS` or `BS` in the key in order U
/// (2026-11-05: `{"pk": {}, "sk": {"SS": []}}` answers `The provided starting
/// key is invalid: One or more parameter values were invalid: An string set
/// may not be empty`). A number, an empty `NS` or a set with duplicates in the
/// key carries no prefix in either order.
const START_KEY_PREFIX: &str = "The provided starting key is invalid: ";

/// A value that the typed deserialization reads, put in the body in place of
/// a value a check here holds back. The request is refused before the scan
/// reads it.
fn stand_in() -> Value {
    serde_json::json!({"S": ""})
}

/// The `ExclusiveStartKey` of a `Scan` body as the request wrote it, with the
/// checks the service makes on the key (measured 2026-10-31 to 2026-11-05,
/// us-east-1; see the module doc), the first faulty member in the fitted
/// member order ([`super::member_order::fitted_member_order`]) and, inside one member, the first
/// fault in the value's element order (see `first_fault`):
///
/// - in order U, after the request parameter checks and ahead of the
///   expression values and checks: a value the service cannot read as one
///   type (empty, an unknown or lower-case type descriptor, more than one, at
///   the top of the member or inside its `L` or `M`) with the prefix `The
///   provided starting key is invalid: `, a number with the number check's
///   text, a set fault with the texts of the older path (see
///   `SetFault::text`) and the prefix on an empty `SS` or `BS`;
/// - in order R, after the expression and request checks and ahead of the
///   table lookup: a number or a set fault, with the texts of the newer path.
///
/// A key with an unreadable or set-faulty value does not deserialize as a map
/// of attribute values. [`StartKeyBody::take`] takes it out of the body, so
/// that the typed deserialization of the body does not report it ahead of the
/// checks the service makes first, and keeps the deserializer's fault for a
/// value the checks here do not classify. A key of the wrong JSON shape (not
/// an object, a value that is not an object, an `N` that is not a string)
/// stays in the body, as the service reports a shape fault ahead of every
/// check; see `shape_fault_first`.
pub(crate) struct StartKeyBody {
    /// The key as the request wrote it; `None` when the body has none.
    raw: Option<Value>,
    /// The member names in the fitted visiting order.
    members: Vec<String>,
    /// The deserializer's fault in a key taken out of the body, reported at the
    /// key's stage when the checks here classify no member (an `SS` member
    /// that is not a string, for one).
    deferred: Option<DynamoDbError>,
}

impl StartKeyBody {
    /// Reads the key from `body` and takes it out of the body when it does not
    /// deserialize for a reason that is not a shape fault. `request_order` is
    /// the key's member names as the request wrote them ([`super::member_orders`]);
    /// when it is `None` or does not name the key's members, the parsed body's
    /// order stands in for it.
    pub(crate) fn take(body: &mut Value, request_order: Option<&[String]>) -> Self {
        let members = fitted_order_of(body, "ExclusiveStartKey", request_order);
        let raw = body.get("ExclusiveStartKey").cloned();
        let deferred = match raw
            .as_ref()
            .map(|key| serde_json::from_value::<Item>(key.clone()))
        {
            Some(Err(e)) => match crate::deserialize_error(e) {
                shape if is_shape_fault(&shape) => None,
                DynamoDbError::ValidationException(msg)
                    if msg == EMPTY_ATTRIBUTE_VALUE || msg == MULTI_ATTRIBUTE_VALUE =>
                {
                    Some(DynamoDbError::ValidationException(format!(
                        "{START_KEY_PREFIX}{msg}"
                    )))
                }
                other => Some(other),
            },
            Some(Ok(_)) | None => None,
        };
        if deferred.is_some()
            && let Some(fields) = body.as_object_mut()
        {
            fields.remove("ExclusiveStartKey");
        }
        Self {
            raw,
            members,
            deferred,
        }
    }

    /// The key's checks for `order`: the first faulty member in the fitted
    /// order; then the deserializer's fault in a key [`StartKeyBody::take`]
    /// took out of the body that the checks here did not classify.
    ///
    /// # Errors
    ///
    /// Returns `DynamoDbError::ValidationException` with the service's text.
    pub(crate) fn validate(&self, order: CheckOrder) -> Result<(), DynamoDbError> {
        let key = self.raw.as_ref().and_then(Value::as_object);
        let fault = self
            .members
            .iter()
            .filter_map(|name| key?.get(name))
            .find_map(|value| first_fault(value, None, true));
        match (fault, &self.deferred) {
            (Some(fault), _) => Err(key_fault_text(fault, order)),
            (None, Some(deferred)) => Err(deferred.clone()),
            (None, None) => Ok(()),
        }
    }
}

/// The service's text for the first fault in a key member in `order`: the
/// number check's text for a number (2026-10-31: `" 1"` and `""` echoed, 39
/// digits the 38-digit text), the set texts of the path, with the key prefix
/// on an empty `SS` or `BS` in order U (2026-11-05, beside an unreadable key
/// member and beside an unreadable expression value, inside an `L` and in a
/// member that is not a key attribute), and the prefix on a value the service
/// cannot read as one type.
fn key_fault_text(fault: Fault<'_>, order: CheckOrder) -> DynamoDbError {
    let older = order == CheckOrder::Unreadable;
    let text = match fault {
        Fault::NotANumber(_) | Fault::Refused(_) => fault.request_time_text(),
        Fault::Set(set) => {
            let prefix = if older && matches!(set, SetFault::Empty("SS" | "BS")) {
                START_KEY_PREFIX
            } else {
                ""
            };
            format!("{prefix}{}", set.text(older))
        }
        shape => format!("{START_KEY_PREFIX}{}", shape.reason()),
    };
    DynamoDbError::ValidationException(text)
}

/// The `ExpressionAttributeValues` of a `Scan` body in order U, whose faults
/// the service reports at the map's own step: after the key's faults and ahead
/// of the nesting check, read by an expression or not (2026-10-31 B4-7 and
/// 2026-11-05), every fault in the form `ExpressionAttributeValues contains
/// invalid value: <text> for key :e`. The first fault inside one entry is the
/// one `first_fault` finds in the value's element order: a value the service
/// cannot read as one type (`Supplied AttributeValue is empty, must contain
/// exactly one of the supported datatypes` for `{}` and for `{"X": "1"}`, the
/// more-than-one text for two descriptors), a malformed number at the top or
/// inside an `L` or `NS` (`The parameter cannot be converted to a numeric
/// value: bob`, and the 38-digit text), a set fault with the texts of the
/// older path (`One or more parameter values were invalid: An number set  may
/// not be empty`, `... Input collection [a, a] contains duplicates.`;
/// 2026-11-05: `{"L": [{"N": "bob"}, {}]}` answers the number). The entries
/// are visited in the fitted order ([`super::member_order::fitted_member_order`]), and the first
/// faulty one is named (2026-11-05: `:n` ahead of `:e`, `:b` ahead of `:p` and
/// `:a`, `:a` ahead of `:e` when `:a` comes first in the request and `:e`
/// ahead of `:a` when `:e` does, `:s` ahead of `:e`; with four and seven
/// entries the eight and sixteen buckets). In order R the numbers and the set
/// faults are request-time checks ([`super::validate_scan_request_body`]) and nothing
/// is taken.
///
/// After the entries the service reports an empty name in
/// `ExpressionAttributeNames` (measured 2026-11-19: `{"#a": ""}` beside `{"X":
/// "1"}` or `{}` answers the value's text; 2026-11-23: an empty key value
/// comes ahead of it); the name keys are request-time checks.
///
/// [`Self::take`] puts a readable stand-in in the body for each faulty entry
/// and each empty name, so that the typed deserialization and the expression
/// checks run on the other values, and keeps the first fault;
/// [`Self::validate`] reports it. An entry of the wrong JSON shape stays in
/// the body.
pub(crate) struct ExpressionValuesBody {
    /// The first entry's fault in the fitted order, else the empty name's.
    fault: Option<DynamoDbError>,
}

impl ExpressionValuesBody {
    /// Replaces each faulty entry and each empty name with a readable stand-in
    /// in order U and keeps the first fault. `request_order` is the entry
    /// names as the request wrote them ([`super::member_orders`]).
    pub(crate) fn take(
        body: &mut Value,
        order: CheckOrder,
        request_order: Option<&[String]>,
    ) -> Self {
        let mut fault = None;
        if order == CheckOrder::Readable {
            return Self { fault };
        }
        let names = fitted_order_of(body, "ExpressionAttributeValues", request_order);
        if let Some(Value::Object(entries)) = body.get_mut("ExpressionAttributeValues") {
            for name in names {
                let Some(value) = entries.get_mut(&name) else {
                    continue;
                };
                if let Some(text) = first_fault(value, None, true).map(Fault::reason) {
                    fault.get_or_insert_with(|| {
                        DynamoDbError::ValidationException(format!(
                            "ExpressionAttributeValues contains invalid value: {text} for key {name}"
                        ))
                    });
                    *value = stand_in();
                }
            }
        }
        if let Err(empty_name) = validate_names(body) {
            if let Some(Value::Object(names)) = body.get_mut("ExpressionAttributeNames") {
                for name in names.values_mut().filter(|name| name.as_str() == Some("")) {
                    *name = Value::from("a");
                }
            }
            fault.get_or_insert(empty_name);
        }
        Self { fault }
    }

    /// The fault [`Self::take`] kept.
    ///
    /// # Errors
    ///
    /// Returns the first faulty entry's fault in the fitted order, else the
    /// empty name's.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        self.fault.clone().map_or(Ok(()), Err)
    }
}

/// The `ScanFilter` lists of a `Scan` body in order U, whose values the service
/// checks after the no-expression checks and ahead of `ConditionalOperator`,
/// the lists' value counts, the key's faults and the table (2026-11-05). The
/// conditions are visited in the fitted order ([`super::member_order::fitted_member_order`]), each
/// list in its order, and the first fault inside one value is the one
/// `first_fault` finds in the value's element order, with no prefix: a value
/// the service cannot read as one type (`Supplied AttributeValue is empty,
/// must contain exactly one of the supported datatypes` for `{}` and for
/// `{"X": "1"}`, the more-than-one text for two descriptors), a malformed
/// number, a set fault with the texts of the older path. Measured on
/// 2026-11-05 at 05:31 and 07:38 UTC: two descriptors, an empty value inside
/// an `L`, the condition `pk` ahead of `sk`, and `ts` ahead of `id` when `ts`
/// comes first; on 2026-11-23: `[{}, {"NS": []}]` answers the empty value
/// and `[{"N": "bob"}, {}]` the number. [`Self::take`] puts a readable
/// stand-in in the body for each faulty value and keeps the first fault;
/// [`Self::validate`] reports it. In order R nothing is taken: a malformed
/// number in a list is #507's check (#564 item 3), and a set fault the typed
/// deserialization reports with the text of the newer path (2026-11-05).
pub(crate) struct ScanFilterBody {
    fault: Option<DynamoDbError>,
}

impl ScanFilterBody {
    /// Replaces each faulty list value with a readable stand-in in order U and
    /// keeps the first fault. `request_order` is the condition names as the
    /// request wrote them ([`super::member_orders`]).
    pub(crate) fn take(
        body: &mut Value,
        order: CheckOrder,
        request_order: Option<&[String]>,
    ) -> Self {
        let mut fault = None;
        if order == CheckOrder::Readable {
            return Self { fault };
        }
        let names = fitted_order_of(body, "ScanFilter", request_order);
        let Some(Value::Object(conditions)) = body.get_mut("ScanFilter") else {
            return Self { fault };
        };
        for name in names {
            let Some(Value::Array(values)) = conditions
                .get_mut(&name)
                .and_then(|condition| condition.get_mut("AttributeValueList"))
            else {
                continue;
            };
            for value in values.iter_mut() {
                if let Some(text) = first_fault(value, None, true).map(Fault::reason) {
                    fault.get_or_insert(DynamoDbError::ValidationException(text));
                    *value = stand_in();
                }
            }
        }
        Self { fault }
    }

    /// The first list value's fault, at the step of order U.
    ///
    /// # Errors
    ///
    /// Returns the fault of the first value taken out.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        self.fault.clone().map_or(Ok(()), Err)
    }
}

/// The `Select` field of a `Scan` body in order R, where the service checks
/// the enum after the no-expression check and the nesting check and ahead of
/// the filter syntax (2026-11-05): [`Self::take`] takes an invalid value out of
/// the body, so that the typed deserialization does not report it first, and
/// keeps the clause; [`Self::validate`] reports it. In order U the enum is a
/// clause of the request-time aggregate (see `older_form_request_checks`) and
/// nothing is taken.
pub(crate) struct SelectBody {
    clause: Option<DynamoDbError>,
}

impl SelectBody {
    /// Takes an invalid `Select` string out of `body` in order R.
    pub(crate) fn take(body: &mut Value, order: CheckOrder) -> Self {
        if order == CheckOrder::Unreadable {
            return Self { clause: None };
        }
        let clause = crate::validate_enum_fields(
            body,
            &[crate::EnumField {
                json_name: "Select",
                valid: SELECT,
                clause: crate::EnumClause::Named("select"),
            }],
        )
        .err();
        if clause.is_some()
            && let Some(fields) = body.as_object_mut()
        {
            fields.remove("Select");
        }
        Self { clause }
    }

    /// The enum clause, at its stage.
    ///
    /// # Errors
    ///
    /// Returns `DynamoDbError::ValidationException` with the clause.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        self.clause.clone().map_or(Ok(()), Err)
    }
}
