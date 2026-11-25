// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The per-element rules for `UpdateTable`'s `GlobalSecondaryIndexUpdates`,
//! checked before any storage read.
//!
//! Measured against Amazon DynamoDB on 2026-10-23 (rows V1a, V1b, T3b, V3c,
//! T3d, T3e):
//!
//! - An element with two or more of `Create`, `Update`, `Delete`:
//!   "Only one global secondary index action is allowed per
//!   GlobalSecondaryIndexUpdate object" (V1a Update plus Create, V1b Create
//!   plus Delete).
//! - Two actions naming the same index, in one element or in two: "Only one
//!   global secondary index update per index is allowed simultaneously.
//!   Index: <name>" (T3b one element, V3c two). It is checked before the
//!   one-action rule, so an Update and a Delete of one index answer it.
//! - An `Update` that carries `WarmThroughput` beside `ProvisionedThroughput`
//!   or `OnDemandThroughput`: "WarmThroughput must be the only operation in
//!   the request for index: <name>", on an on-demand table, on a provisioned
//!   table, and in the request that switches an on-demand table to
//!   PROVISIONED (measured 2026-11-03). It does not depend on the billing
//!   mode, so it is checked here, before the table is read.
//! - An `Update` without `WarmThroughput` on a table that is, or that the
//!   request makes, on-demand, carrying a `ProvisionedThroughput` or nothing
//!   at all: "The only Updates for index: <name> when TableThroughputMode is
//!   PAY_PER_REQUEST can be to OnDemandThroughput, WarmThroughput" (T3d,
//!   T3e). The stored billing mode is known only once the table is read, so
//!   [`refuse_bare_updates`] runs after the read. An `Update` carrying
//!   `ProvisionedThroughput` and no `OnDemandThroughput` on a table that is
//!   PROVISIONED before and after the request is applied: the backend stores
//!   it as the index's throughput, and the server's index buckets take the
//!   new rate from the response. Every other `Update` without
//!   `WarmThroughput` (one carrying `OnDemandThroughput` on a provisioned
//!   table, the index update of a switch to PROVISIONED) is refused as not
//!   yet supported, as before this rule existed.
//! - The units of an `Update`'s `ProvisionedThroughput` (measured 2026-11-25
//!   on a table name that does not exist, each request twice): a missing or
//!   null unit is `Value null at '<path>' failed to satisfy constraint: Member
//!   must not be null`, a unit below 1 is `Value '<n>' at '<path>' failed to
//!   satisfy constraint: Member must have value greater than or equal to 1`,
//!   with `<path>`
//!   `globalSecondaryIndexUpdates.<n>.member.update.provisionedThroughput.<unit>`,
//!   the write units listed before the read units, ahead of the table read.
//!
//! A `Create` has a valid name, a key schema of defined attributes, and (when
//! given) a throughput of at least 1 unit each; a `Delete` has a valid name.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{BillingMode, BillingModeSummary, UpdateTableInput};

/// Row V1a and V1b, whole.
pub const ONE_ACTION_PER_ELEMENT: &str = "One or more parameter values were invalid: Only one \
     global secondary index action is allowed per GlobalSecondaryIndexUpdate object";

/// Rows T3b and V3c: the sentence for `name`.
#[must_use]
pub fn one_update_per_index(name: &str) -> String {
    format!(
        "One or more parameter values were invalid: Only one global secondary index update per \
         index is allowed simultaneously. Index: {name}"
    )
}

/// Rows T3d and T3e: the sentence for an on-demand table's index `name`.
#[must_use]
pub fn on_demand_update_members(name: &str) -> String {
    format!(
        "One or more parameter values were invalid: The only Updates for index: {name} when \
         TableThroughputMode is PAY_PER_REQUEST can be to OnDemandThroughput, WarmThroughput"
    )
}

/// Measured 2026-11-03: an `Update` carrying `WarmThroughput` beside
/// `ProvisionedThroughput` or `OnDemandThroughput`, the sentence for `name`.
#[must_use]
pub fn index_warm_throughput_only_operation(name: &str) -> String {
    format!(
        "One or more parameter values were invalid: WarmThroughput must be the only operation \
         in the request for index: {name}"
    )
}

/// Whether any `Update` element carries no `WarmThroughput`: nothing but its
/// index name, or a throughput. Such an element needs the table's billing
/// mode for its answer, so the caller reads the table first.
#[must_use]
pub(crate) fn has_bare_update(input: &UpdateTableInput) -> bool {
    input
        .global_secondary_index_updates
        .iter()
        .flatten()
        .any(|u| {
            u.update
                .as_ref()
                .is_some_and(|a| a.warm_throughput.is_none())
        })
}

/// Refuse every `Update` element that carries no `WarmThroughput`, except one
/// carrying `ProvisionedThroughput` and no `OnDemandThroughput` on a table
/// that `stays_provisioned` (PROVISIONED before and after the request), which
/// the backend stores. `pay_per_request` is the billing mode the table has
/// after the request: the request's `BillingMode` when it carries one, the
/// stored mode otherwise.
///
/// Under PAY_PER_REQUEST an element carrying `ProvisionedThroughput` or
/// nothing at all answers the measured sentence (rows T3d, T3e). Every other
/// refused element (only `OnDemandThroughput`, which that sentence names as
/// allowed, or any other element under PROVISIONED) answers ExtendDB's own
/// "not yet supported".
///
/// # Errors
///
/// `ValidationException`.
pub(crate) fn refuse_bare_updates(
    input: &UpdateTableInput,
    pay_per_request: bool,
    stays_provisioned: bool,
) -> Result<(), DynamoDbError> {
    if let Some(upd) = input
        .global_secondary_index_updates
        .iter()
        .flatten()
        .filter_map(|u| u.update.as_ref())
        .find(|a| {
            let stored = stays_provisioned
                && a.provisioned_throughput.is_some()
                && a.on_demand_throughput.is_none();
            a.warm_throughput.is_none() && !stored
        })
    {
        let on_demand_rule =
            upd.provisioned_throughput.is_some() || upd.on_demand_throughput.is_none();
        return Err(DynamoDbError::ValidationException(
            if pay_per_request && on_demand_rule {
                on_demand_update_members(&upd.index_name)
            } else {
                "UpdateGlobalSecondaryIndex is not yet supported".to_owned()
            },
        ));
    }
    Ok(())
}

/// Whether the table is PAY_PER_REQUEST after `input`: the request's
/// `BillingMode` when it carries one, else the stored mode. A request that
/// leaves PAY_PER_REQUEST is not held to the on-demand rule of
/// [`refuse_bare_updates`].
pub(crate) fn pay_per_request_after(
    input: &UpdateTableInput,
    stored: Option<&BillingModeSummary>,
) -> bool {
    match input.billing_mode {
        Some(mode) => mode == BillingMode::PayPerRequest,
        None => stored.is_some_and(|s| s.billing_mode == BillingMode::PayPerRequest),
    }
}

/// `validate_index_name` with the pattern clause located at the update's
/// member path, as the service reports it (measured 2026-10-24). The length
/// clauses keep the bare `indexName` location, which is not measured for
/// UpdateTable.
fn validate_update_index_name(
    name: &str,
    position: usize,
    action: &str,
) -> Result<(), DynamoDbError> {
    match extenddb_core::validation::validate_index_name(name) {
        Err(DynamoDbError::ValidationException(m))
            if m.contains("' at 'indexName' ") && m.contains("regular expression pattern") =>
        {
            Err(DynamoDbError::ValidationException(m.replace(
                "' at 'indexName' ",
                &format!(
                    "' at 'globalSecondaryIndexUpdates.{}.member.{action}.indexName' ",
                    position + 1
                ),
            )))
        }
        other => other,
    }
}

/// The units of the `ProvisionedThroughput` of the `Update` at `position`
/// (0-based): each one present, as an integer of at least 1, answered with
/// the measured member-path clauses, the write units first (module doc). A
/// member that is not an object or a unit that is not an integer is answered
/// as the request's deserializer answers a malformed throughput.
fn validate_update_throughput(
    throughput: &serde_json::Value,
    position: usize,
) -> Result<(), DynamoDbError> {
    let mut clauses = Vec::new();
    if throughput.is_object() {
        for (key, member) in [
            ("WriteCapacityUnits", "writeCapacityUnits"),
            ("ReadCapacityUnits", "readCapacityUnits"),
        ] {
            let path = format!(
                "globalSecondaryIndexUpdates.{}.member.update.provisionedThroughput.{member}",
                position + 1
            );
            match throughput.get(key) {
                None | Some(serde_json::Value::Null) => clauses.push(format!(
                    "Value null at '{path}' failed to satisfy constraint: Member must not be null"
                )),
                Some(units) => {
                    if let Some(n) = units.as_i64().filter(|n| *n < 1) {
                        clauses.push(format!(
                            "Value '{n}' at '{path}' failed to satisfy constraint: Member must \
                             have value greater than or equal to 1"
                        ));
                    }
                }
            }
        }
    }
    if !clauses.is_empty() {
        return Err(DynamoDbError::ValidationException(
            extenddb_core::error::validation_errors_detected(&clauses),
        ));
    }
    serde_json::from_value::<extenddb_core::types::ProvisionedThroughput>(throughput.clone())
        .map(drop)
        .map_err(crate::deserialize_error)
}

/// Apply the element rules to `input`.
///
/// # Errors
///
/// `ValidationException` with the measured sentence (or the existing
/// ExtendDB sentence where the service's was not measured).
pub(crate) fn validate_gsi_updates(input: &UpdateTableInput) -> Result<(), DynamoDbError> {
    let Some(updates) = &input.global_secondary_index_updates else {
        return Ok(());
    };
    // Rows T3b and V3c: the per-index rule is applied to every name in every
    // element before any element is looked at on its own, so an element that
    // names one index twice (an Update and a Delete of `gsi`, row T3b) answers
    // it rather than the one-action rule; two actions on different indexes
    // (rows V1a, V1b) pass this and reach the one-action rule below.
    let mut seen: Vec<&str> = Vec::new();
    for update in updates {
        for name in [
            update.create.as_ref().map(|c| c.index_name.as_str()),
            update.update.as_ref().map(|u| u.index_name.as_str()),
            update.delete.as_ref().map(|d| d.index_name.as_str()),
        ]
        .into_iter()
        .flatten()
        {
            if seen.contains(&name) {
                return Err(DynamoDbError::ValidationException(one_update_per_index(
                    name,
                )));
            }
            seen.push(name);
        }
    }
    for (position, update) in updates.iter().enumerate() {
        let actions = usize::from(update.create.is_some())
            + usize::from(update.update.is_some())
            + usize::from(update.delete.is_some());
        if actions > 1 {
            return Err(DynamoDbError::ValidationException(
                ONE_ACTION_PER_ELEMENT.to_owned(),
            ));
        }
        if actions == 0 {
            return Err(DynamoDbError::ValidationException(
                "One or more parameter values were invalid: GlobalSecondaryIndexUpdate must \
                 contain Create, Update, or Delete"
                    .to_owned(),
            ));
        }
        if let Some(upd) = &update.update {
            // An Update carrying only WarmThroughput is stored (checked against
            // the index's current values once the table is read). One carrying
            // a throughput beside it is refused here, in every billing mode;
            // one without it is refused by [`refuse_bare_updates`] once the
            // table's billing mode is known.
            extenddb_core::validation::validate_index_name(&upd.index_name)?;
            if upd.warm_throughput.is_some() && upd.carries_throughput() {
                return Err(DynamoDbError::ValidationException(
                    index_warm_throughput_only_operation(&upd.index_name),
                ));
            }
            if let Some(throughput) = &upd.provisioned_throughput {
                validate_update_throughput(throughput, position)?;
            }
        }
        // Create and Delete names are validated the way CreateTable does,
        // except that the pattern clause carries the member's path (measured
        // 2026-10-24: `globalSecondaryIndexUpdates.<n>.member.create.indexName`,
        // 1-based). The path of an Update's name is not measured.
        if let Some(create) = &update.create {
            validate_update_index_name(&create.index_name, position, "create")?;
            extenddb_core::validation::validate_index_throughput_minimum(
                &create.index_name,
                create.provisioned_throughput.as_ref(),
            )?;
            if create.key_schema.is_empty() {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: KeySchema must not be empty for \
                     GSI creation"
                        .to_owned(),
                ));
            }
            let attr_defs = input.attribute_definitions.as_deref().unwrap_or(&[]);
            for ks in &create.key_schema {
                if !attr_defs
                    .iter()
                    .any(|ad| ad.attribute_name == ks.attribute_name)
                {
                    return Err(DynamoDbError::ValidationException(format!(
                        "One or more parameter values were invalid: Some index key attributes \
                         are not defined in AttributeDefinitions. Keys: [{}], \
                         AttributeDefinitions: [{}]",
                        ks.attribute_name,
                        attr_defs
                            .iter()
                            .map(|ad| ad.attribute_name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            }
        }
        if let Some(delete) = &update.delete {
            validate_update_index_name(&delete.index_name, position, "delete")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn input(updates: Value) -> UpdateTableInput {
        serde_json::from_value(json!({
            "TableName": "t",
            "AttributeDefinitions": [{ "AttributeName": "g", "AttributeType": "S" }],
            "GlobalSecondaryIndexUpdates": updates,
        }))
        .unwrap()
    }

    fn message(result: Result<(), DynamoDbError>) -> String {
        match result.expect_err("refused") {
            DynamoDbError::ValidationException(m) => m,
            other => panic!("{other:?}"),
        }
    }

    /// The `ValidationException` text of a refusal, `None` when accepted.
    fn refusal(result: Result<(), DynamoDbError>) -> Option<String> {
        result.is_err().then(|| message(result))
    }

    fn create(name: &str) -> Value {
        json!({ "Create": {
            "IndexName": name,
            "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
        }})
    }

    fn warm_update(name: &str) -> Value {
        json!({ "Update": { "IndexName": name, "WarmThroughput": { "ReadUnitsPerSecond": 12000 } } })
    }

    /// Rows V1a and V1b: an element with two actions, whichever two, and the
    /// Create or Delete in it is never reached (the Create here keys on an
    /// undefined attribute and would otherwise be refused differently).
    #[test]
    fn an_element_with_two_actions_is_refused_before_its_parts_are_checked() {
        let mut mixed = warm_update("gsi");
        mixed["Create"] = json!({
            "IndexName": "bad",
            "KeySchema": [{ "AttributeName": "nope", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
        });
        assert_eq!(
            message(validate_gsi_updates(&input(json!([mixed])))),
            ONE_ACTION_PER_ELEMENT
        );
        let mut update_delete = warm_update("gsi");
        update_delete["Delete"] = json!({ "IndexName": "other" });
        assert_eq!(
            message(validate_gsi_updates(&input(json!([update_delete])))),
            ONE_ACTION_PER_ELEMENT
        );
        let mut create_delete = create("gsi-two");
        create_delete["Delete"] = json!({ "IndexName": "gsi" });
        assert_eq!(
            message(validate_gsi_updates(&input(json!([create_delete])))),
            ONE_ACTION_PER_ELEMENT
        );
    }

    /// Row T3b: one element carrying an Update and a Delete of the same index
    /// answers the per-index sentence, not the one-action sentence.
    #[test]
    fn one_element_naming_an_index_twice_answers_the_per_index_sentence() {
        let mut same = warm_update("gsi");
        same["Delete"] = json!({ "IndexName": "gsi" });
        assert_eq!(
            message(validate_gsi_updates(&input(json!([same])))),
            one_update_per_index("gsi")
        );
    }

    /// Rows T3b and V3c: two elements on one index.
    #[test]
    fn two_elements_on_one_index_are_refused() {
        let msg = message(validate_gsi_updates(&input(json!([
            { "Delete": { "IndexName": "gsi" } },
            warm_update("gsi"),
        ]))));
        assert_eq!(
            msg,
            "One or more parameter values were invalid: Only one global secondary index update \
             per index is allowed simultaneously. Index: gsi"
        );
        validate_gsi_updates(&input(json!([
            { "Delete": { "IndexName": "gsi" } },
            warm_update("other"),
        ])))
        .expect("different indexes");
    }

    /// Measured 2026-11-03: an Update carrying WarmThroughput beside
    /// ProvisionedThroughput, OnDemandThroughput or both answers the
    /// only-operation sentence before the table is read, so the answer is
    /// the same under either billing mode and the throughput is never
    /// dropped with a 200.
    #[test]
    fn a_throughput_beside_warm_throughput_is_refused_in_either_billing_mode() {
        let warm = json!({ "ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000 });
        let provisioned = json!({ "ReadCapacityUnits": 9, "WriteCapacityUnits": 9 });
        let on_demand = json!({ "MaxReadRequestUnits": 100 });
        for members in [
            json!({ "ProvisionedThroughput": provisioned }),
            json!({ "OnDemandThroughput": on_demand }),
            json!({ "ProvisionedThroughput": provisioned, "OnDemandThroughput": on_demand }),
        ] {
            let mut update = json!({ "IndexName": "gsi", "WarmThroughput": warm });
            update
                .as_object_mut()
                .unwrap()
                .extend(members.as_object().unwrap().clone());
            for mode in [None, Some("PROVISIONED"), Some("PAY_PER_REQUEST")] {
                let mut request = input(json!([{ "Update": update }]));
                request.billing_mode = mode.map(|m| serde_json::from_value(json!(m)).unwrap());
                assert_eq!(
                    message(validate_gsi_updates(&request)),
                    "One or more parameter values were invalid: WarmThroughput must be the only \
                     operation in the request for index: gsi",
                    "{update} with BillingMode {mode:?}"
                );
            }
        }
    }

    /// An Update without WarmThroughput: on an on-demand table one carrying
    /// ProvisionedThroughput or nothing answers the measured sentence (rows
    /// T3d, T3e); on a provisioned table one carrying ProvisionedThroughput
    /// alone is accepted (`None`), and one carrying only OnDemandThroughput
    /// or nothing answers ExtendDB's "not yet supported", as does any on an
    /// on-demand table carrying only OnDemandThroughput.
    #[test]
    fn an_update_without_warm_throughput_answers_by_the_billing_mode_after_the_request() {
        let provisioned = json!({ "ReadCapacityUnits": 3, "WriteCapacityUnits": 3 });
        let on_demand = json!({ "MaxReadRequestUnits": 100 });
        let on_demand_sentence = "One or more parameter values were invalid: The only Updates \
             for index: gsi when TableThroughputMode is PAY_PER_REQUEST can be to \
             OnDemandThroughput, WarmThroughput";
        let not_yet = Some("UpdateGlobalSecondaryIndex is not yet supported");
        let on_demand_sentence = Some(on_demand_sentence);
        for (update, pay_per_request, expected) in [
            (json!({ "IndexName": "gsi" }), true, on_demand_sentence),
            (
                json!({ "IndexName": "gsi", "ProvisionedThroughput": provisioned }),
                true,
                on_demand_sentence,
            ),
            (
                json!({ "IndexName": "gsi", "ProvisionedThroughput": provisioned, "OnDemandThroughput": on_demand }),
                true,
                on_demand_sentence,
            ),
            (
                json!({ "IndexName": "gsi", "OnDemandThroughput": on_demand }),
                true,
                not_yet,
            ),
            (json!({ "IndexName": "gsi" }), false, not_yet),
            (
                json!({ "IndexName": "gsi", "ProvisionedThroughput": provisioned }),
                false,
                None,
            ),
            (
                json!({ "IndexName": "gsi", "ProvisionedThroughput": provisioned, "OnDemandThroughput": on_demand }),
                false,
                not_yet,
            ),
            (
                json!({ "IndexName": "gsi", "OnDemandThroughput": on_demand }),
                false,
                not_yet,
            ),
        ] {
            let request = input(json!([{ "Update": update }]));
            validate_gsi_updates(&request).expect("the element rules pass; the mode decides");
            assert!(has_bare_update(&request));
            assert_eq!(
                refusal(refuse_bare_updates(
                    &request,
                    pay_per_request,
                    !pay_per_request
                )),
                expected.map(str::to_owned),
                "{update} pay_per_request={pay_per_request}"
            );
        }
    }

    /// The units of an Update's ProvisionedThroughput, measured 2026-11-25 on
    /// a table name that does not exist (each request twice): each row the
    /// throughput sent and the answer, or `None` for none (the service then
    /// answered ResourceNotFoundException).
    #[test]
    fn an_update_throughput_is_checked_with_the_measured_member_sentences() {
        let at = |unit: &str| {
            format!("'globalSecondaryIndexUpdates.1.member.update.provisionedThroughput.{unit}'")
        };
        let below = |n: i64, unit: &str| {
            format!(
                "1 validation error detected: Value '{n}' at {} failed to satisfy constraint: \
                 Member must have value greater than or equal to 1",
                at(unit)
            )
        };
        let null = |unit: &str| {
            format!(
                "Value null at {} failed to satisfy constraint: Member must not be null",
                at(unit)
            )
        };
        let rows = [
            (
                json!({ "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 }),
                None,
            ),
            (
                json!({ "ReadCapacityUnits": 1_000_000_000, "WriteCapacityUnits": 5 }),
                None,
            ),
            (
                json!({ "ReadCapacityUnits": 0, "WriteCapacityUnits": 5 }),
                Some(below(0, "readCapacityUnits")),
            ),
            (
                json!({ "ReadCapacityUnits": 5, "WriteCapacityUnits": 0 }),
                Some(below(0, "writeCapacityUnits")),
            ),
            (
                json!({ "ReadCapacityUnits": -1, "WriteCapacityUnits": 5 }),
                Some(below(-1, "readCapacityUnits")),
            ),
            (
                json!({ "ReadCapacityUnits": 5 }),
                Some(format!(
                    "1 validation error detected: {}",
                    null("writeCapacityUnits")
                )),
            ),
            (
                json!({}),
                Some(format!(
                    "2 validation errors detected: {}; {}",
                    null("writeCapacityUnits"),
                    null("readCapacityUnits")
                )),
            ),
        ];
        for (throughput, expected) in rows {
            let request = input(json!([{ "Update": {
                "IndexName": "gsi",
                "ProvisionedThroughput": throughput,
            }}]));
            assert_eq!(
                refusal(validate_gsi_updates(&request)),
                expected,
                "{throughput}"
            );
        }
    }

    /// The mode an Update without WarmThroughput is answered by is the one the
    /// table has after the request. On a PAY_PER_REQUEST table the switch to
    /// PROVISIONED that the service requires (BillingMode PROVISIONED, the
    /// table's and the index's ProvisionedThroughput 3/3) gets "not yet
    /// supported"; the same Update without BillingMode gets the on-demand
    /// sentence.
    #[test]
    fn the_billing_mode_after_the_request_decides_a_bare_updates_answer() {
        let stored = BillingModeSummary {
            billing_mode: BillingMode::PayPerRequest,
            last_update_to_pay_per_request_date_time: None,
        };
        let update = json!([{ "Update": {
            "IndexName": "gsi",
            "ProvisionedThroughput": { "ReadCapacityUnits": 3, "WriteCapacityUnits": 3 },
        }}]);
        let switch: UpdateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "BillingMode": "PROVISIONED",
            "ProvisionedThroughput": { "ReadCapacityUnits": 3, "WriteCapacityUnits": 3 },
            "GlobalSecondaryIndexUpdates": update,
        }))
        .unwrap();
        assert!(!pay_per_request_after(&switch, Some(&stored)));
        assert_eq!(
            message(refuse_bare_updates(
                &switch,
                pay_per_request_after(&switch, Some(&stored)),
                false
            )),
            "UpdateGlobalSecondaryIndex is not yet supported"
        );
        let stay = input(update);
        assert!(pay_per_request_after(&stay, Some(&stored)));
        assert_eq!(
            message(refuse_bare_updates(
                &stay,
                pay_per_request_after(&stay, Some(&stored)),
                false
            )),
            "One or more parameter values were invalid: The only Updates for index: gsi when \
             TableThroughputMode is PAY_PER_REQUEST can be to OnDemandThroughput, WarmThroughput"
        );
        assert!(!pay_per_request_after(&stay, None));
    }

    /// Rows T3d and T3e: an Update with nothing but the index name on an
    /// on-demand table answers the measured sentence; on a provisioned table
    /// ExtendDB's own.
    #[test]
    fn a_bare_update_is_refused_with_the_billing_mode_sentence() {
        let bare = input(json!([{ "Update": { "IndexName": "gsi" } }]));
        validate_gsi_updates(&bare).expect("the element rules pass; the mode decides");
        assert!(has_bare_update(&bare));
        assert!(!has_bare_update(&input(json!([warm_update("gsi")]))));
        assert_eq!(
            message(refuse_bare_updates(&bare, true, false)),
            "One or more parameter values were invalid: The only Updates for index: gsi when \
             TableThroughputMode is PAY_PER_REQUEST can be to OnDemandThroughput, WarmThroughput"
        );
        assert!(message(refuse_bare_updates(&bare, false, true)).contains("not yet supported"));
        refuse_bare_updates(&input(json!([warm_update("gsi")])), true, false).expect("warm update");
    }

    #[test]
    fn a_created_gsi_throughput_below_one_is_refused() {
        for (read, write) in [(0, 0), (0, 1), (1, 0), (-1, 5)] {
            let mut c = create("gix");
            c["Create"]["ProvisionedThroughput"] =
                json!({ "ReadCapacityUnits": read, "WriteCapacityUnits": write });
            let m = message(validate_gsi_updates(&input(json!([c]))));
            assert!(m.ends_with("for index: gix"), "({read}, {write}): {m}");
        }
        let mut ok = create("gix");
        ok["Create"]["ProvisionedThroughput"] =
            json!({ "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 });
        validate_gsi_updates(&input(json!([ok]))).unwrap();
    }

    #[test]
    fn an_empty_element_and_an_undefined_key_attribute_are_refused() {
        assert!(message(validate_gsi_updates(&input(json!([{}])))).contains("must contain"));
        let mut c = create("gix");
        c["Create"]["KeySchema"] = json!([{ "AttributeName": "nope", "KeyType": "HASH" }]);
        assert!(
            message(validate_gsi_updates(&input(json!([c]))))
                .contains("Some index key attributes are not defined")
        );
    }

    #[test]
    fn an_index_name_outside_the_pattern_is_reported_at_its_member_path() {
        let cases = [
            (
                json!([create("g\u{0}2")]),
                "globalSecondaryIndexUpdates.1.member.create.indexName",
            ),
            (
                json!([
                    { "Delete": { "IndexName": "gix" } },
                    { "Delete": { "IndexName": "g\u{0}2" } },
                ]),
                "globalSecondaryIndexUpdates.2.member.delete.indexName",
            ),
        ];
        for (updates, path) in cases {
            assert_eq!(
                message(validate_gsi_updates(&input(updates))),
                format!(
                    "1 validation error detected: Value 'g\u{0}2' at '{path}' failed to \
                     satisfy constraint: Member must satisfy regular expression pattern: \
                     [a-zA-Z0-9_.-]+"
                ),
                "{path}"
            );
        }
        // A length fault keeps the bare location, which is not measured here.
        let m = message(validate_gsi_updates(&input(json!([create("ab")]))));
        assert!(m.contains("' at 'indexName' "), "{m}");
    }
}
