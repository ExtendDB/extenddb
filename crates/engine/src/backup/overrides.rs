// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `RestoreTableFromBackup`'s `GlobalSecondaryIndexOverride`,
//! `LocalSecondaryIndexOverride`, and `OnDemandThroughputOverride`, applied as
//! the service applies them and refused where the service refuses them
//! (every rule below measured on DynamoDB, unless
//! marked as ExtendDB's).
//!
//! An index override list, when given, replaces the backup's list of that
//! kind: an empty list restores no index of that kind, a subset restores
//! those, and a GSI entry's `ProvisionedThroughput` is the restored index's.
//! Each entry must name an index of the backup and give its `KeySchema` (in
//! order) and `Projection` (`NonKeyAttributes` compared as a set): the
//! service refuses an unknown or changed index definition, and ExtendDB
//! cannot restore an index the backup did not record. `OnDemandThroughputOverride`
//! is the restored table's on-demand throughput, on any on-demand restore,
//! including a PROVISIONED backup restored as on-demand.
//!
//! Refused with `ValidationException`: an index name the backup does not have
//! or listed twice, a changed `KeySchema` or `Projection`, a throughput member
//! that contradicts the restored table's billing mode (`ProvisionedThroughput`
//! on a GSI entry or `OnDemandThroughputOverride` on the wrong side), and a
//! PROVISIONED restore of a GSI without an overriding throughput, with the
//! service's text `Invalid Request: Must override ProvisionedThroughput if
//! BillingMode is overridden to PROVISIONED for index <name>`. That text
//! conditions on the override, so the rule does too: whenever
//! `BillingModeOverride` is `PROVISIONED` and the backup has a GSI, every GSI
//! must be supplied with a `ProvisionedThroughput` by a
//! `GlobalSecondaryIndexOverride` entry, whatever the backup's billing mode
//! and whatever throughput the backup recorded for the index. With no GSI
//! list, the first GSI is the one named (measured on an on-demand backup;
//! applied by ExtendDB to a PROVISIONED backup too, which was not measured).
//! With a list, an entry without `ProvisionedThroughput` on a PROVISIONED
//! restore is refused with the same text, also when the restore is
//! PROVISIONED because the backup is and no `BillingModeOverride` was given
//! (the service's reply to that case was not measured; ExtendDB's
//! `CreateTable` accepts a PROVISIONED table whose GSI has no throughput, so
//! there is no `CreateTable` text to borrow). `{0, 0}` and the rest of
//! CreateTable's rules are left to `validate_create_table` on the result.
//! When an index list is given, the attribute definitions only the dropped
//! indexes used go with them, as CreateTable refuses unused definitions.
//!
//! ExtendDB's own choices, where the service's reply was not measured or
//! ExtendDB cannot carry the member: a GSI entry's `OnDemandThroughput` on an
//! on-demand restore is accepted and not recorded (ExtendDB keeps no
//! per-index on-demand maximum); on a PROVISIONED restore it contradicts the
//! billing mode and is refused. A member of an entry outside the service
//! model's is refused, since ExtendDB cannot apply it. A GSI entry's
//! `WarmThroughput` is refused with the service's own sentence (measured
//! 2026-10-23, row S6d). A restored table and its indexes report the
//! on-demand warm throughput defaults, as the service's restore does (row
//! S6c); the backup records no warm throughput (row S6b). An
//! `OnDemandThroughputOverride` replaces the backup's on-demand
//! throughput as a whole: a member it leaves out means no maximum, as on
//! `CreateTable`.

use std::collections::HashSet;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    BillingMode, CreateTableInput, GsiInput, KeySchemaElement, LsiInput, OnDemandThroughput,
    Projection,
};
use serde_json::Value;

const GSI_FIELD: &str = "GlobalSecondaryIndexOverride";
const LSI_FIELD: &str = "LocalSecondaryIndexOverride";
const ON_DEMAND_FIELD: &str = "OnDemandThroughputOverride";

const GSI_MEMBERS: [&str; 5] = [
    "IndexName",
    "KeySchema",
    "Projection",
    "ProvisionedThroughput",
    "OnDemandThroughput",
];
const LSI_MEMBERS: [&str; 3] = ["IndexName", "KeySchema", "Projection"];
const ON_DEMAND_MEMBERS: [&str; 2] = ["MaxReadRequestUnits", "MaxWriteRequestUnits"];

/// One GSI entry as sent.
#[derive(Debug)]
struct GsiEntry {
    index: GsiInput,
    on_demand: Option<OnDemandThroughput>,
    unknown: Vec<String>,
}

/// One LSI entry as sent.
#[derive(Debug)]
struct LsiEntry {
    index: LsiInput,
    unknown: Vec<String>,
}

/// The three overrides of one request, parsed and shape-checked. `None` is
/// a member that was absent or null; an empty list is `Some(vec![])`, since
/// it means "no indexes of this kind".
#[derive(Debug, Default)]
pub(super) struct IndexOverrides {
    gsi: Option<Vec<GsiEntry>>,
    lsi: Option<Vec<LsiEntry>>,
    on_demand: Option<(OnDemandThroughput, Vec<String>)>,
}

/// The members of an entry outside `known`, in the order sent.
fn unknown_members(entry: &Value, known: &[&str]) -> Vec<String> {
    entry
        .as_object()
        .map(|o| {
            o.keys()
                .filter(|k| !known.contains(&k.as_str()))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// A list member, or `None` for absent or null.
fn list<'a>(body: &'a Value, field: &str) -> Result<Option<&'a Vec<Value>>, DynamoDbError> {
    match body.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(list)) => Ok(Some(list)),
        Some(_) => Err(DynamoDbError::SerializationException(format!(
            "Start of structure or map found where not expected: {field}"
        ))),
    }
}

/// The service's text for a PROVISIONED restore of a GSI without throughput
/// (measured on DynamoDB).
pub(super) fn gsi_throughput_required(index_name: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden to \
         PROVISIONED for index {index_name}"
    ))
}

/// The service's refusal of a `WarmThroughput` member on a
/// `GlobalSecondaryIndexOverride` entry (measured, row S6d), after the
/// "One or more parameter values were invalid: " prefix.
const WARM_THROUGHPUT_OVERRIDE: &str =
    "WarmThroughput is not supported on Global Secondary Indexes via RestoreTableFromBackup";

fn invalid(detail: String) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "One or more parameter values were invalid: {detail}"
    ))
}

impl IndexOverrides {
    /// Parse the three members. No storage is read; a malformed entry is
    /// refused here, as CreateTable refuses a malformed index.
    pub(super) fn parse(body: &Value) -> Result<Self, DynamoDbError> {
        let gsi = list(body, GSI_FIELD)?
            .map(|entries| {
                entries
                    .iter()
                    .map(|entry| {
                        let index: GsiInput = serde_json::from_value(entry.clone())
                            .map_err(crate::deserialize_error)?;
                        let on_demand = entry
                            .get("OnDemandThroughput")
                            .filter(|v| !v.is_null())
                            .cloned()
                            .map(serde_json::from_value)
                            .transpose()
                            .map_err(crate::deserialize_error)?;
                        Ok(GsiEntry {
                            index,
                            on_demand,
                            unknown: unknown_members(entry, &GSI_MEMBERS),
                        })
                    })
                    .collect::<Result<Vec<_>, DynamoDbError>>()
            })
            .transpose()?;
        let lsi = list(body, LSI_FIELD)?
            .map(|entries| {
                entries
                    .iter()
                    .map(|entry| {
                        let index: LsiInput = serde_json::from_value(entry.clone())
                            .map_err(crate::deserialize_error)?;
                        Ok(LsiEntry {
                            index,
                            unknown: unknown_members(entry, &LSI_MEMBERS),
                        })
                    })
                    .collect::<Result<Vec<_>, DynamoDbError>>()
            })
            .transpose()?;
        let on_demand = match body.get(ON_DEMAND_FIELD) {
            None | Some(Value::Null) => None,
            Some(v @ Value::Object(_)) => Some((
                serde_json::from_value(v.clone()).map_err(crate::deserialize_error)?,
                unknown_members(v, &ON_DEMAND_MEMBERS),
            )),
            Some(_) => {
                return Err(DynamoDbError::SerializationException(format!(
                    "Start of structure or map found where not expected: {ON_DEMAND_FIELD}"
                )));
            }
        };
        Ok(Self {
            gsi,
            lsi,
            on_demand,
        })
    }

    /// Apply the overrides to `input`, the table the restore creates without
    /// them (the backup's indexes, with the billing and throughput overrides
    /// already applied). `billing_override` is the request's
    /// `BillingModeOverride`, which decides the no-GSI-list refusal.
    ///
    /// # Errors
    ///
    /// `ValidationException` for each refusal in the module documentation.
    pub(super) fn apply(
        &self,
        input: &mut CreateTableInput,
        billing_override: Option<BillingMode>,
    ) -> Result<(), DynamoDbError> {
        let provisioned = input.billing_mode != Some(BillingMode::PayPerRequest);
        let backup_gsis = input.global_secondary_indexes.take().unwrap_or_default();
        input.global_secondary_indexes = match &self.gsi {
            Some(entries) => {
                let gsis = apply_gsis(entries, &backup_gsis, provisioned)?;
                (!gsis.is_empty()).then_some(gsis)
            }
            None => {
                // The override alone decides: the backup's recorded GSI
                // throughput (present on a PROVISIONED backup, or stale on an
                // on-demand one) does not stand in for an override entry.
                if billing_override == Some(BillingMode::Provisioned)
                    && let Some(index) = backup_gsis.first()
                {
                    return Err(gsi_throughput_required(&index.index_name));
                }
                (!backup_gsis.is_empty()).then_some(backup_gsis)
            }
        };
        if let Some(entries) = &self.lsi {
            let backup_lsis = input.local_secondary_indexes.take().unwrap_or_default();
            let lsis = apply_lsis(entries, &backup_lsis)?;
            input.local_secondary_indexes = (!lsis.is_empty()).then_some(lsis);
        }
        if self.gsi.is_some() || self.lsi.is_some() {
            let used: HashSet<&str> = input
                .key_schema
                .iter()
                .chain(
                    input
                        .global_secondary_indexes
                        .iter()
                        .flatten()
                        .flat_map(|g| &g.key_schema),
                )
                .chain(
                    input
                        .local_secondary_indexes
                        .iter()
                        .flatten()
                        .flat_map(|l| &l.key_schema),
                )
                .map(|k| k.attribute_name.as_str())
                .collect();
            let used: HashSet<String> = used.into_iter().map(str::to_owned).collect();
            input
                .attribute_definitions
                .retain(|a| used.contains(&a.attribute_name));
        }
        if let Some((given, unknown)) = &self.on_demand {
            if let Some(member) = unknown.first() {
                return Err(invalid(format!(
                    "{ON_DEMAND_FIELD}: member {member} is not supported"
                )));
            }
            if provisioned {
                return Err(invalid(format!(
                    "{ON_DEMAND_FIELD} can only be specified when the restored table's \
                     BillingMode is PAY_PER_REQUEST"
                )));
            }
            input.on_demand_throughput = Some(given.clone()).filter(|o| {
                o.max_read_request_units.is_some() || o.max_write_request_units.is_some()
            });
        }
        Ok(())
    }
}

/// Each entry names a backup index once; returns the backup index for each.
fn match_entries<'a, T>(
    kind: &str,
    names: impl Iterator<Item = &'a str>,
    backup: &'a [T],
    name_of: impl Fn(&T) -> &str,
) -> Result<Vec<&'a T>, DynamoDbError> {
    let mut seen = HashSet::new();
    names
        .map(|name| {
            if !seen.insert(name) {
                return Err(invalid(format!(
                    "{kind} index {name} is listed more than once"
                )));
            }
            backup
                .iter()
                .find(|i| name_of(i) == name)
                .ok_or_else(|| invalid(format!("{kind} index {name} is not in the backup")))
        })
        .collect()
}

/// The checks every index entry gets: no member ExtendDB cannot apply, and
/// the backup index's `KeySchema` and `Projection`. Pairs are (given,
/// backup).
fn same_definition(
    name: &str,
    unknown: &[String],
    key_schema: (&Vec<KeySchemaElement>, &Vec<KeySchemaElement>),
    projection: (&Projection, &Projection),
) -> Result<(), DynamoDbError> {
    if let Some(member) = unknown.first() {
        return Err(invalid(format!(
            "index {name}: member {member} is not supported"
        )));
    }
    if key_schema.0 != key_schema.1 {
        return Err(invalid(format!(
            "index {name}: KeySchema differs from the backup's"
        )));
    }
    if !same_projection(projection.0, projection.1) {
        return Err(invalid(format!(
            "index {name}: Projection differs from the backup's"
        )));
    }
    Ok(())
}

fn same_projection(a: &Projection, b: &Projection) -> bool {
    let attrs = |p: &Projection| -> HashSet<String> {
        p.non_key_attributes.iter().flatten().cloned().collect()
    };
    a.projection_type == b.projection_type && attrs(a) == attrs(b)
}

/// The restored GSIs for an override list: the listed backup indexes, in the
/// list's order, each with the entry's throughput.
fn apply_gsis(
    entries: &[GsiEntry],
    backup: &[GsiInput],
    provisioned: bool,
) -> Result<Vec<GsiInput>, DynamoDbError> {
    let matched = match_entries(
        "global secondary",
        entries.iter().map(|e| e.index.index_name.as_str()),
        backup,
        |g| g.index_name.as_str(),
    )?;
    entries
        .iter()
        .zip(matched)
        .map(|(entry, recorded)| {
            let given = &entry.index;
            let name = &given.index_name;
            if entry.unknown.iter().any(|m| m == "WarmThroughput") {
                return Err(invalid(WARM_THROUGHPUT_OVERRIDE.to_owned()));
            }
            same_definition(
                name,
                &entry.unknown,
                (&given.key_schema, &recorded.key_schema),
                (&given.projection, &recorded.projection),
            )?;
            if provisioned {
                if given.provisioned_throughput.is_none() {
                    return Err(gsi_throughput_required(name));
                }
                if entry.on_demand.is_some() {
                    return Err(invalid(format!(
                        "index {name}: OnDemandThroughput can only be specified when the \
                         restored table's BillingMode is PAY_PER_REQUEST"
                    )));
                }
            }
            // An on-demand restore: a ProvisionedThroughput given here stays
            // on the index and validate_create_table refuses it with
            // CreateTable's text; an OnDemandThroughput is accepted and not
            // recorded (see the module documentation).
            Ok(GsiInput {
                index_name: recorded.index_name.clone(),
                key_schema: recorded.key_schema.clone(),
                projection: recorded.projection.clone(),
                provisioned_throughput: given.provisioned_throughput.clone(),
                warm_throughput: None,
            })
        })
        .collect()
}

/// The restored LSIs for an override list: the listed backup indexes, in the
/// list's order.
fn apply_lsis(entries: &[LsiEntry], backup: &[LsiInput]) -> Result<Vec<LsiInput>, DynamoDbError> {
    let matched = match_entries(
        "local secondary",
        entries.iter().map(|e| e.index.index_name.as_str()),
        backup,
        |l| l.index_name.as_str(),
    )?;
    entries
        .iter()
        .zip(matched)
        .map(|(entry, recorded)| {
            let given = &entry.index;
            same_definition(
                &given.index_name,
                &entry.unknown,
                (&given.key_schema, &recorded.key_schema),
                (&given.projection, &recorded.projection),
            )?;
            Ok(recorded.clone())
        })
        .collect()
}
