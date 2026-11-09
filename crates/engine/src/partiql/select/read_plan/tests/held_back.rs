// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the disjuncts the caller may not read, held back with the
//! disjuncts that fail a check made when they are read.

use super::*;
use extenddb_core::types::ScalarAttributeType::S;

const HASH: &str = "One or more parameter values were invalid: Size of hashkey has exceeded \
                    the maximum size limit of2048 bytes";
const FILTER: &str = "Filter Expression can only contain non-primary key attributes: Primary \
                      key attribute: sk";

/// How `check_reads` leaves a statement on a string-keyed table when the
/// caller may not read the partitions `denied`: the number of disjuncts read
/// and the refusal left for later, or the refusal the statement gets at
/// once. The refusal of a denied partition `v` is "denied v".
fn held_back(condition: &str, denied: &[&str]) -> Result<(usize, Option<String>), String> {
    let s = select(&format!("SELECT * FROM \"t\" WHERE {condition}"));
    let info = table(S);
    let value_of = |op: &Operand| bound_value(op, &[]).cloned();
    let analysis = WhereAnalysis::new(s.where_clause.as_ref().unwrap(), KEYS, &value_of);
    let refusal = |read: &KeyRead<'_>| {
        let Some(AttributeValue::S(partition)) = value_of(read.partition) else {
            return None;
        };
        denied
            .contains(&partition.as_str())
            .then(|| DynamoDbError::AccessDeniedException(format!("denied {partition}")))
    };
    check_reads(
        &analysis,
        KEYS,
        &info.key_schema,
        &LimitsConfig::default(),
        &[],
        &value_of,
        &refusal,
    )
    .map(|c| (c.reads.len(), c.deferred.map(|e| e.message().to_owned())))
    .map_err(|e| e.message().to_owned())
}

#[test]
fn a_denied_disjunct_is_refused_for_authorization_before_its_checks() {
    // Measured 2026-11-03 under ForAllValues LeadingKeys ["a"]: a disjunct
    // the policy refuses that is past the key size limit gets AccessDenied
    // on the page after the allowed disjuncts (D7, D8), and so does one with
    // a filter on a key attribute (D15, under ["b"]).
    let h = "z".repeat(2049);
    let later = |reads: usize, message: String| Ok((reads, Some(message)));
    assert_eq!(
        held_back(&format!("pk IN ['a', 'b', '{h}']"), &["b", &h]),
        later(1, format!("denied {h}"))
    );
    assert_eq!(
        held_back(&format!("pk IN ['{h}', 'a', 'b']"), &["b", &h]),
        later(1, format!("denied {h}"))
    );
    assert_eq!(
        held_back("(pk = 'a' AND sk = a) OR pk = 'b' OR pk = 'c'", &["a", "c"]),
        later(1, "denied c".to_owned())
    );
    assert_eq!(
        held_back("(pk = 'b' AND sk = a) OR pk = 'a'", &["b"]),
        later(1, "denied b".to_owned())
    );
}

#[test]
fn an_authorization_refusal_ranks_below_the_refusals_of_the_checks() {
    // Measured 2026-11-03 under ForAllValues LeadingKeys ["a"]: both orders
    // are refused at once with the filter refusal of the disjunct the policy
    // allows, though b is read later (D13, D14).
    for condition in [
        "(pk = 'a' AND sk = a) OR pk = 'b'",
        "pk = 'b' OR (pk = 'a' AND sk = a)",
    ] {
        assert_eq!(
            held_back(condition, &["b"]),
            Err(FILTER.to_owned()),
            "{condition}"
        );
    }
    // With a disjunct that is read, the refusal left for later is a
    // check's, the last in read order of those.
    let h = "z".repeat(2049);
    assert_eq!(
        held_back("pk = 'a' OR (pk = 'b' AND sk = a) OR pk = 'c'", &["c"]),
        Ok((1, Some(FILTER.to_owned())))
    );
    assert_eq!(
        held_back(
            &format!("(pk = 'a' AND sk = a) OR pk = '{h}' OR pk = 'b' OR pk = 'c'"),
            &["c"]
        ),
        Ok((1, Some(HASH.to_owned())))
    );
}

#[test]
fn a_statement_whose_every_disjunct_is_denied_gets_the_last_refusal() {
    assert_eq!(
        held_back("pk IN ['c', 'b']", &["b", "c"]),
        Err("denied c".to_owned())
    );
    assert_eq!(
        held_back("pk IN ['a', 'b']", &[]),
        Ok((2, None)),
        "nothing is denied"
    );
}
