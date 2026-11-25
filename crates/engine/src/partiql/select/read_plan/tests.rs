// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the WHERE checks and the read plan of a SELECT.

use super::super::tests::{HASH_ONLY, KEYS, select};
use super::*;

mod decomposed;
mod held_back;
mod key_order;

/// The checks a WHERE of a table read without an ORDER BY gets before
/// anything is read, in the order `run` makes them.
fn check_where<'a, 'b>(
    analysis: &'b WhereAnalysis<'a>,
    keys: KeyNames<'_>,
    target_schema: &[KeySchemaElement],
    key_info: &TableKeyInfo,
    limits: &LimitsConfig,
    params: &[AttributeValue],
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> Result<Checked<'a, 'b>, DynamoDbError> {
    check_keys(Some(analysis), None, keys, key_info, true, params, value_of)?;
    check_reads(
        analysis,
        keys,
        target_schema,
        limits,
        params,
        value_of,
        &|_| None,
    )
}

/// A pk/sk table whose sort key has type `sort`.
fn table(sort: extenddb_core::types::ScalarAttributeType) -> TableKeyInfo {
    use extenddb_core::types::{AttributeDefinition, ScalarAttributeType};
    let key = |name: &str, key_type| KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    };
    let definition = |name: &str, attribute_type| AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type,
    };
    TableKeyInfo {
        table_name: "t".to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: vec![
            definition("pk", ScalarAttributeType::S),
            definition("sk", sort),
        ],
        ..TableKeyInfo::default()
    }
}

/// The refusal `check_where` gives a statement on a table whose sort key
/// has type `sort`, or `None`.
fn answer(
    sort: extenddb_core::types::ScalarAttributeType,
    condition: &str,
    params: &[AttributeValue],
) -> Option<String> {
    let s = select(&format!("SELECT * FROM \"t\" WHERE {condition}"));
    let info = table(sort);
    let value_of = |op: &Operand| bound_value(op, params).cloned();
    let analysis = WhereAnalysis::new(s.where_clause.as_ref().unwrap(), KEYS, &value_of);
    check_where(
        &analysis,
        KEYS,
        &info.key_schema,
        &info,
        &LimitsConfig::default(),
        params,
        &value_of,
    )
    .err()
    .map(|e| e.message().to_owned())
}

fn numbered(condition: &str, params: &[AttributeValue]) -> Option<String> {
    answer(
        extenddb_core::types::ScalarAttributeType::N,
        condition,
        params,
    )
}

fn key_type(name: &str) -> Option<String> {
    Some(format!(
        "Key attribute's data type should match its data type in table's schema: Key {name}"
    ))
}

#[test]
fn each_disjunct_read_by_key_has_its_values_checked_in_order() {
    for (condition, name) in [
        ("pk = 1", "pk"),
        ("1 = pk", "pk"),
        ("pk = 'a' AND sk = 'b'", "sk"),
        ("pk = 'a' AND 'b' = sk", "sk"),
        ("pk = 'a' AND 'b' < sk", "sk"),
        ("pk = 'a' AND sk BETWEEN 1 AND 'z'", "sk"),
        ("pk = 'a' AND begins_with(sk, 'x')", "sk"),
        ("pk IN [1, 2]", "pk"),
        ("pk = 1 OR a = 'x'", "pk"),
        ("pk = 'a' OR pk = 1 OR a = 1", "pk"),
        ("(pk = 1 AND sk = 1) OR (pk = 'b' AND sk = 'x')", "pk"),
        ("(pk = 'b' AND sk = 'x') OR (pk = 1 AND sk = 1)", "sk"),
        // The sort key value is checked before the partition key value.
        ("pk = 1 AND sk = 'b'", "sk"),
        ("sk = 'b' AND pk = 1", "sk"),
        ("pk IN [1, 'b'] AND sk = 'x'", "sk"),
        // Key values come before the overlap refusal.
        ("pk = 'a' AND (a = 1 OR sk = 'x')", "sk"),
    ] {
        assert_eq!(numbered(condition, &[]), key_type(name), "{condition}");
    }
    assert_eq!(
        numbered("pk = ?", &[AttributeValue::N("1".to_owned())]),
        key_type("pk")
    );
}

#[test]
fn values_outside_a_disjunct_read_by_key_are_not_checked() {
    for condition in [
        "pk = 'a' AND sk > 1",
        "sk = 'b'",
        "sk > 'b' AND a = 1",
        "NOT pk = 1",
        "pk <> 1",
        "pk = 'a' AND sk <> 'b'",
        "pk = 'a' AND NOT sk = 'b'",
        "pk = 'a' AND sk = 'b' AND sk = 'c'",
        "pk = 'a' AND sk = 1 AND contains(sk, 'x')",
        "pk = 1 AND pk = 'a'",
        "a = 1 OR pk = 1",
        "a = 'x' OR (b = 1 AND pk = 1)",
        "pk = 'a' OR sk = 'b'",
        "pk = 'a' AND (pk = 1 OR a = 1)",
    ] {
        assert_eq!(numbered(condition, &[]), None, "{condition}");
    }
    // A parameter the request does not supply is left to the count check.
    assert_eq!(numbered("pk = ?", &[]), None);
}

#[test]
fn a_begins_with_key_condition_takes_a_prefix_of_the_key_type() {
    use extenddb_core::types::ScalarAttributeType::{B, N, S};
    let operand = |label: &str| {
        Some(format!(
            "Incorrect operand type for operator or function; operator or function: \
             begins_with, operand type: {label}"
        ))
    };
    let binary = AttributeValue::B(b"x".to_vec());
    let string = AttributeValue::S("x".to_owned());
    for (sort, prefix, want) in [
        (S, AttributeValue::N("1".to_owned()), operand("N")),
        (S, AttributeValue::Bool(true), operand("BOOL")),
        (N, AttributeValue::N("1".to_owned()), operand("N")),
        (S, binary.clone(), key_type("sk")),
        (N, string.clone(), key_type("sk")),
        (N, binary.clone(), key_type("sk")),
        (B, string.clone(), key_type("sk")),
        (S, string, None),
        (B, binary, None),
    ] {
        let params = [prefix];
        assert_eq!(
            answer(sort, "pk = 'a' AND begins_with(sk, ?)", &params),
            want,
            "{sort:?} {:?}",
            params[0]
        );
    }
}

#[test]
fn a_read_of_one_disjunct_has_its_key_sizes_checked() {
    use extenddb_core::types::ScalarAttributeType::S;
    let hash = Some(
        "One or more parameter values were invalid: Size of hashkey has exceeded the \
         maximum size limit of2048 bytes"
            .to_owned(),
    );
    let range = Some(
        "One or more parameter values were invalid: Aggregated size of all range keys has \
         exceeded the size limit of 1024 bytes"
            .to_owned(),
    );
    let (h_over, h_max) = ("a".repeat(2049), "a".repeat(2048));
    let (r_over, r_max) = ("a".repeat(1025), "a".repeat(1024));
    for (condition, want) in [
        (format!("pk = '{h_over}'"), hash.clone()),
        (format!("pk = '{}'", "\u{e9}".repeat(1025)), hash.clone()),
        (format!("pk = '{h_max}'"), None),
        (format!("pk = 'a' AND sk = '{r_over}'"), range.clone()),
        (format!("pk = 'a' AND sk > '{r_over}'"), range.clone()),
        (
            format!("pk = 'a' AND sk BETWEEN 'a' AND '{r_over}'"),
            range.clone(),
        ),
        (
            format!("pk = 'a' AND begins_with(sk, '{r_over}')"),
            range.clone(),
        ),
        (format!("pk = 'a' AND sk = '{r_max}'"), None),
        (format!("pk = '{h_over}' AND sk = '{r_over}'"), hash.clone()),
        // Every disjunct fails, so the statement is refused at once.
        (format!("pk IN ['{h_over}']"), hash.clone()),
        (format!("pk = '{h_over}' OR pk = 'b{h_over}'"), hash.clone()),
        // A disjunct that is fine leaves the others for later, and a
        // scan is not checked.
        (format!("pk IN ['a', '{h_over}']"), None),
        (format!("pk = 'a' OR pk = '{h_over}'"), None),
        (format!("pk > '{h_over}'"), None),
        (format!("sk = '{r_over}'"), None),
    ] {
        let shown: String = condition.chars().take(40).collect();
        assert_eq!(answer(S, &condition, &[]), want, "{shown}");
    }
    let params = [AttributeValue::S(h_over)];
    assert_eq!(answer(S, "pk = ?", &params), hash);
}

#[test]
fn a_keyed_filter_with_a_key_attribute_is_refused_after_the_overlap_check() {
    use extenddb_core::types::ScalarAttributeType::S;
    assert_eq!(
        answer(S, "pk = 'a' AND sk = a", &[]),
        Some(
            "Filter Expression can only contain non-primary key attributes: Primary key \
             attribute: sk"
                .to_owned()
        )
    );
    assert_eq!(
        answer(S, "pk = 'a' AND (sk = a OR a = 'x')", &[]),
        Some("Overlapping conditions with range keys are not supported in where clause".to_owned())
    );
    // A key value of the wrong type is reported first.
    assert_eq!(
        numbered("pk = 'a' AND sk = 'x' AND sk = a", &[]),
        key_type("sk")
    );
    // In a scan the filter may name a key attribute.
    assert_eq!(answer(S, "sk = a", &[]), None);
    assert_eq!(answer(S, "pk = 'a' AND size(sk) = 1", &[]), None);
}

/// A table keyed on a string pk alone.
fn hash_table() -> TableKeyInfo {
    let mut info = table(extenddb_core::types::ScalarAttributeType::S);
    info.key_schema.truncate(1);
    info.attribute_definitions.truncate(1);
    info
}

/// How `check_where` leaves a statement on a string-keyed table: whether
/// it reads by key, how many disjuncts are read, and the refusal left
/// for later; or the refusal it gets at once.
fn deferral(condition: &str) -> Result<(bool, usize, Option<String>), String> {
    let s = select(&format!("SELECT * FROM \"t\" WHERE {condition}"));
    let info = table(extenddb_core::types::ScalarAttributeType::S);
    let value_of = |op: &Operand| bound_value(op, &[]).cloned();
    let analysis = WhereAnalysis::new(s.where_clause.as_ref().unwrap(), KEYS, &value_of);
    check_where(
        &analysis,
        KEYS,
        &info.key_schema,
        &info,
        &LimitsConfig::default(),
        &[],
        &value_of,
    )
    .map(|c| {
        (
            c.keyed,
            c.reads.len(),
            c.deferred.map(|e| e.message().to_owned()),
        )
    })
    .map_err(|e| e.message().to_owned())
}

#[test]
fn a_disjunct_that_fails_when_read_is_left_for_later() {
    let hash = "One or more parameter values were invalid: Size of hashkey has exceeded the \
                maximum size limit of2048 bytes";
    let range = "One or more parameter values were invalid: Aggregated size of all range \
                 keys has exceeded the size limit of 1024 bytes";
    let filter = "Filter Expression can only contain non-primary key attributes: Primary \
                  key attribute: sk";
    let scalar = "Key attributes must be scalars; list random access '[]' and map lookup \
                  '.' are not allowed: Key sk";
    let (h, r) = ("z".repeat(2049), "z".repeat(1025));
    let later = |reads: usize, message: &str| Ok((true, reads, Some(message.to_owned())));
    for (condition, want) in [
        (format!("pk IN ['a', '{h}']"), later(1, hash)),
        (format!("pk IN ['{h}', 'a']"), later(1, hash)),
        (
            format!("pk = 'a' OR pk = '{h}' OR pk = 'b'"),
            later(2, hash),
        ),
        (
            format!("(pk = 'a' AND sk = 'x') OR (pk = 'b' AND sk = '{r}')"),
            later(1, range),
        ),
        (format!("pk = 'a' AND sk IN ['x', '{r}']"), later(1, range)),
        (
            "(pk = 'a' AND sk = a) OR pk = 'b'".to_owned(),
            later(1, filter),
        ),
        (
            "pk = 'b' OR (pk = 'a' AND sk = a)".to_owned(),
            later(1, filter),
        ),
        (
            "(pk = 'a' AND sk = a) OR pk = 'b' OR pk = 'c'".to_owned(),
            later(2, filter),
        ),
        // A path into a key attribute is refused at once, ahead of the
        // overlap and key type refusals, and when a later disjunct makes
        // the statement a scan.
        (
            "(pk = 'a' AND sk.x = 1) OR pk = 'b'".to_owned(),
            Err(scalar.to_owned()),
        ),
        (
            "(pk = 'a' AND sk.x = 1) OR (pk = 'a' AND sk = 'y')".to_owned(),
            Err(scalar.to_owned()),
        ),
        (
            "(pk = 'a' AND sk = 'y') OR (pk = 'a' AND sk.x = 1)".to_owned(),
            Err(scalar.to_owned()),
        ),
        (
            "(pk = 'a' AND sk.x = 1) OR pk = 1".to_owned(),
            Err(scalar.to_owned()),
        ),
        (
            "pk = 1 OR (pk = 'a' AND sk.x = 1)".to_owned(),
            Err(scalar.to_owned()),
        ),
        (
            "(pk = 'a' AND sk.x = 1) OR x = 1".to_owned(),
            Err(scalar.to_owned()),
        ),
        // Of several failing disjuncts, the one read last is reported.
        (
            format!("(pk = 'a' AND sk = a) OR pk = '{h}' OR pk = 'b'"),
            later(1, hash),
        ),
        (
            format!("pk = '{h}' OR (pk = 'a' AND sk = a) OR pk = 'b'"),
            later(1, hash),
        ),
        (
            format!("(pk = 'b' AND sk = '{r}') OR pk = '{h}' OR pk = 'a'"),
            later(1, hash),
        ),
        (
            "(pk = 'b' AND sk = a) OR (pk = 'a' AND pk = b) OR pk = 'c'".to_owned(),
            later(1, filter),
        ),
        (
            "(pk = 'a' AND pk = b) OR (pk = 'b' AND sk = a) OR pk = 'c'".to_owned(),
            later(1, filter),
        ),
        // Every disjunct fails: refused at once.
        (
            "pk IN ['a', 'b'] AND sk = a".to_owned(),
            Err(filter.to_owned()),
        ),
        (
            "(pk = 'a' AND sk = a) OR (pk = 'b' AND sk = b)".to_owned(),
            Err(filter.to_owned()),
        ),
        // Nothing fails, or the statement is a scan.
        ("pk IN ['a', 'b']".to_owned(), Ok((true, 2, None))),
        (
            "(pk = 'a' AND sk = a) OR x = 1".to_owned(),
            Ok((false, 1, None)),
        ),
    ] {
        let shown: String = condition.chars().take(60).collect();
        assert_eq!(deferral(&condition), want, "{shown}");
    }
}

#[test]
fn the_read_follows_the_disjuncts() {
    let plan = |condition: &str, info: &TableKeyInfo, on_table: bool| {
        let keys = if info.key_schema.len() == 1 {
            HASH_ONLY
        } else {
            KEYS
        };
        let s = select(&format!("SELECT * FROM \"t\" WHERE {condition}"));
        let value_of = |op: &Operand| bound_value(op, &[]).cloned();
        let analysis = WhereAnalysis::new(s.where_clause.as_ref().unwrap(), keys, &value_of);
        let checked = check_where(
            &analysis,
            keys,
            &info.key_schema,
            info,
            &LimitsConfig::default(),
            &[],
            &value_of,
        )
        .unwrap();
        let text = |v: &AttributeValue| match v {
            AttributeValue::S(s) => s.clone(),
            other => format!("{other:?}"),
        };
        match ReadPlan::of(
            Some(&checked),
            keys,
            on_table,
            Direction::default(),
            &value_of,
        ) {
            ReadPlan::Key(read) => format!("key +{}", read.filter.len()),
            // A step that names one item shows its key; a Query shows its
            // partition and a star.
            ReadPlan::Steps(steps) => {
                let shown: Vec<String> = steps
                    .iter()
                    .map(|step| {
                        let partition = text(&value_of(step.read.partition).unwrap());
                        let shown = match (&step.point, keys.sort, &step.read.sort) {
                            (None, _, _) => format!("{partition}*"),
                            (
                                Some(_),
                                Some(_),
                                Some(SortCondition::Compare(CompareOp::Eq, sort)),
                            ) => format!("{partition}/{}", text(&value_of(sort).unwrap())),
                            (Some(_), _, _) => partition,
                        };
                        // A step held back for its refusal shows a mark.
                        if step.held_back {
                            format!("!{shown}")
                        } else {
                            shown
                        }
                    })
                    .collect();
                format!("steps {}", shown.join(" "))
            }
            ReadPlan::Scan => "scan".to_owned(),
        }
    };
    let both = table(extenddb_core::types::ScalarAttributeType::S);
    let hash = hash_table();
    for (condition, info, on_table, want) in [
        ("pk = 'a' AND sk = 'b'", &both, true, "steps a/b"),
        ("pk = 'a' AND sk = 'b'", &both, false, "key +0"),
        ("pk = 'a' AND sk = 'b' AND n = 1", &both, true, "steps a/b"),
        ("pk = 'a' AND name = 'x'", &both, true, "key +1"),
        ("pk = 'a' AND (x = 1 OR y = 2)", &both, true, "key +1"),
        ("pk = 'a' AND sk > 'b'", &both, true, "key +0"),
        (
            "pk = 'a' AND sk IN ['y', 'x']",
            &both,
            true,
            "steps a/x a/y",
        ),
        // An index is read by one Query for each disjunct.
        ("pk = 'a' AND sk IN ['x', 'y']", &both, false, "steps a* a*"),
        (
            "pk = 'a' AND sk = 'x' OR 'a' = pk AND sk > 'y'",
            &both,
            true,
            "steps a/x a*",
        ),
        (
            "pk = 'a' AND sk > 'y' OR 'a' = pk AND sk < 'x'",
            &both,
            true,
            "steps a* a*",
        ),
        (
            "(pk = 'b' AND sk = 'x') OR (pk = 'a' AND sk = 'x')",
            &both,
            true,
            "steps a/x b/x",
        ),
        ("pk IN ['b', 'a']", &both, true, "steps a* b*"),
        (
            "pk = 'b' OR (pk = 'a' AND sk = 'x')",
            &both,
            true,
            "steps a/x b*",
        ),
        ("pk IN ['a', 'b']", &both, false, "steps a* b*"),
        (
            "(pk = 'a' AND sk = a) OR pk = 'b'",
            &both,
            false,
            "steps !a* b*",
        ),
        ("pk = 'a' OR sk = 'b'", &both, true, "scan"),
        ("pk = 'a' AND sk <> 'b'", &both, true, "scan"),
        ("pk = \"a\"", &both, true, "scan"),
        ("pk IN ['c', 'a', 'b']", &hash, true, "steps a b c"),
        (
            "pk = 'c' OR pk = 'a' OR pk = 'zz'",
            &hash,
            true,
            "steps a c zz",
        ),
        ("pk = 'a'", &hash, true, "steps a"),
        // The disjuncts held back for a refusal take their places among
        // the others and read nothing.
        (
            "(pk = 'a' AND sk = a) OR pk = 'b'",
            &both,
            true,
            "steps !a* b*",
        ),
        (
            "(pk = 'a' AND sk = a) OR (pk = 'b' AND sk = 'x')",
            &both,
            true,
            "steps !a* b/x",
        ),
        (
            "(pk = 'c' AND sk = a) OR pk = 'b' OR pk = 'a'",
            &both,
            true,
            "steps a* b* !c*",
        ),
    ] {
        assert_eq!(plan(condition, info, on_table), want, "{condition}");
    }
}

#[test]
fn disjuncts_are_read_in_key_order_or_its_reverse() {
    let s = select("SELECT * FROM \"t\" WHERE pk IN [10, 9, 100]");
    let value_of = |op: &Operand| bound_value(op, &[]).cloned();
    let analysis = WhereAnalysis::new(s.where_clause.as_ref().unwrap(), HASH_ONLY, &value_of);
    let reads: Vec<&KeyRead<'_>> = analysis.reads().iter().collect();
    let order = |partitions_descending| {
        let direction = Direction {
            partitions_descending,
            items_descending: false,
        };
        Step::all(&reads, &[], Some(HASH_ONLY), direction, &value_of)
            .into_iter()
            .map(|step| step.point.unwrap())
            .collect::<Vec<_>>()
    };
    let n = |v: &str| serde_json::json!({ "pk": { "N": v } });
    assert_eq!(order(false), vec![n("9"), n("10"), n("100")]);
    assert_eq!(order(true), vec![n("100"), n("10"), n("9")]);
}

#[test]
fn partitions_and_the_items_of_a_partition_take_their_own_directions() {
    let s = select("SELECT * FROM \"t\" WHERE pk IN ['b', 'a'] AND sk IN ['y', 'x']");
    let value_of = |op: &Operand| bound_value(op, &[]).cloned();
    let analysis = WhereAnalysis::new(s.where_clause.as_ref().unwrap(), KEYS, &value_of);
    let reads: Vec<&KeyRead<'_>> = analysis.reads().iter().collect();
    let order = |partitions_descending, items_descending| {
        let direction = Direction {
            partitions_descending,
            items_descending,
        };
        Step::all(&reads, &[], Some(KEYS), direction, &value_of)
            .into_iter()
            .map(|step| {
                let key = step.point.unwrap();
                format!(
                    "{}/{}",
                    key["pk"]["S"].as_str().unwrap(),
                    key["sk"]["S"].as_str().unwrap()
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(order(false, false), ["a/x", "a/y", "b/x", "b/y"]);
    assert_eq!(order(true, false), ["b/x", "b/y", "a/x", "a/y"]);
    assert_eq!(order(false, true), ["a/y", "a/x", "b/y", "b/x"]);
    assert_eq!(order(true, true), ["b/y", "b/x", "a/y", "a/x"]);
}
