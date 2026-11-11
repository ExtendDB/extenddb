// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the authorization of PartiQL statements: what each
//! statement is authorized for, and the decision for each disjunct of a read
//! by key.

use std::sync::Mutex;

use serde_json::{Value, json};

use extenddb_core::partiql::parse;

use super::*;
use crate::ItemAuthorization;
use crate::partiql::test_context::block_on;

const KEYS: KeyNames<'static> = KeyNames {
    partition: "pk",
    sort: Some("sk"),
};

const INDEX_KEYS: KeyNames<'static> = KeyNames {
    partition: "g",
    sort: None,
};

/// The key attributes of the table the statements name.
const TABLE_KEYS: [&str; 2] = ["pk", "sk"];

fn s(value: &str) -> AttributeValue {
    AttributeValue::S(value.to_owned())
}

fn n(value: &str) -> AttributeValue {
    AttributeValue::N(value.to_owned())
}

fn strings(names: &[&str]) -> Option<Vec<String>> {
    Some(names.iter().map(|n| (*n).to_owned()).collect())
}

/// A request for `sql` with the wire-form parameters `params`.
fn request(sql: &str, params: Value) -> SingleRequest {
    SingleRequest::from_body(&json!({ "Statement": sql, "Parameters": params })).unwrap()
}

/// What `sql`, sent with `params` in a request of `surface`, is authorized
/// for on a table keyed on `table_keys` (`None` for a missing table) whose
/// target has the key names `keys`.
fn access_on(
    sql: &str,
    params: Value,
    table_keys: Option<&[&str]>,
    keys: Option<KeyNames<'_>>,
    surface: Surface,
) -> ItemAccess {
    let req = request(sql, params);
    let stmt = parse(sql).unwrap();
    let value_of = |op: &Operand| bound_value(op, &req.parameters).cloned();
    let analysis = match (where_clause(&stmt), keys) {
        (Some(where_clause), Some(keys)) => Some(WhereAnalysis::new(where_clause, keys, &value_of)),
        _ => None,
    };
    let target = Target {
        table_keys: table_keys.map(<[&str]>::to_vec),
        keys,
    };
    access(&stmt, &target, analysis.as_ref(), &req, surface)
}

/// What `sql`, sent with `params`, is authorized for in an
/// `ExecuteStatement` on the pk/sk table.
fn access_with(sql: &str, params: Value) -> ItemAccess {
    access_on(
        sql,
        params,
        Some(&TABLE_KEYS),
        Some(KEYS),
        Surface::Statement,
    )
}

/// What `sql` is authorized for in an `ExecuteStatement` on the pk/sk
/// table.
fn access_of(sql: &str) -> ItemAccess {
    access_with(sql, json!([]))
}

#[test]
fn a_scan_is_a_full_table_scan_and_names_the_table_key() {
    // Measured 2026-11-03: `SELECT *` with no WHERE is refused under `Null`
    // "true" on dynamodb:Attributes, so the key attributes are in the set.
    assert_eq!(
        access_of("SELECT * FROM \"t\""),
        ItemAccess {
            action: "PartiQLSelect",
            table: "t".to_owned(),
            attributes: strings(&["pk", "sk"]),
            select: Some("ALL_ATTRIBUTES"),
            full_table_scan: Some(true),
            ..ItemAccess::default()
        }
    );
    let filtered = access_of("SELECT * FROM \"t\" WHERE a = '1'");
    assert_eq!(filtered.full_table_scan, Some(true));
    assert_eq!(filtered.partition_keys, None);
    assert_eq!(filtered.attributes, strings(&["pk", "sk", "a"]));
}

#[test]
fn the_table_key_is_named_whether_or_not_the_statement_names_it() {
    // Measured 2026-11-03: `SELECT pk ... WHERE pk = 'a'` is refused under
    // ForAllValues [pk, q] and `SELECT pk ... WHERE a = '1'` under [pk, a].
    assert_eq!(
        access_of("SELECT pk FROM \"t\" WHERE pk = 'a'").attributes,
        strings(&["pk", "sk"])
    );
    assert_eq!(
        access_of("SELECT pk FROM \"t\" WHERE a = '1'").attributes,
        strings(&["pk", "sk", "a"])
    );
    // A nested path counts by its top-level name.
    assert_eq!(
        access_of("SELECT m.x FROM \"t\" WHERE pk = 'a' ORDER BY sk DESC").attributes,
        strings(&["pk", "sk", "m"])
    );
}

#[test]
fn a_read_by_key_carries_its_partition_values_and_no_full_table_scan() {
    let access = access_of("SELECT pk, a FROM \"t\" WHERE pk = 'a' AND sk = 'x' AND g = 'ga'");
    assert_eq!(access.full_table_scan, Some(false));
    assert_eq!(access.partition_keys, Some(vec![s("a")]));
    assert_eq!(access.attributes, strings(&["pk", "sk", "a", "g"]));
    assert_eq!(access.select, Some("SPECIFIC_ATTRIBUTES"));
    let disjuncts = access_of("SELECT * FROM \"t\" WHERE pk IN ['a', 'b', 'a']");
    assert_eq!(disjuncts.partition_keys, Some(vec![s("a"), s("b")]));
}

#[test]
fn a_parameter_gives_its_value_and_a_missing_one_leaves_the_key_absent() {
    let sql = "SELECT * FROM \"t\" WHERE pk = ?";
    let bound = access_with(sql, json!([{ "S": "a" }]));
    assert_eq!(bound.partition_keys, Some(vec![s("a")]));
    let unbound = access_of(sql);
    assert_eq!(unbound.partition_keys, None);
}

#[test]
fn a_number_partition_value_is_given_as_the_request_writes_it() {
    // Measured 2026-11-03 under ForAllValues LeadingKeys ["10"]: `pk = 10`
    // and `pk = 010` run, and `pk = 10.0`, `pk = 1E1` and the parameter
    // {"N": "10.0"} are refused. The server writes the text as Java's
    // BigDecimal prints it, so the text reaches it as written.
    for literal in ["10", "010", "10.0", "1E1", "-10"] {
        let sql = format!("SELECT * FROM \"t\" WHERE pk = {literal}");
        assert_eq!(
            access_of(&sql).partition_keys,
            Some(vec![n(literal)]),
            "{literal}"
        );
    }
    let sql = "SELECT * FROM \"t\" WHERE pk = ?";
    // The parameters a statement runs with hold the number normalized.
    assert_eq!(request(sql, json!([{ "N": "10.0" }])).parameters, [n("10")]);
    assert_eq!(
        access_with(sql, json!([{ "N": "10.0" }])).partition_keys,
        Some(vec![n("10.0")])
    );
    // The same in every statement that names a partition value.
    for (sql, params) in [
        (
            "UPDATE \"t\" SET q = 1 WHERE pk = ? AND sk = 'x'",
            json!([{ "N": "10.0" }]),
        ),
        ("DELETE FROM \"t\" WHERE pk = 10.0 AND sk = 'x'", json!([])),
        (
            "INSERT INTO \"t\" VALUE {'pk': ?, 'sk': 'x'}",
            json!([{ "N": "10.0" }]),
        ),
        ("INSERT INTO \"t\" VALUE {'pk': 10.0, 'sk': 'x'}", json!([])),
    ] {
        assert_eq!(
            access_with(sql, params).partition_keys,
            Some(vec![n("10.0")]),
            "{sql}"
        );
    }
}

#[test]
fn an_index_read_names_the_index_and_reads_its_key() {
    let on_index = |sql: &str| {
        access_on(
            sql,
            json!([]),
            Some(&TABLE_KEYS),
            Some(INDEX_KEYS),
            Surface::Statement,
        )
    };
    let access = on_index("SELECT * FROM \"t\".\"gix\" WHERE g = 'ga'");
    assert_eq!(access.index.as_deref(), Some("gix"));
    assert_eq!(access.partition_keys, Some(vec![s("ga")]));
    assert_eq!(access.full_table_scan, Some(false));
    assert_eq!(access.select, Some("ALL_PROJECTED_ATTRIBUTES"));
    assert_eq!(access.attributes, strings(&["pk", "sk", "g"]));
    // The index's key attributes are in the set only when the statement
    // names them (not measured).
    let scan = on_index("SELECT pk FROM \"t\".\"gix\"");
    assert_eq!(scan.full_table_scan, Some(true));
    assert_eq!(scan.select, Some("SPECIFIC_ATTRIBUTES"));
    assert_eq!(scan.attributes, strings(&["pk", "sk"]));
}

#[test]
fn an_index_read_of_a_missing_table_is_authorized_against_the_table() {
    // Measured 2026-11-03: the refusal names the table's ARN for a missing
    // table, and the index's for an existing table's missing index. Neither
    // carries FullTableScan.
    let sql = "SELECT * FROM \"t\".\"nix\" WHERE g = 'ga'";
    let missing_table = access_on(sql, json!([]), None, None, Surface::Statement);
    assert_eq!(missing_table.index, None);
    assert_eq!(missing_table.partition_keys, None);
    assert_eq!(missing_table.full_table_scan, None);
    assert_eq!(missing_table.attributes, strings(&["g"]));
    let missing_index = access_on(sql, json!([]), Some(&TABLE_KEYS), None, Surface::Statement);
    assert_eq!(missing_index.index.as_deref(), Some("nix"));
    assert_eq!(missing_index.partition_keys, None);
    assert_eq!(missing_index.full_table_scan, None);
    assert_eq!(missing_index.attributes, strings(&["pk", "sk", "g"]));
}

#[test]
fn batch_and_transaction_reads_carry_their_operation_and_no_full_table_scan() {
    let sql = "SELECT * FROM \"t\" WHERE pk = 'a' AND sk = 'x'";
    let on = |surface| access_on(sql, json!([]), Some(&TABLE_KEYS), Some(KEYS), surface);
    let batch = on(Surface::Batch);
    assert_eq!(batch.enclosing_operation, Some("BatchExecuteStatement"));
    assert_eq!(batch.full_table_scan, None);
    assert_eq!(batch.partition_keys, Some(vec![s("a")]));
    let transaction = on(Surface::Transaction);
    assert_eq!(transaction.enclosing_operation, Some("ExecuteTransaction"));
    assert_eq!(transaction.full_table_scan, None);
    assert_eq!(on(Surface::Statement).enclosing_operation, None);
}

#[test]
fn an_exists_check_is_authorized_as_its_select() {
    // Measured 2026-11-03 (X3 to X7).
    let access = access_on(
        "EXISTS(SELECT * FROM \"t\" WHERE pk = 'a' AND sk = 'x' AND a = '1')",
        json!([]),
        Some(&TABLE_KEYS),
        Some(KEYS),
        Surface::Transaction,
    );
    assert_eq!(access.action, "PartiQLSelect");
    assert_eq!(access.index, None);
    assert_eq!(access.partition_keys, Some(vec![s("a")]));
    assert_eq!(access.attributes, strings(&["pk", "sk", "a"]));
    assert_eq!(access.enclosing_operation, Some("ExecuteTransaction"));
}

#[test]
fn an_insert_carries_its_item_partition_key_and_names() {
    let access = access_with(
        "INSERT INTO \"t\" VALUE {'pk': ?, 'sk': 's', 'g': 'gz'}",
        json!([{ "S": "b" }]),
    );
    assert_eq!(
        access,
        ItemAccess {
            action: "PartiQLInsert",
            table: "t".to_owned(),
            partition_keys: Some(vec![s("b")]),
            attributes: strings(&["pk", "sk", "g"]),
            return_values: Some("NONE"),
            ..ItemAccess::default()
        }
    );
    let keyless = access_of("INSERT INTO \"t\" VALUE {'sk': 's'}");
    assert_eq!(keyless.partition_keys, None);
    assert_eq!(keyless.attributes, strings(&["pk", "sk"]));
}

#[test]
fn an_update_carries_the_partition_key_of_its_where_and_every_name() {
    let access = access_on(
        "UPDATE \"t\" SET q = a + 1 REMOVE r WHERE 'a' = pk AND sk = 'x' AND g = 'ga' \
         RETURNING MODIFIED OLD *",
        json!([]),
        Some(&TABLE_KEYS),
        Some(KEYS),
        Surface::Batch,
    );
    assert_eq!(access.action, "PartiQLUpdate");
    assert_eq!(access.partition_keys, Some(vec![s("a")]));
    assert_eq!(
        access.attributes,
        strings(&["pk", "sk", "q", "a", "r", "g"])
    );
    assert_eq!(access.return_values, Some("UPDATED_OLD"));
    assert_eq!(access.full_table_scan, None);
    assert_eq!(access.enclosing_operation, Some("BatchExecuteStatement"));
    // Measured 2026-11-03: `SET q = g` is refused under ForAllValues [pk,
    // sk, q].
    assert_eq!(
        access_of("UPDATE \"t\" SET q = g WHERE pk = 'a' AND sk = 'x'").attributes,
        strings(&["pk", "sk", "q", "g"])
    );
    // Without a partition key equality the key is absent.
    let partial = access_of("UPDATE \"t\" SET q = 1 WHERE sk = 'x'");
    assert_eq!(partial.partition_keys, None);
}

#[test]
fn every_returning_mode_has_its_return_values() {
    for (mode, value) in [
        ("ALL OLD", "ALL_OLD"),
        ("MODIFIED OLD", "UPDATED_OLD"),
        ("ALL NEW", "ALL_NEW"),
        ("MODIFIED NEW", "UPDATED_NEW"),
    ] {
        let sql = format!("UPDATE \"t\" SET q = 1 WHERE pk = 'a' AND sk = 'x' RETURNING {mode} *");
        assert_eq!(access_of(&sql).return_values, Some(value), "{mode}");
    }
    let delete = access_of("DELETE FROM \"t\" WHERE pk = 'b' AND sk = 'x' RETURNING ALL OLD *");
    assert_eq!(delete.action, "PartiQLDelete");
    assert_eq!(delete.partition_keys, Some(vec![s("b")]));
    assert_eq!(delete.attributes, strings(&["pk", "sk"]));
    assert_eq!(delete.return_values, Some("ALL_OLD"));
}

#[test]
fn a_write_without_returning_returns_none_in_every_operation() {
    // Measured 2026-11-03: an Allow on StringEquals "NONE" runs an INSERT,
    // an UPDATE and a DELETE without RETURNING and one on Null "true"
    // refuses them, in all three operations; an Allow on Bool "false" for
    // dynamodb:FullTableScan refuses every write.
    for sql in [
        "INSERT INTO \"t\" VALUE {'pk': 'a', 'sk': 'x'}",
        "UPDATE \"t\" SET q = 1 WHERE pk = 'a' AND sk = 'x'",
        "DELETE FROM \"t\" WHERE pk = 'a' AND sk = 'x'",
    ] {
        for surface in [Surface::Statement, Surface::Batch, Surface::Transaction] {
            let access = access_on(sql, json!([]), Some(&TABLE_KEYS), Some(KEYS), surface);
            assert_eq!(access.return_values, Some("NONE"), "{sql} {surface:?}");
            assert_eq!(access.full_table_scan, None, "{sql} {surface:?}");
        }
    }
    // A read carries none.
    assert_eq!(
        access_of("SELECT * FROM \"t\" WHERE pk = 'a'").return_values,
        None
    );
}

#[test]
fn a_write_with_two_partition_values_carries_both() {
    let access = access_of("DELETE FROM \"t\" WHERE pk = 'a' AND pk = 'b' AND sk = 'x'");
    assert_eq!(access.partition_keys, Some(vec![s("a"), s("b")]));
}

#[test]
fn a_multi_attribute_key_has_no_key_names() {
    use extenddb_core::types::KeyType;
    let key = |name: &str, key_type| KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    };
    let single = [key("pk", KeyType::Hash), key("sk", KeyType::Range)];
    let names = single_attribute_keys(&single).unwrap();
    assert_eq!((names.partition, names.sort), ("pk", Some("sk")));
    let multi = [key("a", KeyType::Hash), key("b", KeyType::Hash)];
    assert!(single_attribute_keys(&multi).is_none());
}

// --- the decisions -----------------------------------------------------

/// Allows every access but those whose partition keys include one of
/// `refused`, and records every access it is asked about.
struct Fence {
    refused: Vec<AttributeValue>,
    asked: Mutex<Vec<ItemAccess>>,
}

impl Fence {
    fn refusing(values: &[&str]) -> Self {
        Self {
            refused: values.iter().map(|value| s(value)).collect(),
            asked: Mutex::new(Vec::new()),
        }
    }

    fn asked(&self) -> Vec<ItemAccess> {
        self.asked.lock().unwrap().clone()
    }
}

impl ItemAuthorizer for Fence {
    fn authorize<'a>(&'a self, access: &'a ItemAccess) -> ItemAuthorization<'a> {
        Box::pin(async move {
            self.asked.lock().unwrap().push(access.clone());
            match &access.partition_keys {
                Some(values) if values.iter().any(|value| self.refused.contains(value)) => Err(
                    DynamoDbError::AccessDeniedException(format!("refused {values:?}")),
                ),
                _ => Ok(()),
            }
        })
    }
}

/// Refuses every access, naming its partition values.
struct RefuseAll;

impl ItemAuthorizer for RefuseAll {
    fn authorize<'a>(&'a self, access: &'a ItemAccess) -> ItemAuthorization<'a> {
        Box::pin(async move {
            Err(DynamoDbError::AccessDeniedException(format!(
                "refused {:?}",
                access.partition_keys
            )))
        })
    }
}

/// Fails every access with an internal error.
struct Broken;

impl ItemAuthorizer for Broken {
    fn authorize<'a>(&'a self, _access: &'a ItemAccess) -> ItemAuthorization<'a> {
        Box::pin(async {
            Err(DynamoDbError::InternalServerError(
                "Internal error during authorization".to_owned(),
            ))
        })
    }
}

/// Decide each disjunct of `sql`, a read by key of the pk/sk table, with
/// `authorizer`: the decision, and the refusal of each disjunct in read
/// order when the statement is not refused.
fn decide(
    sql: &str,
    authorizer: Option<&dyn ItemAuthorizer>,
) -> (Result<Refused, DynamoDbError>, Vec<Option<DynamoDbError>>) {
    let req = request(sql, json!([]));
    let stmt = parse(sql).unwrap();
    let value_of = |op: &Operand| bound_value(op, &req.parameters).cloned();
    let analysis = WhereAnalysis::new(where_clause(&stmt).unwrap(), KEYS, &value_of);
    assert!(analysis.is_keyed(), "{sql}");
    let target = Target {
        table_keys: Some(TABLE_KEYS.to_vec()),
        keys: Some(KEYS),
    };
    let base = access(&stmt, &target, Some(&analysis), &req, Surface::Statement);
    let result = block_on(each_disjunct(authorizer, &base, &analysis, &req, &value_of));
    let per_read = match &result {
        Ok(refused) => analysis
            .reads()
            .iter()
            .map(|read| refused.refusal(read, &req))
            .collect(),
        Err(_) => Vec::new(),
    };
    (result, per_read)
}

fn refused_message(result: &Result<Refused, DynamoDbError>) -> Option<&str> {
    match result {
        Err(DynamoDbError::AccessDeniedException(message)) => Some(message),
        _ => None,
    }
}

#[test]
fn each_disjunct_is_authorized_with_its_own_partition() {
    let fence = Fence::refusing(&["b"]);
    let (result, per_read) = decide("SELECT * FROM \"t\" WHERE pk IN ['a', 'b']", Some(&fence));
    assert!(result.is_ok());
    assert!(per_read[0].is_none(), "a is allowed");
    assert!(
        matches!(per_read[1], Some(DynamoDbError::AccessDeniedException(_))),
        "b is held back for its refusal"
    );
    let asked = fence.asked();
    assert_eq!(asked.len(), 2);
    assert_eq!(asked[0].partition_keys, Some(vec![s("a")]));
    assert_eq!(asked[1].partition_keys, Some(vec![s("b")]));
    for access in &asked {
        assert_eq!(access.full_table_scan, Some(false));
        assert_eq!(access.action, "PartiQLSelect");
        assert_eq!(access.attributes, strings(&["pk", "sk"]));
    }
}

#[test]
fn a_partition_is_authorized_once() {
    let fence = Fence::refusing(&[]);
    let (result, _) = decide(
        "SELECT * FROM \"t\" WHERE (pk = 'a' AND sk = 'x') OR (pk = 'a' AND sk = 'y')",
        Some(&fence),
    );
    assert!(result.is_ok());
    assert_eq!(fence.asked().len(), 1);
}

#[test]
fn a_statement_whose_every_disjunct_is_refused_is_refused() {
    let fence = Fence::refusing(&["b"]);
    let (result, _) = decide("SELECT * FROM \"t\" WHERE pk = 'b'", Some(&fence));
    assert!(refused_message(&result).is_some());
}

#[test]
fn of_several_refused_disjuncts_the_one_read_last_is_reported() {
    let (result, _) = decide(
        "SELECT * FROM \"t\" WHERE pk IN ['c', 'a', 'b']",
        Some(&RefuseAll),
    );
    let want = format!("refused {:?}", Some(vec![s("c")]));
    assert_eq!(refused_message(&result), Some(want.as_str()));
}

/// A read by key of `n` partitions, v00 to v(n-1), in key order.
fn partitions(n: usize) -> String {
    let values: Vec<String> = (0..n).map(|i| format!("'v{i:02}'")).collect();
    format!("SELECT * FROM \"t\" WHERE pk IN [{}]", values.join(", "))
}

#[test]
fn at_most_50_partitions_are_authorized_and_the_others_are_refused() {
    // A statement with more than 50 disjuncts is refused whatever the
    // decision, so the partitions past the 50th are not asked about, and
    // are refused if they are ever read.
    let fence = Fence::refusing(&[]);
    let (result, per_read) = decide(&partitions(60), Some(&fence));
    assert!(result.is_ok());
    assert_eq!(fence.asked().len(), MAX_DECOMPOSED_READS);
    assert_eq!(per_read.len(), 60);
    assert!(per_read[..50].iter().all(Option::is_none));
    assert!(
        per_read[50..]
            .iter()
            .all(|refusal| matches!(refusal, Some(DynamoDbError::AccessDeniedException(_))))
    );
    // When the 50 asked about are refused, the statement gets the refusal
    // of the last of them in read order.
    let (result, _) = decide(&partitions(60), Some(&RefuseAll));
    let want = format!("refused {:?}", Some(vec![s("v49")]));
    assert_eq!(refused_message(&result), Some(want.as_str()));
}

#[test]
fn a_statement_authorized_as_a_whole_refuses_no_disjunct() {
    let sql = "SELECT * FROM \"t\" WHERE pk IN ['a', 'b']";
    let req = request(sql, json!([]));
    let stmt = parse(sql).unwrap();
    let value_of = |op: &Operand| bound_value(op, &req.parameters).cloned();
    let analysis = WhereAnalysis::new(where_clause(&stmt).unwrap(), KEYS, &value_of);
    let whole = Refused::default();
    for read in analysis.reads() {
        assert!(whole.refusal(read, &req).is_none());
    }
}

#[test]
fn an_authorizer_fault_fails_the_statement() {
    let (result, _) = decide("SELECT * FROM \"t\" WHERE pk IN ['a', 'b']", Some(&Broken));
    assert!(matches!(result, Err(DynamoDbError::InternalServerError(_))));
}

#[test]
fn without_an_authorizer_every_statement_is_refused() {
    let scan = access_of("SELECT * FROM \"t\"");
    let result = block_on(authorize_access(None, &scan));
    assert!(matches!(
        result,
        Err(DynamoDbError::AccessDeniedException(_))
    ));
    // A read by key is refused disjunct by disjunct, so as a whole.
    let (result, _) = decide("SELECT * FROM \"t\" WHERE pk IN ['a', 'b']", None);
    assert!(refused_message(&result).is_some());
}
