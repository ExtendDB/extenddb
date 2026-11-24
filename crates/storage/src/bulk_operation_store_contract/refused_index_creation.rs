// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! An index creation that `UpdateTable` refuses does not refuse a start
//! that crossed it, and one it accepts does. A start is refused when an
//! index build on its table was accepted between its call and its
//! transaction; a request refused before it created anything started no
//! build.

use extenddb_core::types::{DescribeTableInput, UpdateTableInput};

use super::*;

fn update_request(body: serde_json::Value) -> UpdateTableInput {
    serde_json::from_value(body).expect("a valid UpdateTable request")
}

/// Creates the global secondary index `index` keyed on `g`.
fn create_gsi(table_name: &str, index: &str) -> serde_json::Value {
    json!({
        "TableName": table_name,
        "AttributeDefinitions": [
            { "AttributeName": "pk", "AttributeType": "S" },
            { "AttributeName": "g", "AttributeType": "S" }
        ],
        "GlobalSecondaryIndexUpdates": [{ "Create": {
            "IndexName": index,
            "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" }
        }}]
    })
}

/// Builds the body of the refused `UpdateTable` for a table name.
type RefusedBody = fn(&str) -> serde_json::Value;

/// The status of every global secondary index on the table.
async fn index_states<E: StorageEngine>(fx: &Fixture<E>, table_name: &str) -> Vec<String> {
    let description = fx
        .engine
        .describe_table(
            &fx.account_a,
            DescribeTableInput {
                table_name: table_name.to_owned(),
            },
        )
        .await
        .expect("describe the table");
    let value = serde_json::to_value(&description).expect("serialize the description");
    value
        .get("GlobalSecondaryIndexes")
        .and_then(serde_json::Value::as_array)
        .map(|indexes| {
            indexes
                .iter()
                .map(|index| {
                    index
                        .get("IndexStatus")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("?")
                        .to_owned()
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A table with the global secondary index `gix` `ACTIVE`.
async fn table_with_active_index<E: StorageEngine>(
    fx: &Fixture<E>,
    table_name: &str,
) -> TableDescription {
    let table = create_table(fx, &fx.account_a, table_name).await;
    fx.engine
        .update_table(&fx.account_a, update_request(create_gsi(table_name, "gix")))
        .await
        .expect("create the first index");
    for _ in 0..300 {
        let states = index_states(fx, table_name).await;
        if !states.is_empty() && states.iter().all(|s| s == "ACTIVE") {
            return table;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "the first index never became ACTIVE: {:?}",
        index_states(fx, table_name).await
    );
}

/// With `gix` `ACTIVE`, a start is called (its build mark taken), then an
/// `UpdateTable` that is refused, then the start is awaited: it is
/// `Created`. Run for a creation of `gix` again (`IndexAlreadyExists`) and
/// for a creation of a new index listed with a deletion of an absent one
/// (`IndexNotFound`, refused after the creation's own checks passed).
pub async fn a_refused_index_creation_does_not_refuse_a_start_that_crossed_it<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let cases: [(&str, RefusedBody); 2] = [
        ("refused_dup", |name| create_gsi(name, "gix")),
        ("refused_absent_delete", |name| {
            let mut body = create_gsi(name, "gix2");
            body["GlobalSecondaryIndexUpdates"]
                .as_array_mut()
                .expect("the updates array")
                .push(json!({ "Delete": { "IndexName": "absent" } }));
            body
        }),
    ];
    for (label, refused_body) in cases {
        let name = format!("{label}_{}", unique());
        let table = table_with_active_index(fx, &name).await;
        let req = new_op(fx, &fx.account_a, &table, None, "fp", 1000);
        let pending = fx.engine.start_bulk_operation(&fx.account_a, req, 1_000);

        let refused = fx
            .engine
            .update_table(&fx.account_a, update_request(refused_body(&name)))
            .await;
        match (label, &refused) {
            ("refused_dup", Err(StorageError::IndexAlreadyExists(index))) => {
                assert_eq!(index, "gix");
            }
            ("refused_absent_delete", Err(StorageError::IndexNotFound(index))) => {
                assert_eq!(index, "absent");
            }
            _ => panic!("{label}: the UpdateTable is refused, got {refused:?}"),
        }

        let outcome = pending
            .await
            .expect("start_bulk_operation must not error at the transport level");
        assert_eq!(
            start_kind(&outcome),
            "Created",
            "{label}: no build ran, so the start is accepted (indexes {:?})",
            index_states(fx, &name).await
        );
    }
}

/// A start is called, its build mark taken, then an `UpdateTable` creates a
/// global secondary index and its build ends with the index `ACTIVE`, then
/// the start is awaited. The two crossed and the creation was accepted, so
/// the start is refused, though its transaction reads every index `ACTIVE`.
pub async fn an_accepted_index_creation_refuses_a_start_called_before_it<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let name = format!("accepted_before_{}", unique());
    let table = create_table(fx, &fx.account_a, &name).await;
    let req = new_op(fx, &fx.account_a, &table, None, "fp", 1000);
    let pending = fx.engine.start_bulk_operation(&fx.account_a, req, 1_000);

    fx.engine
        .update_table(&fx.account_a, update_request(create_gsi(&name, "gix")))
        .await
        .expect("the index creation is accepted");
    let mut states = index_states(fx, &name).await;
    for _ in 0..300 {
        if !states.is_empty() && states.iter().all(|s| s == "ACTIVE") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        states = index_states(fx, &name).await;
    }
    assert_eq!(states, vec!["ACTIVE".to_owned()], "the build ended");

    let outcome = pending
        .await
        .expect("start_bulk_operation must not error at the transport level");
    assert_eq!(
        start_kind(&outcome),
        "IndexOperationInProgress",
        "the start crossed an accepted index creation"
    );
}

/// An `UpdateTable` that creates an index fails at its data table, slowly:
/// `fail_index_tables` makes every later table creation in the data
/// database wait a second and then fail. A start called while it runs is
/// created once the creation has failed; the failed creation never counted
/// as a build.
pub async fn a_start_called_during_a_failing_index_creation_is_created<E, F, Fut>(
    fx: &Fixture<E>,
    fail_index_tables: F,
) where
    E: StorageEngine + 'static,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let name = format!("failing_creation_{}", unique());
    let table = create_table(fx, &fx.account_a, &name).await;
    fail_index_tables().await;

    let engine = Arc::clone(&fx.engine);
    let account = fx.account_a.clone();
    let request = update_request(create_gsi(&name, "gix"));
    let update = tokio::spawn(async move { engine.update_table(&account, request).await });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let outcome = fx
        .engine
        .start_bulk_operation(
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            1_000,
        )
        .await
        .expect("start_bulk_operation must not error at the transport level");
    let updated = update.await.expect("join the UpdateTable");
    assert!(
        updated.is_err(),
        "the index creation fails at its data table, got {:?}",
        updated.map(|d| d.table_status)
    );
    assert_eq!(
        start_kind(&outcome),
        "Created",
        "no index was created, so the start is accepted (indexes {:?})",
        index_states(fx, &name).await
    );
}
