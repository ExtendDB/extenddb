// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use futures::future::BoxFuture;
use serde_json::json;

use super::*;

/// A catalog of named tables, for the target-name check.
struct Catalog(std::collections::HashMap<String, Result<TableDescription, StorageError>>);

fn unused<T>() -> BoxFuture<'static, Result<T, StorageError>> {
    unimplemented!("the target-name check reads describe_table only")
}

impl TableEngine for Catalog {
    fn create_table(
        &self,
        _: &str,
        _: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unused()
    }
    fn delete_table(
        &self,
        _: &str,
        _: extenddb_core::types::DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unused()
    }
    fn describe_table(
        &self,
        _: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let found = self.0.get(&input.table_name).cloned();
        Box::pin(async move { found.unwrap_or(Err(StorageError::TableNotFound(input.table_name))) })
    }
    fn list_tables(
        &self,
        _: &str,
        _: extenddb_core::types::ListTablesInput,
    ) -> BoxFuture<'_, Result<extenddb_core::types::ListTablesOutput, StorageError>> {
        unused()
    }
    fn update_table(
        &self,
        _: &str,
        _: extenddb_core::types::UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unused()
    }
    fn table_key_info(
        &self,
        _: &str,
        _: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::TableKeyInfo, StorageError>> {
        unused()
    }
    fn index_info(
        &self,
        _: &str,
        _: &str,
        _: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::IndexInfo, StorageError>> {
        unused()
    }
    fn index_info_by_table_id(
        &self,
        _: &str,
        _: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::IndexInfo, StorageError>> {
        unused()
    }
}

fn table(name: &str, status: TableStatus, restoring: bool) -> TableDescription {
    TableDescription {
        table_name: name.to_owned(),
        table_status: status,
        restore_summary: restoring.then(|| {
            serde_json::from_value(json!({
                "SourceBackupArn": "arn:aws:dynamodb:us-east-1:000000000000:table/src/backup/b",
                "RestoreDateTime": 1_794_284_272.0,
                "RestoreInProgress": true
            }))
            .expect("summary")
        }),
        ..Default::default()
    }
}

/// Measured on the live service: an ACTIVE target (two samples) and a
/// restore target still being created.
#[tokio::test]
async fn the_target_name_must_be_free() {
    let catalog = Catalog(
        [
            ("taken", Ok(table("taken", TableStatus::Active, false))),
            ("loading", Ok(table("loading", TableStatus::Creating, true))),
            (
                "broken",
                Err(StorageError::Internal(
                    "the catalog is unreadable".to_owned(),
                )),
            ),
        ]
        .into_iter()
        .map(|(n, t)| (n.to_owned(), t))
        .collect(),
    );
    let check = |name: &'static str| check_target_name(&catalog, "000000000000", name);
    match check("taken").await {
        Err(DynamoDbError::TableAlreadyExistsException(m)) => {
            assert_eq!(m, "Table already exists: taken");
        }
        other => panic!("{other:?}"),
    }
    match check("loading").await {
        Err(DynamoDbError::TableInUseException(m)) => {
            assert_eq!(m, "Table is in the process of being created: loading");
        }
        other => panic!("{other:?}"),
    }
    assert!(check("free").await.is_ok());
    assert!(matches!(
        check("broken").await,
        Err(DynamoDbError::InternalServerError(_))
    ));
}
