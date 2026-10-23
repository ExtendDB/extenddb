// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `UpdateTable` side of stored `WarmThroughput` for `SqliteEngine`.
//!
//! The record is written as sent and read back by DescribeTable; nothing
//! pre-warms or scales on it, and it has no effect on throughput, throttling
//! or storage. See `extenddb_core::types::WarmThroughputRecord`.

use extenddb_core::types::UpdateTableInput;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{warm_throughput_record_for_update, warm_throughput_record_json};

use crate::store::SqliteEngine;

impl SqliteEngine {
    /// `control_plane_delay_seconds` from the settings table (0.25 when
    /// unset): the window a fresh record reports `UPDATING` for, the same
    /// one the CREATING to ACTIVE flip uses.
    pub(crate) async fn control_plane_delay_secs(
        conn: &mut sqlx::SqliteConnection,
    ) -> Result<f64, StorageError> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT value FROM settings WHERE key = 'control_plane_delay_seconds'",
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.25))
    }

    /// Store the request's table-level `WarmThroughput` and each
    /// `GlobalSecondaryIndexUpdates.Update.WarmThroughput`, merged over what
    /// is stored, inside the caller's catalog transaction. The engine has
    /// already validated the values against the current ones.
    pub(crate) async fn store_warm_throughput_updates(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        account_id: &str,
        table_id: &str,
        input: &UpdateTableInput,
    ) -> Result<(), StorageError> {
        let gsi_updates: Vec<_> = input
            .global_secondary_index_updates
            .iter()
            .flatten()
            .filter_map(|u| u.update.as_ref())
            .filter_map(|u| u.warm_throughput.as_ref().map(|wt| (&u.index_name, wt)))
            .collect();
        if input.warm_throughput.is_none() && gsi_updates.is_empty() {
            return Ok(());
        }
        let delay_secs = Self::control_plane_delay_secs(tx).await?;

        if let Some(spec) = &input.warm_throughput {
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT warm_throughput FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(account_id)
            .bind(&input.table_name)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let record = warm_throughput_record_for_update(existing.as_deref(), spec, delay_secs)?;
            sqlx::query(
                "UPDATE tables SET warm_throughput = ? WHERE account_id = ? AND table_name = ?",
            )
            .bind(warm_throughput_record_json(&record)?)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        for (index_name, spec) in gsi_updates {
            let existing: Option<Option<String>> = sqlx::query_scalar(
                "SELECT warm_throughput FROM indexes \
                 WHERE table_id = ? AND index_name = ? AND index_type = 'GSI'",
            )
            .bind(table_id)
            .bind(index_name)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let Some(existing) = existing else {
                return Err(StorageError::IndexNotFound(index_name.clone()));
            };
            let record = warm_throughput_record_for_update(existing.as_deref(), spec, delay_secs)?;
            sqlx::query(
                "UPDATE indexes SET warm_throughput = ? WHERE table_id = ? AND index_name = ?",
            )
            .bind(warm_throughput_record_json(&record)?)
            .bind(table_id)
            .bind(index_name)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::SqliteEngine;
    use extenddb_core::types::{WarmThroughputStatus, epoch_seconds_now};
    use extenddb_storage::TableEngine;
    use serde_json::json;

    async fn engine_with_delay(delay: &str) -> SqliteEngine {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        sqlx::query("UPDATE settings SET value = ? WHERE key = 'control_plane_delay_seconds'")
            .bind(delay)
            .execute(&engine.pool)
            .await
            .expect("delay");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(ACCOUNT)
            .execute(&engine.pool)
            .await
            .expect("account");
        engine
    }

    const ACCOUNT: &str = "000000000000";

    async fn describe(engine: &SqliteEngine) -> extenddb_core::types::TableDescription {
        let mut desc = engine
            .describe_table(
                ACCOUNT,
                extenddb_core::types::DescribeTableInput {
                    table_name: "t".to_owned(),
                },
            )
            .await
            .expect("describe");
        desc.prepare_for_response();
        desc
    }

    fn on_demand_with_gsi(
        warm: Option<serde_json::Value>,
    ) -> extenddb_core::types::CreateTableInput {
        let mut v = json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }]
        });
        if let Some(w) = warm {
            v["WarmThroughput"] = w;
        }
        serde_json::from_value(v).expect("input")
    }

    /// No request member: the on-demand defaults, ACTIVE, on the table and the
    /// index, once the table is ACTIVE (delay 0 makes it so at once).
    #[tokio::test]
    async fn an_untouched_on_demand_table_reports_the_defaults_once_active() {
        let engine = engine_with_delay("0").await;
        engine
            .create_table_impl(ACCOUNT, on_demand_with_gsi(None), false)
            .await
            .expect("create");
        let desc = describe(&engine).await;
        let wt = desc.warm_throughput.expect("table member");
        assert_eq!(
            (
                wt.read_units_per_second,
                wt.write_units_per_second,
                wt.status
            ),
            (Some(12_000), Some(4_000), WarmThroughputStatus::Active)
        );
        let gsi = &desc.global_secondary_indexes.expect("gsis")[0];
        let gwt = gsi.warm_throughput.expect("index member");
        assert_eq!(
            (gwt.read_units_per_second, gwt.write_units_per_second),
            (Some(12_000), Some(4_000))
        );
    }

    /// The stored record survives the round trip and an UpdateTable merges
    /// over it: the deadline makes it UPDATING for the delay, then ACTIVE.
    #[tokio::test]
    async fn a_stored_record_is_read_back_and_an_update_merges_over_it() {
        let engine = engine_with_delay("0").await;
        engine
            .create_table_impl(
                ACCOUNT,
                on_demand_with_gsi(Some(json!({"ReadUnitsPerSecond": 20000}))),
                false,
            )
            .await
            .expect("create");
        let desc = describe(&engine).await;
        let wt = desc.warm_throughput.expect("table member");
        assert_eq!(
            (
                wt.read_units_per_second,
                wt.write_units_per_second,
                wt.status
            ),
            (Some(20_000), Some(4_000), WarmThroughputStatus::Active)
        );

        // A long delay so the UPDATING window is observable.
        sqlx::query("UPDATE settings SET value = '3600' WHERE key = 'control_plane_delay_seconds'")
            .execute(&engine.pool)
            .await
            .expect("delay");
        let update: extenddb_core::types::UpdateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "WarmThroughput": {"WriteUnitsPerSecond": 8000},
            "GlobalSecondaryIndexUpdates": [{"Update": {
                "IndexName": "gsi",
                "WarmThroughput": {"ReadUnitsPerSecond": 24000, "WriteUnitsPerSecond": 4000}
            }}]
        }))
        .expect("update");
        // While UPDATING the previous pair is reported (as the service does).
        let mut returned = engine.update_table(ACCOUNT, update).await.expect("update");
        returned.prepare_for_response();
        let wt = returned.warm_throughput.expect("table member");
        assert_eq!(
            (
                wt.read_units_per_second,
                wt.write_units_per_second,
                wt.status
            ),
            (Some(20_000), Some(4_000), WarmThroughputStatus::Updating)
        );
        let gsi = &returned.global_secondary_indexes.expect("gsis")[0];
        let gwt = gsi.warm_throughput.expect("index member");
        assert_eq!(
            (
                gwt.read_units_per_second,
                gwt.write_units_per_second,
                gwt.status
            ),
            (Some(12_000), Some(4_000), WarmThroughputStatus::Updating)
        );
        // Past the deadline the same record reads ACTIVE.
        let mut later = engine
            .describe_table(
                ACCOUNT,
                extenddb_core::types::DescribeTableInput {
                    table_name: "t".to_owned(),
                },
            )
            .await
            .expect("describe");
        later.populate_warm_throughput(epoch_seconds_now() + 3601.0);
        let settled = later.warm_throughput.expect("member");
        assert_eq!(
            (
                settled.read_units_per_second,
                settled.write_units_per_second,
                settled.status
            ),
            (Some(20_000), Some(8_000), WarmThroughputStatus::Active)
        );
        let gsi = &later.global_secondary_indexes.expect("gsis")[0];
        let gwt = gsi.warm_throughput.expect("index member");
        assert_eq!(
            (gwt.read_units_per_second, gwt.write_units_per_second),
            (Some(24_000), Some(4_000))
        );
    }

    /// An Update naming an index the table does not have is IndexNotFound.
    #[tokio::test]
    async fn an_update_of_a_missing_index_is_index_not_found() {
        let engine = engine_with_delay("0").await;
        engine
            .create_table_impl(ACCOUNT, on_demand_with_gsi(None), false)
            .await
            .expect("create");
        let update: extenddb_core::types::UpdateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "GlobalSecondaryIndexUpdates": [{"Update": {
                "IndexName": "nope",
                "WarmThroughput": {"ReadUnitsPerSecond": 24000}
            }}]
        }))
        .expect("update");
        let err = engine
            .update_table(ACCOUNT, update)
            .await
            .expect_err("refused");
        assert!(
            matches!(err, extenddb_storage::error::StorageError::IndexNotFound(ref n) if n == "nope"),
            "{err:?}"
        );
    }
}
