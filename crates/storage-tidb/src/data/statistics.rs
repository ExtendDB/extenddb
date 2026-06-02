// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB-native optimizer statistics maintenance for data tables.

use extenddb_core::types::TableKeyInfo;
use extenddb_storage::error::StorageError;

use super::data_table_name;
use crate::TidbEngine;

fn analyze_table_sql(table_id: &str) -> String {
    format!("ANALYZE TABLE {}", data_table_name(table_id))
}

impl TidbEngine {
    pub(crate) async fn refresh_table_statistics_impl(
        &self,
        key_info: &TableKeyInfo,
    ) -> Result<(), StorageError> {
        sqlx::query(&analyze_table_sql(&key_info.table_id))
            .execute(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::analyze_table_sql;

    #[test]
    fn analyze_table_sql_uses_physical_data_table_identifier() {
        assert_eq!(analyze_table_sql("abc123"), "ANALYZE TABLE `_ddb_abc123`");
    }
}
