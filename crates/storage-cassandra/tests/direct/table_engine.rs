// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for TableEngine.
//!
//! Run with: cargo test -- --nocapture

#[cfg(test)]
mod tests {
    use extenddb_storage_cassandra::CassandraEngine;

    /// Tests table lifecycle operations.
    ///
    /// The test automatically provisions a test account and adjusts replication factors
    /// for single-node testing. No manual setup required beyond running Cassandra and
    /// `extenddb init --backend cassandra --keyspace-prefix extenddb_test`.
    #[tokio::test]
    async fn test_table_lifecycle() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        use crate::helpers::test_config;
        use extenddb_core::types::{
            AttributeDefinition, BillingMode, CreateTableInput, DeleteTableInput,
            DescribeTableInput, KeySchemaElement, KeyType, ListTablesInput, ScalarAttributeType,
        };
        use extenddb_storage::TableEngine;

        let config = test_config();
        let engine = CassandraEngine::new(&config, "us-east-1")
            .await
            .expect("Failed to create engine");

        let account_id = crate::helpers::test_account_id(&engine)
            .await
            .expect("Failed to get test account");
        let table_name = "DirectTestTable";

        println!("=== Testing Table Lifecycle ===");

        // Test create_table
        let create_input = CreateTableInput {
            vector_indexes: None,
            table_throughput_mode: None,
            table_name: table_name.to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            }],
            billing_mode: Some(BillingMode::PayPerRequest),
            global_secondary_indexes: None,
            local_secondary_indexes: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            sse_specification: None,
            stream_specification: None,
            tags: None,
            deletion_protection_enabled: None,
            table_class: None,
        };

        engine
            .create_table(&account_id, create_input)
            .await
            .expect("create_table failed");

        // Test describe_table
        let describe_input = DescribeTableInput {
            table_name: table_name.to_string(),
        };
        let desc = engine
            .describe_table(&account_id, describe_input)
            .await
            .expect("describe_table failed");
        assert_eq!(desc.table_name, table_name);

        // Test list_tables
        let list_input = ListTablesInput {
            exclusive_start_table_name: None,
            limit: None,
        };
        let listed = engine
            .list_tables(&account_id, list_input)
            .await
            .expect("list_tables failed");
        assert!(
            listed.table_names.iter().any(|t| t == table_name),
            "created table missing from list_tables"
        );

        // Test delete_table
        let delete_input = DeleteTableInput {
            table_name: table_name.to_string(),
        };
        engine
            .delete_table(&account_id, delete_input)
            .await
            .expect("delete_table failed");

        // Verify deletion
        let describe_input = DescribeTableInput {
            table_name: table_name.to_string(),
        };
        assert!(
            engine
                .describe_table(&account_id, describe_input)
                .await
                .is_err(),
            "table still describable after deletion"
        );
    }

    /// Tests index_info_by_table_id against a table with a GSI.
    ///
    /// Uses the table_id path (the engine's hot path for index Query/Scan
    /// routing) because it doesn't require the table to be ACTIVE — in these
    /// direct tests there is no control-plane worker to transition the table
    /// out of CREATING, which index_info()-by-name would require via
    /// fetch_table_key_info. The by-name wrapper shares this logic and is
    /// covered by the Python integration suite (where tables are ACTIVE).
    #[tokio::test]
    async fn test_index_info() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        use crate::helpers::{TestTable, setup_engine};
        use extenddb_core::types::IndexType;
        use extenddb_storage::TableEngine;

        let engine = setup_engine().await;
        let table = TestTable::with_gsi(&engine, "IndexInfoTable", "StatusIndex", "status").await;

        // Known index resolves to its metadata.
        let info = engine
            .index_info_by_table_id(&table.key_info.table_id, "StatusIndex")
            .await
            .expect("index_info_by_table_id should succeed");
        assert_eq!(info.index_name, "StatusIndex");
        assert!(matches!(info.index_type, IndexType::Gsi));
        assert_eq!(info.key_schema[0].attribute_name, "status");

        // Unknown index returns an error (not the old "not yet implemented" stub).
        let missing = engine
            .index_info_by_table_id(&table.key_info.table_id, "NoSuchIndex")
            .await;
        assert!(missing.is_err(), "unknown index should return an error");
    }
}
