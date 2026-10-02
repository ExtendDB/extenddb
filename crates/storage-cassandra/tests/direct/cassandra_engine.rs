// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for CassandraEngine.
//!
//! Run with: cargo test -- --nocapture

#[cfg(test)]
mod tests {
    use crate::helpers::test_config;
    use extenddb_storage_cassandra::engine::CassandraEngine;

    #[tokio::test]
    async fn test_cassandra_connection() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let config = test_config();

        let result = CassandraEngine::create_session(&config).await;

        match result {
            Ok(_session) => {
                println!("✓ Successfully connected to Cassandra");
            }
            Err(e) => {
                panic!("Failed to connect to Cassandra: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_keyspace_operations() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let config = test_config();

        let engine = CassandraEngine::new(&config, "us-east-1")
            .await
            .expect("Failed to create engine");

        let test_keyspace = "test_connectivity_ks";

        // Create keyspace
        engine
            .create_keyspace(test_keyspace)
            .await
            .expect("Failed to create keyspace");

        println!("✓ Created keyspace: {}", test_keyspace);

        // Verify exists
        let exists = engine
            .keyspace_exists(test_keyspace)
            .await
            .expect("Failed to check keyspace existence");

        assert!(exists, "Keyspace should exist after creation");
        println!("✓ Verified keyspace exists");

        // Cleanup
        engine
            .drop_keyspace(test_keyspace)
            .await
            .expect("Failed to drop keyspace");

        println!("✓ Dropped keyspace: {}", test_keyspace);
    }

    /// Tests table_key_info() method.
    #[tokio::test]
    async fn test_table_key_info() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        use extenddb_core::types::{
            AttributeDefinition, BillingMode, CreateTableInput, DeleteTableInput, KeySchemaElement,
            KeyType, ScalarAttributeType,
        };
        use extenddb_storage::TableEngine;

        let config = test_config();
        let engine = CassandraEngine::new(&config, "us-east-1")
            .await
            .expect("Failed to create engine");

        let account_id = crate::helpers::test_account_id(&engine)
            .await
            .expect("Failed to get test account");
        let table_name = "TestKeyInfoTable";

        // Create a test table
        let create_input = CreateTableInput {
            vector_indexes: None,
            table_throughput_mode: None,
            table_name: table_name.to_string(),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            attribute_definitions: vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_string(),
                    attribute_type: ScalarAttributeType::N,
                },
            ],
            billing_mode: Some(BillingMode::PayPerRequest),
            global_secondary_indexes: None,
            local_secondary_indexes: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            sse_specification: None,
            stream_specification: None,
            deletion_protection_enabled: None,
            table_class: None,
            tags: None,
        };

        // Create table
        match engine.create_table(&account_id, create_input).await {
            Ok(_) => println!("✓ Created test table"),
            Err(extenddb_storage::error::StorageError::TableAlreadyExists(_)) => {
                println!("✓ Table already exists, continuing")
            }
            Err(e) => panic!("Failed to create table: {:?}", e),
        }

        // Manually update table status to ACTIVE for testing
        // (bypass control plane delay mechanism)
        let update_query = format!(
            "UPDATE {}_catalog.tables SET table_status = 'ACTIVE' WHERE account_id = ? AND table_name = ?",
            config.keyspace_prefix
        );
        engine
            .session_arc()
            .query_with_values(
                &update_query,
                cdrs_tokio::query_values!(account_id.as_str(), table_name),
            )
            .await
            .expect("Failed to update table status");
        println!("✓ Table is ACTIVE");

        // Test table_key_info
        let key_info = engine
            .table_key_info(&account_id, table_name)
            .await
            .expect("Failed to fetch table_key_info");

        println!("✓ Fetched table_key_info");
        assert_eq!(key_info.table_name, table_name);
        assert_eq!(key_info.account_id, account_id);
        assert_eq!(key_info.key_schema.len(), 2);
        assert_eq!(key_info.key_schema[0].attribute_name, "pk");
        assert_eq!(key_info.key_schema[0].key_type, KeyType::Hash);
        assert_eq!(key_info.key_schema[1].attribute_name, "sk");
        assert_eq!(key_info.key_schema[1].key_type, KeyType::Range);
        assert_eq!(key_info.attribute_definitions.len(), 2);
        assert!(!key_info.table_id.is_empty());
        assert!(!key_info.has_lsi);
        assert!(key_info.stream_specification.is_none());
        println!("✓ All assertions passed");

        // Clean up
        let _ = engine
            .delete_table(
                &account_id,
                DeleteTableInput {
                    table_name: table_name.to_string(),
                },
            )
            .await;
    }

    /// Tests table_key_info() with non-existent table.
    #[tokio::test]
    async fn test_table_key_info_not_found() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        use extenddb_storage::TableEngine;

        let config = test_config();
        let engine = CassandraEngine::new(&config, "us-east-1")
            .await
            .expect("Failed to create engine");

        let account_id = crate::helpers::test_account_id(&engine)
            .await
            .expect("Failed to get test account");

        let result = engine.table_key_info(&account_id, "NonExistentTable").await;

        match result {
            Err(extenddb_storage::error::StorageError::TableNotFound(name)) => {
                println!("✓ Correctly returned TableNotFound for: {}", name);
                assert_eq!(name, "NonExistentTable");
            }
            other => panic!("Expected TableNotFound, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_verify_admin_password() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        use extenddb_storage::management_store::AdminStore;

        let config = test_config();
        let engine = crate::helpers::setup_engine().await;

        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        // Create an admin user for testing
        let test_admin = format!("testadmin_{}", crate::helpers::unique_test_id());
        let test_password = "test_password_123";

        // Hash the password like bootstrapper does
        let password_hash: String = tokio::task::spawn_blocking({
            let password = test_password.to_string();
            move || bcrypt::hash(password, bcrypt::DEFAULT_COST).unwrap()
        })
        .await
        .unwrap();

        catalog_store
            .create_admin(&test_admin, &password_hash)
            .await
            .expect("Failed to create test admin user");

        // Test with correct password
        let result = catalog_store
            .verify_admin_password(&test_admin, test_password)
            .await;

        match result {
            Ok(Some(true)) => {
                println!("✓ Admin password verification succeeded with correct password");
            }
            Ok(Some(false)) => {
                panic!("Should verify with correct password!");
            }
            Ok(None) => {
                panic!("Admin user '{}' should exist", test_admin);
            }
            Err(e) => {
                panic!("verify_admin_password failed: {:?}", e);
            }
        }

        // Test with wrong password
        let result = catalog_store
            .verify_admin_password(&test_admin, "wrong_password")
            .await;

        match result {
            Ok(Some(false)) => {
                println!(
                    "✓ Admin password verification correctly returned false for wrong password"
                );
            }
            Ok(Some(true)) => {
                panic!("Should not verify with wrong password!");
            }
            Ok(None) => {
                panic!("Admin user '{}' should exist", test_admin);
            }
            Err(e) => {
                panic!("verify_admin_password failed: {:?}", e);
            }
        }

        // Test with non-existent user
        let result = catalog_store
            .verify_admin_password("nonexistent", "password")
            .await;
        match result {
            Ok(None) => {
                println!(
                    "✓ Admin password verification correctly returned None for non-existent user"
                );
            }
            Ok(Some(_)) => {
                panic!("Non-existent user should return None");
            }
            Err(e) => {
                panic!("verify_admin_password failed: {:?}", e);
            }
        }
    }
}
