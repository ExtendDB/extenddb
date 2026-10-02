// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Account management operations for `CassandraCatalogStore`.

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, test_config, unique_test_account, unique_test_id};
    use extenddb_storage::management_store::ManagementStore;

    #[tokio::test]
    async fn test_create_account() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .expect("Failed to create account");

        println!("✓ Account created successfully");

        let result = catalog_store
            .create_account(&account_id, &account_name)
            .await;
        match result {
            Err(extenddb_storage::management_store::OpError::AlreadyExists(_)) => {
                println!("✓ Duplicate account correctly rejected");
            }
            other => panic!("Expected AlreadyExists, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_account_operations() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());

        // Happy case: create account
        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .expect("Failed to create account");

        println!("✓ Account created successfully");

        // Unhappy case: duplicate account (after keyspace ensured)
        let result = catalog_store
            .create_account(&account_id, &account_name)
            .await;

        match result {
            Err(extenddb_storage::management_store::OpError::AlreadyExists(_)) => {
                println!("✓ Duplicate account correctly rejected");
            }
            other => panic!("Expected AlreadyExists, got: {:?}", other),
        }

        // Happy case: list all accounts
        let accounts = catalog_store
            .list_all_accounts()
            .await
            .expect("Failed to list accounts");

        assert!(accounts.iter().any(|(id, _)| id == &account_id));
        println!("✓ Account listed in list_all_accounts");

        // Happy case: list accounts full
        let accounts_full = catalog_store
            .list_all_accounts_full()
            .await
            .expect("Failed to list accounts full");

        assert!(accounts_full.iter().any(|(id, _, _)| id == &account_id));
        println!("✓ Account listed in list_all_accounts_full");

        // Happy case: list accounts for specific account
        let accounts_for = catalog_store
            .list_accounts_for(&account_id)
            .await
            .expect("Failed to list accounts for");

        assert_eq!(accounts_for.len(), 1);
        assert_eq!(accounts_for[0].0, account_id);
        println!("✓ list_accounts_for works");

        // Happy case: get account detail
        let detail = catalog_store
            .get_account_detail(&account_id)
            .await
            .expect("Failed to get account detail")
            .expect("Account detail should exist");

        assert_eq!(detail.account_name, account_name);
        assert_eq!(detail.users.len(), 0);
        assert_eq!(detail.groups.len(), 0);
        assert_eq!(detail.roles.len(), 0);
        println!("✓ Account detail retrieved");

        // Happy case: dashboard counts
        let (account_count, _admin_count) = catalog_store
            .dashboard_counts()
            .await
            .expect("Failed to get dashboard counts");

        assert!(account_count > 0);
        println!("✓ Dashboard counts: {} accounts", account_count);

        // Unhappy case: delete account with tables (would need to create a table first)
        // Skip this as it requires full table engine setup

        // Happy case: delete account
        catalog_store
            .delete_account(&account_id)
            .await
            .expect("Failed to delete account");

        println!("✓ Account deleted successfully");

        // Unhappy case: delete non-existent account
        let result = catalog_store.delete_account(&account_id).await;

        match result {
            Err(extenddb_storage::management_store::OpError::NotFound(_)) => {
                println!("✓ Delete non-existent account correctly rejected");
            }
            other => panic!("Expected NotFound, got: {:?}", other),
        }
    }
}
