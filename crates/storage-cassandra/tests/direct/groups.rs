// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Group management operations for `CassandraCatalogStore`.

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, test_config, unique_test_account, unique_test_id};
    use extenddb_storage::management_store::ManagementStore;

    #[tokio::test]
    async fn test_group_lifecycle() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let group_name = format!("testgroup_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .expect("Failed to create account");

        // Happy case: create group
        catalog_store
            .create_group(&account_id, &group_name)
            .await
            .expect("Failed to create group");

        println!("✓ Group created successfully");

        // Unhappy case: duplicate group
        let result = catalog_store.create_group(&account_id, &group_name).await;

        match result {
            Err(extenddb_storage::management_store::OpError::AlreadyExists(_)) => {
                println!("✓ Duplicate group correctly rejected");
            }
            other => panic!("Expected AlreadyExists, got: {:?}", other),
        }

        // Happy case: list groups
        let groups = catalog_store
            .list_groups(&account_id)
            .await
            .expect("Failed to list groups");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].1, group_name);
        println!("✓ Group listed successfully");

        // Happy case: delete group
        catalog_store
            .delete_group(&account_id, &group_name)
            .await
            .expect("Failed to delete group");

        println!("✓ Group deleted successfully");

        // Unhappy case: delete non-existent group
        let result = catalog_store.delete_group(&account_id, &group_name).await;

        match result {
            Err(extenddb_storage::management_store::OpError::NotFound(_)) => {
                println!("✓ Delete non-existent group correctly rejected");
            }
            other => panic!("Expected NotFound, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_group_members() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let group_name = format!("testgroup_{}", unique_test_id());
        let user1 = format!("user1_{}", unique_test_id());
        let user2 = format!("user2_{}", unique_test_id());

        // Setup
        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .expect("Failed to create account");

        catalog_store
            .create_group(&account_id, &group_name)
            .await
            .expect("Failed to create group");

        catalog_store
            .create_user(&account_id, &user1, None)
            .await
            .expect("Failed to create user1");

        catalog_store
            .create_user(&account_id, &user2, None)
            .await
            .expect("Failed to create user2");

        // Happy case: add member
        catalog_store
            .add_group_member(&account_id, &group_name, &user1)
            .await
            .expect("Failed to add member");

        println!("✓ Member added successfully");

        // Unhappy case: add duplicate member
        let result = catalog_store
            .add_group_member(&account_id, &group_name, &user1)
            .await;

        match result {
            Err(extenddb_storage::management_store::OpError::AlreadyExists(_)) => {
                println!("✓ Duplicate member correctly rejected");
            }
            other => panic!("Expected AlreadyExists, got: {:?}", other),
        }

        // Add second member
        catalog_store
            .add_group_member(&account_id, &group_name, &user2)
            .await
            .expect("Failed to add second member");

        // Happy case: get group detail
        let detail = catalog_store
            .get_group_detail(&account_id, &group_name)
            .await
            .expect("Failed to get group detail")
            .expect("Group detail should exist");

        assert_eq!(detail.members.len(), 2);
        assert!(detail.members.contains(&user1));
        assert!(detail.members.contains(&user2));
        assert_eq!(detail.all_users.len(), 2);
        println!("✓ Group detail retrieved successfully");

        // Happy case: remove member
        catalog_store
            .remove_group_member(&account_id, &group_name, &user1)
            .await
            .expect("Failed to remove member");

        println!("✓ Member removed successfully");

        // Unhappy case: remove non-existent membership
        let result = catalog_store
            .remove_group_member(&account_id, &group_name, &user1)
            .await;

        match result {
            Err(extenddb_storage::management_store::OpError::NotFound(_)) => {
                println!("✓ Remove non-existent membership correctly rejected");
            }
            other => panic!("Expected NotFound, got: {:?}", other),
        }

        // Verify only one member remains
        let detail = catalog_store
            .get_group_detail(&account_id, &group_name)
            .await
            .expect("Failed to get group detail")
            .expect("Group detail should exist");

        assert_eq!(detail.members.len(), 1);
        assert!(detail.members.contains(&user2));
        println!("✓ Membership update verified");
    }
}
