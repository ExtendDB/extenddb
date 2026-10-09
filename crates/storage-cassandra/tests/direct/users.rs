// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for User management operations for `CassandraCatalogStore`.

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, test_config, unique_test_account, unique_test_id};
    use extenddb_storage::management_store::ManagementStore;

    #[tokio::test]
    async fn test_create_user() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let user_name = format!("testuser_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .expect("Failed to create account");

        catalog_store
            .create_user(&account_id, &user_name, None)
            .await
            .expect("Failed to create user");

        println!("✓ User created successfully");

        let result = catalog_store
            .create_user(&account_id, &user_name, None)
            .await;
        match result {
            Err(extenddb_storage::management_store::OpError::AlreadyExists(_)) => {
                println!("✓ Duplicate user correctly rejected");
            }
            other => panic!("Expected AlreadyExists, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_foreign_key_checks() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let nonexistent_account = unique_test_account();
        let user_name = format!("testuser_{}", unique_test_id());

        let result = catalog_store
            .create_user(&nonexistent_account, &user_name, None)
            .await;

        match result {
            Err(extenddb_storage::management_store::OpError::NotFound(_)) => {
                println!("✓ Foreign key check correctly rejected user creation");
            }
            other => panic!("Expected NotFound for account, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_delete_user() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let user_name = format!("testuser_{}", unique_test_id());

        // Setup: create account and user
        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .expect("Failed to create account");

        catalog_store
            .create_user(&account_id, &user_name, None)
            .await
            .expect("Failed to create user");

        // Happy case: delete existing user
        catalog_store
            .delete_user(&account_id, &user_name)
            .await
            .expect("Failed to delete user");

        println!("✓ User deleted successfully");

        // Unhappy case: delete non-existent user
        let result = catalog_store.delete_user(&account_id, &user_name).await;

        match result {
            Err(extenddb_storage::management_store::OpError::NotFound(_)) => {
                println!("✓ Delete non-existent user correctly rejected");
            }
            other => panic!("Expected NotFound, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_list_users() {
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

        // Happy case: list users when none exist
        let users = catalog_store
            .list_users(&account_id)
            .await
            .expect("Failed to list users");

        assert_eq!(users.len(), 0, "Expected no users initially");
        println!("✓ List empty users works");

        // Create multiple users
        for i in 0..3 {
            let user_name = format!("testuser_{}_{}", unique_test_id(), i);
            catalog_store
                .create_user(&account_id, &user_name, None)
                .await
                .expect("Failed to create user");
        }

        // Happy case: list multiple users
        let users = catalog_store
            .list_users(&account_id)
            .await
            .expect("Failed to list users");

        assert_eq!(users.len(), 3, "Expected 3 users");
        println!("✓ Listed {} users successfully", users.len());

        // Verify structure: (account_id, user_name, user_arn, has_password, created_at)
        for (aid, uname, arn, has_pw, _created) in &users {
            assert_eq!(aid, &account_id);
            assert!(uname.starts_with("testuser_"));
            assert!(arn.contains(&account_id));
            assert!(!(*has_pw), "No password set");
        }

        println!("✓ All users have correct structure");
    }

    #[tokio::test]
    async fn test_get_user_detail() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();

        // Need encryption key for access keys
        use extenddb_storage::bootstrapper::helpers::generate_encryption_key;
        let enc_key = generate_encryption_key();
        let catalog_store = extenddb_storage_cassandra::CassandraCatalogStore::with_encryption_key(
            engine.session_arc(),
            config.keyspace_prefix.clone(),
            config.datacenter.clone(),
            config.replication_factor,
            enc_key,
        );

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let user_name = format!("testuser_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .expect("Failed to create account");

        catalog_store
            .create_user(&account_id, &user_name, None)
            .await
            .expect("Failed to create user");

        // Add access key
        catalog_store
            .create_access_key(&account_id, &user_name)
            .await
            .expect("Failed to create access key");

        // Add tags
        let tags = vec![("Env".to_string(), "Test".to_string())];
        catalog_store
            .tag_user(&account_id, &user_name, &tags)
            .await
            .expect("Failed to tag user");

        // Happy case: get user detail
        let detail = catalog_store
            .get_user_detail(&account_id, &user_name)
            .await
            .expect("Failed to get user detail")
            .expect("User detail should exist");

        assert_eq!(detail.keys.len(), 1);
        assert_eq!(detail.policies.len(), 1); // SelfServicePolicy
        assert_eq!(detail.policies[0], "SelfServicePolicy");
        assert_eq!(detail.tags.len(), 1);
        assert_eq!(detail.groups.len(), 0);

        println!("✓ User detail retrieved successfully");
        println!("  Keys: {}", detail.keys.len());
        println!("  Policies: {}", detail.policies.len());
        println!("  Tags: {}", detail.tags.len());
        println!("  Groups: {}", detail.groups.len());

        // Unhappy case: get detail for non-existent user
        let detail = catalog_store
            .get_user_detail(&account_id, "nonexistent_user")
            .await
            .expect("Failed to get user detail");

        assert!(detail.is_none(), "Non-existent user should return None");
        println!("✓ Non-existent user returns None");
    }

    #[tokio::test]
    async fn test_user_tags() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let user_name = format!("testuser_{}", unique_test_id());

        // Setup
        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .expect("Failed to create account");

        catalog_store
            .create_user(&account_id, &user_name, None)
            .await
            .expect("Failed to create user");

        // Happy case: add tags
        let tags = vec![
            ("Environment".to_string(), "Production".to_string()),
            ("Team".to_string(), "Platform".to_string()),
        ];

        catalog_store
            .tag_user(&account_id, &user_name, &tags)
            .await
            .expect("Failed to tag user");

        println!("✓ Tagged user successfully");

        // Happy case: list tags
        let fetched_tags = catalog_store
            .list_user_tags(&account_id, &user_name)
            .await
            .expect("Failed to list tags");

        assert_eq!(fetched_tags.len(), 2);
        assert!(fetched_tags.contains(&("Environment".to_string(), "Production".to_string())));
        assert!(fetched_tags.contains(&("Team".to_string(), "Platform".to_string())));
        println!("✓ Listed {} tags correctly", fetched_tags.len());

        // Happy case: update existing tag (upsert)
        let updated_tags = vec![("Environment".to_string(), "Staging".to_string())];

        catalog_store
            .tag_user(&account_id, &user_name, &updated_tags)
            .await
            .expect("Failed to update tag");

        let fetched_tags = catalog_store
            .list_user_tags(&account_id, &user_name)
            .await
            .expect("Failed to list tags after update");

        assert_eq!(fetched_tags.len(), 2);
        assert!(fetched_tags.contains(&("Environment".to_string(), "Staging".to_string())));
        println!("✓ Tag upsert works correctly");

        // Happy case: untag
        let tag_keys = vec!["Environment".to_string()];
        catalog_store
            .untag_user(&account_id, &user_name, &tag_keys)
            .await
            .expect("Failed to untag user");

        let fetched_tags = catalog_store
            .list_user_tags(&account_id, &user_name)
            .await
            .expect("Failed to list tags after untag");

        assert_eq!(fetched_tags.len(), 1);
        assert!(fetched_tags.contains(&("Team".to_string(), "Platform".to_string())));
        println!("✓ Untag works correctly");

        // Unhappy case: tag non-existent user
        let result = catalog_store
            .tag_user(&account_id, "nonexistent_user", &tags)
            .await;

        match result {
            Err(extenddb_storage::management_store::OpError::NotFound(_)) => {
                println!("✓ Tag non-existent user correctly rejected");
            }
            other => panic!("Expected NotFound, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_user_passwords() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let user_name = format!("testuser_{}", unique_test_id());

        // Setup
        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .expect("Failed to create account");

        // Create user with password
        let password = "TestPassword123!";
        let password_hash =
            bcrypt::hash(password, bcrypt::DEFAULT_COST).expect("Failed to hash password");

        catalog_store
            .create_user(&account_id, &user_name, Some(&password_hash))
            .await
            .expect("Failed to create user with password");

        println!("✓ User with password created");

        // Happy case: verify correct password
        let verified = catalog_store
            .verify_iam_user_password(&account_id, &user_name, password)
            .await
            .expect("Failed to verify password");

        assert!(verified, "Password should verify");
        println!("✓ Correct password verified");

        // Unhappy case: verify wrong password
        let verified = catalog_store
            .verify_iam_user_password(&account_id, &user_name, "WrongPassword")
            .await
            .expect("Failed to verify password");

        assert!(!verified, "Wrong password should not verify");
        println!("✓ Wrong password correctly rejected");

        // Happy case: change password
        let new_password = "NewPassword456!";
        let new_hash =
            bcrypt::hash(new_password, bcrypt::DEFAULT_COST).expect("Failed to hash new password");

        catalog_store
            .change_user_password(&account_id, &user_name, &new_hash)
            .await
            .expect("Failed to change password");

        // Verify old password no longer works
        let verified = catalog_store
            .verify_iam_user_password(&account_id, &user_name, password)
            .await
            .expect("Failed to verify password");

        assert!(!verified, "Old password should not work");

        // Verify new password works
        let verified = catalog_store
            .verify_iam_user_password(&account_id, &user_name, new_password)
            .await
            .expect("Failed to verify new password");

        assert!(verified, "New password should verify");
        println!("✓ Password changed successfully");

        // Unhappy case: change password for non-existent user
        let result = catalog_store
            .change_user_password(&account_id, "nonexistent", &new_hash)
            .await;

        match result {
            Err(extenddb_storage::management_store::OpError::NotFound(_)) => {
                println!("✓ Change password for non-existent user correctly rejected");
            }
            other => panic!("Expected NotFound, got: {:?}", other),
        }
    }
}
