// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for AuthorizationStore trait implementation.

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, test_config, unique_test_account, unique_test_id};
    use extenddb_storage::authorization_store::AuthorizationStore;
    use extenddb_storage::management_store::ManagementStore;

    #[tokio::test]
    async fn test_fetch_user_policies() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let user_name = format!("testuser_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &format!("TestAccount_{}", unique_test_id()))
            .await
            .unwrap();

        catalog_store
            .create_user(&account_id, &user_name, None)
            .await
            .unwrap();

        let policy_doc = serde_json::json!({"Version": "2012-10-17", "Statement": []});
        catalog_store
            .put_policy(&account_id, "user", &user_name, "TestPolicy", &policy_doc)
            .await
            .unwrap();

        let policies = catalog_store
            .fetch_user_policies(&account_id, &user_name)
            .await
            .unwrap();

        assert!(!policies.is_empty());
        println!("✓ Fetched {} user policies", policies.len());
    }

    #[tokio::test]
    async fn test_fetch_user_group_policies() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let user_name = format!("testuser_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &format!("TestAccount_{}", unique_test_id()))
            .await
            .unwrap();

        catalog_store
            .create_user(&account_id, &user_name, None)
            .await
            .unwrap();

        let policies = catalog_store
            .fetch_user_group_policies(&account_id, &user_name)
            .await
            .unwrap();

        // Should be empty - user not in any groups
        assert!(policies.is_empty());
        println!("✓ Fetched group policies (empty as expected)");
    }

    #[tokio::test]
    async fn test_fetch_user_boundary() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let user_name = format!("testuser_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &format!("TestAccount_{}", unique_test_id()))
            .await
            .unwrap();

        catalog_store
            .create_user(&account_id, &user_name, None)
            .await
            .unwrap();

        let boundary = catalog_store
            .fetch_user_boundary(&account_id, &user_name)
            .await
            .unwrap();

        assert!(boundary.is_none());
        println!("✓ User has no permissions boundary");
    }

    #[tokio::test]
    async fn test_fetch_role_policies() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let role_name = format!("testrole_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .unwrap();

        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        });

        catalog_store
            .create_role(&account_id, &role_name, &trust_policy)
            .await
            .unwrap();

        // Add a policy to the role
        let policy_doc = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "*"
            }]
        });

        catalog_store
            .put_policy(&account_id, "role", &role_name, "TestPolicy", &policy_doc)
            .await
            .unwrap();

        // Fetch role policies
        let policies = catalog_store
            .fetch_role_policies(&account_id, &role_name)
            .await
            .unwrap();

        assert_eq!(policies.len(), 1);
        println!("✓ Fetched {} role policy(ies)", policies.len());

        // Verify policy content
        let fetched_policy: serde_json::Value = serde_json::from_str(&policies[0]).unwrap();
        assert_eq!(fetched_policy, policy_doc);
        println!("✓ Policy content matches");

        // Non-existent role should return empty
        let policies = catalog_store
            .fetch_role_policies(&account_id, "nonexistent")
            .await
            .unwrap();
        assert!(policies.is_empty());
        println!("✓ Non-existent role returns empty policies");
    }

    #[tokio::test]
    async fn test_fetch_role_boundary() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let role_name = format!("testrole_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .unwrap();

        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        });

        catalog_store
            .create_role(&account_id, &role_name, &trust_policy)
            .await
            .unwrap();

        // Initially no boundary
        let boundary = catalog_store
            .fetch_role_boundary(&account_id, &role_name)
            .await
            .unwrap();
        assert!(boundary.is_none());
        println!("✓ Role has no permissions boundary initially");

        // Set a boundary
        let boundary_doc = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": "*"
            }]
        });

        catalog_store
            .set_role_boundary(&account_id, &role_name, &boundary_doc)
            .await
            .unwrap();

        // Fetch boundary
        let boundary = catalog_store
            .fetch_role_boundary(&account_id, &role_name)
            .await
            .unwrap();
        assert!(boundary.is_some());
        println!("✓ Role boundary set and fetched");

        // Verify boundary content
        let fetched_boundary: serde_json::Value = serde_json::from_str(&boundary.unwrap()).unwrap();
        assert_eq!(fetched_boundary, boundary_doc);
        println!("✓ Boundary content matches");

        // Non-existent role should return None
        let boundary = catalog_store
            .fetch_role_boundary(&account_id, "nonexistent")
            .await
            .unwrap();
        assert!(boundary.is_none());
        println!("✓ Non-existent role returns None boundary");
    }

    #[tokio::test]
    async fn test_fetch_user_and_role_tags() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let user_name = format!("testuser_{}", unique_test_id());
        let role_name = format!("testrole_{}", unique_test_id());

        catalog_store
            .create_account(&account_id, &account_name)
            .await
            .unwrap();

        catalog_store
            .create_user(&account_id, &user_name, None)
            .await
            .unwrap();

        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        });

        catalog_store
            .create_role(&account_id, &role_name, &trust_policy)
            .await
            .unwrap();

        // Tag user
        catalog_store
            .tag_user(
                &account_id,
                &user_name,
                &[
                    ("Department".to_string(), "Engineering".to_string()),
                    ("Team".to_string(), "Platform".to_string()),
                ],
            )
            .await
            .unwrap();

        // Tag role
        catalog_store
            .tag_role(
                &account_id,
                &role_name,
                &[
                    ("Environment".to_string(), "Production".to_string()),
                    ("Owner".to_string(), "DevOps".to_string()),
                ],
            )
            .await
            .unwrap();

        // Fetch user tags
        let user_tags = catalog_store
            .fetch_user_tags(&account_id, &user_name)
            .await
            .unwrap();
        assert_eq!(user_tags.len(), 2);
        println!("✓ Fetched {} user tag(s)", user_tags.len());

        // Fetch role tags
        let role_tags = catalog_store
            .fetch_role_tags(&account_id, &role_name)
            .await
            .unwrap();
        assert_eq!(role_tags.len(), 2);
        println!("✓ Fetched {} role tag(s)", role_tags.len());

        // Non-existent resources should return empty
        let user_tags = catalog_store
            .fetch_user_tags(&account_id, "nonexistent")
            .await
            .unwrap();
        assert!(user_tags.is_empty());

        let role_tags = catalog_store
            .fetch_role_tags(&account_id, "nonexistent")
            .await
            .unwrap();
        assert!(role_tags.is_empty());
        println!("✓ Non-existent resources return empty tags");
    }
}
