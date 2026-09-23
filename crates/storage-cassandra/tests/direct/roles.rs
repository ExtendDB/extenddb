// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Roles management operations for `CassandraCatalogStore`.

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, test_config, unique_test_account, unique_test_id};
    use extenddb_storage::management_store::ManagementStore;

    #[tokio::test]
    async fn test_role_lifecycle() {
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

        // Create a role
        let role_name = "test-role";
        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        });

        catalog_store
            .create_role(&account_id, role_name, &trust_policy)
            .await
            .expect("Failed to create role");
        println!("✓ Role created");

        // Duplicate create should fail
        let result = catalog_store
            .create_role(&account_id, role_name, &trust_policy)
            .await;
        assert!(matches!(
            result,
            Err(extenddb_storage::management_store::OpError::AlreadyExists(
                _
            ))
        ));
        println!("✓ Duplicate role rejected");

        // List roles (should have 1)
        let roles = catalog_store
            .list_roles(&account_id)
            .await
            .expect("Failed to list roles");
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].1, role_name);
        println!("✓ List roles: {} role(s)", roles.len());

        // Get role trust policy
        let retrieved_policy = catalog_store
            .get_role_trust_policy(&account_id, role_name)
            .await
            .expect("Failed to get trust policy")
            .expect("Trust policy should exist");
        assert_eq!(retrieved_policy, trust_policy);
        println!("✓ Trust policy retrieved");

        // Delete role
        catalog_store
            .delete_role(&account_id, role_name)
            .await
            .expect("Failed to delete role");
        println!("✓ Role deleted");

        // Delete non-existent should fail
        let result = catalog_store.delete_role(&account_id, role_name).await;
        assert!(matches!(
            result,
            Err(extenddb_storage::management_store::OpError::NotFound(_))
        ));
        println!("✓ Delete non-existent role rejected");

        // List should be empty
        let roles = catalog_store
            .list_roles(&account_id)
            .await
            .expect("Failed to list roles");
        assert_eq!(roles.len(), 0);
        println!("✓ Role list empty after deletion");
    }

    #[tokio::test]
    async fn test_role_tags() {
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

        // Create role
        let role_name = "tagged-role";
        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        });

        catalog_store
            .create_role(&account_id, role_name, &trust_policy)
            .await
            .expect("Failed to create role");

        // Tag role (should fail for non-existent)
        let result = catalog_store
            .tag_role(
                &account_id,
                "nonexistent",
                &[("key".to_owned(), "value".to_owned())],
            )
            .await;
        assert!(matches!(
            result,
            Err(extenddb_storage::management_store::OpError::NotFound(_))
        ));
        println!("✓ Tag non-existent role rejected");

        // Tag role
        catalog_store
            .tag_role(
                &account_id,
                role_name,
                &[
                    ("Environment".to_owned(), "Production".to_owned()),
                    ("Team".to_owned(), "Platform".to_owned()),
                ],
            )
            .await
            .expect("Failed to tag role");
        println!("✓ Role tagged");

        // List tags
        let tags = catalog_store
            .list_role_tags(&account_id, role_name)
            .await
            .expect("Failed to list tags");
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], ("Environment".to_owned(), "Production".to_owned()));
        assert_eq!(tags[1], ("Team".to_owned(), "Platform".to_owned()));
        println!("✓ Tags listed: {:?}", tags);

        // Update tag (upsert)
        catalog_store
            .tag_role(
                &account_id,
                role_name,
                &[("Environment".to_owned(), "Staging".to_owned())],
            )
            .await
            .expect("Failed to update tag");

        let tags = catalog_store
            .list_role_tags(&account_id, role_name)
            .await
            .expect("Failed to list tags");
        assert_eq!(tags[0].1, "Staging");
        println!("✓ Tag updated");

        // Untag role
        catalog_store
            .untag_role(&account_id, role_name, &["Team".to_owned()])
            .await
            .expect("Failed to untag role");

        let tags = catalog_store
            .list_role_tags(&account_id, role_name)
            .await
            .expect("Failed to list tags");
        assert_eq!(tags.len(), 1);
        println!("✓ Role untagged");
    }

    #[tokio::test]
    async fn test_role_detail() {
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

        // Create role
        let role_name = "detailed-role";
        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        });

        catalog_store
            .create_role(&account_id, role_name, &trust_policy)
            .await
            .expect("Failed to create role");

        // Add tags
        catalog_store
            .tag_role(
                &account_id,
                role_name,
                &[("Owner".to_owned(), "Engineering".to_owned())],
            )
            .await
            .expect("Failed to tag role");

        // Add policy
        let policy_name = "test-policy";
        let policy_doc = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "*"
            }]
        });

        catalog_store
            .put_policy(&account_id, "role", role_name, policy_name, &policy_doc)
            .await
            .expect("Failed to put policy");

        // Get role detail
        let detail = catalog_store
            .get_role_detail(&account_id, role_name)
            .await
            .expect("Failed to get role detail")
            .expect("Role detail should exist");

        assert_eq!(detail.trust_policy, trust_policy);
        assert_eq!(detail.policies.len(), 1);
        assert_eq!(detail.policies[0], policy_name);
        assert_eq!(detail.tags.len(), 1);
        assert_eq!(detail.tags[0].0, "Owner");
        println!(
            "✓ Role detail retrieved: {} policies, {} tags",
            detail.policies.len(),
            detail.tags.len()
        );

        // Non-existent role
        let detail = catalog_store
            .get_role_detail(&account_id, "nonexistent")
            .await
            .expect("Failed to get role detail");
        assert!(detail.is_none());
        println!("✓ Non-existent role detail returns None");
    }
}
