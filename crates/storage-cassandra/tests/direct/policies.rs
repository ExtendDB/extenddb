// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Policies management operations for `CassandraCatalogStore`.

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, test_config, unique_test_account, unique_test_id};
    use extenddb_storage::management_store::ManagementStore;

    #[tokio::test]
    async fn test_put_policy() {
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

        let policy_doc = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": "*"
            }]
        });

        catalog_store
            .put_policy(&account_id, "user", &user_name, "TestPolicy", &policy_doc)
            .await
            .expect("Failed to put policy");

        println!("✓ Policy created successfully");

        let updated_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:GetItem",
                "Resource": "*"
            }]
        });

        catalog_store
            .put_policy(
                &account_id,
                "user",
                &user_name,
                "TestPolicy",
                &updated_policy,
            )
            .await
            .expect("Failed to update policy");

        // Read back: the update must be visible, not just accepted.
        let policies = catalog_store
            .list_policies(&account_id, "user", &user_name)
            .await
            .expect("Failed to list policies");
        let (_, stored_document, _) = policies
            .iter()
            .find(|(name, _, _)| name == "TestPolicy")
            .expect("updated policy missing from list");
        assert!(
            stored_document.to_string().contains("dynamodb:GetItem"),
            "policy read-back does not reflect the update: {stored_document}"
        );
    }
}
