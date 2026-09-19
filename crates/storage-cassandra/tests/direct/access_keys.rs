// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for access key operations.

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, test_config, unique_test_account, unique_test_id};
    use extenddb_storage::management_store::ManagementStore;

    #[tokio::test]
    async fn test_create_access_key() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();

        // Need encryption key for access key creation
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

        catalog_store
            .create_access_key(&account_id, &user_name)
            .await
            .expect("Failed to create access key");

        println!("✓ Access key created successfully");
    }

    #[tokio::test]
    async fn test_store_and_fetch_session() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();

        // Need encryption key for session storage
        use extenddb_storage::bootstrapper::helpers::generate_encryption_key;
        let enc_key = generate_encryption_key();
        let catalog_store = extenddb_storage_cassandra::CassandraCatalogStore::with_encryption_key(
            engine.session_arc(),
            config.keyspace_prefix.clone(),
            config.datacenter.clone(),
            config.replication_factor,
            enc_key.clone(),
        );

        let account_id = unique_test_account();
        let account_name = format!("TestAccount_{}", unique_test_id());
        let role_name = format!("testrole_{}", unique_test_id());
        let session_name = format!("session_{}", unique_test_id());

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

        let session_token = format!("session_token_{}", unique_test_id());
        let access_key_id = format!("AKIATEST{}", unique_test_id());
        let secret_key = b"test_secret_key_12345678";
        let session_tags = Some(serde_json::json!([
            {"Key": "Department", "Value": "Engineering"},
            {"Key": "Project", "Value": "TestProject"}
        ]));
        let session_policy = Some(serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "*"
            }]
        }));
        let expires_at = time::OffsetDateTime::now_utc() + time::Duration::hours(1);

        // Store session
        catalog_store
            .store_session(
                &session_token,
                &access_key_id,
                secret_key,
                &account_id,
                &role_name,
                &session_name,
                &session_tags,
                &session_policy,
                expires_at,
            )
            .await
            .unwrap();
        println!("✓ Session stored");

        // Fetch session data via AuthorizationStore
        use extenddb_storage::authorization_store::AuthorizationStore;
        let session_data = catalog_store
            .fetch_session_data(&account_id, &role_name, &session_name)
            .await
            .unwrap();

        assert!(session_data.is_some());
        let data = session_data.unwrap();

        assert!(data.session_policy.is_some());
        assert_eq!(data.session_tags.len(), 2);
        println!("✓ Session data fetched: {} tags", data.session_tags.len());

        // Verify session tags content
        assert!(
            data.session_tags
                .iter()
                .any(|(k, v)| k == "Department" && v == "Engineering")
        );
        assert!(
            data.session_tags
                .iter()
                .any(|(k, v)| k == "Project" && v == "TestProject")
        );
        println!("✓ Session tags verified");
    }

    #[tokio::test]
    async fn test_fetch_caller_tags() {
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
                "Principal": {"Service": "ec2.amazonaws.com"},
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
                    ("Environment".to_string(), "Production".to_string()),
                    ("Owner".to_string(), "TeamA".to_string()),
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
                    ("Department".to_string(), "Engineering".to_string()),
                    ("CostCenter".to_string(), "CC123".to_string()),
                ],
            )
            .await
            .unwrap();

        // Fetch caller tags for user
        let user_resource = format!("user/{}", user_name);
        let user_tags = catalog_store
            .fetch_caller_tags(&account_id, &user_resource)
            .await
            .unwrap();
        assert_eq!(user_tags.len(), 2);
        println!("✓ Fetched {} user caller tags", user_tags.len());

        // Fetch caller tags for role
        let role_resource = format!("role/{}", role_name);
        let role_tags = catalog_store
            .fetch_caller_tags(&account_id, &role_resource)
            .await
            .unwrap();
        assert_eq!(role_tags.len(), 2);
        println!("✓ Fetched {} role caller tags", role_tags.len());

        // Fetch caller tags for assumed-role
        let assumed_role_resource = format!("assumed-role/{}/session-name", role_name);
        let assumed_tags = catalog_store
            .fetch_caller_tags(&account_id, &assumed_role_resource)
            .await
            .unwrap();
        assert_eq!(assumed_tags.len(), 2);
        println!("✓ Fetched {} assumed-role caller tags", assumed_tags.len());

        // Non-existent resource should return empty
        let empty_tags = catalog_store
            .fetch_caller_tags(&account_id, "invalid/format")
            .await
            .unwrap();
        assert!(empty_tags.is_empty());
        println!("✓ Invalid resource returns empty tags");

        // Non-existent user should return empty
        let empty_tags = catalog_store
            .fetch_caller_tags(&account_id, "user/nonexistent")
            .await
            .unwrap();
        assert!(empty_tags.is_empty());
        println!("✓ Non-existent user returns empty tags");
    }
}
