// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A deleted principal or account leaves nothing behind (#693, #694).

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, test_config, unique_test_account};
    use extenddb_storage::management_store::{ManagementStore, PasswordHashStore};

    #[tokio::test]
    async fn deleted_principals_and_accounts_leave_nothing() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let store = crate::helpers::create_catalog_store(&engine, &config);
        extenddb_storage::iam_leftovers_conformance::run(&store, &unique_test_account()).await;
    }

    /// Before account deletes removed them, a deleted account's users and
    /// roles stayed. With only the `accounts` row removed, as such a delete
    /// left it, the user cannot sign in to the console and the role cannot be
    /// assumed.
    #[tokio::test]
    async fn a_user_or_role_of_a_deleted_account_grants_nothing() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let store = crate::helpers::create_catalog_store(&engine, &config);
        let account = unique_test_account();
        store.create_account(&account, "acct").await.unwrap();
        let hash = bcrypt::hash("correct horse", 4).unwrap();
        store
            .create_user(&account, "alice", Some(&hash))
            .await
            .unwrap();
        let trust = serde_json::json!({"Version": "2012-10-17", "Statement": []});
        store.create_role(&account, "app", &trust).await.unwrap();
        assert!(
            store
                .get_user_password_hash(&account, "alice")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .verify_iam_user_password(&account, "alice", "correct horse")
                .await
                .unwrap()
        );
        assert!(
            store
                .get_role_trust_policy(&account, "app")
                .await
                .unwrap()
                .is_some()
        );

        // `accounts` is written with IF NOT EXISTS, so the delete is an LWT.
        engine
            .session_arc()
            .query_with_values(
                format!(
                    "DELETE FROM {}_catalog.accounts WHERE account_id = ? IF EXISTS",
                    config.keyspace_prefix
                ),
                cdrs_tokio::query_values!(account.as_str()),
            )
            .await
            .expect("remove the accounts row only");

        assert_eq!(
            store
                .get_user_password_hash(&account, "alice")
                .await
                .unwrap(),
            None
        );
        assert!(
            !store
                .verify_iam_user_password(&account, "alice", "correct horse")
                .await
                .unwrap()
        );
        assert_eq!(
            store.get_role_trust_policy(&account, "app").await.unwrap(),
            None
        );

        // The account created again with this id starts empty.
        store.create_account(&account, "acct-again").await.unwrap();
        assert!(store.list_users(&account).await.unwrap().is_empty());
        assert!(store.list_roles(&account).await.unwrap().is_empty());
        store.delete_account(&account).await.unwrap();
    }
}
