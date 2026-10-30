// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for AdminStore trait implementation.

#[cfg(test)]
mod tests {
    use crate::helpers::{setup_engine, test_config};
    use extenddb_storage::management_store::AdminStore;

    #[tokio::test]
    async fn test_admin_lifecycle() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let admin_name = format!("test-admin-{}", crate::helpers::unique_test_id());
        let password_hash = "test-hash-123";

        // Create admin
        catalog_store
            .create_admin(&admin_name, password_hash)
            .await
            .expect("Failed to create admin");
        println!("✓ Admin created");

        // Duplicate create should fail
        let result = catalog_store.create_admin(&admin_name, password_hash).await;
        assert!(matches!(
            result,
            Err(extenddb_storage::management_store::OpError::AlreadyExists(
                _
            ))
        ));
        println!("✓ Duplicate admin rejected");

        // List admins (should have at least the test admin we just created)
        let admins = catalog_store
            .list_admins()
            .await
            .expect("Failed to list admins");
        assert!(!admins.is_empty());
        assert!(admins.iter().any(|a| a.admin_name == admin_name));
        println!("✓ List admins: {} admin(s)", admins.len());

        // Delete admin
        catalog_store
            .delete_admin(&admin_name)
            .await
            .expect("Failed to delete admin");
        println!("✓ Admin deleted");

        // Delete non-existent should fail
        let result = catalog_store.delete_admin(&admin_name).await;
        assert!(matches!(
            result,
            Err(extenddb_storage::management_store::OpError::NotFound(_))
        ));
        println!("✓ Delete non-existent admin rejected");

        // List should not contain deleted admin
        let admins = catalog_store
            .list_admins()
            .await
            .expect("Failed to list admins");
        assert!(!admins.iter().any(|a| a.admin_name == admin_name));
        println!("✓ Admin list no longer contains deleted admin");
    }

    #[tokio::test]
    async fn test_admin_password() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = setup_engine().await;
        let config = test_config();
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        let admin_name = format!("password-test-admin-{}", crate::helpers::unique_test_id());
        let password = "test-password-123";

        // Hash password using bcrypt
        let password_hash = tokio::task::spawn_blocking({
            let password = password.to_string();
            move || bcrypt::hash(password, bcrypt::DEFAULT_COST).unwrap()
        })
        .await
        .unwrap();

        // Create admin with password
        catalog_store
            .create_admin(&admin_name, &password_hash)
            .await
            .expect("Failed to create admin");
        println!("✓ Admin created with password");

        // Verify correct password
        let result = catalog_store
            .verify_admin_password(&admin_name, password)
            .await
            .expect("Failed to verify password");
        assert_eq!(result, Some(true));
        println!("✓ Correct password verified");

        // Verify wrong password
        let result = catalog_store
            .verify_admin_password(&admin_name, "wrong-password")
            .await
            .expect("Failed to verify password");
        assert_eq!(result, Some(false));
        println!("✓ Wrong password rejected");

        // Verify non-existent admin
        let result = catalog_store
            .verify_admin_password("nonexistent", password)
            .await
            .expect("Failed to verify password");
        assert_eq!(result, None);
        println!("✓ Non-existent admin returns None");

        // Change password
        let new_password = "new-password-456";
        let new_password_hash = tokio::task::spawn_blocking({
            let password = new_password.to_string();
            move || bcrypt::hash(password, bcrypt::DEFAULT_COST).unwrap()
        })
        .await
        .unwrap();

        catalog_store
            .change_admin_password(&admin_name, &new_password_hash)
            .await
            .expect("Failed to change password");
        println!("✓ Password changed");

        // Old password should fail
        let result = catalog_store
            .verify_admin_password(&admin_name, password)
            .await
            .expect("Failed to verify password");
        assert_eq!(result, Some(false));
        println!("✓ Old password no longer works");

        // New password should work
        let result = catalog_store
            .verify_admin_password(&admin_name, new_password)
            .await
            .expect("Failed to verify password");
        assert_eq!(result, Some(true));
        println!("✓ New password works");

        // Change password for non-existent admin should fail
        let result = catalog_store
            .change_admin_password("nonexistent", &new_password_hash)
            .await;
        assert!(matches!(
            result,
            Err(extenddb_storage::management_store::OpError::NotFound(_))
        ));
        println!("✓ Change password for non-existent admin rejected");

        // Cleanup
        catalog_store
            .delete_admin(&admin_name)
            .await
            .expect("Failed to delete admin");
    }
}
