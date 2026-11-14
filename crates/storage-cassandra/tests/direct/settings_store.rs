// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for SettingsStore.
//!
//! Run with: cargo test -- --nocapture

#[cfg(test)]
mod tests {
    use crate::helpers::test_config;
    use extenddb_storage::management_store::SettingsStore;
    use extenddb_storage_cassandra::CassandraEngine;

    #[tokio::test]
    async fn test_settings_store_direct() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let config = test_config();

        let region = "us-east-1";

        // Create engine directly
        let engine = CassandraEngine::new(&config, region)
            .await
            .expect("Failed to create engine");

        // Create catalog store directly
        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        // This key is a live control-plane knob in the shared catalog, so
        // remember what was there and restore it — the mutation must not
        // outlive the test.
        let original = catalog_store
            .get_setting("control_plane_delay_seconds")
            .await
            .expect("get_setting failed");

        catalog_store
            .set_setting("control_plane_delay_seconds", "0.05")
            .await
            .expect("set_setting failed");

        let value = catalog_store
            .get_setting("control_plane_delay_seconds")
            .await
            .expect("get_setting after set failed")
            .expect("setting missing after set");
        assert_eq!(value, "0.05");

        // No delete_setting on the trait; when the key was absent, restore
        // the value the readers default to when unset (0.25 in
        // read_control_plane_delay) rather than leaving the test value.
        let restore = original.unwrap_or_else(|| "0.25".to_string());
        catalog_store
            .set_setting("control_plane_delay_seconds", &restore)
            .await
            .expect("restore setting failed");
    }

    #[tokio::test]
    async fn test_list_settings() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let config = test_config();
        let engine = CassandraEngine::new(&config, "us-east-1")
            .await
            .expect("Failed to create engine");

        let catalog_store = crate::helpers::create_catalog_store(&engine, &config);

        // Set a few test settings
        let _ = catalog_store.set_setting("test_key_1", "value1").await;
        let _ = catalog_store.set_setting("test_key_2", "value2").await;
        let _ = catalog_store.set_setting("test_key_3", "value3").await;

        // List all settings
        match catalog_store.list_settings().await {
            Ok(settings) => {
                println!(
                    "✓ list_settings succeeded: {} settings found",
                    settings.len()
                );
                for (key, value) in &settings {
                    println!("  {} = {}", key, value);
                }

                // Verify our test settings exist
                assert!(
                    settings
                        .iter()
                        .any(|(k, v)| k == "test_key_1" && v == "value1")
                );
                assert!(
                    settings
                        .iter()
                        .any(|(k, v)| k == "test_key_2" && v == "value2")
                );
                assert!(
                    settings
                        .iter()
                        .any(|(k, v)| k == "test_key_3" && v == "value3")
                );
                println!("✓ Verified test settings in list");
            }
            Err(e) => panic!("list_settings failed: {:?}", e),
        }
    }
}
