// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings validation and write operations.
//!
//! Validation logic lives here in the server layer. The actual database
//! write is delegated to the `SettingsStore` trait implementation.

use extenddb_storage::management_store::{OpError, OpResult};

/// Validator function for a setting value.
pub type Validator = fn(&str) -> Result<(), &'static str>;

/// Backend capabilities that determine which runtime settings are writable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSettingContext {
    backend_native_control_plane: bool,
    backend_native_secondary_indexes: bool,
    backend_native_capacity_control: bool,
}

impl RuntimeSettingContext {
    pub fn from_storage_config(config: &dyn extenddb_storage::config::StorageConfig) -> Self {
        Self {
            backend_native_control_plane: config.uses_backend_native_control_plane(),
            backend_native_secondary_indexes: config.uses_backend_native_secondary_indexes(),
            backend_native_capacity_control: config.uses_backend_native_capacity_control(),
        }
    }

    pub const fn frontend_owned() -> Self {
        Self {
            backend_native_control_plane: false,
            backend_native_secondary_indexes: false,
            backend_native_capacity_control: false,
        }
    }

    pub const fn backend_native() -> Self {
        Self {
            backend_native_control_plane: true,
            backend_native_secondary_indexes: true,
            backend_native_capacity_control: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettingScope {
    AllBackends,
    FrontendControlPlane,
    FrontendSecondaryIndexes,
    FrontendCapacityControl,
}

impl SettingScope {
    fn is_supported(self, context: RuntimeSettingContext) -> bool {
        match self {
            Self::AllBackends => true,
            Self::FrontendControlPlane => !context.backend_native_control_plane,
            Self::FrontendSecondaryIndexes => !context.backend_native_secondary_indexes,
            Self::FrontendCapacityControl => !context.backend_native_capacity_control,
        }
    }

    fn unsupported_reason(self) -> &'static str {
        match self {
            Self::AllBackends => "is supported by every backend",
            Self::FrontendControlPlane => {
                "this backend uses native online DDL and control-plane coordination"
            }
            Self::FrontendSecondaryIndexes => {
                "this backend uses native secondary indexes maintained from base-row writes"
            }
            Self::FrontendCapacityControl => {
                "this backend uses native distributed capacity control"
            }
        }
    }
}

/// Runtime setting definition.
#[derive(Debug, Clone, Copy)]
pub struct SettingSpec {
    pub key: &'static str,
    pub validator: Validator,
    scope: SettingScope,
}

/// Known writable setting keys and their validators.
pub const KNOWN_KEYS: &[SettingSpec] = &[
    SettingSpec {
        key: "allow_credential_import",
        validator: validate_bool,
        scope: SettingScope::AllBackends,
    },
    SettingSpec {
        key: "control_plane_delay_seconds",
        validator: validate_delay_seconds,
        scope: SettingScope::FrontendControlPlane,
    },
    SettingSpec {
        key: "gsi_propagation_delay_ms",
        validator: validate_gsi_delay_ms,
        scope: SettingScope::FrontendSecondaryIndexes,
    },
    SettingSpec {
        key: "log_level",
        validator: validate_log_level,
        scope: SettingScope::AllBackends,
    },
    SettingSpec {
        key: "sqlx_log_level",
        validator: validate_log_level,
        scope: SettingScope::AllBackends,
    },
    SettingSpec {
        key: "throttling_enabled",
        validator: validate_bool,
        scope: SettingScope::FrontendCapacityControl,
    },
];

/// Read-only keys that cannot be changed via the settings API.
pub const READONLY_KEYS: &[&str] = &[
    "catalog_version",
    "data_database_connection_string",
    "data_database_name",
];

fn validate_log_level(value: &str) -> Result<(), &'static str> {
    match value {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err("must be one of: trace, debug, info, warn, error"),
    }
}

fn validate_bool(value: &str) -> Result<(), &'static str> {
    match value {
        "true" | "false" => Ok(()),
        _ => Err("must be 'true' or 'false'"),
    }
}

fn validate_delay_seconds(value: &str) -> Result<(), &'static str> {
    match value.parse::<f64>() {
        Ok(v) if (0.0..=300.0).contains(&v) => Ok(()),
        Ok(_) => Err("must be between 0 and 300"),
        Err(_) => Err("must be a non-negative number"),
    }
}

fn validate_gsi_delay_ms(value: &str) -> Result<(), &'static str> {
    match value.parse::<u32>() {
        Ok(0..=10000) => Ok(()),
        Ok(_) => Err("must be between 0 and 10000"),
        Err(_) => Err("must be a non-negative integer"),
    }
}

fn setting_spec(key: &str) -> Option<&'static SettingSpec> {
    KNOWN_KEYS.iter().find(|spec| spec.key == key)
}

pub fn setting_is_supported(context: RuntimeSettingContext, key: &str) -> bool {
    setting_spec(key).is_some_and(|spec| spec.scope.is_supported(context))
}

pub fn known_writable_keys(context: RuntimeSettingContext) -> Vec<&'static str> {
    KNOWN_KEYS
        .iter()
        .filter(|spec| spec.scope.is_supported(context))
        .map(|spec| spec.key)
        .collect()
}

pub fn validate_setting(context: RuntimeSettingContext, key: &str, value: &str) -> OpResult<()> {
    if READONLY_KEYS.contains(&key) {
        return Err(OpError::Validation(format!("Setting '{key}' is read-only")));
    }

    let Some(spec) = setting_spec(key) else {
        return Err(OpError::Validation(format!(
            "Unknown setting '{key}'. Known writable keys: {}",
            known_writable_keys(context).join(", ")
        )));
    };

    if !spec.scope.is_supported(context) {
        return Err(OpError::Validation(format!(
            "Setting '{key}' is not supported for this deployment because {}",
            spec.scope.unsupported_reason()
        )));
    }

    (spec.validator)(value)
        .map_err(|reason| OpError::Validation(format!("Invalid value for '{key}': {reason}")))?;
    Ok(())
}

/// Set a runtime setting with validation.
///
/// Validates the key and value, then delegates the write to the
/// `SettingsStore` implementation. Validation stays in the server layer;
/// the storage layer trusts validated input.
///
/// # Errors
///
/// Returns `OpError::Validation` if the key is read-only, unknown, or the value
/// fails validation. Returns `OpError::Internal` on database errors.
pub async fn set_setting(
    store: &dyn extenddb_storage::management_store::SettingsStore,
    context: RuntimeSettingContext,
    key: &str,
    value: &str,
) -> OpResult<()> {
    validate_setting(context, key, value)?;
    store.set_setting(key, value).await?;

    tracing::warn!(
        target: "extenddb::audit::settings",
        "settings-set: key={key}, value={value}",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        RuntimeSettingContext, known_writable_keys, setting_is_supported, validate_setting,
    };
    use extenddb_storage::management_store::OpError;

    fn validation_message(result: Result<(), OpError>) -> String {
        match result {
            Err(OpError::Validation(message)) => message,
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn frontend_owned_context_accepts_frontend_settings() {
        let context = RuntimeSettingContext::frontend_owned();

        assert!(validate_setting(context, "control_plane_delay_seconds", "0.25").is_ok());
        assert!(validate_setting(context, "gsi_propagation_delay_ms", "10").is_ok());
        assert!(validate_setting(context, "throttling_enabled", "true").is_ok());
    }

    #[test]
    fn backend_native_context_rejects_noop_frontend_settings() {
        let context = RuntimeSettingContext::backend_native();

        let control_plane = validation_message(validate_setting(
            context,
            "control_plane_delay_seconds",
            "0.25",
        ));
        assert!(control_plane.contains("native online DDL"));

        let indexes =
            validation_message(validate_setting(context, "gsi_propagation_delay_ms", "10"));
        assert!(indexes.contains("native secondary indexes"));

        let throttling =
            validation_message(validate_setting(context, "throttling_enabled", "true"));
        assert!(throttling.contains("native distributed capacity control"));
    }

    #[test]
    fn writable_key_list_is_capability_filtered() {
        let context = RuntimeSettingContext::backend_native();

        assert!(setting_is_supported(context, "log_level"));
        assert!(!setting_is_supported(
            context,
            "control_plane_delay_seconds"
        ));
        assert_eq!(
            known_writable_keys(context),
            vec!["allow_credential_import", "log_level", "sqlx_log_level"]
        );
    }

    #[test]
    fn unknown_setting_message_lists_only_supported_keys() {
        let context = RuntimeSettingContext::backend_native();
        let message = validation_message(validate_setting(context, "not_a_setting", "true"));

        assert!(message.contains("allow_credential_import"));
        assert!(message.contains("log_level"));
        assert!(!message.contains("gsi_propagation_delay_ms"));
    }
}
