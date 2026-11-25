// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SearchVectors` with the extended `SearchConditionExpression` grammar.
//!
//! The handler parses a search condition with both grammars. [`parse`] is
//! the extended grammar's parser, and [`resolve_scope`] gives the partition
//! and the filter of a condition it parsed. When the two grammars answer a
//! request alike, the handler does not read the setting; otherwise
//! [`enabled_for`] says which grammar the caller gets. With the
//! `search-inequality-filters` feature off, this module defines no parser
//! and the handler runs the equality-only grammar alone.
//!
//! With the feature on, the `search_inequality_filters` runtime setting says
//! which accounts get the grammar. It is read through the settings store the
//! server installs with [`install_search_filter_settings`] (see
//! `search_vectors_settings.rs`), so a change applies without a restart.
//! Until a store is installed, every account gets the grammar.

#[cfg(feature = "search-inequality-filters")]
pub(crate) use imp::{Extended, enabled_for, parse, resolve_scope};

#[cfg(feature = "search-inequality-filters")]
#[path = "search_vectors_settings.rs"]
mod settings;

#[cfg(feature = "search-inequality-filters")]
pub use settings::{install_search_filter_settings, search_filter_settings_changed};

#[cfg(feature = "search-inequality-filters")]
mod imp {
    use std::collections::HashMap;
    use std::sync::Arc;

    use extenddb_core::error::DynamoDbError;
    use extenddb_core::expression::search_filter::{
        DEFAULT_MAX_CONDITIONS, SearchFilter, name_refs, parse_search_filter,
    };
    use extenddb_core::expression::search_params::with_unused_names;
    use extenddb_core::types::{AttributeDefinition, AttributeValue, Item, SearchSchemaElement};
    use extenddb_storage::error::StorageError;
    use extenddb_storage::{StorageEngine, VectorItemFilter};

    /// The HASH attribute and the value that selects its partition, and the
    /// filter the backend applies to each candidate.
    pub(crate) type Scope<'a> = (
        Option<(&'a str, &'a AttributeValue)>,
        Arc<dyn VectorItemFilter>,
    );

    /// A search condition parsed with the extended grammar.
    pub(crate) struct Extended(Arc<SearchFilter>);

    /// Parse a search condition with the extended grammar. A name that
    /// neither the search condition nor `projection` uses is refused once
    /// the condition parses.
    ///
    /// # Errors
    ///
    /// Returns the `ValidationException` of the first failed check.
    pub(crate) fn parse(
        expr: &str,
        names: Option<&HashMap<String, String>>,
        values: Option<&HashMap<String, AttributeValue>>,
        projection: Option<&str>,
    ) -> Result<Extended, DynamoDbError> {
        with_unused_names(
            parse_search_filter(expr, names, values, DEFAULT_MAX_CONDITIONS),
            names,
            &name_refs(expr),
            projection.unwrap_or_default(),
        )
        .map(|filter| Extended(Arc::new(filter)))
    }

    /// Whether the setting gives `account_id` the extended grammar on the
    /// server `storage` belongs to.
    ///
    /// # Errors
    ///
    /// Returns `ServiceUnavailableException` when the setting cannot be read
    /// and no earlier read answered.
    pub(crate) async fn enabled_for(
        storage: &Arc<dyn StorageEngine>,
        account_id: &str,
    ) -> Result<bool, DynamoDbError> {
        // Cross-account access is refused before dispatch, so the caller is
        // the table owner.
        Ok(super::settings::enabled_accounts(storage)
            .await?
            .enabled_for(account_id, None))
    }

    /// The partition the HASH condition selects and the filter the backend
    /// applies to each candidate.
    ///
    /// # Errors
    ///
    /// Returns the `ValidationException` of the first failed schema check.
    pub(crate) fn resolve_scope<'a>(
        extended: &'a Extended,
        schema: Option<&[SearchSchemaElement]>,
        definitions: &[AttributeDefinition],
    ) -> Result<Scope<'a>, DynamoDbError> {
        let hash_key = extended.0.validate_against_schema(schema, definitions)?;
        Ok((hash_key, Arc::new(Matcher(Arc::clone(&extended.0)))))
    }

    struct Matcher(Arc<SearchFilter>);

    impl VectorItemFilter for Matcher {
        fn matches(&self, item: &Item) -> Result<bool, StorageError> {
            self.0
                .matches(item)
                .map_err(|e| StorageError::Internal(format!("search condition: {e}")))
        }
    }
}

#[cfg(all(test, feature = "search-inequality-filters"))]
#[path = "search_vectors_extended_tests.rs"]
mod tests;
