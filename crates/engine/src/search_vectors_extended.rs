// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SearchVectors` with the extended `SearchConditionExpression` grammar.
//!
//! The handler calls [`parse_request`] and, when it returns a filter,
//! [`resolve_scope`] in place of the equality-only validator. With the
//! `search-inequality-filters` feature off, [`parse_request`] always returns
//! `None` and the handler runs exactly as it does without this module.
//!
//! With the feature on, the `search_inequality_filters` runtime setting says
//! which accounts get the grammar. It is read through the settings store the
//! server installs with [`install_search_filter_settings`] (see
//! `search_vectors_settings.rs`), so a change applies without a restart.
//! Until a store is installed, every account gets the grammar.

pub(crate) use imp::{Extended, parse_request, resolve_scope};

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
        DEFAULT_MAX_CONDITIONS, SearchFilter, parse_search_filter,
    };
    use extenddb_core::expression::search_params::with_unused_names;
    use extenddb_core::types::{AttributeDefinition, AttributeValue, Item, SearchSchemaElement};
    use extenddb_storage::error::StorageError;
    use extenddb_storage::{StorageEngine, VectorItemFilter, VectorSearchEngine};

    /// The HASH attribute and the value that selects its partition, and the
    /// filter the backend applies to each candidate.
    pub(crate) type Scope<'a> = (
        Option<(&'a str, &'a AttributeValue)>,
        Arc<dyn VectorItemFilter>,
    );

    /// A search condition parsed with the extended grammar.
    pub(crate) struct Extended(Arc<SearchFilter>);

    /// Parse the request's search condition with the extended grammar when
    /// the setting enables it for the caller. `None` when it does not, or when
    /// the request has no search condition; the handler then runs its
    /// equality-only validator. A name that neither the search condition nor
    /// `projection` uses is refused once the condition parses.
    ///
    /// Taking the backend's vector search engine means the grammar is only
    /// parsed once the backend has been found to serve vector search: a
    /// backend without it refuses the request before this runs.
    ///
    /// # Errors
    ///
    /// Returns the `ValidationException` of the first failed check, and
    /// `ServiceUnavailableException` when the setting cannot be read.
    pub(crate) async fn parse_request(
        storage: &Arc<dyn StorageEngine>,
        _served_by: &dyn VectorSearchEngine,
        account_id: &str,
        expr: Option<&str>,
        names: Option<&HashMap<String, String>>,
        values: Option<&HashMap<String, AttributeValue>>,
        projection: Option<&str>,
    ) -> Result<Option<Extended>, DynamoDbError> {
        let Some(expr) = expr else {
            return Ok(None);
        };
        // Cross-account access is refused before dispatch, so the caller is
        // the table owner.
        if !super::settings::enabled_accounts(storage)
            .await?
            .enabled_for(account_id, None)
        {
            return Ok(None);
        }
        with_unused_names(
            parse_search_filter(expr, names, values, DEFAULT_MAX_CONDITIONS),
            names,
            &[expr, projection.unwrap_or_default()],
        )
        .map(|filter| Some(Extended(Arc::new(filter))))
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

#[cfg(not(feature = "search-inequality-filters"))]
mod imp {
    use std::collections::HashMap;
    use std::sync::Arc;

    use extenddb_core::error::DynamoDbError;
    use extenddb_core::types::{AttributeDefinition, AttributeValue, SearchSchemaElement};
    use extenddb_storage::{StorageEngine, VectorItemFilter, VectorSearchEngine};

    /// The HASH attribute and the value that selects its partition, and the
    /// filter the backend applies to each candidate.
    pub(crate) type Scope<'a> = (
        Option<(&'a str, &'a AttributeValue)>,
        Arc<dyn VectorItemFilter>,
    );

    /// No value exists: without the feature no request is parsed this way.
    pub(crate) enum Extended {}

    #[allow(clippy::unused_async)]
    pub(crate) async fn parse_request(
        _storage: &Arc<dyn StorageEngine>,
        _served_by: &dyn VectorSearchEngine,
        _account_id: &str,
        _expr: Option<&str>,
        _names: Option<&HashMap<String, String>>,
        _values: Option<&HashMap<String, AttributeValue>>,
        _projection: Option<&str>,
    ) -> Result<Option<Extended>, DynamoDbError> {
        Ok(None)
    }

    pub(crate) fn resolve_scope<'a>(
        extended: &'a Extended,
        _schema: Option<&[SearchSchemaElement]>,
        _definitions: &[AttributeDefinition],
    ) -> Result<Scope<'a>, DynamoDbError> {
        match *extended {}
    }
}
