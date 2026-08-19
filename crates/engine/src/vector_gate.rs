// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend capability gate for vector operations.
//!
//! The decision lives here, in core, rather than in each handler or each backend.
//! A backend hands over a [`VectorSearchEngine`] or it does not: it needs no code
//! to refuse a feature it has not implemented, and the refusal is identical
//! whichever backend is installed. The gate takes the accessor's result rather
//! than a boolean, so there is no state in which a backend claims support without
//! having provided an implementation.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    IndexStatus, TableDescription, VectorIndexDescription, VectorIndexSpecification,
    VectorIndexUpdate,
};
use extenddb_storage::VectorSearchEngine;

/// Reject a `CreateTable` that asks for vector indexes the backend cannot serve.
///
/// Without this the request passes shape validation, reaches a backend that does
/// not read `vector_indexes`, and produces a table with no index. The caller is
/// told the index exists and only finds out otherwise on the first search, which
/// is the worst way to learn it: silently, and later.
///
/// A request carrying no vector indexes, or an empty list, is unaffected, so an
/// ordinary `CreateTable` against a backend without vector support still works.
pub(crate) fn ensure_create_table_supported(
    vector_indexes: Option<&Vec<VectorIndexSpecification>>,
    vector_search: Option<&dyn VectorSearchEngine>,
) -> Result<(), DynamoDbError> {
    let asked_for_vector_indexes = vector_indexes.is_some_and(|v| !v.is_empty());
    if asked_for_vector_indexes && vector_search.is_none() {
        return Err(DynamoDbError::ValidationException(
            "Vector indexes are not supported by this storage backend".to_owned(),
        ));
    }
    Ok(())
}

/// Reject an `UpdateTable` that changes vector indexes on a backend that cannot
/// serve them.
///
/// `UpdateTable` is the second creation path, and it is the one the service's own
/// backfill lifecycle is observable through, so leaving it ungated would let a
/// caller add an index that a backend silently ignores: the same silent-drop hole
/// `CreateTable` was fixed for.
///
/// `Delete` is gated too, deliberately. A backend without vector support cannot
/// hold an index to delete, so the honest answer is that the operation is not
/// supported here, rather than letting the request through to be interpreted as a
/// no-op or a not-found.
pub(crate) fn ensure_update_table_supported(
    vector_index_updates: Option<&Vec<VectorIndexUpdate>>,
    vector_search: Option<&dyn VectorSearchEngine>,
) -> Result<(), DynamoDbError> {
    let asked_for_changes = vector_index_updates.is_some_and(|u| !u.is_empty());
    if asked_for_changes && vector_search.is_none() {
        return Err(DynamoDbError::ValidationException(
            "Vector indexes are not supported by this storage backend".to_owned(),
        ));
    }
    Ok(())
}

/// Resolve the backend's vector-search implementation, or reject the request.
///
/// Returns the engine rather than a unit, so the handler cannot reach a search
/// without passing the gate: there is no separate accessor call it could make
/// instead, and no defaulted method on the backend for it to fall through to.
pub(crate) fn ensure_search_supported(
    vector_search: Option<&dyn VectorSearchEngine>,
) -> Result<&dyn VectorSearchEngine, DynamoDbError> {
    vector_search.ok_or_else(|| {
        DynamoDbError::ValidationException(
            "SearchVectors is not supported by this storage backend".to_owned(),
        )
    })
}

/// Fail an `UpdateTable` whose vector index changes the backend did not apply.
///
/// A post-condition rather than a pre-condition, and the reason it exists is worth
/// stating plainly: the capability gate above only proves a backend *can* serve
/// vector indexes, not that it acted on this request. A backend that declares the
/// capability but never reads `vector_index_updates` returns 200 with the field
/// silently dropped, and the caller is told the change happened. Measured against
/// the SQLite backend before it implemented this path: a Create returned 200 and
/// created nothing, discoverable only on the first search, and a Delete returned
/// 200 while the index stayed ACTIVE, stayed in `DescribeTable`, and kept
/// returning hits. The delete case is the dangerous one, because deleting an index
/// to stop serving embeddings is a thing people do for reasons that are not
/// performance.
///
/// Checked against the description the backend itself returned, so no backend can
/// opt out. Deliberately tolerant about *which* post-state is correct, since that
/// is unmeasured: a created index must merely be present, whatever its status, and
/// a deleted one must merely not be present and `ACTIVE`. That catches doing
/// nothing without asserting a lifecycle this contract has not yet observed.
pub(crate) fn ensure_vector_updates_applied(
    updates: Option<&Vec<VectorIndexUpdate>>,
    description: &TableDescription,
) -> Result<(), DynamoDbError> {
    let Some(updates) = updates else {
        return Ok(());
    };
    for update in updates {
        if let Some(create) = &update.create {
            let present = find_index(description, &create.index_name).is_some();
            if !present {
                return Err(dropped(&create.index_name, "create"));
            }
        }
        if let Some(delete) = &update.delete {
            let still_serving = find_index(description, &delete.index_name)
                .is_some_and(|index| index.index_status == IndexStatus::Active);
            if still_serving {
                return Err(dropped(&delete.index_name, "delete"));
            }
        }
    }
    Ok(())
}

/// Fail a `CreateTable` whose vector indexes the backend did not create.
///
/// Same reasoning as [`ensure_vector_updates_applied`], on the other path. No
/// in-tree backend has failed this, but a backend that reads `vector_indexes` on
/// one path and not the other is a likelier mistake than one that ignores both,
/// because the paths are implemented separately.
pub(crate) fn ensure_vector_indexes_applied(
    requested: Option<&Vec<VectorIndexSpecification>>,
    description: &TableDescription,
) -> Result<(), DynamoDbError> {
    for spec in requested.into_iter().flatten() {
        if find_index(description, &spec.index_name).is_none() {
            return Err(dropped(&spec.index_name, "create"));
        }
    }
    Ok(())
}

fn find_index<'a>(
    description: &'a TableDescription,
    index_name: &str,
) -> Option<&'a VectorIndexDescription> {
    description
        .vector_indexes
        .iter()
        .flatten()
        .find(|index| index.index_name == index_name)
}

/// Reported as an internal fault, not a validation error, because it is a bug in
/// the backend rather than anything the caller did wrong.
fn dropped(index_name: &str, verb: &str) -> DynamoDbError {
    tracing::error!(
        index_name,
        operation = verb,
        "backend declared vector support but did not apply the vector index change"
    );
    DynamoDbError::InternalServerError("Internal server error".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A description carrying the named vector indexes at the given status.
    fn described(indexes: &[(&str, IndexStatus)]) -> TableDescription {
        use extenddb_core::types::{DistanceFunction, VectorAttribute};
        TableDescription {
            vector_indexes: Some(
                indexes
                    .iter()
                    .map(|(name, status)| VectorIndexDescription {
                        index_name: (*name).to_owned(),
                        vector_attribute: VectorAttribute {
                            attribute_name: "emb".to_owned(),
                        },
                        dimensions: 4,
                        search_schema: None,
                        distance_function: DistanceFunction::Cosine,
                        index_status: *status,
                        backfilling: None,
                        index_size_bytes: 0,
                        item_count: 0,
                        index_arn: format!("arn:aws:dynamodb:us-east-1:1:table/t/index/{name}"),
                        projection: None,
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    /// The exact behaviour measured against the SQLite backend before it
    /// implemented this path: `UpdateTable` returned 200 and created nothing, and
    /// the caller only found out on the first search.
    #[test]
    fn a_create_the_backend_ignored_is_an_error_not_a_success() {
        let err = ensure_vector_updates_applied(Some(&vec![create_update()]), &described(&[]))
            .expect_err("a dropped create must not pass");
        assert!(
            matches!(err, DynamoDbError::InternalServerError(_)),
            "a backend bug is not the caller's validation error: {err:?}"
        );
    }

    /// The other measured behaviour, and the more dangerous one: `UpdateTable`
    /// returned 200 while the index stayed ACTIVE and kept returning hits.
    #[test]
    fn a_delete_the_backend_ignored_is_an_error_not_a_success() {
        let err = ensure_vector_updates_applied(
            Some(&vec![delete_update()]),
            &described(&[("vidx", IndexStatus::Active)]),
        )
        .expect_err("a dropped delete must not pass");
        assert!(matches!(err, DynamoDbError::InternalServerError(_)));
    }

    /// Deliberately tolerant about which post-state is correct, since that is
    /// unmeasured. A created index need only be present, at any status.
    #[test]
    fn a_created_index_passes_at_any_status() {
        for status in [
            IndexStatus::Creating,
            IndexStatus::Active,
            IndexStatus::Updating,
        ] {
            assert!(
                ensure_vector_updates_applied(
                    Some(&vec![create_update()]),
                    &described(&[("vidx", status)])
                )
                .is_ok(),
                "status {status:?} must be accepted"
            );
        }
    }

    /// A deleted index may be gone or on its way out; only still serving is wrong.
    #[test]
    fn a_deleted_index_passes_when_absent_or_deleting() {
        assert!(
            ensure_vector_updates_applied(Some(&vec![delete_update()]), &described(&[])).is_ok()
        );
        assert!(
            ensure_vector_updates_applied(
                Some(&vec![delete_update()]),
                &described(&[("vidx", IndexStatus::Deleting)])
            )
            .is_ok()
        );
    }

    /// The guard must not fire on the ordinary case, or every `UpdateTable` breaks.
    #[test]
    fn no_vector_updates_is_always_fine() {
        assert!(ensure_vector_updates_applied(None, &described(&[])).is_ok());
        assert!(ensure_vector_updates_applied(Some(&vec![]), &described(&[])).is_ok());
    }

    /// Only the requested index is checked: an unrelated one being absent, or
    /// present, says nothing about this request.
    #[test]
    fn only_the_requested_index_is_checked() {
        assert!(
            ensure_vector_updates_applied(
                Some(&vec![create_update()]),
                &described(&[
                    ("other", IndexStatus::Active),
                    ("vidx", IndexStatus::Active)
                ])
            )
            .is_ok()
        );
        assert!(
            ensure_vector_updates_applied(
                Some(&vec![delete_update()]),
                &described(&[("other", IndexStatus::Active)])
            )
            .is_ok()
        );
    }

    /// The same hole on the CreateTable path, which is implemented separately and
    /// so can regress independently.
    #[test]
    fn create_table_indexes_the_backend_ignored_are_an_error() {
        assert!(ensure_vector_indexes_applied(Some(&vec![spec()]), &described(&[])).is_err());
        assert!(
            ensure_vector_indexes_applied(
                Some(&vec![spec()]),
                &described(&[("vidx", IndexStatus::Creating)])
            )
            .is_ok()
        );
        assert!(ensure_vector_indexes_applied(None, &described(&[])).is_ok());
    }

    /// Stands in for a backend that implements vector search. Defining it is the
    /// point: the gate cannot be handed a "supported" value without something
    /// that actually implements the trait.
    struct Capable;

    impl VectorSearchEngine for Capable {
        fn search_vectors(
            &self,
            _req: extenddb_storage::VectorSearch<'_>,
        ) -> extenddb_storage::BoxedFuture<'_, extenddb_storage::VectorSearchResult> {
            Box::pin(async { unreachable!("the gate tests never run a search") })
        }
    }

    fn capable() -> &'static dyn VectorSearchEngine {
        &Capable
    }
    use extenddb_core::types::{
        DeleteVectorIndexAction, DistanceFunction, Projection, ProjectionType, VectorAttribute,
    };

    fn spec() -> VectorIndexSpecification {
        VectorIndexSpecification {
            index_name: "vidx".to_owned(),
            vector_attribute: VectorAttribute {
                attribute_name: "emb".to_owned(),
            },
            dimensions: 4,
            distance_function: DistanceFunction::Cosine,
            search_schema: None,
            projection: Some(Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            }),
        }
    }

    fn create_update() -> VectorIndexUpdate {
        VectorIndexUpdate {
            create: Some(spec()),
            delete: None,
        }
    }

    fn delete_update() -> VectorIndexUpdate {
        VectorIndexUpdate {
            create: None,
            delete: Some(DeleteVectorIndexAction {
                index_name: "vidx".to_owned(),
            }),
        }
    }

    fn message(err: &DynamoDbError) -> String {
        match err {
            DynamoDbError::ValidationException(m) => m.clone(),
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn create_table_without_vector_indexes_is_allowed_on_any_backend() {
        // The common case: a backend with no vector support must still serve
        // ordinary CreateTable requests.
        assert!(ensure_create_table_supported(None, None).is_ok());
    }

    #[test]
    fn an_empty_vector_index_list_is_not_a_request_for_vector_indexes() {
        let empty: Vec<VectorIndexSpecification> = Vec::new();
        assert!(ensure_create_table_supported(Some(&empty), None).is_ok());
    }

    #[test]
    fn create_table_with_vector_indexes_is_rejected_on_an_unsupporting_backend() {
        let specs = vec![spec()];
        let err = ensure_create_table_supported(Some(&specs), None)
            .expect_err("must be rejected rather than silently dropped");
        assert_eq!(
            message(&err),
            "Vector indexes are not supported by this storage backend"
        );
    }

    #[test]
    fn create_table_with_vector_indexes_is_allowed_on_a_supporting_backend() {
        let specs = vec![spec()];
        assert!(ensure_create_table_supported(Some(&specs), Some(capable())).is_ok());
    }

    #[test]
    fn search_is_rejected_on_an_unsupporting_backend() {
        let err = ensure_search_supported(None)
            .map(|_| ())
            .expect_err("must be rejected before reaching the backend");
        assert_eq!(
            message(&err),
            "SearchVectors is not supported by this storage backend"
        );
    }

    #[test]
    fn search_is_allowed_on_a_supporting_backend() {
        assert!(ensure_search_supported(Some(capable())).is_ok());
        assert!(ensure_search_supported(Some(capable())).is_ok());
    }

    #[test]
    fn update_table_without_vector_changes_is_allowed_on_any_backend() {
        // The common case. An ordinary UpdateTable, adding a GSI or changing
        // throughput, must still work on a backend with no vector support.
        assert!(ensure_update_table_supported(None, None).is_ok());
        let empty: Vec<VectorIndexUpdate> = Vec::new();
        assert!(ensure_update_table_supported(Some(&empty), None).is_ok());
    }

    #[test]
    fn update_table_creating_a_vector_index_is_rejected_on_an_unsupporting_backend() {
        let updates = vec![create_update()];
        let err = ensure_update_table_supported(Some(&updates), None)
            .expect_err("UpdateTable is the second creation path and must be gated too");
        assert_eq!(
            message(&err),
            "Vector indexes are not supported by this storage backend"
        );
    }

    /// Deleting is gated as well. A backend without support cannot hold an index
    /// to delete, so "not supported here" is the honest answer rather than
    /// letting it through to look like a no-op or a not-found.
    #[test]
    fn update_table_deleting_a_vector_index_is_rejected_on_an_unsupporting_backend() {
        let updates = vec![delete_update()];
        assert!(ensure_update_table_supported(Some(&updates), None).is_err());
    }

    #[test]
    fn update_table_vector_changes_are_allowed_on_a_supporting_backend() {
        let updates = vec![create_update(), delete_update()];
        assert!(ensure_update_table_supported(Some(&updates), Some(capable())).is_ok());
    }
}
