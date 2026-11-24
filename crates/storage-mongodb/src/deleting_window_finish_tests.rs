// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `DeleteTable` that stops after its `DELETING` mark is finished: by the
//! call's own task when its caller gives up, or by the control plane, and a
//! second run of the same delete leaves a new table of the name whole. The
//! replica set and the fixture come from `deleting_window_tests.rs`.

use std::time::{Duration, Instant};

use extenddb_storage::WorkerStore;

use super::*;

/// A DeleteTable whose caller gives up while a drop waits (a client read
/// timeout) still finishes the delete once the drop can run, with no further
/// call: the steps after the mark run in a task the request only awaits. A
/// transaction holds the data collection, so the drop waits for it, and the
/// call is dropped after 2 s, as an HTTP client's timeout drops the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_table_whose_caller_gives_up_still_finishes() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (desc, index_ids) = f.create_full_table().await;

    let mut session = f.engine.client.start_session().await.expect("session");
    session
        .start_transaction()
        .await
        .expect("start a transaction");
    f.engine
        .data_db
        .collection::<Document>(&crate::data::data_collection_name(&desc.table_id))
        .insert_one(doc! { "_id": "held", "pk": "held" })
        .session(&mut session)
        .await
        .expect("write in the transaction");

    let gave_up = tokio::time::timeout(
        Duration::from_secs(2),
        f.engine.delete_table(ACCOUNT, delete_input()),
    )
    .await;
    assert!(
        gave_up.is_err(),
        "the drop waits for the transaction, so the call is still running at 2 s: {gave_up:?}"
    );
    assert_eq!(f.status().await.as_deref(), Some("DELETING"));

    session
        .abort_transaction()
        .await
        .expect("end the transaction");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let left = f.leftovers(&desc, &index_ids).await;
        if left.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "30 s after the transaction ended, with no further call, these remain: {left:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    f.finish().await;
}

/// The control plane finishes a delete whose DeleteTable stopped after the
/// mark (a step that failed, a process that died): the document, the
/// collections, the index documents, the tags and the stream shards all go,
/// and the name can be created again. A transition time in the past is one a
/// stopped DeleteTable leaves once `DELETE_FINISH_AFTER` has passed; none at
/// all is what a build before this one left.
#[tokio::test]
async fn the_control_plane_finishes_a_delete_left_deleting() {
    for due in [transition_in(-1), Bson::Null] {
        let Some(f) = Fixture::new().await else {
            return;
        };
        let (desc, index_ids) = f.create_full_table().await;
        f.leave_deleting(due.clone()).await;

        let transitions = f
            .engine
            .process_control_plane_transitions()
            .await
            .expect("control plane pass");
        assert_eq!(
            transitions,
            [(TABLE.to_owned(), "DELETING → deleted")],
            "due {due:?}"
        );
        let left = f.leftovers(&desc, &index_ids).await;
        assert!(left.is_empty(), "due {due:?}: these remain: {left:?}");
        f.create_full_table().await;

        f.finish().await;
    }
}

/// Two runs of one delete can overlap (the request's task and a control-plane
/// pass), so one can reach its last steps after the other has removed the
/// document and the name has been created again. That late run leaves the new
/// table whole, its tag included: tags are keyed by the ARN, which the new
/// table shares.
#[tokio::test]
async fn a_late_second_run_of_a_delete_leaves_a_new_table_of_the_name_whole() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (old, old_index_ids) = f.create_full_table().await;
    f.leave_deleting(transition_in(60)).await;

    f.engine
        .finish_table_delete(ACCOUNT, TABLE, &old.table_id, &old.table_arn)
        .await
        .expect("the first run");
    let left = f.leftovers(&old, &old_index_ids).await;
    assert!(
        left.is_empty(),
        "after the first run these remain: {left:?}"
    );

    let (new, new_index_ids) = f.create_full_table().await;
    assert_eq!(
        new.table_arn, old.table_arn,
        "precondition: the ARN is shared"
    );
    f.engine
        .finish_table_delete(ACCOUNT, TABLE, &old.table_id, &old.table_arn)
        .await
        .expect("the late second run");
    let left = f.leftovers(&new, &new_index_ids).await;
    assert_eq!(
        left.len(),
        6,
        "the new table's document, collections, index document, tag and stream shards \
         stay: {left:?}"
    );
    assert_eq!(f.status().await.as_deref(), Some("ACTIVE"));

    f.finish().await;
}

/// A delete whose transition time is still ahead belongs to a DeleteTable
/// that may still be running, and the control plane leaves it alone.
#[tokio::test]
async fn the_control_plane_leaves_a_delete_that_is_not_due() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (desc, index_ids) = f.create_full_table().await;
    f.leave_deleting(transition_in(60)).await;

    let transitions = f
        .engine
        .process_control_plane_transitions()
        .await
        .expect("control plane pass");
    assert!(transitions.is_empty(), "{transitions:?}");
    assert_eq!(f.status().await.as_deref(), Some("DELETING"));
    assert_eq!(f.leftovers(&desc, &index_ids).await.len(), 6);

    f.finish().await;
}

/// A run whose last step meets another run removing the document (a write
/// conflict on the transaction) answers `Ok`, not an internal error, and
/// leaves the document to that run. The other run is held inside its
/// transaction, with the document and the tag deleted and not yet committed.
#[tokio::test]
async fn a_run_that_meets_another_removing_the_document_leaves_it_to_that_run() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (desc, index_ids) = f.create_full_table().await;
    f.leave_deleting(transition_in(60)).await;

    let mut other = f.engine.client.start_session().await.expect("session");
    other
        .start_transaction()
        .await
        .expect("start a transaction");
    f.engine
        .catalog_db
        .collection::<Document>("tables")
        .delete_one(doc! {
            "_id": { "account_id": ACCOUNT, "table_name": TABLE },
            "table_id": &desc.table_id,
            "table_status": "DELETING",
        })
        .session(&mut other)
        .await
        .expect("the other run deletes the document");
    f.engine
        .catalog_db
        .collection::<Document>("tags")
        .delete_many(doc! { "resource_arn": &desc.table_arn })
        .session(&mut other)
        .await
        .expect("the other run deletes the tag");

    f.engine
        .finish_table_delete(ACCOUNT, TABLE, &desc.table_id, &desc.table_arn)
        .await
        .expect("a write conflict leaves the document to the other run");
    assert_eq!(f.status().await.as_deref(), Some("DELETING"));

    other
        .commit_transaction()
        .await
        .expect("the other run commits");
    let left = f.leftovers(&desc, &index_ids).await;
    assert!(left.is_empty(), "these remain: {left:?}");

    f.finish().await;
}
