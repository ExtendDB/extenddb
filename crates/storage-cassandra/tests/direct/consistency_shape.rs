// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PolicySession::run` panics in a debug build when a statement's CQL does
//! not match its kind (#770). The unit tests on `shape_error` cover the rule
//! itself; this test covers that `run` applies it. The panic comes before
//! `run` sends anything, so the statement never reaches Cassandra.

#[cfg(test)]
mod tests {
    use extenddb_storage_cassandra::cassandra_util::{PolicySession as _, Stmt};

    #[cfg(debug_assertions)]
    #[tokio::test]
    async fn run_panics_on_an_lwt_without_a_condition() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = crate::helpers::setup_engine().await;
        let session = engine.session_arc();
        let task = tokio::spawn(async move {
            session
                .run(
                    Stmt::Lwt,
                    "UPDATE ks.t SET a = 1 WHERE pk = 1",
                    cdrs_tokio::query::QueryValues::SimpleValues(vec![]),
                )
                .await
        });
        let error = task
            .await
            .expect_err("run() panics on an LWT with no IF condition");
        assert!(error.is_panic(), "{error:?}");
        let payload = error.into_panic();
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .unwrap_or_default();
        assert!(
            message.contains("an LWT must have an IF condition"),
            "{message}"
        );
        assert!(
            !message.contains("ks.t"),
            "the message must not hold the CQL text: {message}"
        );
    }

    /// `run_batch` panics on a batch that was not built by
    /// `cassandra_util::batch()`, before it sends anything.
    #[cfg(debug_assertions)]
    #[tokio::test]
    async fn run_batch_panics_on_a_batch_at_the_driver_default() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let engine = crate::helpers::setup_engine().await;
        let session = engine.session_arc();
        let batch = cdrs_tokio::query::BatchQueryBuilder::default()
            .add_query(
                "INSERT INTO ks.t (id) VALUES (1)",
                cdrs_tokio::query::QueryValues::SimpleValues(vec![]),
            )
            .build()
            .expect("a batch");
        let task = tokio::spawn(async move { session.run_batch(batch).await });
        let error = task
            .await
            .expect_err("run_batch() panics on a batch at ONE");
        assert!(error.is_panic(), "{error:?}");
        let payload = error.into_panic();
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .unwrap_or_default();
        assert!(
            message.contains("a batch must run at LOCAL_QUORUM"),
            "{message}"
        );
    }
}
