// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A start and an index creation sent together on one table. Start refuses
//! a table with an index that is not `ACTIVE` or being built, and an index
//! creation is refused while the table has a non-terminal operation; with
//! both checks inside the transaction that acts on them, exactly one of two
//! requests that cross is accepted.

use std::time::Instant;

use extenddb_core::types::UpdateTableInput;

use crate::bulk_operation_store::index_creation_refused_message;

use super::*;

/// Crossing trials required per order of the two requests.
const CROSSINGS_PER_ORDER: u64 = 50;

/// Trials allowed per order to collect them.
const TRIALS_PER_ORDER: u64 = 200;

/// The bound on each request of a trial.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How long before `UpdateTable` answered the start must have been called
/// for the trial to count as crossing. `UpdateTable` ends its build a few
/// statements before it answers; the margin keeps a start called in that
/// tail, after the build, out of the count.
const CROSSING_MARGIN: Duration = Duration::from_millis(2);

/// An `UpdateTable` that creates one global secondary index.
fn create_index_request(table_name: &str) -> UpdateTableInput {
    serde_json::from_value(json!({
        "TableName": table_name,
        "AttributeDefinitions": [
            { "AttributeName": "pk", "AttributeType": "S" },
            { "AttributeName": "g", "AttributeType": "S" }
        ],
        "GlobalSecondaryIndexUpdates": [{ "Create": {
            "IndexName": "gix",
            "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" }
        }}]
    }))
    .expect("a valid UpdateTable request")
}

/// What one trial's two requests answered, and when each was called and
/// answered.
struct Trial {
    start_accepted: bool,
    index_accepted: bool,
    start_span: (Instant, Instant),
    update_span: (Instant, Instant),
}

impl Trial {
    /// Each request was called before the other answered, the start with
    /// [`CROSSING_MARGIN`] to spare.
    fn crossed(&self) -> bool {
        self.start_span.0 + CROSSING_MARGIN < self.update_span.1
            && self.update_span.0 < self.start_span.1
    }

    fn timings(&self) -> String {
        let origin = self.start_span.0.min(self.update_span.0);
        let at = |t: Instant| t.saturating_duration_since(origin);
        format!(
            "start {:?} to {:?}, UpdateTable {:?} to {:?}",
            at(self.start_span.0),
            at(self.start_span.1),
            at(self.update_span.0),
            at(self.update_span.1),
        )
    }
}

/// One trial on a fresh table: both requests are released by one barrier,
/// and the one sent second waits `delay` first.
async fn one_trial<E: StorageEngine + 'static>(
    fx: &Fixture<E>,
    trial: u64,
    start_first: bool,
    delay: Duration,
) -> Trial {
    let name = format!("cross_{trial}_{}", unique());
    let table = create_table(fx, &fx.account_a, &name).await;
    let req = new_op(fx, &fx.account_a, &table, None, "fp", 1000);
    let update = create_index_request(&name);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let (start_delay, update_delay) = if start_first {
        (Duration::ZERO, delay)
    } else {
        (delay, Duration::ZERO)
    };

    let start_task = {
        let engine = Arc::clone(&fx.engine);
        let account = fx.account_a.clone();
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            tokio::time::sleep(start_delay).await;
            // Every accepted start stays IN_PROGRESS on its own table, so the
            // account limit sits above the trial count. The time is taken
            // after the call, which does its first read before it returns
            // the future.
            let call = engine.start_bulk_operation(&account, req, 1_000);
            let begin = Instant::now();
            let answer = tokio::time::timeout(REQUEST_TIMEOUT, call).await;
            (answer, (begin, Instant::now()))
        })
    };
    let update_task = {
        let engine = Arc::clone(&fx.engine);
        let account = fx.account_a.clone();
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            tokio::time::sleep(update_delay).await;
            let begin = Instant::now();
            let answer =
                tokio::time::timeout(REQUEST_TIMEOUT, engine.update_table(&account, update)).await;
            (answer, (begin, Instant::now()))
        })
    };

    let (started, start_span) = start_task.await.expect("join the start");
    let started = started
        .unwrap_or_else(|_| panic!("trial {trial}: the start did not answer in time"))
        .expect("start_bulk_operation must not error at the transport level");
    let (updated, update_span) = update_task.await.expect("join the UpdateTable");
    let updated =
        updated.unwrap_or_else(|_| panic!("trial {trial}: UpdateTable did not answer in time"));

    let start_accepted = match &started {
        StartOutcome::Created(_) => true,
        StartOutcome::IndexOperationInProgress => false,
        other => panic!(
            "trial {trial}: unexpected start outcome {}",
            start_kind(other)
        ),
    };
    let index_accepted = match &updated {
        Ok(_) => true,
        Err(StorageError::IndexesInUse(message)) => {
            assert_eq!(message, &index_creation_refused_message(&name));
            false
        }
        Err(other) => panic!("trial {trial}: unexpected UpdateTable error {other:?}"),
    };
    Trial {
        start_accepted,
        index_accepted,
        start_span,
        update_span,
    }
}

/// A start and a global secondary index creation on a fresh table each
/// trial, in both orders, the second sent up to two milliseconds after the
/// first: in no trial where they cross are both accepted, and in every such
/// trial one of them is. Runs until each order has
/// [`CROSSINGS_PER_ORDER`] crossing trials.
pub async fn a_start_and_an_index_creation_that_cross_are_never_both_accepted<
    E: StorageEngine + 'static,
>(
    fx: &Fixture<E>,
) {
    for start_first in [true, false] {
        let (mut crossings, mut starts_won, mut indexes_won) = (0u64, 0u64, 0u64);
        let mut trial = 0;
        while crossings < CROSSINGS_PER_ORDER && trial < TRIALS_PER_ORDER {
            let delay = Duration::from_micros((trial % 20) * 100);
            let outcome = one_trial(fx, trial, start_first, delay).await;
            trial += 1;
            if !outcome.crossed() {
                continue;
            }
            crossings += 1;
            assert!(
                !(outcome.start_accepted && outcome.index_accepted),
                "start first {start_first}, trial {trial}: both were accepted ({})",
                outcome.timings()
            );
            assert!(
                outcome.start_accepted || outcome.index_accepted,
                "start first {start_first}, trial {trial}: both were refused ({})",
                outcome.timings()
            );
            if outcome.start_accepted {
                starts_won += 1;
            } else {
                indexes_won += 1;
            }
        }
        assert_eq!(
            crossings, CROSSINGS_PER_ORDER,
            "start first {start_first}: only {crossings} of {trial} trials crossed"
        );
        eprintln!(
            "start first {start_first}: {trial} trials, {crossings} crossed, start accepted in \
             {starts_won}, index creation in {indexes_won}"
        );
    }
}

/// Starts beyond the engine's pool size, sent while an index creation on the
/// same table holds the table row. The creation takes a second connection
/// for the new index's data table while it holds the row, and each start
/// takes one before it waits on the account's lock or the row. Every request
/// still answers, the creation within a few seconds, and a start and the
/// creation are never both accepted.
pub async fn starts_beyond_the_pool_behind_an_index_creation_all_answer<
    E: StorageEngine + 'static,
>(
    fx: &Fixture<E>,
) {
    for round in 0..3u64 {
        let table = create_table(fx, &fx.account_a, &format!("pool_wait_{round}")).await;
        let engine = Arc::clone(&fx.engine);
        let account = fx.account_a.clone();
        let request = create_index_request(&table.table_name);
        let update = tokio::spawn(async move {
            let called = Instant::now();
            let answer =
                tokio::time::timeout(REQUEST_TIMEOUT, engine.update_table(&account, request)).await;
            (answer, called.elapsed())
        });
        tokio::time::sleep(Duration::from_millis(round)).await;
        let starts: Vec<_> = (0..16)
            .map(|i| {
                let engine = Arc::clone(&fx.engine);
                let account = fx.account_a.clone();
                let req = new_op(fx, &fx.account_a, &table, None, "fp", 1000 + i);
                tokio::spawn(async move {
                    tokio::time::timeout(
                        REQUEST_TIMEOUT,
                        engine.start_bulk_operation(&account, req, 50),
                    )
                    .await
                })
            })
            .collect();
        let mut created = 0;
        for task in starts {
            match task.await.expect("join a start") {
                Ok(Ok(StartOutcome::Created(_))) => created += 1,
                Ok(Ok(StartOutcome::TableBusy | StartOutcome::IndexOperationInProgress)) => {}
                Ok(Ok(other)) => panic!("round {round}: a start answered {}", start_kind(&other)),
                Ok(Err(error)) => panic!("round {round}: a start failed: {error:?}"),
                Err(_) => panic!("round {round}: a start did not answer in {REQUEST_TIMEOUT:?}"),
            }
        }
        let (answer, elapsed) = update.await.expect("join UpdateTable");
        let Ok(answer) = answer else {
            panic!("round {round}: UpdateTable did not answer in {REQUEST_TIMEOUT:?}");
        };
        match answer {
            Ok(_) => {
                assert_eq!(
                    created, 0,
                    "round {round}: a start and the index were both accepted"
                );
                assert!(
                    elapsed < Duration::from_secs(10),
                    "round {round}: UpdateTable answered after {elapsed:?}"
                );
            }
            Err(StorageError::IndexesInUse(message)) => {
                assert_eq!(message, index_creation_refused_message(&table.table_name));
                assert_eq!(
                    created, 1,
                    "round {round}: the creation was refused for one start"
                );
            }
            Err(error) => panic!("round {round}: UpdateTable failed after {elapsed:?}: {error:?}"),
        }
    }
}
