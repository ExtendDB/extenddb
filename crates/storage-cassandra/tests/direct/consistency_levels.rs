// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Each statement kind reaches Cassandra at its policy level (#770).
//!
//! Other tests check two parts of the policy. The unit tests on `params()`
//! check the level of each kind. The scan test checks that each statement
//! goes through `PolicySession::run`. These tests check the level that
//! `run`, the helpers and `run_batch` send to Cassandra. They use two
//! signals that one node gives:
//!
//! - The Unavailable error. The cluster has N nodes. The test keyspace has
//!   a replication factor of 2N + 1, so a quorum of its replicas is N + 1
//!   nodes. The cluster cannot supply N + 1 nodes. Cassandra therefore
//!   returns an Unavailable error for each statement at a quorum level. The
//!   error names the level that Cassandra could not get. A statement at
//!   `ONE` needs only one node, so Cassandra runs it.
//!
//!   For a statement at `LOCAL_SERIAL`, Cassandra 4.1 names `LOCAL_QUORUM`
//!   in the error. For a statement at `SERIAL`, it names `QUORUM`. So the
//!   error does not tell `LOCAL_SERIAL` from `LOCAL_QUORUM`.
//! - The `system.paxos` table. A read at `LOCAL_SERIAL` starts a Paxos
//!   round. Each replica of the partition saves a promise in its own
//!   `system.paxos` table. A read at `LOCAL_QUORUM` saves no promise. So
//!   this table tells a read at `LOCAL_SERIAL` from a read at
//!   `LOCAL_QUORUM`.
//!
//! These tests do not check two levels:
//!
//! - The commit level of an LWT. An LWT fails in its Paxos round, before
//!   its commit. The unit test on `params()` checks the commit level.
//! - The level of `Ddl` and `SystemRead`. A DDL statement and a read of
//!   `system.local` do not wait for the replicas of the test keyspace, at
//!   any level. These tests show only that the two kinds do not wait for
//!   those replicas.
//!
//! The tests use their own session with `FallthroughRetryPolicy`. The
//! driver's default policy sends a statement that gets an Unavailable error
//! to the next node. When no node is left, the driver returns "No nodes
//! available in query plan!", not the Unavailable error.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
    use cdrs_tokio::cluster::NodeTcpConfigBuilder;
    use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::frame::message_error::ErrorType;
    use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
    use cdrs_tokio::query::QueryValues;
    use cdrs_tokio::retry::FallthroughRetryPolicy;
    use extenddb_storage::error::StorageError;
    use extenddb_storage_cassandra::CassandraSession;
    use extenddb_storage_cassandra::cassandra_util::{self, PolicySession as _, Stmt};

    fn none() -> QueryValues {
        QueryValues::SimpleValues(vec![])
    }

    /// A session to the test cluster that returns each error as Cassandra
    /// sends it.
    async fn session_without_retries() -> Arc<CassandraSession> {
        let config = crate::helpers::test_config();
        let mut node = NodeTcpConfigBuilder::new();
        for contact_point in &config.contact_points {
            node = node.with_contact_point(contact_point.clone().into());
        }
        let (Some(username), Some(password)) = (&config.username, &config.password) else {
            panic!("test_config sets no credentials");
        };
        let cluster = node
            .with_authenticator_provider(Arc::new(StaticPasswordAuthenticatorProvider::new(
                username, password,
            )))
            .build()
            .await
            .expect("cluster config");
        let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster)
            .with_retry_policy(Box::new(FallthroughRetryPolicy))
            .build()
            .await
            .expect("session");
        Arc::new(session)
    }

    /// A test keyspace with one table `t (pk text PRIMARY KEY, v int)`, and
    /// a session that does not retry. `drop` removes the keyspace.
    struct TestKeyspace {
        session: Arc<CassandraSession>,
        keyspace: String,
    }

    impl TestKeyspace {
        /// A keyspace with `replicas` replicas in the test datacenter.
        async fn with_replicas(replicas: usize) -> Self {
            let session = session_without_retries().await;
            let keyspace = format!("consistency_{}", crate::helpers::unique_test_id());
            let datacenter = crate::helpers::test_config().datacenter;
            for cql in [
                format!(
                    "CREATE KEYSPACE {keyspace} WITH replication = \
                     {{'class': 'NetworkTopologyStrategy', '{datacenter}': {replicas}}}"
                ),
                format!("CREATE TABLE {keyspace}.t (pk text PRIMARY KEY, v int)"),
            ] {
                cassandra_util::ddl::<StorageError>(&session, &cql, "set up")
                    .await
                    .expect("set up the keyspace");
            }
            Self { session, keyspace }
        }

        /// A keyspace whose quorum is one more than the cluster's node count.
        async fn under_replicated() -> Self {
            let session = session_without_retries().await;
            let peers = cassandra_util::system_read::<StorageError>(
                &session,
                "SELECT peer FROM system.peers",
                none(),
                "count the peers",
            )
            .await
            .expect("read system.peers");
            Self::with_replicas(2 * (peers.len() + 1) + 1).await
        }

        fn session(&self) -> Arc<CassandraSession> {
            Arc::clone(&self.session)
        }

        fn select(&self, pk: &str) -> String {
            format!("SELECT v FROM {}.t WHERE pk = '{pk}'", self.keyspace)
        }

        /// The number of Paxos promises that the node holds for partition
        /// `pk` of table `t`.
        async fn paxos_rows(&self, pk: &str) -> usize {
            let ids = cassandra_util::system_read::<StorageError>(
                &self.session,
                "SELECT id FROM system_schema.tables WHERE keyspace_name = ? AND table_name = 't'",
                cdrs_tokio::query_values!(self.keyspace.as_str()),
                "table id",
            )
            .await
            .expect("read the table id");
            let id: uuid::Uuid = cassandra_util::get_column::<_, StorageError>(&ids[0], "id", "id")
                .expect("the table id");
            cassandra_util::system_read::<StorageError>(
                &self.session,
                "SELECT cf_id FROM system.paxos WHERE row_key = textAsBlob((text) ?) AND cf_id = ?",
                cdrs_tokio::query_values!(pk, id),
                "paxos rows",
            )
            .await
            .expect("read system.paxos")
            .len()
        }

        async fn drop(self) {
            let cql = format!("DROP KEYSPACE IF EXISTS {}", self.keyspace);
            cassandra_util::ddl::<StorageError>(&self.session, &cql, "tear down")
                .await
                .expect("drop the keyspace");
        }
    }

    /// Collect the `tracing` events emitted on the current thread. Each entry
    /// is `"<LEVEL> <message>"`. The design is the one in
    /// `storage-sqlite/src/schema.rs`: one process-wide subscriber forwards
    /// each event to the buffer of the thread that emitted it. A per-thread
    /// `set_default` could leave a callsite cached as disabled by a test on
    /// another thread.
    mod log_capture {
        use std::cell::RefCell;
        use std::sync::{Arc, Mutex};

        pub(super) type Lines = Arc<Mutex<Vec<String>>>;

        thread_local! {
            static CAPTURING: RefCell<Option<Lines>> = const { RefCell::new(None) };
        }

        struct Forward;

        struct Message(String);

        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }

        impl tracing::Subscriber for Forward {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, event: &tracing::Event<'_>) {
                CAPTURING.with(|c| {
                    if let Some(lines) = c.borrow().as_ref() {
                        let mut message = Message(String::new());
                        event.record(&mut message);
                        if let Ok(mut lines) = lines.lock() {
                            lines.push(format!("{} {}", event.metadata().level(), message.0));
                        }
                    }
                });
            }
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }

        /// Stops capturing on drop.
        pub(super) struct Guard;

        impl Drop for Guard {
            fn drop(&mut self) {
                CAPTURING.with(|c| c.borrow_mut().take());
            }
        }

        /// Capture this thread's events until the guard is dropped. Use it
        /// from a current-thread runtime (the `#[tokio::test]` default).
        pub(super) fn capture() -> (Guard, Lines) {
            static INSTALL: std::sync::Once = std::sync::Once::new();
            INSTALL.call_once(|| {
                tracing::subscriber::set_global_default(Forward)
                    .expect("no other global subscriber in this test binary");
            });
            let lines = Lines::default();
            CAPTURING.with(|c| *c.borrow_mut() = Some(Arc::clone(&lines)));
            (Guard, lines)
        }
    }

    /// The level that Cassandra names in an Unavailable error, or a panic
    /// that names what came back instead.
    fn unavailable_level<T: std::fmt::Debug>(
        result: Result<T, cdrs_tokio::error::Error>,
    ) -> Consistency {
        match result {
            Err(cdrs_tokio::error::Error::Server { body, .. }) => match body.ty {
                ErrorType::Unavailable(error) => error.cl,
                other => panic!("not an Unavailable error: {other:?}"),
            },
            other => panic!("not a server error: {other:?}"),
        }
    }

    /// The level named in the Unavailable error inside a helper's error
    /// text. The helpers that return rows, and `query_lwt`, keep the
    /// driver's error text.
    fn level_in(error: &str) -> Consistency {
        for (name, level) in [
            ("cl: LocalQuorum", Consistency::LocalQuorum),
            ("cl: Quorum", Consistency::Quorum),
            ("cl: One", Consistency::One),
        ] {
            if error.contains("Unavailable") && error.contains(name) {
                return level;
            }
        }
        panic!("no Unavailable level in: {error}")
    }

    #[tokio::test]
    async fn run_sends_each_kind_at_its_level() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let keyspace = TestKeyspace::under_replicated().await;
        let session = keyspace.session();
        let ks = keyspace.keyspace.clone();
        let insert = format!("INSERT INTO {ks}.t (pk, v) VALUES ('a', 1)");

        for (kind, cql) in [
            (Stmt::Read, keyspace.select("a")),
            (Stmt::SerialRead, keyspace.select("a")),
            (Stmt::Write, insert.clone()),
            (Stmt::Lwt, format!("{insert} IF NOT EXISTS")),
        ] {
            let result = session.run(kind, &cql, none()).await;
            assert_eq!(
                unavailable_level(result),
                Consistency::LocalQuorum,
                "{kind:?}"
            );
        }

        session
            .run(
                Stmt::Ddl,
                format!("ALTER TABLE {ks}.t WITH comment = 'x'"),
                none(),
            )
            .await
            .expect("Ddl does not wait for the keyspace replicas");
        session
            .run(
                Stmt::SystemRead,
                "SELECT release_version FROM system.local",
                none(),
            )
            .await
            .expect("SystemRead does not wait for the keyspace replicas");

        keyspace.drop().await;
    }

    #[tokio::test]
    async fn only_a_serial_read_runs_a_paxos_round() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        // `system.paxos` is local to each node, and the read of it can go to
        // any node. On one node, that node holds every promise.
        let session = session_without_retries().await;
        let peers = cassandra_util::system_read::<StorageError>(
            &session,
            "SELECT peer FROM system.peers",
            none(),
            "count the peers",
        )
        .await
        .expect("read system.peers");
        if !peers.is_empty() {
            eprintln!("skipping: system.paxos is checked on a one-node cluster only");
            return;
        }
        let keyspace = TestKeyspace::with_replicas(1).await;
        let session = keyspace.session();

        session
            .run(Stmt::Read, keyspace.select("read"), none())
            .await
            .expect("Read");
        session
            .run(Stmt::SerialRead, keyspace.select("serial"), none())
            .await
            .expect("SerialRead");
        cassandra_util::query_rows::<StorageError>(&session, &keyspace.select("rows"), none(), "t")
            .await
            .expect("query_rows");
        cassandra_util::query_optional::<StorageError>(
            &session,
            &keyspace.select("optional"),
            none(),
            "t",
        )
        .await
        .expect("query_optional");
        cassandra_util::query_rows_serial::<StorageError>(
            &session,
            &keyspace.select("rows_serial"),
            none(),
            "t",
        )
        .await
        .expect("query_rows_serial");

        for (pk, promises) in [
            ("read", 0),
            ("serial", 1),
            ("rows", 0),
            ("optional", 0),
            ("rows_serial", 1),
        ] {
            assert_eq!(keyspace.paxos_rows(pk).await, promises, "{pk}");
        }

        keyspace.drop().await;
    }

    #[tokio::test]
    async fn the_helpers_send_at_their_levels() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let keyspace = TestKeyspace::under_replicated().await;
        let session = keyspace.session();
        let ks = keyspace.keyspace.clone();
        let select = keyspace.select("a");
        let insert = format!("INSERT INTO {ks}.t (pk, v) VALUES ('a', 1)");
        let insert_if = format!("{insert} IF NOT EXISTS");

        let rows = cassandra_util::query_rows::<StorageError>(&session, &select, none(), "t")
            .await
            .expect_err("query_rows");
        let serial =
            cassandra_util::query_rows_serial::<StorageError>(&session, &select, none(), "t")
                .await
                .expect_err("query_rows_serial");
        let optional =
            cassandra_util::query_optional::<StorageError>(&session, &select, none(), "t")
                .await
                .expect_err("query_optional");
        let lwt = cassandra_util::query_lwt(&session, &insert_if, none())
            .await
            .expect_err("query_lwt");
        for (helper, error) in [
            ("query_rows", rows.to_string()),
            ("query_rows_serial", serial.to_string()),
            ("query_optional", optional.to_string()),
            ("query_lwt", lwt.to_string()),
        ] {
            assert_eq!(level_in(&error), Consistency::LocalQuorum, "{helper}");
        }

        // `execute` and `apply_lwt` return only "Database error", and log the
        // driver's error. The level comes from the log line.
        let (guard, lines) = log_capture::capture();
        cassandra_util::execute::<StorageError>(&session, &insert, none(), "execute")
            .await
            .expect_err("execute");
        cassandra_util::apply_lwt::<StorageError>(&session, &insert_if, none(), "apply_lwt")
            .await
            .expect_err("apply_lwt");
        drop(guard);
        let lines = lines.lock().expect("the log lines").clone();
        for helper in ["execute", "apply_lwt"] {
            let line = lines
                .iter()
                .find(|line| line.contains(&format!("{helper} ")))
                .unwrap_or_else(|| panic!("no log line for {helper}: {lines:?}"));
            assert_eq!(level_in(line), Consistency::LocalQuorum, "{helper}");
        }

        cassandra_util::ddl::<StorageError>(
            &session,
            &format!("ALTER TABLE {ks}.t WITH comment = 'y'"),
            "t",
        )
        .await
        .expect("ddl does not wait for the keyspace replicas");
        cassandra_util::system_read::<StorageError>(
            &session,
            "SELECT release_version FROM system.local",
            none(),
            "t",
        )
        .await
        .expect("system_read does not wait for the keyspace replicas");

        keyspace.drop().await;
    }

    #[tokio::test]
    async fn run_batch_sends_a_batch_from_batch_at_its_levels() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let keyspace = TestKeyspace::under_replicated().await;
        let session = keyspace.session();
        let ks = keyspace.keyspace.clone();

        let plain = cassandra_util::batch()
            .add_query(
                format!("INSERT INTO {ks}.t (pk, v) VALUES ('a', 1)"),
                none(),
            )
            .add_query(
                format!("INSERT INTO {ks}.t (pk, v) VALUES ('b', 2)"),
                none(),
            )
            .build()
            .expect("a batch");
        assert_eq!(
            unavailable_level(session.run_batch(plain).await),
            Consistency::LocalQuorum,
            "a batch with no condition"
        );

        // With no serial level, the Paxos round of a conditional batch runs
        // at SERIAL, and Cassandra names QUORUM. LOCAL_QUORUM here shows
        // that `batch()` sets LOCAL_SERIAL.
        let conditional = cassandra_util::batch()
            .add_query(
                format!("INSERT INTO {ks}.t (pk, v) VALUES ('a', 1) IF NOT EXISTS"),
                none(),
            )
            .build()
            .expect("a batch");
        assert_eq!(
            unavailable_level(session.run_batch(conditional).await),
            Consistency::LocalQuorum,
            "a conditional batch"
        );

        keyspace.drop().await;
    }
}
