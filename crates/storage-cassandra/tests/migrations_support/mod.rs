// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What the catalog migration tests share: a session to the test node,
//! keyspaces of a test's own prefix, and statements followed by a schema
//! agreement wait.

use std::sync::Arc;
use std::time::Duration;

use extenddb_storage_cassandra::cassandra_util::CassandraSession;
use extenddb_storage_cassandra::engine::CassandraEngine;
use extenddb_storage_cassandra::migrations::wait_for_schema_agreement;

use crate::helpers;

pub const TIMEOUT: Duration = Duration::from_secs(60);

pub async fn session() -> Arc<CassandraSession> {
    Arc::new(
        CassandraEngine::create_session(&helpers::test_config())
            .await
            .expect("connect"),
    )
}

/// A prefix of this test's own, whose keyspaces are dropped at the end.
pub fn new_prefix(tag: &str) -> (String, helpers::DropKeyspacesOnDrop) {
    let prefix = format!("p2m_{tag}_{}", &helpers::unique_test_id()[..8]);
    let drops = helpers::DropKeyspacesOnDrop(vec![
        format!("{prefix}_catalog"),
        format!("{prefix}_account_111111111111"),
    ]);
    (prefix, drops)
}

/// Run `statement`, then wait until every live node agrees on the schema,
/// so the next statement finds what this one created on whichever node it
/// reaches (#637).
pub async fn cql(session: &CassandraSession, statement: &str) {
    session
        .query(statement)
        .await
        .unwrap_or_else(|e| panic!("{statement}: {e}"));
    wait_for_schema_agreement(session)
        .await
        .unwrap_or_else(|e| panic!("after {statement}: {e}"));
}

pub async fn keyspace(session: &CassandraSession, name: &str) {
    cql(
        session,
        &format!(
            "CREATE KEYSPACE IF NOT EXISTS {name} WITH replication = {{'class': \
             'SimpleStrategy', 'replication_factor': 1}}"
        ),
    )
    .await;
}

/// A TCP proxy in front of the test node that stops passing bytes on
/// command: a node that never answers, for the tests of the request bounds.
/// Connections through it stay open, so the driver waits for an answer that
/// never comes rather than failing on a closed connection.
// tests/migrations_stopped.rs compiles this module too and uses no proxy.
#[allow(dead_code)]
pub struct SilencingProxy {
    pub address: String,
    silent: Arc<std::sync::atomic::AtomicBool>,
    accepting: tokio::task::JoinHandle<()>,
}

#[allow(dead_code)]
impl SilencingProxy {
    pub async fn in_front_of(upstream: String) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the proxy");
        let address = listener.local_addr().expect("proxy address").to_string();
        let silent = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&silent);
        let accepting = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                let upstream = upstream.clone();
                let flag = Arc::clone(&flag);
                tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    let (mut from_client, mut to_client) = client.into_split();
                    let (mut from_server, mut to_server) = server.into_split();
                    // Each direction: pass bytes until the proxy is silent,
                    // then read and drop them, so neither side sees the
                    // connection close.
                    let silent = Arc::clone(&flag);
                    let up = tokio::spawn(async move {
                        let mut buf = [0u8; 16384];
                        loop {
                            let n = match from_client.read(&mut buf).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => n,
                            };
                            if silent.load(std::sync::atomic::Ordering::SeqCst) {
                                continue;
                            }
                            if to_server.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    });
                    let silent = Arc::clone(&flag);
                    let down = tokio::spawn(async move {
                        let mut buf = [0u8; 16384];
                        loop {
                            let n = match from_server.read(&mut buf).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => n,
                            };
                            if silent.load(std::sync::atomic::Ordering::SeqCst) {
                                continue;
                            }
                            if to_client.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    });
                    let _ = tokio::join!(up, down);
                });
            }
        });
        Self {
            address,
            silent,
            accepting,
        }
    }

    /// From now on no byte passes in either direction.
    pub fn go_silent(&self) {
        self.silent.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// A session to the test node that sends every request through the
    /// proxy. The driver reads the other nodes of a cluster from
    /// `system.peers` and connects to them directly, so on two nodes the
    /// second one would answer while the proxy is silent; this session
    /// leaves every node but the proxy out of its query plans.
    pub async fn session(&self) -> Arc<CassandraSession> {
        use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
        use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
        use cdrs_tokio::cluster::topology::NodeDistance;
        use cdrs_tokio::cluster::{NodeInfo, NodeTcpConfigBuilder};
        use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
        use cdrs_tokio::load_balancing::node_distance_evaluator::NodeDistanceEvaluator;

        struct OnlyThrough(std::net::SocketAddr);

        impl NodeDistanceEvaluator for OnlyThrough {
            fn compute_distance(&self, node: &NodeInfo) -> Option<NodeDistance> {
                (node.broadcast_rpc_address == self.0).then_some(NodeDistance::Local)
            }
        }

        let config = helpers::test_config();
        let proxy = self.address.parse().expect("the proxy's address");
        let nodes = NodeTcpConfigBuilder::new()
            .with_contact_point(self.address.clone().into())
            .with_authenticator_provider(Arc::new(StaticPasswordAuthenticatorProvider::new(
                config.username.unwrap_or_default(),
                config.password.unwrap_or_default(),
            )))
            .build()
            .await
            .expect("the proxy's node config");
        Arc::new(
            TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), nodes)
                .with_node_distance_evaluator(Box::new(OnlyThrough(proxy)))
                .build()
                .await
                .expect("connect through the proxy"),
        )
    }
}

impl Drop for SilencingProxy {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}
