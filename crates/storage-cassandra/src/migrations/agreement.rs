// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Waiting until every live node agrees on the schema (#637).
//!
//! A statement that follows a `CREATE TABLE` can reach a node that does not
//! have the table yet. The runner waits after every DDL statement, before
//! every ledger row and before it releases the lock, until every live node
//! reports the same `schema_version`. Each node is asked for its own version
//! in `system.local`, on a connection to that node: reading `system.local`
//! from one node and `system.peers` from another never sees the second node's
//! own version, and a node that is down keeps its last version in its peers'
//! `system.peers`, so the wait in #457 could not agree while a node was down.
//! A node the driver reports down is not live and is left out; it takes the
//! schema from its peers when it comes back. A node the driver does not
//! report down, and that does not answer the version read within the
//! request timeout, is still in the topology the next statement is routed
//! over, so its schema is unknown and the wait goes on; when the agreement
//! timeout passes the error names it. cdrs-tokio 9.0.2 has no agreement wait
//! of its own.

use std::time::{Duration, Instant};

use cdrs_tokio::cluster::topology::NodeState;
use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::frame::{Envelope, Flags, Version};
use cdrs_tokio::transport::CdrsTransport;
use extenddb_storage::migrations::MigrationError;

use super::connect::{REQUEST_TIMEOUT, column};
use crate::cassandra_util::CassandraSession;

/// How long the runner waits for agreement before it fails the step.
pub const AGREEMENT_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the versions are read again while they differ.
const POLL: Duration = Duration::from_millis(200);

/// One node the driver does not report down: its schema version, or no
/// answer within the request timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeVersion {
    pub(crate) node: String,
    /// `None` when the node did not answer within the request timeout. It is
    /// still up in the driver's topology, so the next statement may reach it,
    /// and its schema is unknown.
    pub(crate) version: Option<String>,
}

/// Wait until every live node reports the same schema version, for up to
/// `timeout`.
///
/// # Errors
///
/// The versions still differ, a live node did not answer, or no node
/// answered, when `timeout` passes; the error lists each node's version or
/// that it did not answer.
pub(crate) async fn wait_for_agreement(
    session: &CassandraSession,
    timeout: Duration,
) -> Result<(), MigrationError> {
    wait_for_agreement_with(session, timeout, || {}).await
}

/// [`wait_for_agreement`] calling `answered` after each round in which a
/// node answered: a run under the migration lock records progress there, so
/// a wait that lasts longer than the lease's stall while the nodes answer
/// does not stop the renewal.
///
/// # Errors
///
/// As [`wait_for_agreement`].
pub(crate) async fn wait_for_agreement_with(
    session: &CassandraSession,
    timeout: Duration,
    answered: impl Fn(),
) -> Result<(), MigrationError> {
    wait_over(|| live_versions(session), timeout, answered).await
}

/// The wait itself, over `round`, which reads the live nodes' versions once.
async fn wait_over<Round, Read>(
    mut round: Round,
    timeout: Duration,
    answered: impl Fn(),
) -> Result<(), MigrationError>
where
    Round: FnMut() -> Read,
    Read: std::future::Future<Output = Vec<NodeVersion>>,
{
    let started = Instant::now();
    loop {
        let versions = round().await;
        if any_answered(&versions) {
            answered();
        }
        if agree(&versions) {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            let listed: Vec<String> = versions
                .iter()
                .map(|v| match &v.version {
                    Some(version) => format!("{} at {version}", v.node),
                    None => format!(
                        "{} did not answer within {} s",
                        v.node,
                        REQUEST_TIMEOUT.as_secs()
                    ),
                })
                .collect();
            return Err(MigrationError::Store(format!(
                "the live Cassandra nodes did not agree on the schema within {} s: {}",
                // A round reads every live node, each within the request
                // timeout, so the wait can end past `timeout`: say how long
                // it waited.
                started.elapsed().as_secs().max(timeout.as_secs()),
                if listed.is_empty() {
                    "no node answered".to_owned()
                } else {
                    listed.join(", ")
                }
            )));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Whether a node answered in a round of the wait: what a run under the
/// lock records as progress.
pub(crate) fn any_answered(versions: &[NodeVersion]) -> bool {
    versions.iter().any(|v| v.version.is_some())
}

/// Whether the nodes agree: at least one node answered, every node
/// answered, and every answer is the same version. A node that did not
/// answer is unknown, not absent: it may serve the next statement.
pub(crate) fn agree(versions: &[NodeVersion]) -> bool {
    let Some(first) = versions.first().and_then(|first| first.version.as_ref()) else {
        return false;
    };
    versions.iter().all(|v| v.version.as_ref() == Some(first))
}

/// The schema version of every node the driver does not report down, read
/// from that node's own `system.local` on a connection to it; `None` for a
/// node that cannot be reached, or does not answer, within the request
/// timeout.
async fn live_versions(session: &CassandraSession) -> Vec<NodeVersion> {
    let metadata = session.cluster_metadata();
    let mut versions = Vec::new();
    for node in metadata.nodes().values() {
        if node.state() == NodeState::Down {
            continue;
        }
        let address = node.broadcast_rpc_address().to_string();
        let read = async {
            let connection = node.persistent_connection().await.ok()?;
            let envelope = Envelope::new_req_query(
                "SELECT schema_version FROM system.local".to_owned(),
                Consistency::One,
                None,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
                Flags::empty(),
                Version::V4,
            );
            let answer = connection.write_envelope(&envelope, false).await.ok()?;
            let row = answer
                .response_body()
                .ok()?
                .into_rows()?
                .into_iter()
                .next()?;
            let version: uuid::Uuid = column(&row, "schema_version", "read schema_version").ok()?;
            Some(version.to_string())
        };
        let version = tokio::time::timeout(REQUEST_TIMEOUT, read)
            .await
            .ok()
            .flatten();
        versions.push(NodeVersion {
            node: address,
            version,
        });
    }
    versions.sort_by(|a, b| a.node.cmp(&b.node));
    versions
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::{NodeVersion, agree, any_answered, wait_over};

    fn at(node: &str, version: &str) -> NodeVersion {
        NodeVersion {
            node: node.to_owned(),
            version: Some(version.to_owned()),
        }
    }

    fn silent(node: &str) -> NodeVersion {
        NodeVersion {
            node: node.to_owned(),
            version: None,
        }
    }

    #[test]
    fn the_nodes_agree_only_when_one_answered_and_all_answers_match() {
        assert!(!agree(&[]));
        assert!(agree(&[at("a", "v1")]));
        assert!(agree(&[at("a", "v1"), at("b", "v1")]));
        assert!(!agree(&[at("a", "v1"), at("b", "v2")]));
    }

    /// A node the driver does not report down, and that did not answer the
    /// version read, may serve the next statement, so the nodes do not agree
    /// until it answers (finding 3).
    #[test]
    fn a_live_node_that_did_not_answer_is_not_counted_as_agreeing() {
        assert!(!agree(&[silent("a")]));
        assert!(!agree(&[at("a", "v1"), silent("b")]));
        assert!(!agree(&[silent("a"), at("b", "v1")]));
        assert!(!agree(&[at("a", "v1"), silent("b"), at("c", "v1")]));
        assert!(agree(&[at("a", "v1"), at("b", "v1"), at("c", "v1")]));
    }

    /// A round in which a node answered is progress of a run under the
    /// lock, even when the nodes do not agree yet; a round in which none
    /// answered is not.
    #[test]
    fn a_round_with_an_answer_is_progress_and_one_without_is_not() {
        assert!(any_answered(&[at("a", "v1"), at("b", "v2")]));
        assert!(any_answered(&[silent("a"), at("b", "v1")]));
        assert!(!any_answered(&[silent("a"), silent("b")]));
        assert!(!any_answered(&[]));
    }

    /// The wait reports each round in which a node answered, including one
    /// in which the nodes still differ, and not a round in which none did.
    #[tokio::test]
    async fn the_wait_reports_each_round_in_which_a_node_answered() {
        let rounds = [
            vec![silent("a"), silent("b")],
            vec![at("a", "v1"), at("b", "v2")],
            vec![at("a", "v2"), at("b", "v2")],
        ];
        let read = AtomicUsize::new(0);
        let answered = AtomicUsize::new(0);
        let waited = wait_over(
            || {
                let versions = rounds[read.fetch_add(1, Ordering::SeqCst)].clone();
                async move { versions }
            },
            Duration::from_secs(30),
            || {
                answered.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await;
        assert!(waited.is_ok(), "{waited:?}");
        assert_eq!(read.load(Ordering::SeqCst), 3);
        assert_eq!(answered.load(Ordering::SeqCst), 2);
    }
}
