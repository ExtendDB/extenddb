// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The S3 clients of an ExtendDB store and the server's AWS credentials
//! they sign with. [`connect`] opens one store's clients from its
//! [`S3StoreSettings`]: the region from the settings, the endpoint from
//! `endpoint_url` alone, the pinned behaviour version and deadlines, and
//! the standard AWS credential chain, resolved once at startup with the
//! outcome logged under the store's name ([`Startup`]). Resolved temporary
//! keys the SDK cannot renew are a warning, and without an `endpoint_url`
//! one STS GetCallerIdentity request is sent at startup, in a task of its
//! own and over the S3 clients' HTTP client, to log the identity the
//! credentials sign as.
//! [`S3Clients`] follows the region redirects AWS S3 answers and sends a
//! transient failure again under one bound ([`retry`]).
//! [`is_server_credential_error`] says whether a failure is about the
//! server's own credentials, account or clock, which no caller of the
//! store can fix.
//!
//! A store with one configured bucket uses the crate as it is:
//!
//! ```no_run
//! use extenddb_s3::{S3StoreSettings, Startup, connect, is_server_credential_error};
//!
//! # async fn example() {
//! let settings = S3StoreSettings {
//!     region: "us-east-1".to_owned(),
//!     endpoint_url: None,
//!     force_path_style: false,
//! };
//! let startup = Startup {
//!     label: "S3 example store",
//!     until_resolved: "its requests fail until they resolve",
//! };
//! let clients = connect(&settings, &startup).await;
//!
//! // One call on the client of the bucket's region: a redirect to another
//! // region is followed once and remembered, and a transient failure is
//! // sent again under the retry bound.
//! let listed = clients
//!     .call("example-bucket", |client| {
//!         client.list_objects_v2().bucket("example-bucket").send()
//!     })
//!     .await;
//! if let Err(error) = &listed
//!     && is_server_credential_error(error)
//! {
//!     // The server's own credentials, account or clock: the operator's
//!     // fix, never the caller's.
//! }
//!
//! // Then the client of the region the call above found the bucket in,
//! // with the retries held by the caller under the same bound. Only `call`
//! // and `call_within` learn a bucket's region: a request sent on this
//! // client alone gets a redirect as its answer.
//! let client = clients.client_for("example-bucket");
//! let mut backoff = clients.backoff();
//! loop {
//!     let Err(error) = client.head_bucket().bucket("example-bucket").send().await else {
//!         break;
//!     };
//!     if !extenddb_s3::retry::is_transient(&error) {
//!         break;
//!     }
//!     let Some(wait) = backoff.next_wait() else {
//!         break;
//!     };
//!     tokio::time::sleep(wait).await;
//! }
//! # }
//! ```
//!
//! The `test-util` feature adds the hooks a store's tests build clients
//! with: `S3Clients::new` over a configuration of the test's own,
//! `S3Clients::with_retry`, `S3Clients::config` and
//! `retry::RetryPolicy::NONE`. A store enables it in its dev-dependencies.

mod client;
mod clients;
mod codes;
mod credentials;
mod identity;
pub mod retry;

#[cfg(test)]
mod tests;

pub use client::{
    CONNECT_TIMEOUT, READ_TIMEOUT, behavior_version, clients_from, connect, connect_with, timeouts,
};
pub use clients::S3Clients;
pub use codes::{SERVER_CREDENTIAL_CODES, is_server_credential_code, is_server_credential_error};
pub use credentials::{CREDENTIALS_LATER, Startup};

/// The settings of one S3 store's clients.
#[derive(Debug, Clone)]
pub struct S3StoreSettings {
    /// The signing region and the first region a bucket is tried in.
    pub region: String,
    /// An S3-compatible endpoint; `None` for AWS S3.
    pub endpoint_url: Option<String>,
    /// Address buckets in the path rather than the host name.
    pub force_path_style: bool,
}
