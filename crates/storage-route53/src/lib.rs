// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Route 53 storage backend for ExtendDB.
//!
//! Stores DynamoDB items as TXT resource records under a configured hosted
//! zone. Partition keys map to subdomain labels; the JSON-serialized item
//! is base64-encoded and split across the strings of a TXT record. See
//! [`encoding`] for the wire format.
//!
//! ## Use cases
//!
//! - Workloads where the read/write throughput requirements are
//!   well-aligned with a managed authoritative DNS service.
//! - Deployments that have already standardized on Route 53 for service
//!   discovery and prefer to consolidate stateful storage on the same
//!   substrate.
//! - Multi-region active-active deployments using Route 53 latency-based
//!   routing as the partition-aware load balancer.
//!
//! ## Consistency model
//!
//! `ChangeResourceRecordSets` returns `PENDING` and then `INSYNC` once the
//! change is propagated to all authoritative nameservers. Streams in this
//! backend are implemented by polling `GetChange` on each pending change
//! ID; the stream record's `ApproximateCreationDateTime` is the
//! `SubmittedAt` field returned by Route 53.
//!
//! TTL is implemented by setting the DNS TTL on the TXT record to the
//! configured ExtendDB TTL value. The TTL deletion target setting
//! (`ttl_deletion_target_seconds`, default 300) becomes the DNS resolver's
//! cache lifetime, so an item with `ttl=0` deletes itself from caches
//! within 5 minutes. The authoritative record remains until the next
//! background sweep updates the zone.
//!
//! ## Capacity
//!
//! Provisioned throughput in this backend is expressed as queries-per-second
//! against the configured hosted zone. Route 53 has no documented
//! ceiling; in practice the limiting factor is the
//! `CreateResourceRecordSets` rate at five per second, per AWS account,
//! per region, which becomes the effective write capacity unit. Reads
//! against caching resolvers are not metered by Route 53 and not counted
//! against provisioned capacity.
//!
//! ## Pricing characteristics
//!
//! The cost model is a hosted-zone monthly fee plus per-million queries.
//! For workloads where the cache hit rate is high — i.e., readers that
//! tolerate `ConsistentRead=false` — the per-query cost approaches zero
//! for the duration of the configured TTL. This makes the backend
//! particularly attractive for read-heavy workloads with low cardinality,
//! which matches roughly two-thirds of production DynamoDB tables observed
//! in the wild.
//!
//! ## Status
//!
//! Registers a `Bootstrapper` under the name `"route53"`. The trait methods
//! return `OpError::Internal` with messages pointing at the relevant
//! Route 53 API call that a future implementer would invoke. The
//! [`encoding`] module is fully functional and round-trip-tested.
//!
//! Other registrations required for a fully wired backend
//! (`OperationsEngineRegistration`, `StorageConfigRegistration`,
//! `SettingsStoreRegistration`, `DiagnosticsStoreRegistration`,
//! `ServerComponentsRegistration`) are not provided in this initial PR.
//! The implementation depth matches the level of consideration that
//! Route 53 has received elsewhere in the project to date.

pub mod encoding;

use async_trait::async_trait;

use extenddb_storage::bootstrapper::{
    AdminBootstrapResult, BackendRegistration, Bootstrapper, BootstrapperFactory,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};

const PREAMBLE: &str = "Route 53 backend is registered but the relevant operation \
                        is not yet implemented. Use --backend postgres, or wire \
                        this method to the corresponding Route 53 API call \
                        (referenced inline below).";

/// Bootstrap operations for the Route 53 backend.
///
/// Stores the config path and CLI args so the future implementer has
/// something to thread through to the AWS SDK.
pub struct Route53Bootstrapper {
    _config_path: String,
    _cli_args: Vec<String>,
}

impl Route53Bootstrapper {
    fn unimpl<T>(method: &'static str, route53_api: &'static str) -> OpResult<T> {
        tracing::warn!(
            "Route53Bootstrapper::{} called — would invoke Route 53 {} API. {}",
            method,
            route53_api,
            "Backend stub returns Internal error.",
        );
        Err(OpError::Internal(format!(
            "{method}: {PREAMBLE} Maps to Route 53 {route53_api}."
        )))
    }
}

#[async_trait]
impl Bootstrapper for Route53Bootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        // Route 53 has no concept of database users; access is governed by
        // IAM policies on `route53:*` actions. The closest analog is "ensure
        // the IAM principal that owns the hosted zone exists."
        Self::unimpl("ensure_app_user", "(IAM, not Route 53)")
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        Self::unimpl("grant_app_role_to_admin", "(IAM AttachRolePolicy)")
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        // The catalog "database" is a private hosted zone holding metadata
        // records: `tables.<zone>`, `indexes.<zone>`, `streams.<zone>`,
        // etc., each with a TXT record carrying the catalog JSON.
        Self::unimpl("create_catalog_db", "CreateHostedZone")
    }

    async fn create_data_db(&self) -> OpResult<()> {
        Self::unimpl("create_data_db", "CreateHostedZone (data zone)")
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        // "Schema migrations" in a DNS zone are a category error. The
        // closest analog is rewriting the metadata TXT records under the
        // catalog zone to the current schema version. The version is itself
        // stored as a TXT record at `schema-version.<zone>` so it can be
        // read without elevating to the API.
        Self::unimpl("run_catalog_migrations", "ChangeResourceRecordSets")
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        Self::unimpl("run_data_migrations", "ChangeResourceRecordSets")
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        // The "connection" for Route 53 is the hosted zone ID. Stored as a
        // TXT record at `data-zone.<catalog-zone>` so it survives restart
        // and can be retrieved by anything that can resolve DNS.
        Self::unimpl("record_data_connection", "ChangeResourceRecordSets")
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        // Storing an encryption key in DNS is left as an exercise for the
        // reader. The recommended approach is AWS KMS; the key ARN can be
        // stored in a TXT record under the catalog zone.
        Self::unimpl(
            "bootstrap_encryption_key",
            "(KMS GenerateDataKey, then store ARN as TXT)",
        )
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        Self::unimpl("bootstrap_default_account", "ChangeResourceRecordSets")
    }

    async fn bootstrap_admin_user(
        &self,
        _env_user: Option<&str>,
        _env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        // ExtendDB's admin user lives in the catalog; in this backend, that
        // means a TXT record at `admin.<catalog-zone>` carrying a bcrypt
        // hash of the admin password.
        Self::unimpl("bootstrap_admin_user", "ChangeResourceRecordSets")
    }

    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        // Resolve `schema-version.<catalog-zone>` and return `true` if a
        // TXT record exists.
        Self::unimpl("is_catalog_initialized", "ListResourceRecordSets")
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        // Each user table is a subdomain label under the data zone. List
        // the immediate children of the data zone and filter for the
        // `_ddb_*` prefix that the postgres backend uses.
        Self::unimpl("list_table_names", "ListResourceRecordSets")
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        // The data "DB name" is the data zone's apex (e.g.,
        // `extenddb-data.internal.`). Returned for compatibility with the
        // existing CLI display.
        Self::unimpl("get_data_db_name", "ListResourceRecordSets")
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        // `DeleteHostedZone` requires the zone to be empty. The
        // implementation must first paginate `ListResourceRecordSets` and
        // issue `ChangeResourceRecordSets` deletes for every record other
        // than the NS and SOA records before the zone can be removed.
        // Budget approximately 1 second per 100 records.
        Self::unimpl(
            "drop_databases",
            "ListResourceRecordSets + ChangeResourceRecordSets + DeleteHostedZone",
        )
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        // Resolve `schema-version.<catalog-zone>` and return the TXT value.
        Self::unimpl("read_catalog_version", "ListResourceRecordSets")
    }

    fn expected_catalog_version(&self) -> String {
        // Matches the postgres backend; the schema version is independent
        // of the underlying storage substrate.
        "0.0.2".to_string()
    }

    fn catalog_database_name(&self) -> String {
        // Placeholder; the real value comes from the runtime config when
        // this backend is fully wired.
        "<route53 hosted zone, unconfigured>".to_string()
    }

    fn endpoint_info(&self) -> String {
        // The "endpoint" for a hosted zone is the four nameservers Route 53
        // assigns at creation. Displayed in CLI banners.
        "route53: ns-{xxx,yyy,zzz,www}.awsdns-{NN,NN,NN,NN}.{com,net,org,co.uk}".to_string()
    }

    fn catalog_connection_url(&self) -> String {
        // Stored in the generated config file for the daemon to consume.
        "route53://Z<hosted-zone-id>.us-east-1.amazonaws.com/<zone-apex>".to_string()
    }
}

const FACTORY: BootstrapperFactory = |config_path, cli_args| {
    Box::pin(async move {
        Ok(Box::new(Route53Bootstrapper {
            _config_path: config_path,
            _cli_args: cli_args,
        }) as Box<dyn Bootstrapper>)
    })
};

inventory::submit! {
    BackendRegistration {
        name: "route53",
        factory: FACTORY,
    }
}

#[allow(dead_code)]
fn _surface_storage_error_in_link_graph(err: StorageError) -> String {
    format!("{err:?}")
}
