// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cloud Bigtable admin operations (`CreateTable`, `DeleteTable`, `ListTables`,
//! `ModifyColumnFamilies`, and startup `GetAppProfile` validation).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use extenddb_storage::error::StorageError;
use gcp_auth::TokenProvider;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{
    AppProfile, ColumnFamily, CreateTableRequest, DeleteTableRequest, GcRule, GetAppProfileRequest,
    GetTableRequest, ListTablesRequest, ModifyColumnFamiliesRequest, Table,
    app_profile::RoutingPolicy,
    bigtable_instance_admin_client::BigtableInstanceAdminClient,
    bigtable_table_admin_client::BigtableTableAdminClient,
    gc_rule,
    modify_column_families_request::{Modification, modification::Mod},
    table,
};
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::data::client::{BigtableClient, MAX_GRPC_MESSAGE_BYTES, classify_grpc_status};

const BIGTABLE_ADMIN_ENDPOINT: &str = "https://bigtableadmin.googleapis.com:443";
const BIGTABLE_ADMIN_HOST: &str = "bigtableadmin.googleapis.com";
const BIGTABLE_ADMIN_SCOPE: &str = "https://www.googleapis.com/auth/bigtable.admin";
const ADMIN_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const ADMIN_RPC_TIMEOUT: Duration = Duration::from_mins(1);

/// Typed Cloud Bigtable Table/Instance Admin error preserving `tonic::Status`
/// codes so callers classify transient quota, transport, and deadline errors
/// via [`classify_grpc_status`] rather than display-string matching.
#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("{context}: {status}")]
    Rpc {
        context: String,
        #[source]
        status: Status,
    },

    #[error("{0}")]
    Transport(String),

    #[error("{0}")]
    Auth(String),

    #[error("{0}")]
    InvalidAppProfile(String),
}

impl AdminError {
    /// Convert this admin error into [`StorageError`], delegating gRPC status
    /// codes (`ResourceExhausted`, `Unavailable`, `DeadlineExceeded`,
    /// `Aborted`, `NotFound`, and HTTP/2 `GoAway`) to [`classify_grpc_status`].
    #[must_use]
    pub fn into_storage_error(self) -> StorageError {
        match self {
            Self::Rpc { context, status } => classify_grpc_status(&context, &status),
            Self::Transport(msg) => StorageError::Connection(msg),
            Self::Auth(msg) => StorageError::Internal(msg),
            Self::InvalidAppProfile(msg) => StorageError::Validation(msg),
        }
    }
}

/// Convenience helper to build a GC rule that keeps at most `n` cell versions.
#[must_use]
pub fn gc_max_versions(n: i32) -> GcRule {
    GcRule {
        rule: Some(gc_rule::Rule::MaxNumVersions(n)),
    }
}

/// Validate that an [`AppProfile`] uses single-cluster routing with single-row
/// transactions (`allow_transactional_writes`) enabled, which Cloud Bigtable
/// requires for `CheckAndMutateRow`.
///
/// # Errors
///
/// Returns an actionable configuration error message when `profile` uses
/// multi-cluster routing, lacks a routing policy, or disables
/// `allow_transactional_writes`.
pub fn validate_app_profile_proto(
    profile: &AppProfile,
    instance_id: &str,
    profile_id: &str,
) -> Result<(), String> {
    match &profile.routing_policy {
        Some(RoutingPolicy::SingleClusterRouting(single)) => {
            if !single.allow_transactional_writes {
                return Err(format!(
                    "Bigtable app profile '{profile_id}' on instance '{instance_id}' routes to \
                     single cluster '{}' with allow_transactional_writes = false; enable \
                     single-row transactions on the app profile so CheckAndMutateRow is supported",
                    single.cluster_id
                ));
            }
            Ok(())
        }
        Some(RoutingPolicy::MultiClusterRoutingUseAny(_)) => Err(format!(
            "Bigtable app profile '{profile_id}' on instance '{instance_id}' uses multi-cluster \
             routing, which does not support single-row transactions (CheckAndMutateRow); \
             configure a single-cluster routing app profile with allow_transactional_writes = true"
        )),
        None => Err(format!(
            "Bigtable app profile '{profile_id}' on instance '{instance_id}' has no routing_policy \
             set; configure single-cluster routing with allow_transactional_writes = true"
        )),
    }
}

async fn connect_admin_channel(
    client: &BigtableClient,
) -> Result<(Channel, Option<Arc<dyn TokenProvider>>), AdminError> {
    if let Some(host) = &client.emulator_host {
        let uri = if host.starts_with("http://") || host.starts_with("https://") {
            host.clone()
        } else {
            format!("http://{host}")
        };
        let endpoint = Endpoint::from_shared(uri)
            .map_err(|e| AdminError::Transport(format!("admin endpoint: {e}")))?
            .connect_timeout(ADMIN_CONNECT_TIMEOUT)
            .timeout(ADMIN_RPC_TIMEOUT)
            .tcp_nodelay(true);
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| AdminError::Transport(format!("admin connect emulator {host}: {e}")))?;
        Ok((channel, None))
    } else {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tls = ClientTlsConfig::new()
            .with_native_roots()
            .domain_name(BIGTABLE_ADMIN_HOST);
        let endpoint = Endpoint::from_static(BIGTABLE_ADMIN_ENDPOINT)
            .tls_config(tls)
            .map_err(|e| AdminError::Transport(format!("admin tls: {e}")))?
            .connect_timeout(ADMIN_CONNECT_TIMEOUT)
            .timeout(ADMIN_RPC_TIMEOUT)
            .http2_keep_alive_interval(Duration::from_mins(5))
            .keep_alive_timeout(Duration::from_secs(20))
            .tcp_nodelay(true);
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| AdminError::Transport(format!("admin connect real: {e}")))?;
        let provider: Arc<dyn TokenProvider> = if let Some(shared) = client.token_provider() {
            Arc::clone(shared)
        } else if let Some(cred_path) = &client.credentials_path {
            let sa = gcp_auth::CustomServiceAccount::from_file(cred_path).map_err(|e| {
                AdminError::Auth(format!("gcp_auth CustomServiceAccount load: {e}"))
            })?;
            Arc::new(sa)
        } else {
            gcp_auth::provider()
                .await
                .map_err(|e| AdminError::Auth(format!("gcp_auth provider: {e}")))?
        };
        Ok((channel, Some(provider)))
    }
}

fn attach_admin_headers<T>(
    payload: T,
    param_key: &str,
    resource_name: &str,
    bearer_header: Option<&str>,
) -> Result<tonic::Request<T>, AdminError> {
    let mut req = tonic::Request::new(payload);
    if let Some(b) = bearer_header {
        let value = MetadataValue::try_from(b)
            .map_err(|e| AdminError::Auth(format!("bad bearer header: {e}")))?;
        req.metadata_mut().insert("authorization", value);
    }
    let routing = format!("{param_key}={}", urlencoding::encode(resource_name));
    if let Ok(val) = MetadataValue::try_from(routing) {
        req.metadata_mut().insert("x-goog-request-params", val);
    }
    Ok(req)
}

async fn make_admin_request<T>(
    payload: T,
    param_key: &str,
    resource_name: &str,
    token_provider: Option<&Arc<dyn TokenProvider>>,
) -> Result<tonic::Request<T>, AdminError> {
    let bearer = if let Some(provider) = token_provider {
        let token = provider
            .token(&[BIGTABLE_ADMIN_SCOPE])
            .await
            .map_err(|e| AdminError::Auth(format!("gcp_auth token: {e}")))?;
        Some(format!("Bearer {}", token.as_str()))
    } else {
        None
    };
    attach_admin_headers(payload, param_key, resource_name, bearer.as_deref())
}

pub struct AdminClient {
    inner: BigtableTableAdminClient<Channel>,
    token_provider: Option<Arc<dyn TokenProvider>>,
    instance_name: String,
}

impl AdminClient {
    /// Connect an admin channel reusing `client`'s configured endpoint and
    /// shared `OAuth2` [`TokenProvider`].
    ///
    /// # Errors
    ///
    /// Returns [`AdminError`] if transport connection or credential setup fails.
    pub async fn connect(client: &BigtableClient) -> Result<Self, AdminError> {
        let (channel, token_provider) = connect_admin_channel(client).await?;
        let inner = BigtableTableAdminClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
        Ok(Self {
            inner,
            token_provider,
            instance_name: client.instance_name(),
        })
    }

    /// Validate the configured Bigtable application profile against the
    /// Instance Admin API at startup when connected to Google Cloud Bigtable.
    ///
    /// Skipped on the local emulator (`cbtemulator` does not implement
    /// `BigtableInstanceAdmin`).
    ///
    /// # Errors
    ///
    /// Returns [`AdminError`] if `GetAppProfile` fails or the profile lacks
    /// single-cluster transactional routing.
    pub async fn validate_app_profile(client: &BigtableClient) -> Result<(), AdminError> {
        if client.emulator_host.is_some() {
            return Ok(());
        }
        let profile_id = client
            .app_profile_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("default");
        let (channel, token_provider) = connect_admin_channel(client).await?;
        let mut instance_admin = BigtableInstanceAdminClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
        let name = format!("{}/appProfiles/{profile_id}", client.instance_name());
        let req = make_admin_request(
            GetAppProfileRequest { name: name.clone() },
            "name",
            &name,
            token_provider.as_ref(),
        )
        .await?;
        let resp = instance_admin
            .get_app_profile(req)
            .await
            .map_err(|status| AdminError::Rpc {
                context: format!(
                    "get_app_profile('{profile_id}') on instance '{}'",
                    client.instance_id
                ),
                status,
            })?
            .into_inner();
        validate_app_profile_proto(&resp, &client.instance_id, profile_id)
            .map_err(AdminError::InvalidAppProfile)
    }

    /// Create a Bigtable table with `families` (or ensure missing families exist
    /// if the table was already created).
    ///
    /// # Errors
    ///
    /// Returns [`AdminError`] if `CreateTable` or `ModifyColumnFamilies` fails.
    pub async fn create_table(
        &mut self,
        table_id: &str,
        families: &[(&str, Option<GcRule>)],
    ) -> Result<(), AdminError> {
        let mut column_families = HashMap::with_capacity(families.len());
        for (name, rule) in families {
            column_families.insert(
                (*name).to_owned(),
                ColumnFamily {
                    gc_rule: rule.clone(),
                    ..ColumnFamily::default()
                },
            );
        }
        let parent = self.instance_name.clone();
        let req = make_admin_request(
            CreateTableRequest {
                parent: parent.clone(),
                table_id: table_id.to_owned(),
                table: Some(Table {
                    column_families,
                    ..Table::default()
                }),
                initial_splits: vec![],
            },
            "parent",
            &parent,
            self.token_provider.as_ref(),
        )
        .await?;
        match self.inner.create_table(req).await {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::AlreadyExists => {
                self.ensure_families(table_id, families).await
            }
            Err(status) => Err(AdminError::Rpc {
                context: format!("create_table({table_id})"),
                status,
            }),
        }
    }

    /// Ensure all `families` exist on `table_id`.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError`] if `ModifyColumnFamilies` fails with a non-idempotent error.
    // Justification: schema inspection, per-family GC-rule comparison, ModifyColumnFamilies retry, and verification run sequentially.
    #[allow(clippy::too_many_lines)]
    pub async fn ensure_families(
        &mut self,
        table_id: &str,
        families: &[(&str, Option<GcRule>)],
    ) -> Result<(), AdminError> {
        if families.is_empty() {
            return Ok(());
        }
        let table_name = format!("{}/tables/{table_id}", self.instance_name);
        let get_req = make_admin_request(
            GetTableRequest {
                name: table_name.clone(),
                view: table::View::SchemaView as i32,
            },
            "name",
            &table_name,
            self.token_provider.as_ref(),
        )
        .await?;
        let existing_families = self
            .inner
            .get_table(get_req)
            .await
            .ok()
            .map(|r| r.into_inner().column_families);

        let missing: Vec<(&str, &Option<GcRule>)> = families
            .iter()
            .filter(|(name, _)| {
                existing_families
                    .as_ref()
                    .is_none_or(|present| !present.contains_key(*name))
            })
            .map(|(name, rule)| (*name, rule))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }

        let modifications: Vec<Modification> = missing
            .iter()
            .map(|(name, rule)| Modification {
                id: (*name).to_owned(),
                update_mask: None,
                r#mod: Some(Mod::Create(ColumnFamily {
                    gc_rule: (*rule).clone(),
                    ..ColumnFamily::default()
                })),
            })
            .collect();
        let batch_req = make_admin_request(
            ModifyColumnFamiliesRequest {
                name: table_name.clone(),
                modifications,
                ignore_warnings: false,
            },
            "name",
            &table_name,
            self.token_provider.as_ref(),
        )
        .await?;
        match self.inner.modify_column_families(batch_req).await {
            Ok(_) => return Ok(()),
            Err(status)
                if missing.len() == 1
                    && matches!(
                        status.code(),
                        tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition
                    ) =>
            {
                return Ok(());
            }
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition
                ) => {}
            Err(status) => {
                return Err(AdminError::Rpc {
                    context: format!("ensure_families({table_id})"),
                    status,
                });
            }
        }

        for (name, rule) in missing {
            let req = make_admin_request(
                ModifyColumnFamiliesRequest {
                    name: table_name.clone(),
                    modifications: vec![Modification {
                        id: name.to_owned(),
                        update_mask: None,
                        r#mod: Some(Mod::Create(ColumnFamily {
                            gc_rule: rule.clone(),
                            ..ColumnFamily::default()
                        })),
                    }],
                    ignore_warnings: false,
                },
                "name",
                &table_name,
                self.token_provider.as_ref(),
            )
            .await?;
            match self.inner.modify_column_families(req).await {
                Ok(_) => {}
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition
                    ) => {}
                Err(status) => {
                    return Err(AdminError::Rpc {
                        context: format!("ensure_families({table_id}, {name})"),
                        status,
                    });
                }
            }
        }
        Ok(())
    }

    /// Delete `table_id` under the configured instance (idempotent if absent).
    ///
    /// # Errors
    ///
    /// Returns [`AdminError`] if `DeleteTable` fails with an error other than `NotFound`.
    pub async fn delete_table(&mut self, table_id: &str) -> Result<(), AdminError> {
        let table_name = format!("{}/tables/{table_id}", self.instance_name);
        let req = make_admin_request(
            DeleteTableRequest {
                name: table_name.clone(),
            },
            "name",
            &table_name,
            self.token_provider.as_ref(),
        )
        .await?;
        match self.inner.delete_table(req).await {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(AdminError::Rpc {
                context: format!("delete_table({table_id})"),
                status,
            }),
        }
    }

    /// List all table short-names under the instance, paging through all pages.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError`] if any `ListTables` page fails.
    pub async fn list_tables(&mut self) -> Result<Vec<String>, AdminError> {
        let mut out = Vec::new();
        let mut page_token = String::new();
        let parent = self.instance_name.clone();
        loop {
            let req = make_admin_request(
                ListTablesRequest {
                    parent: parent.clone(),
                    view: table::View::NameOnly as i32,
                    page_size: 0,
                    page_token,
                },
                "parent",
                &parent,
                self.token_provider.as_ref(),
            )
            .await?;
            let resp = self
                .inner
                .list_tables(req)
                .await
                .map_err(|status| AdminError::Rpc {
                    context: "list_tables".to_owned(),
                    status,
                })?
                .into_inner();
            out.extend(resp.tables.into_iter().map(|t| {
                t.name
                    .rsplit_once('/')
                    .map_or(t.name.clone(), |(_, last)| last.to_owned())
            }));
            if resp.next_page_token.is_empty() {
                break;
            }
            page_token = resp.next_page_token;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::app_profile::{
        MultiClusterRoutingUseAny, SingleClusterRouting,
    };

    #[test]
    fn app_profile_validation_requires_single_cluster_with_transactional_writes() {
        let valid = AppProfile {
            routing_policy: Some(RoutingPolicy::SingleClusterRouting(SingleClusterRouting {
                cluster_id: "c1".to_owned(),
                allow_transactional_writes: true,
            })),
            ..AppProfile::default()
        };
        assert!(validate_app_profile_proto(&valid, "inst-1", "default").is_ok());

        let no_tx = AppProfile {
            routing_policy: Some(RoutingPolicy::SingleClusterRouting(SingleClusterRouting {
                cluster_id: "c1".to_owned(),
                allow_transactional_writes: false,
            })),
            ..AppProfile::default()
        };
        let err = validate_app_profile_proto(&no_tx, "inst-1", "prof-a").unwrap_err();
        assert!(err.contains("allow_transactional_writes = false"), "{err}");

        let multi = AppProfile {
            routing_policy: Some(RoutingPolicy::MultiClusterRoutingUseAny(
                MultiClusterRoutingUseAny::default(),
            )),
            ..AppProfile::default()
        };
        let err = validate_app_profile_proto(&multi, "inst-1", "prof-multi").unwrap_err();
        assert!(err.contains("multi-cluster routing"), "{err}");
    }

    #[test]
    fn admin_request_url_encodes_x_goog_request_params_and_preserves_auth() {
        let parent = "projects/example.com:proj-a/instances/inst-b";
        let intercepted =
            attach_admin_headers((), "parent", parent, Some("Bearer test-token")).expect("headers");
        assert_eq!(
            intercepted
                .metadata()
                .get("x-goog-request-params")
                .expect("routing header")
                .to_str()
                .expect("ascii"),
            "parent=projects%2Fexample.com%3Aproj-a%2Finstances%2Finst-b"
        );
        assert_eq!(
            intercepted
                .metadata()
                .get("authorization")
                .expect("authorization header")
                .to_str()
                .expect("ascii"),
            "Bearer test-token"
        );

        let table_name = format!("{parent}/tables/__extenddb_catalog__");
        let treq = attach_admin_headers((), "name", &table_name, None).expect("table headers");
        assert_eq!(
            treq.metadata()
                .get("x-goog-request-params")
                .expect("table routing header")
                .to_str()
                .expect("ascii"),
            "name=projects%2Fexample.com%3Aproj-a%2Finstances%2Finst-b%2Ftables%2F__extenddb_catalog__"
        );
    }

    #[test]
    fn admin_error_classifies_grpc_status_codes_via_tonic_code() {
        for status in [
            Status::resource_exhausted("Resource has been exhausted (e.g. check quota)."),
            Status::unavailable("backend restarting"),
            Status::deadline_exceeded("admin call timed out"),
            Status::aborted("concurrent schema modification"),
            Status::internal("h2 protocol error: GoAway(ENHANCE_YOUR_CALM)"),
        ] {
            let err = AdminError::Rpc {
                context: "create_table(t1)".to_owned(),
                status,
            }
            .into_storage_error();
            assert!(
                matches!(err, StorageError::Transient(_)),
                "expected Transient, got {err:?}"
            );
        }

        let not_found = AdminError::Rpc {
            context: "ensure_families(t1)".to_owned(),
            status: Status::not_found("table missing"),
        }
        .into_storage_error();
        assert!(matches!(not_found, StorageError::TableNotFound(_)));

        let perm = AdminError::Rpc {
            context: "create_table(t1)".to_owned(),
            status: Status::permission_denied("missing bigtable.tables.create"),
        }
        .into_storage_error();
        assert!(matches!(perm, StorageError::Internal(_)));

        let transport = AdminError::Transport("dial error".to_owned()).into_storage_error();
        assert!(matches!(transport, StorageError::Connection(_)));
    }
}
