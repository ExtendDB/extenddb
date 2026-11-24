// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! BigTable admin operations (`CreateTable`, `DeleteTable`, `ListTables`,
//! `ModifyColumnFamilies`, and startup `GetAppProfile` validation).

use std::collections::HashMap;
use std::sync::Arc;

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
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::data::client::{BigtableClient, MAX_GRPC_MESSAGE_BYTES};

const BIGTABLE_ADMIN_ENDPOINT: &str = "https://bigtableadmin.googleapis.com:443";
const BIGTABLE_ADMIN_HOST: &str = "bigtableadmin.googleapis.com";
const BIGTABLE_ADMIN_SCOPE: &str = "https://www.googleapis.com/auth/bigtable.admin";

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

/// Interceptor that adds an `Authorization: Bearer ...` header to every
/// outgoing request when a token is present.
#[derive(Clone)]
pub struct AuthInterceptor {
    bearer: Option<Arc<String>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(b) = &self.bearer {
            let value = MetadataValue::try_from(b.as_str())
                .map_err(|e| Status::unauthenticated(format!("bad bearer header: {e}")))?;
            req.metadata_mut().insert("authorization", value);
        }
        Ok(req)
    }
}

async fn connect_admin_channel(
    client: &BigtableClient,
) -> Result<(Channel, AuthInterceptor), String> {
    let (channel, bearer) = match &client.emulator_host {
        Some(host) => {
            let uri = if host.starts_with("http://") || host.starts_with("https://") {
                host.clone()
            } else {
                format!("http://{host}")
            };
            let endpoint =
                Endpoint::from_shared(uri).map_err(|e| format!("admin endpoint: {e}"))?;
            let channel = endpoint
                .connect()
                .await
                .map_err(|e| format!("admin connect emulator {host}: {e}"))?;
            (channel, None)
        }
        None => {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let tls = ClientTlsConfig::new()
                .with_native_roots()
                .domain_name(BIGTABLE_ADMIN_HOST);
            let endpoint = Endpoint::from_static(BIGTABLE_ADMIN_ENDPOINT)
                .tls_config(tls)
                .map_err(|e| format!("admin tls: {e}"))?;
            let channel = endpoint
                .connect()
                .await
                .map_err(|e| format!("admin connect real: {e}"))?;
            let token = if let Some(cred_path) = &client.credentials_path {
                let provider = gcp_auth::CustomServiceAccount::from_file(cred_path)
                    .map_err(|e| format!("gcp_auth CustomServiceAccount load: {e}"))?;
                provider
                    .token(&[BIGTABLE_ADMIN_SCOPE])
                    .await
                    .map_err(|e| format!("gcp_auth token: {e}"))?
            } else {
                let provider = gcp_auth::provider()
                    .await
                    .map_err(|e| format!("gcp_auth provider: {e}"))?;
                provider
                    .token(&[BIGTABLE_ADMIN_SCOPE])
                    .await
                    .map_err(|e| format!("gcp_auth token: {e}"))?
            };
            let bearer = Arc::new(format!("Bearer {}", token.as_str()));
            (channel, Some(bearer))
        }
    };
    Ok((channel, AuthInterceptor { bearer }))
}

fn admin_request<T>(payload: T, param_key: &str, resource_name: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(payload);
    let routing = format!("{param_key}={}", urlencoding::encode(resource_name));
    if let Ok(val) = MetadataValue::try_from(routing) {
        req.metadata_mut().insert("x-goog-request-params", val);
    }
    req
}

pub struct AdminClient {
    inner: BigtableTableAdminClient<InterceptedService<Channel, AuthInterceptor>>,
    instance_name: String,
}

impl AdminClient {
    pub async fn connect(client: &BigtableClient) -> Result<Self, String> {
        let (channel, interceptor) = connect_admin_channel(client).await?;
        let inner = BigtableTableAdminClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
        Ok(Self {
            inner,
            instance_name: client.instance_name(),
        })
    }

    /// Validate the configured Bigtable application profile against the
    /// Instance Admin API at startup when connected to Google Cloud Bigtable.
    ///
    /// Skipped on the local emulator (`cbtemulator` does not implement
    /// `BigtableInstanceAdmin`).
    pub async fn validate_app_profile(client: &BigtableClient) -> Result<(), String> {
        if client.emulator_host.is_some() {
            return Ok(());
        }
        let profile_id = client
            .app_profile_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("default");
        let (channel, interceptor) = connect_admin_channel(client).await?;
        let mut instance_admin =
            BigtableInstanceAdminClient::with_interceptor(channel, interceptor)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
        let name = format!("{}/appProfiles/{profile_id}", client.instance_name());
        let req = admin_request(GetAppProfileRequest { name: name.clone() }, "name", &name);
        let resp = instance_admin
            .get_app_profile(req)
            .await
            .map_err(|status| {
                format!(
                    "get_app_profile('{profile_id}') on instance '{}': {status}",
                    client.instance_id
                )
            })?
            .into_inner();
        validate_app_profile_proto(&resp, &client.instance_id, profile_id)
    }

    pub async fn create_table(
        &mut self,
        table_id: &str,
        families: &[(&str, Option<GcRule>)],
    ) -> Result<(), String> {
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
        let req = admin_request(
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
        );
        match self.inner.create_table(req).await {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::AlreadyExists => {
                self.ensure_families(table_id, families).await
            }
            Err(status) => Err(format!("create_table({table_id}): {status}")),
        }
    }

    pub async fn ensure_families(
        &mut self,
        table_id: &str,
        families: &[(&str, Option<GcRule>)],
    ) -> Result<(), String> {
        if families.is_empty() {
            return Ok(());
        }
        let table_name = format!("{}/tables/{table_id}", self.instance_name);
        let existing_families = self
            .inner
            .get_table(admin_request(
                GetTableRequest {
                    name: table_name.clone(),
                    view: table::View::SchemaView as i32,
                },
                "name",
                &table_name,
            ))
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
        let batch_req = admin_request(
            ModifyColumnFamiliesRequest {
                name: table_name.clone(),
                modifications,
                ignore_warnings: false,
            },
            "name",
            &table_name,
        );
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
            Err(status) => return Err(format!("ensure_families({table_id}): {status}")),
        }

        for (name, rule) in missing {
            let req = admin_request(
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
            );
            match self.inner.modify_column_families(req).await {
                Ok(_) => {}
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition
                    ) => {}
                Err(status) => {
                    return Err(format!("ensure_families({table_id}, {name}): {status}"));
                }
            }
        }
        Ok(())
    }

    pub async fn delete_table(&mut self, table_id: &str) -> Result<(), String> {
        let table_name = format!("{}/tables/{table_id}", self.instance_name);
        let req = admin_request(
            DeleteTableRequest {
                name: table_name.clone(),
            },
            "name",
            &table_name,
        );
        match self.inner.delete_table(req).await {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(format!("delete_table({table_id}): {status}")),
        }
    }

    /// List all table short-names under the instance, paging through all pages.
    pub async fn list_tables(&mut self) -> Result<Vec<String>, String> {
        let mut out = Vec::new();
        let mut page_token = String::new();
        let parent = self.instance_name.clone();
        loop {
            let req = admin_request(
                ListTablesRequest {
                    parent: parent.clone(),
                    view: table::View::NameOnly as i32,
                    page_size: 0,
                    page_token,
                },
                "parent",
                &parent,
            );
            let resp = self
                .inner
                .list_tables(req)
                .await
                .map_err(|status| format!("list_tables: {status}"))?
                .into_inner();
            out.extend(resp.tables.into_iter().map(|t| {
                t.name
                    .rsplit_once('/')
                    .map(|(_, last)| last.to_owned())
                    .unwrap_or(t.name)
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
        let req = admin_request((), "parent", parent);
        let mut interceptor = AuthInterceptor {
            bearer: Some(Arc::new("Bearer test-token".to_owned())),
        };
        let intercepted = interceptor.call(req).expect("interceptor");
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
        let treq = admin_request((), "name", &table_name);
        assert_eq!(
            treq.metadata()
                .get("x-goog-request-params")
                .expect("table routing header")
                .to_str()
                .expect("ascii"),
            "name=projects%2Fexample.com%3Aproj-a%2Finstances%2Finst-b%2Ftables%2F__extenddb_catalog__"
        );
    }
}
