//! BigTable admin operations (CreateTable, DeleteTable, ListTables,
//! ModifyColumnFamilies, ...). Uses googleapis-tonic-google-bigtable-admin-v2's
//! generated tonic stubs.
//!
//! Transport selection:
//! - Emulator (`emulator_host` set): plain HTTP/2, no auth.
//! - Real BigTable: TLS to `bigtableadmin.googleapis.com:443` with a
//!   gcp_auth-provided bearer token injected on every request.

use std::collections::HashMap;
use std::sync::Arc;

use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{
    ColumnFamily, CreateTableRequest, DeleteTableRequest, GcRule, ListTablesRequest,
    ModifyColumnFamiliesRequest, Table,
    bigtable_table_admin_client::BigtableTableAdminClient,
    modify_column_families_request::{Modification, modification::Mod},
};
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use gcp_auth::TokenProvider;

use crate::data::client::BigtableClient;

const BIGTABLE_ADMIN_ENDPOINT: &str = "https://bigtableadmin.googleapis.com:443";
const BIGTABLE_ADMIN_HOST: &str = "bigtableadmin.googleapis.com";
const BIGTABLE_ADMIN_SCOPE: &str = "https://www.googleapis.com/auth/bigtable.admin";

/// Interceptor that adds an `Authorization: Bearer ...` header to every
/// outgoing request when a token is present (set for real GCP, absent for
/// the emulator).
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

pub struct AdminClient {
    inner: BigtableTableAdminClient<InterceptedService<Channel, AuthInterceptor>>,
    instance_name: String,
}

impl AdminClient {
    pub async fn connect(client: &BigtableClient) -> Result<Self, String> {
        let (channel, bearer) = match &client.emulator_host {
            Some(host) => {
                let endpoint = Endpoint::from_shared(format!("http://{host}"))
                    .map_err(|e| format!("admin endpoint: {e}"))?;
                let channel = endpoint
                    .connect()
                    .await
                    .map_err(|e| format!("admin connect emulator {host}: {e}"))?;
                (channel, None)
            }
            None => {
                // Real BigTable: TLS endpoint + gcp_auth bearer token.
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
        let interceptor = AuthInterceptor { bearer };
        let inner = BigtableTableAdminClient::with_interceptor(channel, interceptor);
        Ok(Self {
            inner,
            instance_name: client.instance_name(),
        })
    }

    /// Create a BigTable table with the given column families. Each family
    /// gets the supplied (or no) GC rule. Returns Ok even if the table
    /// already exists (idempotent for our bootstrap path).
    ///
    /// We always follow CreateTable with a ModifyColumnFamilies pass. Real
    /// BigTable accepts CreateTable requests where the `table.column_families`
    /// map round-trips empty (the wire format silently drops the map under
    /// some conditions we haven't pinned down), so the ModifyColumnFamilies
    /// pass is the reliable way to land the families. The emulator handles
    /// both inputs identically, so this is a no-op cost in dev.
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
        let req = CreateTableRequest {
            parent: self.instance_name.clone(),
            table_id: table_id.to_owned(),
            table: Some(Table {
                column_families,
                ..Table::default()
            }),
            initial_splits: vec![],
        };
        match self.inner.create_table(req).await {
            Ok(_) => {}
            Err(status) if status.code() == tonic::Code::AlreadyExists => {}
            Err(status) => return Err(format!("create_table({table_id}): {status}")),
        }
        self.ensure_families(table_id, families).await
    }

    /// Idempotently ensure the given column families exist on `table_id`.
    /// Used to repair tables created with an empty families map, and as a
    /// safety net for create_table.
    pub async fn ensure_families(
        &mut self,
        table_id: &str,
        families: &[(&str, Option<GcRule>)],
    ) -> Result<(), String> {
        let modifications: Vec<Modification> = families
            .iter()
            .map(|(name, rule)| Modification {
                id: (*name).to_owned(),
                update_mask: None,
                r#mod: Some(Mod::Create(ColumnFamily {
                    gc_rule: rule.clone(),
                    ..ColumnFamily::default()
                })),
            })
            .collect();
        let req = ModifyColumnFamiliesRequest {
            name: format!("{}/tables/{}", self.instance_name, table_id),
            modifications,
            ignore_warnings: false,
        };
        match self.inner.modify_column_families(req).await {
            Ok(_) => Ok(()),
            // The Create modification rejects existing families with AlreadyExists.
            Err(status) if status.code() == tonic::Code::AlreadyExists => Ok(()),
            // Bigtable surfaces "FailedPrecondition" when one of the families
            // already exists in a ModifyColumnFamilies request — also benign for
            // our idempotent bootstrap.
            Err(status) if status.code() == tonic::Code::FailedPrecondition => Ok(()),
            Err(status) => Err(format!("ensure_families({table_id}): {status}")),
        }
    }

    pub async fn delete_table(&mut self, table_id: &str) -> Result<(), String> {
        let req = DeleteTableRequest {
            name: format!("{}/tables/{}", self.instance_name, table_id),
        };
        match self.inner.delete_table(req).await {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(format!("delete_table({table_id}): {status}")),
        }
    }

    /// List all table short-names under the instance.
    pub async fn list_tables(&mut self) -> Result<Vec<String>, String> {
        let req = ListTablesRequest {
            parent: self.instance_name.clone(),
            view: 0,        // NAME_ONLY
            page_size: 0,   // server default
            page_token: String::new(),
        };
        let resp = self
            .inner
            .list_tables(req)
            .await
            .map_err(|status| format!("list_tables: {status}"))?
            .into_inner();
        Ok(resp
            .tables
            .into_iter()
            .map(|t| {
                // t.name is "projects/.../instances/.../tables/<id>"; strip prefix.
                t.name
                    .rsplit_once('/')
                    .map(|(_, last)| last.to_owned())
                    .unwrap_or(t.name)
            })
            .collect())
    }
}
