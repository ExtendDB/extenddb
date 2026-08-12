//! BigTable connection wrapper providing both data and admin clients.

use std::time::Duration;

use bigtable_rs::bigtable::{BigTable, BigTableConnection};

use crate::config::BigtableStorageConfig;

/// Bundles a data-plane connection to BigTable. The admin-plane handle is
/// built lazily by `data::admin` when needed (it uses a different transport
/// crate and we don't want to pay for its setup when only data ops happen).
pub struct BigtableClient {
    pub project_id: String,
    pub instance_id: String,
    pub emulator_host: Option<String>,
    pub credentials_path: Option<String>,
    connection: BigTableConnection,
}

impl BigtableClient {
    pub async fn connect(config: &BigtableStorageConfig) -> Result<Self, String> {
        // bigtable_rs honors BIGTABLE_EMULATOR_HOST for emulator detection.
        if let Some(host) = &config.emulator_host {
            // SAFETY: setting a process env var. Safe at backend init time
            // before any threads are spawned by us.
            unsafe { std::env::set_var("BIGTABLE_EMULATOR_HOST", host) };
        } else {
            if let Some(cred_path) = &config.credentials_path {
                // SAFETY: setting a process env var. Safe at backend init time.
                unsafe { std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", cred_path) };
            }
            // Real BigTable uses rustls (via tonic). The crate has both
            // aws-lc-rs and ring providers transitively enabled (bigtable_rs's
            // tls-aws-lc + tonic's tls-native-roots → ring), so rustls can't
            // auto-pick. Install aws-lc-rs explicitly once; subsequent calls
            // are no-ops.
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        }

        let channel_size = config.pool_size.max(1) as usize;
        let timeout = Some(Duration::from_secs(30));

        let connection = BigTableConnection::new(
            &config.project_id,
            &config.instance_id,
            /* is_read_only */ false,
            channel_size,
            timeout,
        )
        .await
        .map_err(|e| format!("bigtable connect: {e}"))?;

        Ok(Self {
            project_id: config.project_id.clone(),
            instance_id: config.instance_id.clone(),
            emulator_host: config.emulator_host.clone(),
            credentials_path: config.credentials_path.clone(),
            connection,
        })
    }

    /// Returns a per-request BigTable data client. Cheap to clone.
    pub fn data(&self) -> BigTable {
        self.connection.client()
    }

    /// Convenience: build a fully-qualified BigTable table name from a short
    /// name. Format: `projects/<project>/instances/<instance>/tables/<table>`.
    pub fn full_table_name(&self, short: &str) -> String {
        format!(
            "projects/{}/instances/{}/tables/{}",
            self.project_id, self.instance_id, short
        )
    }

    /// Instance resource name (admin API needs this for CreateTable parent).
    pub fn instance_name(&self) -> String {
        format!("projects/{}/instances/{}", self.project_id, self.instance_id)
    }
}
