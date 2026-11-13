// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra implementation of `Bootstrapper`.

use async_trait::async_trait;
use cdrs_tokio::TryFromRow;
use cdrs_tokio::frame::TryFromRow as TryFromRowTrait;
use extenddb_storage::bootstrapper::{
    AdminBootstrapResult, BootstrapConfig, Bootstrapper,
    helpers::{
        check_conflict, extract_arg, generate_account_id, generate_encryption_key,
        generate_random_password, hash_password_async,
    },
};
use extenddb_storage::management_store::{OpError, OpResult};

use extenddb_storage::migrations::{AdminRole, HeldLock, Target};

use crate::config::CassandraStorageConfig;
use crate::engine::CassandraEngine;

mod guards;
use guards::{HeldLease, check_datacenter, check_name};

// Helper structs for parsing Cassandra rows
#[derive(Debug, Clone, TryFromRow)]
struct TableNameRow {
    table_name: String,
}

#[derive(Debug, Clone, TryFromRow)]
struct KeyspaceNameRow {
    keyspace_name: String,
}

/// Cassandra bootstrapper for init/destroy/migrate operations.
pub struct CassandraBootstrapper {
    engine: CassandraEngine,
    config: BootstrapConfig,
    keyspace_prefix: String,
    /// The connection string of the admin session, without credentials;
    /// [`Bootstrapper::init_target`] adds the admin role.
    engine_connection_string: String,
    /// The lease of the migration lock `init` holds while the bootstrap rows
    /// are written, set by [`Bootstrapper::hold`].
    held: HeldLease,
    /// The replication `init` creates its keyspaces with, written to the
    /// config so `serve` creates later account keyspaces the same way.
    replication_factor: u32,
    datacenter: String,
}

/// The replication factor and datacenter `init` uses: `--replication-factor`
/// and `--datacenter` when given, else the config file's values when there is
/// one (`init --overwrite`), else 1 and `datacenter1`, for a single node. A
/// flag that disagrees with the config file is refused, as for the other
/// flags, and so is a replication factor that is not a positive integer.
fn resolve_replication(
    cli_replication_factor: Option<&str>,
    cli_datacenter: Option<&str>,
    in_file: Option<(u32, &str)>,
) -> Result<(u32, String), extenddb_storage::error::StorageError> {
    use extenddb_storage::error::StorageError;

    let cli_replication_factor = cli_replication_factor
        .map(|rf| match rf.parse::<u32>() {
            Ok(n) if n > 0 => Ok(n),
            _ => Err(StorageError::Internal(format!(
                "Invalid --replication-factor '{rf}': must be a positive integer"
            ))),
        })
        .transpose()?;
    let (replication_factor, datacenter) = match in_file {
        Some((file_rf, file_dc)) => {
            if file_rf == 0 {
                return Err(StorageError::Internal(
                    "replication_factor in the config file must be a positive integer".to_owned(),
                ));
            }
            check_conflict(
                cli_replication_factor.as_ref(),
                &file_rf,
                "--replication-factor",
            )?;
            check_conflict(
                cli_datacenter.map(str::to_owned).as_ref(),
                &file_dc.to_owned(),
                "--datacenter",
            )?;
            check_datacenter("the datacenter in the config file", file_dc)?;
            (file_rf, file_dc.to_owned())
        }
        None => {
            let datacenter = cli_datacenter.unwrap_or("datacenter1");
            check_datacenter("the datacenter (--datacenter)", datacenter)?;
            (cli_replication_factor.unwrap_or(1), datacenter.to_owned())
        }
    };
    Ok((replication_factor, datacenter))
}

impl CassandraBootstrapper {
    /// Create a new bootstrapper from Cassandra config.
    pub async fn new(
        cassandra_config: &CassandraStorageConfig,
        bootstrap_config: BootstrapConfig,
    ) -> OpResult<Self> {
        let engine = CassandraEngine::new(cassandra_config, "us-east-1")
            .await
            .map_err(|e| OpError::Internal(format!("Create engine: {e}")))?;

        Ok(Self {
            engine,
            config: bootstrap_config,
            keyspace_prefix: cassandra_config.keyspace_prefix.clone(),
            engine_connection_string: format!(
                "{}/{}",
                cassandra_config.contact_points.join(","),
                cassandra_config.keyspace_prefix
            ),
            held: HeldLease::default(),
            replication_factor: cassandra_config.replication_factor,
            datacenter: cassandra_config.datacenter.clone(),
        })
    }

    /// Grant the application role what `migrate`, `serve` and CreateAccount
    /// need under `CassandraAuthorizer` on `keyspace`, which `init` creates as
    /// the admin role, so the application role holds no permission on it
    /// yet: see [`crate::migrations::grants`]. A role that creates a keyspace
    /// holds every permission on it, so the keyspaces CreateAccount creates
    /// need no grant. Under `AllowAllAuthorizer` nothing is granted.
    async fn grant_app_role(&self, keyspace: &str, account: bool) -> OpResult<()> {
        let session = self.engine.session_arc();
        let role = &self.config.app_user;
        // Checked against the held lease before each request, so a holder
        // whose lease has lapsed sends no further GRANT.
        let what = format!("Grant on {keyspace}");
        let granted = crate::migrations::grant_role(
            &session,
            keyspace,
            role,
            account,
            || self.held.check_run(&what),
            || self.held.progress(),
        )
        .await
        .map_err(|e| OpError::Internal(e.to_string()))?;
        if let Some(authorizer) = granted {
            println!("    Granted '{role}' its permissions on {keyspace} ({authorizer}).");
        }
        Ok(())
    }

    /// Create a bootstrapper from config file and CLI args. Parses
    /// Cassandra-specific arguments and merges with config.
    pub async fn from_config(
        config_path: &str,
        cli_args: &[String],
    ) -> Result<Self, extenddb_storage::error::StorageError> {
        use extenddb_storage::error::StorageError;

        // Extract Cassandra-specific CLI args
        let cassandra_contact_points = extract_arg(cli_args, "--cassandra-contact-points");
        let cassandra_user = extract_arg(cli_args, "--cassandra-user");
        let cassandra_pass = extract_arg(cli_args, "--cassandra-pass");
        let keyspace_prefix = extract_arg(cli_args, "--keyspace-prefix");
        let replication_factor = extract_arg(cli_args, "--replication-factor");
        let datacenter = extract_arg(cli_args, "--datacenter");
        let extenddb_user = extract_arg(cli_args, "--extenddb-user");
        let extenddb_pass = extract_arg(cli_args, "--extenddb-pass");

        // Load config file if it exists
        let (contact_points, user, password, prefix, replication_in_file) = if std::path::Path::new(
            config_path,
        )
        .exists()
        {
            println!("--- Loading defaults from {config_path}");

            // Parse Cassandra config from file
            let config_content = std::fs::read_to_string(config_path)
                .map_err(|e| StorageError::Internal(format!("Failed to read config: {e}")))?;
            let app_config: toml::Value = toml::from_str(&config_content)
                .map_err(|e| StorageError::Internal(format!("Failed to parse config: {e}")))?;

            let cassandra_config = app_config
                .get("storage")
                .and_then(|s| s.get("cassandra"))
                .ok_or_else(|| {
                    StorageError::Internal("Missing storage.cassandra section".into())
                })?;

            let config: CassandraStorageConfig =
                cassandra_config
                    .clone()
                    .try_into()
                    .map_err(|e: toml::de::Error| {
                        StorageError::Internal(format!("Invalid Cassandra config: {e}"))
                    })?;

            // Check for conflicts between CLI args and config values
            if let Some(ref cli_cp) = cassandra_contact_points {
                let cli_list: Vec<&str> = cli_cp.split(',').collect();
                let config_list: Vec<&str> = config
                    .contact_points
                    .iter()
                    .map(std::string::String::as_str)
                    .collect();
                if cli_list != config_list {
                    return Err(StorageError::Internal(format!(
                        "--cassandra-contact-points '{}' conflicts with config file contact points '{}'",
                        cli_cp,
                        config.contact_points.join(",")
                    )));
                }
            }

            check_conflict(
                extenddb_user.as_ref(),
                config.username.as_ref().unwrap_or(&"extenddb".to_string()),
                "--extenddb-user",
            )?;
            check_conflict(
                extenddb_pass.as_ref(),
                config
                    .password
                    .as_ref()
                    .unwrap_or(&"extenddb-local-dev".to_string()),
                "--extenddb-pass",
            )?;

            if let Some(ref cli_prefix) = keyspace_prefix
                && cli_prefix != &config.keyspace_prefix
            {
                return Err(StorageError::Internal(format!(
                    "--keyspace-prefix '{}' conflicts with config file keyspace prefix '{}'",
                    cli_prefix, config.keyspace_prefix
                )));
            }

            (
                config.contact_points,
                config.username.unwrap_or_else(|| "extenddb".to_string()),
                config
                    .password
                    .unwrap_or_else(|| "extenddb-local-dev".to_string()),
                config.keyspace_prefix,
                Some((config.replication_factor, config.datacenter)),
            )
        } else {
            // No config file - use defaults (single-node dev environment)
            (
                vec!["localhost:9042".to_string()],
                "extenddb".to_string(),
                "extenddb-local-dev".to_string(),
                "extenddb".to_string(),
                None,
            )
        };
        let (resolved_replication_factor, resolved_datacenter) = resolve_replication(
            replication_factor.as_deref(),
            datacenter.as_deref(),
            replication_in_file
                .as_ref()
                .map(|(rf, dc)| (*rf, dc.as_str())),
        )?;

        // CLI args override config (or use config values if no CLI arg provided)
        let resolved_contact_points = cassandra_contact_points.map_or(contact_points, |cp| {
            cp.split(',')
                .map(std::string::ToString::to_string)
                .collect()
        });
        let resolved_admin_user = cassandra_user
            .unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "cassandra".to_owned()));
        let resolved_keyspace_prefix = keyspace_prefix.unwrap_or(prefix);
        let resolved_app_user = extenddb_user.unwrap_or(user);
        let resolved_app_password = extenddb_pass.unwrap_or(password);
        check_name("the application role (--extenddb-user)", &resolved_app_user)?;
        check_name(
            "the keyspace prefix (--keyspace-prefix)",
            &resolved_keyspace_prefix,
        )?;

        // Extract host and port from first contact point
        let (host, port) = resolved_contact_points
            .first()
            .and_then(|cp| {
                let parts: Vec<&str> = cp.split(':').collect();
                if parts.len() == 2 {
                    Some((parts[0].to_string(), parts[1].parse().ok()?))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| ("localhost".to_string(), 9042));

        let bootstrap_config = BootstrapConfig {
            host,
            port,
            admin_user: resolved_admin_user.clone(),
            admin_password: cassandra_pass.clone(),
            app_user: resolved_app_user,
            app_password: resolved_app_password,
            catalog_db: format!("{resolved_keyspace_prefix}_catalog"),
            data_db: String::new(), // Not used for Cassandra
        };

        // For bootstrap operations, connect as admin user
        let mut cassandra_config = CassandraStorageConfig {
            contact_points: resolved_contact_points,
            username: Some(resolved_admin_user),
            password: cassandra_pass,
            keyspace_prefix: resolved_keyspace_prefix,
            replication_factor: resolved_replication_factor,
            datacenter: resolved_datacenter,
            max_connections: 10,
            cached_connection_string: None,
            instance_id: None,
        };
        cassandra_config.ensure_cached_connection_string();

        // The message alone, not `{e:?}`, which nested the variant name into it.
        Self::new(&cassandra_config, bootstrap_config)
            .await
            .map_err(|e| match e {
                OpError::Validation(m)
                | OpError::AlreadyExists(m)
                | OpError::NotFound(m)
                | OpError::HasDependents(m)
                | OpError::Internal(m)
                | OpError::Database { message: m, .. } => StorageError::Internal(m),
            })
    }
}

impl CassandraBootstrapper {
    /// Refuse `keyspace` when its RF or datacenter differs from the ones this
    /// `init` uses. `CREATE KEYSPACE IF NOT EXISTS` keeps a keyspace an
    /// earlier `init` created, with that `init`'s RF and datacenter. Without
    /// this check, `init` would write its own values to `extenddb.toml`, and
    /// the file would not describe the keyspaces.
    ///
    /// The error offers `DROP KEYSPACE` only for a keyspace that holds no
    /// data: no table, or for the catalog keyspace only the `settings` table
    /// that `init` creates before it takes the lock. For a keyspace that
    /// holds other tables, it offers only the fixes that keep the data, with
    /// the steps in the order that keeps every write for the keyspace's
    /// change ([`crate::migrations::alter_steps`]).
    ///
    /// With `unreadable`, the catalog's status read failed: the keyspace has
    /// more replicas than the cluster has nodes, and an `init` with the
    /// keyspace's values stops again. The error then does not offer them.
    async fn refuse_other_replication(&self, keyspace: &str, unreadable: bool) -> OpResult<()> {
        let found = crate::migrations::replication_differs(
            self.engine.session(),
            keyspace,
            &self.datacenter,
            self.replication_factor,
        )
        .await
        .map_err(|e| OpError::Internal(e.to_string()))?;
        let Some((found, change)) = found else {
            return Ok(());
        };
        let tables = crate::migrations::tables_of(self.engine.session(), keyspace)
            .await
            .map_err(|e| OpError::Internal(e.to_string()))?;
        let is_catalog = keyspace == self.engine.catalog_keyspace();
        let holds_no_data = tables.iter().all(|table| is_catalog && table == "settings");
        let mismatch = format!(
            "Keyspace {keyspace} already exists with replication {found}, but this init \
             uses replication factor {} in datacenter {}.",
            self.replication_factor, self.datacenter
        );
        let same_values = if unreadable {
            "To fix it, "
        } else {
            "Give init the values the keyspace has (with --replication-factor and \
             --datacenter, or in extenddb.toml for init --overwrite). Or "
        };
        Err(OpError::Internal(if holds_no_data {
            format!(
                "{mismatch} The keyspace holds no data. {same_values}change the keyspace \
                 with ALTER KEYSPACE, or remove it with DROP KEYSPACE {keyspace} \
                 (docs/cassandra-setup.md, Keyspace replication)."
            )
        } else {
            let what = if is_catalog {
                "a deployment's catalog"
            } else {
                "an account's tables"
            };
            format!(
                "{mismatch} The keyspace holds {what}: do not drop it. {same_values}{} \
                 (docs/cassandra-setup.md, Keyspace replication).",
                crate::migrations::alter_steps(change)
            )
        }))
    }

    /// Refuse a catalog keyspace that exists, is not a catalog, and holds
    /// what another program put there ([`foreign_contents`]), naming it.
    /// Then refuse a catalog keyspace whose RF or datacenter differs from
    /// the ones this `init` uses. `init` calls it before it writes anything,
    /// the application role included, and again before it creates the
    /// keyspace.
    ///
    /// An initialized catalog, other than one whose `init` stopped before
    /// its admin user ([`finishes_a_stopped_init`]), gets no replication
    /// check: `init` refuses it under the lock as already initialized, and
    /// advice to change its replication would only change a live catalog.
    /// Returns whether the catalog is such a catalog.
    ///
    /// [`foreign_contents`]: crate::migrations::foreign_contents
    /// [`finishes_a_stopped_init`]: extenddb_storage::bootstrapper::finishes_a_stopped_init
    async fn refuse_a_foreign_keyspace(&self) -> OpResult<bool> {
        let keyspace = self.engine.catalog_keyspace();
        let session = self.engine.session_arc();
        let status =
            match crate::migrations::catalog_status(session.clone(), &self.keyspace_prefix).await {
                Ok(status) => status,
                Err(e) => {
                    // The status read is at QUORUM, so it fails on a catalog
                    // keyspace that an earlier `init` left with more replicas
                    // than the cluster has nodes. The RF check reads at ONE
                    // and names the cause, so its error comes first.
                    self.refuse_other_replication(&keyspace, true).await?;
                    return Err(OpError::Internal(e.to_string()));
                }
            };
        if status == extenddb_storage::migrations::CatalogStatus::Uninitialized {
            let foreign = crate::migrations::foreign_contents(&session, &keyspace)
                .await
                .map_err(|e| OpError::Internal(e.to_string()))?;
            if !foreign.is_empty() {
                return Err(OpError::AlreadyExists(format!(
                    "Keyspace '{keyspace}' exists and is not an ExtendDB catalog: it holds \
                     {}. Choose another --keyspace-prefix.",
                    foreign.join(", ")
                )));
            }
        } else if !extenddb_storage::bootstrapper::finishes_a_stopped_init(self, &status).await? {
            return Ok(true);
        }
        // After the foreign-keyspace check, so a keyspace of another program
        // gets that error, whatever its replication.
        self.refuse_other_replication(&keyspace, false).await?;
        Ok(false)
    }

    /// The lowest account id in the catalog's `accounts` table, read through
    /// the held lease: the default account of an `init` that wrote its row
    /// and stopped before its admin user.
    async fn first_account(&self) -> OpResult<Option<String>> {
        let keyspace = self.engine.catalog_keyspace();
        let rows = self
            .held
            .request(
                "Read the accounts",
                crate::cassandra_util::query_rows_quorum::<extenddb_storage::error::StorageError>(
                    &self.engine.session_arc(),
                    &format!("SELECT account_id FROM {keyspace}.accounts"),
                    cdrs_tokio::query_values!(),
                    "bootstrap_default_account",
                ),
            )
            .await?;
        let ids = crate::cassandra_util::map_rows(
            rows,
            |row| {
                crate::cassandra_util::get_column::<String, OpError>(
                    row,
                    "account_id",
                    "bootstrap_default_account",
                )
            },
            "bootstrap_default_account",
        )?;
        Ok(ids.into_iter().min())
    }

    /// Create `account_id`'s keyspace when it is missing, run every account
    /// migration on it through the runner, as CreateAccount does, and grant
    /// the application role its permissions on it, all under the lease of
    /// the lock `init` holds: the CREATE KEYSPACE through the held lease with
    /// the bound of a schema statement under the lock, and the account run
    /// checking the lease before each write and recording progress on it.
    /// Each step is safe to send again, so a resumed `init` completes a
    /// keyspace an earlier one created.
    async fn complete_account_keyspace(&self, account_id: &str) -> OpResult<()> {
        let account_keyspace = self.engine.account_keyspace(account_id);
        println!("--- Creating account keyspace...");
        self.held
            .request_within(
                "Create account keyspace",
                crate::migrations::LOCKED_DDL_TIMEOUT,
                self.engine.create_keyspace(&account_keyspace),
            )
            .await?;
        self.refuse_other_replication(&account_keyspace, false)
            .await?;
        println!("--- Running the account migrations for the default account...");
        self.held.check("Migrate the default account's keyspace")?;
        let session = self.engine.session_arc();
        let release = env!("CARGO_PKG_VERSION");
        let run = match self.held.handle() {
            Some(lease) => {
                crate::migrations::migrate_account_under(
                    session,
                    &self.keyspace_prefix,
                    &account_keyspace,
                    release,
                    lease,
                )
                .await
            }
            None => {
                crate::migrations::migrate_account(
                    session,
                    &self.keyspace_prefix,
                    &account_keyspace,
                    release,
                )
                .await
            }
        };
        run.map_err(|e| OpError::Internal(format!("Migrate the default account's keyspace: {e}")))?;
        // `init` created this keyspace as the admin role, so the application
        // role holds no permission on it yet.
        self.grant_app_role(&account_keyspace, true).await
    }
}

#[async_trait]
impl Bootstrapper for CassandraBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        // A keyspace of another program, or a catalog keyspace with another
        // RF or datacenter, is refused before the role exists, so a refused
        // `init` creates nothing.
        self.refuse_a_foreign_keyspace().await?;
        let user = &self.config.app_user;
        let password = &self.config.app_password;

        println!("--- Ensuring application user '{user}' exists...");

        // Check if user exists
        let check_cql = "SELECT role FROM system_auth.roles WHERE role = ?";
        let exists = self
            .engine
            .session()
            .query_with_values(check_cql, cdrs_tokio::query_values!(user.as_str()))
            .await
            .ok()
            .and_then(|frame| frame.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .is_some_and(|rows| !rows.is_empty());

        if exists {
            if password.is_empty() {
                return Err(OpError::AlreadyExists(format!(
                    "Application role '{user}' already exists. \
                     Supply its password with --extenddb-pass (or EXTENDDB_APP_PASSWORD) \
                     to regenerate the config, or run 'extenddb destroy' first."
                )));
            }
            println!("    User '{user}' already exists; using supplied password.");
            return Ok(());
        }

        // Validate password character set
        if !password
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.,!@#$%^&*()+=~` ".contains(c))
        {
            return Err(OpError::Validation(
                "Application password contains disallowed characters. \
                 Only ASCII letters, digits, and -_.,!@#$%^&*()+=~` space are permitted."
                    .to_owned(),
            ));
        }

        // Create user with password
        let create_cql = format!(
            "CREATE ROLE IF NOT EXISTS '{user}' WITH PASSWORD = '{password}' AND LOGIN = true"
        );
        self.engine
            .session()
            .query(create_cql)
            .await
            .map_err(|e| OpError::Internal(format!("Create user: {e}")))?;

        println!("    Created user '{user}'.");
        Ok(())
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        if self.config.admin_user == self.config.app_user {
            return Ok(());
        }

        // Check if admin already has the app role
        let check_cql =
            "SELECT role, member FROM system_auth.role_members WHERE role = ? AND member = ?";
        let already_granted = self
            .engine
            .session()
            .query_with_values(
                check_cql,
                cdrs_tokio::query_values!(
                    self.config.app_user.as_str(),
                    self.config.admin_user.as_str()
                ),
            )
            .await
            .ok()
            .and_then(|frame| frame.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .is_some_and(|rows| !rows.is_empty());

        if already_granted {
            return Ok(());
        }

        let grant_cql = format!(
            "GRANT '{}' TO '{}'",
            self.config.app_user, self.config.admin_user
        );
        self.engine.session().query(grant_cql).await.map_err(|e| {
            OpError::Internal(format!(
                "Cannot grant {} to {}: {}",
                self.config.app_user, self.config.admin_user, e
            ))
        })?;
        Ok(())
    }

    /// Create the catalog keyspace and its `settings` table with
    /// `IF NOT EXISTS`, wait for schema agreement, and grant the application
    /// role what it needs. The migration lock is a row of `settings`, so
    /// `init` creates the table before it takes the lock; the catalog is
    /// created under the lock, which refuses one that is already
    /// initialized. Two `init`s that start together can both send these two
    /// statements before either holds the lock; that window is accepted. A
    /// keyspace of the catalog's name that holds tables and is not a catalog
    /// is refused before anything is written to it.
    async fn create_catalog_db(&self) -> OpResult<()> {
        let keyspace = self.engine.catalog_keyspace();
        println!("--- Creating catalog keyspace '{keyspace}'...");
        let session = self.engine.session_arc();
        let initialized = self.refuse_a_foreign_keyspace().await?;
        self.engine
            .create_keyspace(&keyspace)
            .await
            .map_err(|e| OpError::Internal(format!("Create catalog keyspace: {e}")))?;
        if !initialized {
            self.refuse_other_replication(&keyspace, false).await?;
        }
        crate::migrations::create_lock_table(&session, &keyspace)
            .await
            .map_err(|e| OpError::Internal(e.to_string()))?;
        self.grant_app_role(&keyspace, false).await?;
        println!("    Created.");
        Ok(())
    }

    async fn create_data_db(&self) -> OpResult<()> {
        // Cassandra uses per-account keyspaces, not a single data keyspace
        println!("--- Cassandra uses per-account keyspaces (created on demand)");
        Ok(())
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        // Not applicable for Cassandra (no separate data connection)
        Ok(())
    }

    /// Keep the lease of the lock `init` holds, checked before each
    /// bootstrap row; a lock another backend took carries none.
    fn hold(&self, lock: &HeldLock) {
        if let Some(lease) = crate::migrations::lease_of(lock) {
            self.held.hold(lease);
        }
    }

    /// Store a new encryption key unless the catalog has one, with
    /// `INSERT ... IF NOT EXISTS`, so a key a concurrent `init` stored first
    /// is never replaced: credentials a server encrypted with it stay
    /// readable. Nothing else writes that row.
    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        let keyspace = self.engine.catalog_keyspace();
        println!("--- Generating AES-256-GCM encryption key...");
        let key_b64 = generate_encryption_key().map_err(OpError::Internal)?;
        let insert_cql = format!(
            "INSERT INTO {keyspace}.settings (key, value) VALUES ('encryption_key', ?) IF NOT \
             EXISTS"
        );
        let applied = self
            .held
            .request(
                "Store encryption key",
                crate::cassandra_util::apply_lwt::<extenddb_storage::error::StorageError>(
                    &self.engine.session_arc(),
                    &insert_cql,
                    cdrs_tokio::query_values!(key_b64),
                    "bootstrap_encryption_key",
                ),
            )
            .await?;
        if applied {
            println!("    Encryption key stored.");
        } else {
            println!("--- Encryption key already exists, skipping.");
        }
        Ok(())
    }

    /// Create the default account unless the catalog has an account, whose
    /// keyspace is completed instead, so an `init` resumed after this step
    /// creates no second account. The keyspace is created and migrated, and
    /// the application role granted its permissions on it, before the
    /// `accounts` row is written: the catalog's status reads the account
    /// keyspaces from `accounts`, so an `init` stopped before the row leaves
    /// a catalog whose status is `Current`, which a resumed `init` finishes.
    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let keyspace = self.engine.catalog_keyspace();
        if let Some(account_id) = self.first_account().await? {
            println!("--- Default account already exists; completing its keyspace.");
            return self.complete_account_keyspace(&account_id).await;
        }

        let account_id = generate_account_id();
        let account_name = "default";
        self.complete_account_keyspace(&account_id).await?;

        let insert_cql = format!(
            "INSERT INTO {keyspace}.accounts (account_id, account_name, created_at) \
             VALUES (?, ?, toTimestamp(now())) IF NOT EXISTS"
        );
        let applied = self
            .held
            .request(
                "Create account",
                crate::cassandra_util::apply_lwt::<extenddb_storage::error::StorageError>(
                    &self.engine.session_arc(),
                    &insert_cql,
                    cdrs_tokio::query_values!(account_id.as_str(), account_name),
                    "bootstrap_default_account",
                ),
            )
            .await?;
        if applied {
            println!("--- Default account created");
        } else {
            println!("--- Default account already exists, skipping.");
        }
        Ok(())
    }

    async fn bootstrap_admin_user(
        &self,
        env_user: Option<&str>,
        env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        let keyspace = self.engine.catalog_keyspace();

        let username = env_user.unwrap_or("admin");
        let from_env = env_user.is_some() && env_password.is_some();

        println!("--- Creating admin user...");

        // Generate or use provided password
        let password = if let Some(pw) = env_password {
            pw.to_string()
        } else {
            generate_random_password()
        };

        // Hash password (using bcrypt in blocking task to avoid blocking async runtime)
        let password_hash = hash_password_async(password.clone()).await?;

        // Insert admin user — IF NOT EXISTS makes this safe for concurrent bootstrap
        let insert_cql = format!(
            "INSERT INTO {keyspace}.admin_users (admin_name, password_hash) VALUES (?, ?) IF NOT EXISTS"
        );

        let applied = self
            .held
            .request(
                "Create admin user",
                crate::cassandra_util::apply_lwt::<extenddb_storage::error::StorageError>(
                    &self.engine.session_arc(),
                    &insert_cql,
                    cdrs_tokio::query_values!(username, password_hash),
                    "bootstrap_admin_user",
                ),
            )
            .await?;

        if !applied {
            println!("--- Admin user already exists, skipping.");
            return Ok(AdminBootstrapResult {
                username: username.to_string(),
                generated_password: None,
                already_existed: true,
                from_env,
            });
        }

        let generated_password = if from_env { None } else { Some(password) };

        Ok(AdminBootstrapResult {
            username: username.to_string(),
            generated_password,
            already_existed: false,
            from_env,
        })
    }

    /// Read at `LOCAL_QUORUM`, since `init` finishes a catalog this answers
    /// `false` for: at `ONE` a replica that missed the admin user's write
    /// would read as empty.
    async fn has_admin_user(&self) -> OpResult<bool> {
        let keyspace = self.engine.catalog_keyspace();
        let rows = self
            .held
            .request(
                "Read the admin users",
                crate::cassandra_util::query_rows_quorum::<extenddb_storage::error::StorageError>(
                    &self.engine.session_arc(),
                    &format!("SELECT admin_name FROM {keyspace}.admin_users LIMIT 1"),
                    cdrs_tokio::query_values!(),
                    "has_admin_user",
                ),
            )
            .await?;
        Ok(!rows.is_empty())
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        let keyspace = self.engine.catalog_keyspace();

        let cql = format!("SELECT table_name FROM {keyspace}.tables ORDER BY table_name");
        let rows = match self.engine.session().query(cql).await {
            Ok(frame) => frame
                .response_body()
                .ok()
                .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
                .unwrap_or_default(),
            Err(_) => return Ok(Vec::new()),
        };

        let table_names: Vec<String> = rows
            .into_iter()
            .filter_map(|row| {
                TryFromRowTrait::try_from_row(row)
                    .ok()
                    .map(|r: TableNameRow| r.table_name)
            })
            .collect();

        Ok(table_names)
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        // Cassandra doesn't have a single data database
        Ok(None)
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        println!("--- Dropping all ExtendDB keyspaces...");

        // List all keyspaces with our prefix
        let prefix = format!(
            "{}_",
            self.engine.catalog_keyspace().trim_end_matches("_catalog")
        );

        let cql = "SELECT keyspace_name FROM system_schema.keyspaces";
        let rows = self
            .engine
            .session()
            .query(cql)
            .await
            .map_err(|e| OpError::Internal(format!("List keyspaces: {e}")))?
            .response_body()
            .map_err(|e| OpError::Internal(format!("Get response: {e}")))?
            .into_rows()
            .ok_or_else(|| OpError::Internal("No rows returned".to_string()))?;

        let keyspaces: Vec<String> = rows
            .into_iter()
            .filter_map(|row| {
                TryFromRowTrait::try_from_row(row)
                    .ok()
                    .map(|r: KeyspaceNameRow| r.keyspace_name)
            })
            .filter(|name| name.starts_with(&prefix))
            .collect();

        for keyspace in keyspaces {
            println!("    Dropping keyspace: {keyspace}");
            self.engine
                .drop_keyspace(&keyspace)
                .await
                .map_err(|e| OpError::Internal(e.to_string()))?;
        }

        println!("    All ExtendDB keyspaces dropped");
        Ok(())
    }

    /// The deployment `init` creates, reached as the admin role `init`
    /// connects with.
    fn init_target(&self) -> Target {
        Target {
            location: self.engine_connection_string.clone(),
            data_store: None,
            admin: Some(AdminRole {
                user: self.config.admin_user.clone(),
                password: self.config.admin_password.clone(),
            }),
            data_connection: None,
        }
    }

    fn catalog_database_name(&self) -> String {
        self.engine.catalog_keyspace()
    }

    fn endpoint_info(&self) -> String {
        self.config.host.clone() + ":" + &self.config.port.to_string()
    }

    fn catalog_connection_url(&self) -> String {
        format!("{}:{}", self.config.host, self.config.port)
    }

    fn generate_backend_config_section(&self) -> String {
        Self::backend_config_section(
            &self.catalog_connection_url(),
            &self.keyspace_prefix,
            &self.config.app_user,
            &self.config.app_password,
            self.replication_factor,
            &self.datacenter,
        )
    }
}

impl CassandraBootstrapper {
    /// Pure helper so the output can be tested without a live Cassandra connection.
    ///
    /// Writes the application role `ensure_app_user` created or verified, so
    /// `serve` logs in as it: a node with `PasswordAuthenticator` refuses a
    /// connection without credentials. The values are TOML-quoted (a role
    /// that already existed keeps whatever password the operator supplied).
    /// `write_config` makes the file 0600.
    ///
    /// Writes the replication factor and datacenter `init` created its
    /// keyspaces with, so the account keyspaces `serve` creates later match.
    fn backend_config_section(
        contact_point: &str,
        keyspace_prefix: &str,
        app_user: &str,
        app_password: &str,
        replication_factor: u32,
        datacenter: &str,
    ) -> String {
        let user = toml::Value::String(app_user.to_owned());
        let password = toml::Value::String(app_password.to_owned());
        let datacenter = toml::Value::String(datacenter.to_owned());
        format!(
            r#"[storage.cassandra]
contact_points = ["{contact_point}"]
username = {user}               # Application role
password = {password}
keyspace_prefix = "{keyspace_prefix}"
replication_factor = {replication_factor}           # Use 3 or more for a multi-node cluster
datacenter = {datacenter}
max_connections = 10"#
        )
    }
}

#[cfg(test)]
mod tests {
    use super::CassandraBootstrapper;

    #[test]
    fn backend_config_section_uses_actual_keyspace_prefix() {
        let section = CassandraBootstrapper::backend_config_section(
            "cassandra-node-1:9042",
            "mycompany",
            "extenddb",
            "pw",
            1,
            "datacenter1",
        );
        assert!(
            section.contains("keyspace_prefix = \"mycompany\""),
            "config must use the supplied prefix, got:\n{section}"
        );
        assert!(
            !section.contains("keyspace_prefix = \"extenddb\""),
            "config must not contain hardcoded 'extenddb' prefix, got:\n{section}"
        );
        assert!(section.contains("contact_points = [\"cassandra-node-1:9042\"]"));
    }

    /// `init` once left `username`/`password` commented out, so `serve`
    /// connected without credentials and a `PasswordAuthenticator` node
    /// refused it. The section must parse into the config `serve` reads,
    /// carrying the application role, including a password with TOML-special
    /// characters (a pre-existing role's password is not validated).
    #[test]
    fn backend_config_section_carries_the_application_role() {
        for password in ["extenddb-local-dev", r#"a"b\c 'd' #e"#, ""] {
            let section = CassandraBootstrapper::backend_config_section(
                "127.0.0.1:9042",
                "pfx",
                "app_role",
                password,
                1,
                "datacenter1",
            );
            let doc: toml::Value = toml::from_str(&section).expect("section parses");
            let config: crate::config::CassandraStorageConfig = doc
                .get("storage")
                .and_then(|s| s.get("cassandra"))
                .expect("storage.cassandra")
                .clone()
                .try_into()
                .expect("section is a CassandraStorageConfig");
            // No message: the section holds the password.
            assert_eq!(config.username.as_deref(), Some("app_role"));
            assert!(
                config.password.as_deref() == Some(password),
                "password not carried"
            );
            assert_eq!(config.keyspace_prefix, "pfx");
        }
    }

    /// `init` once wrote `replication_factor = 1` and `datacenter1` whatever
    /// it created its keyspaces with, so the account keyspaces `serve`
    /// created later could differ from the catalog's.
    #[test]
    fn backend_config_section_carries_the_replication() {
        let section = CassandraBootstrapper::backend_config_section(
            "127.0.0.1:9042",
            "pfx",
            "app_role",
            "pw",
            3,
            "dc-east.1",
        );
        let doc: toml::Value = toml::from_str(&section).expect("section parses");
        let config: crate::config::CassandraStorageConfig = doc
            .get("storage")
            .and_then(|s| s.get("cassandra"))
            .expect("storage.cassandra")
            .clone()
            .try_into()
            .expect("section is a CassandraStorageConfig");
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.datacenter, "dc-east.1");
    }

    #[test]
    fn replication_defaults_to_one_replica_in_datacenter1() {
        assert_eq!(
            super::resolve_replication(None, None, None).expect("defaults"),
            (1, "datacenter1".to_owned())
        );
    }

    #[test]
    fn replication_flags_are_used_without_a_config_file() {
        assert_eq!(
            super::resolve_replication(Some("3"), Some("dc1"), None).expect("flags"),
            (3, "dc1".to_owned())
        );
    }

    /// The config file's datacenter was once ignored for `datacenter1`.
    #[test]
    fn replication_comes_from_the_config_file() {
        assert_eq!(
            super::resolve_replication(None, None, Some((3, "dc1"))).expect("file"),
            (3, "dc1".to_owned())
        );
        assert_eq!(
            super::resolve_replication(Some("3"), Some("dc1"), Some((3, "dc1")))
                .expect("agreeing flags"),
            (3, "dc1".to_owned())
        );
    }

    #[test]
    fn a_replication_flag_that_disagrees_with_the_config_file_is_refused() {
        for (rf, dc) in [(Some("1"), None), (None, Some("dc2"))] {
            let err = super::resolve_replication(rf, dc, Some((3, "dc1")))
                .expect_err("conflict refused")
                .to_string();
            assert!(err.contains("conflicts with config file"), "{err}");
        }
    }

    /// The bootstrapper refuses an RF that is not a positive integer, as
    /// clap does for the flag: from `--replication-factor`, and from the
    /// config file on `init --overwrite`.
    #[test]
    fn a_replication_factor_that_is_not_a_positive_integer_is_refused() {
        for rf in ["0", "-1", "three", ""] {
            let err = super::resolve_replication(Some(rf), None, None)
                .expect_err("refused")
                .to_string();
            assert!(err.contains("must be a positive integer"), "{rf}: {err}");
        }
        let err = super::resolve_replication(None, None, Some((0, "dc1")))
            .expect_err("zero in the file refused")
            .to_string();
        assert!(err.contains("must be a positive integer"), "{err}");
    }

    /// The error names where the bad datacenter came from: the flag, or the
    /// config file on `init --overwrite`.
    #[test]
    fn a_bad_datacenter_error_names_its_source() {
        let from_flag = super::resolve_replication(None, Some("dc 1"), None)
            .expect_err("refused")
            .to_string();
        assert!(from_flag.contains("--datacenter"), "{from_flag}");

        let from_file = super::resolve_replication(None, None, Some((1, "dc 1")))
            .expect_err("refused")
            .to_string();
        assert!(
            from_file.contains("datacenter in the config file")
                && !from_file.contains("--datacenter"),
            "{from_file}"
        );
    }

    #[test]
    fn a_datacenter_that_would_break_the_create_keyspace_is_refused() {
        for dc in ["", "dc'1", "dc 1", "dc1}"] {
            assert!(
                super::resolve_replication(None, Some(dc), None).is_err(),
                "{dc:?} accepted"
            );
        }
    }
}
