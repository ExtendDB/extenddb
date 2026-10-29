// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb migrate` — apply catalog schema migrations (REQ-CAT-014).
//!
//! Reads current catalog version, runs pending migrations, and reports the result.
//!
//! Exit status: 0 when nothing is pending or the migrations were applied;
//! [`EXIT_PENDING`] (3) when work is pending and `--yes` was not given (the
//! preview); 1 on any error. A scripted upgrade can run the preview under
//! `set -e` by checking for 3 explicitly.

use clap::Args;

use extenddb_config as config;
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::OpError;

/// Exit status of a preview (`migrate` without `--yes`) that found work to do.
/// Distinct from 1 so a script can tell "pending" from "failed".
pub const EXIT_PENDING: u8 = 3;

#[derive(Args)]
pub struct MigrateArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// `PostgreSQL` admin user (for catalog migrations)
    #[arg(long)]
    pg_user: Option<String>,

    /// `PostgreSQL` admin password.
    /// Prefer `EXTENDDB_PG_PASSWORD` to keep the value out of process arguments.
    #[arg(long)]
    pg_pass: Option<String>,

    /// `PostgreSQL` data database to migrate (PostgreSQL only; ignored on
    /// other backends). Normally not needed: the name `init` recorded in the
    /// catalog's settings is used, else the catalog database name without its
    /// `_catalog` suffix. Pass it for a catalog that has no recorded name and
    /// a data database named otherwise.
    #[arg(long)]
    data_db: Option<String>,

    /// Cassandra admin user (the role `init` used); without it the
    /// connection is attempted as `$USER`
    #[arg(long)]
    cassandra_user: Option<String>,

    /// Cassandra admin password.
    /// Prefer `EXTENDDB_CASSANDRA_PASSWORD` to keep the value out of process arguments.
    #[arg(long)]
    cassandra_pass: Option<String>,

    /// Apply the pending migrations. Without it, migrate only reports what is
    /// pending and exits 3 if there is any (0 if everything is up to date)
    #[arg(long)]
    yes: bool,
}

/// The message of a bootstrapper error, without the variant name that `{e:?}`
/// adds (which printed `Internal("Internal(\"Cannot connect ...\")")`).
pub(crate) fn op_err(e: OpError) -> anyhow::Error {
    match e {
        OpError::Validation(m)
        | OpError::AlreadyExists(m)
        | OpError::NotFound(m)
        | OpError::HasDependents(m)
        | OpError::Internal(m)
        | OpError::Database { message: m, .. } => anyhow::anyhow!(m),
    }
}

/// The message of a storage error; `Internal` carries a complete sentence
/// already, so its "Internal error: " prefix is dropped.
pub(crate) fn storage_err(e: StorageError) -> anyhow::Error {
    match e {
        StorageError::Internal(m) => anyhow::anyhow!(m),
        other => anyhow::anyhow!(other.to_string()),
    }
}

/// Run `extenddb migrate`; returns the process exit status (see the module docs).
pub async fn run(args: MigrateArgs) -> anyhow::Result<u8> {
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found. Run 'extenddb init' to set up a deployment, \
             or use --config <path> to specify a different location.",
            args.config,
        );
    }
    // Load the config for validation and to reach the catalog for the policy
    // rewrite: migrate drives the schema steps through the bootstrapper, but a
    // malformed config should fail here rather than midway through a migration.
    let app_config = config::load(&args.config)?;

    println!("=== extenddb migrate ===");
    println!("Config:           {}", args.config);
    println!();

    // Collect CLI args for backend-specific parsing. The environment-sourced
    // secret is appended only to this in-process copy, not OS-visible argv.
    let cli_args = backend_args(
        std::env::args().collect(),
        std::env::var_os("EXTENDDB_PG_PASSWORD"),
        std::env::var_os("EXTENDDB_CASSANDRA_PASSWORD"),
    )?;

    // Create bootstrapper via registry
    let bootstrap = extenddb_storage::bootstrapper::create_bootstrapper(&args.config, &cli_args)
        .await
        .map_err(storage_err)?;

    // Serialize concurrent migrators, such as several replicas starting at
    // once, so they don't race each other applying schema changes.
    bootstrap.acquire_migration_lock().await.map_err(op_err)?;
    let result = match CatalogResourceTagStep::open(app_config.storage.connection_config()).await {
        Ok(rtag) => apply_migrations(bootstrap.as_ref(), &args, &rtag).await,
        Err(e) => Err(e),
    };
    if let Err(e) = bootstrap.release_migration_lock().await {
        tracing::warn!("Failed to release migration lock: {}", op_err(e));
    }
    result
}

/// The argument vector the backend bootstrapper parses: `argv`, plus
/// `--pg-pass` from `EXTENDDB_PG_PASSWORD` and `--cassandra-pass` from
/// `EXTENDDB_CASSANDRA_PASSWORD` when the flag itself was not given.
fn backend_args(
    argv: Vec<String>,
    pg_pass_env: Option<std::ffi::OsString>,
    cassandra_pass_env: Option<std::ffi::OsString>,
) -> anyhow::Result<Vec<String>> {
    let mut args = argv;
    crate::util::append_secret_arg(&mut args, "--pg-pass", pg_pass_env, "EXTENDDB_PG_PASSWORD")?;
    crate::util::append_secret_arg(
        &mut args,
        "--cassandra-pass",
        cassandra_pass_env,
        "EXTENDDB_CASSANDRA_PASSWORD",
    )?;
    Ok(args)
}

/// Run the version checks and apply pending migrations while the migration lock
/// is held. Split out of `run` so that the lock is always released afterwards.
async fn apply_migrations(
    bootstrap: &dyn extenddb_storage::bootstrapper::Bootstrapper,
    args: &MigrateArgs,
    rtag: &dyn ResourceTagStep,
) -> anyhow::Result<u8> {
    // Show current catalog version.
    println!("--- Checking current catalog version...");
    let current = bootstrap.read_catalog_version().await.map_err(op_err)?;
    let current_display = current.as_deref().unwrap_or("none");
    println!("  Current version: {current_display}");

    let expected = bootstrap.expected_catalog_version();
    refuse_newer_catalog(current.as_deref(), &expected)?;
    let catalog_pending = current.as_deref() != Some(expected.as_str());

    // Data migrations are tracked in the data database's own ledger, separate
    // from and independent of the catalog version, so they must be checked (and
    // applied) on their own — a release that only changes the data schema does
    // not bump the catalog version.
    println!("--- Checking data migrations...");
    let data_pending = bootstrap.pending_data_migrations().await.map_err(op_err)?;
    if data_pending.is_empty() {
        println!("  Data migrations up to date.");
    } else {
        println!("  Pending: {}", data_pending.join(", "));
    }

    // Detect table metadata damaged by the pre-fix UpdateTable (#259), which
    // deleted a table's own key attribute definitions when a GSI was added.
    //
    // Detection is read-only and runs even when the catalog version is already
    // current: the damage is in a row's contents, not in the schema, so an
    // up-to-date deployment is exactly the case that needs checking. Nothing is
    // written until --yes has been given, below.
    println!("--- Checking table key metadata...");
    let detected = bootstrap
        .repair_lost_sort_key_definitions(false)
        .await
        .map_err(op_err)?;
    let repair_pending = !detected.repaired.is_empty();
    if !repair_pending && detected.needs_attention.is_empty() {
        println!("  Table key metadata intact.");
    } else {
        for line in &detected.repaired {
            println!("  Repairable: {line}");
        }
        for line in &detected.needs_attention {
            println!("  NEEDS ATTENTION: {line}");
        }
    }

    // Detect stored policies that still name a dynamodb:ResourceTag condition
    // key. Detection is read-only and runs on every migrate (like the repair
    // above), so an interrupted run is finished by the next. The catalog is
    // reached through a catalog-only management store, not the bootstrapper's
    // schema connection.
    println!("--- Checking resource-tag condition keys...");
    let (rtag_detect, marker_present) = rtag.detect().await?;
    if rtag_detect.rewritten.is_empty() && rtag_detect.needs_attention.is_empty() {
        println!("  No dynamodb:ResourceTag condition keys found.");
    } else {
        for r in &rtag_detect.rewritten {
            println!(
                "  Rewrite: account {} {} policy {} ({} key(s))",
                r.account_id, r.principal, r.policy_name, r.rewrites
            );
        }
        for n in &rtag_detect.needs_attention {
            println!(
                "  NEEDS ATTENTION: account {} {} policy {} (aws:ResourceTag and \
                 dynamodb:ResourceTag forms collide; left unchanged)",
                n.account_id, n.principal, n.policy_name
            );
        }
    }
    if rtag_detect.errors > 0 {
        println!(
            "  WARNING: {} policy lookup(s) failed during detection.",
            rtag_detect.errors
        );
    }
    let rtag_pending = !rtag_detect.rewritten.is_empty() || rtag_detect.errors > 0;
    // A catalog with no marker must have one written, even when there is
    // nothing to rewrite, so that serve will start.
    let marker_pending = !marker_present;

    if !catalog_pending
        && data_pending.is_empty()
        && !repair_pending
        && !rtag_pending
        && !marker_pending
    {
        println!();
        if detected.needs_attention.is_empty() && rtag_detect.needs_attention.is_empty() {
            println!(
                "Everything is up to date (catalog version {expected}). No migrations needed."
            );
        } else {
            // Nothing is automatically applicable, but saying "everything is up
            // to date" straight after a NEEDS ATTENTION line would be
            // contradictory: those items require a human.
            println!(
                "No migrations needed (catalog version {expected}), but {} item(s) above \
                 need manual attention.",
                detected.needs_attention.len() + rtag_detect.needs_attention.len()
            );
        }
        return Ok(0);
    }

    if !args.yes {
        let mut what = Vec::new();
        if catalog_pending {
            what.push(format!("catalog {current_display} -> {expected}"));
        }
        if !data_pending.is_empty() {
            what.push(format!("data migrations [{}]", data_pending.join(", ")));
        }
        if repair_pending {
            what.push(format!(
                "table key metadata repairs [{}]",
                detected.repaired.join("; ")
            ));
        }
        if !rtag_detect.rewritten.is_empty() {
            what.push(format!(
                "resource-tag condition-key rewrites [{} policy(ies)]",
                rtag_detect.rewritten.len()
            ));
        }
        if marker_pending {
            what.push("record the resource-tag migration marker".to_owned());
        }
        if rtag_detect.errors > 0 {
            what.push(format!(
                "{} policy lookup(s) that failed and must be retried",
                rtag_detect.errors
            ));
        }
        if !rtag_detect.needs_attention.is_empty() {
            what.push(format!(
                "{} policy(ies) listed as NEEDS ATTENTION, which must be fixed by hand before \
                 the migration can complete",
                rtag_detect.needs_attention.len()
            ));
        }
        // A preview, not a failure: report and exit EXIT_PENDING so a script
        // under `set -e` can tell it from an error (exit 1).
        println!();
        println!(
            "Pending: {}. Nothing was changed; run again with --yes to apply \
             (exit status {EXIT_PENDING}: work pending).",
            what.join("; ")
        );
        return Ok(EXIT_PENDING);
    }

    if catalog_pending {
        bootstrap.run_catalog_migrations().await.map_err(op_err)?;
    }

    // Always run data migrations: the data-database ledger is idempotent
    // (already-applied migrations are skipped) and independent of the catalog
    // version, so an upgrade that only touched the data schema is still applied.
    bootstrap.run_data_migrations().await.map_err(op_err)?;

    // Apply the table key metadata repair after the schema migrations, so it never
    // reads or writes a pre-migration catalog shape.
    if repair_pending {
        let applied = bootstrap
            .repair_lost_sort_key_definitions(true)
            .await
            .map_err(op_err)?;
        for line in &applied.repaired {
            println!("  Restored sort key definition: {line}");
        }
    }

    // Rewrite stored policies from dynamodb:ResourceTag to aws:ResourceTag,
    // after the schema migrations. Every storage error is counted; on a
    // non-zero count migrate reports it, does NOT record the completion
    // marker, and exits non-zero, so the next run finishes the rewrite. No
    // policy body is logged.
    let rtag_applied = rtag.rewrite().await;
    for r in &rtag_applied.rewritten {
        println!(
            "  Rewrote resource-tag keys: account {} {} policy {} ({} key(s))",
            r.account_id, r.principal, r.policy_name, r.rewrites
        );
    }
    for n in &rtag_applied.needs_attention {
        println!(
            "  NEEDS ATTENTION: account {} {} policy {} left unchanged (collision)",
            n.account_id, n.principal, n.policy_name
        );
    }
    if rtag_applied.errors > 0 {
        anyhow::bail!(
            "resource-tag migration incomplete: {} policy lookup(s) failed. \
             Re-run `extenddb migrate --yes` to finish; the migration marker was not recorded.",
            rtag_applied.errors
        );
    }
    // A policy left unchanged because both resource-tag forms name the same
    // tag key in one operator block is not migrated. Recording the marker
    // would let serve evaluate it with the dynamodb: half absent, which can
    // turn a Deny off. Withhold the marker so serve keeps refusing until the
    // policy is fixed by hand and migrate is run again.
    if !rtag_applied.needs_attention.is_empty() {
        anyhow::bail!(
            "resource-tag migration incomplete: {} policy(ies) above name both \
             aws:ResourceTag/<key> and dynamodb:ResourceTag/<key> for the same key in one \
             condition operator and were left unchanged. Remove one of the two forms from \
             each, then re-run `extenddb migrate --yes`; the migration marker was not recorded.",
            rtag_applied.needs_attention.len()
        );
    }
    // Record the completion marker only after a clean rewrite. serve reads this
    // row and refuses to start without it.
    rtag.record_marker().await?;

    // Read new catalog version.
    let new = bootstrap.read_catalog_version().await.map_err(op_err)?;
    let new_display = new.as_deref().unwrap_or("none");

    println!();
    println!("=== extenddb migrate complete ===");
    println!("Catalog version: {current_display} -> {new_display}");

    Ok(0)
}

/// Boxed future returned by [`ResourceTagStep`].
type StepFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// The resource-tag condition-key migration (stored policies rewritten from
/// `dynamodb:ResourceTag` to `aws:ResourceTag`, then a completion marker),
/// behind a trait so the rest of [`apply_migrations`] can be exercised without
/// a catalog.
trait ResourceTagStep: Sync {
    /// The read-only detection report, and whether the completion marker is
    /// recorded.
    fn detect(
        &self,
    ) -> StepFuture<'_, anyhow::Result<(extenddb_storage::policy_migration::MigrationReport, bool)>>;
    /// Rewrite the stored policies.
    fn rewrite(&self) -> StepFuture<'_, extenddb_storage::policy_migration::MigrationReport>;
    /// Record the completion marker `serve` requires.
    fn record_marker(&self) -> StepFuture<'_, anyhow::Result<()>>;
}

/// [`ResourceTagStep`] over the deployment's catalog.
struct CatalogResourceTagStep {
    mgmt: Box<dyn extenddb_storage::management_store::ManagementStore>,
    settings: Box<dyn extenddb_storage::management_store::SettingsStore>,
}

impl CatalogResourceTagStep {
    /// Open catalog-only management and settings stores (not the
    /// bootstrapper's schema connection).
    async fn open(connection: &str) -> anyhow::Result<Self> {
        let mgmt = extenddb_storage::management_store_factory::create_management_store(connection)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let settings = extenddb_storage::settings_store::create_settings_store(connection)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(Self { mgmt, settings })
    }
}

impl ResourceTagStep for CatalogResourceTagStep {
    fn detect(
        &self,
    ) -> StepFuture<'_, anyhow::Result<(extenddb_storage::policy_migration::MigrationReport, bool)>>
    {
        Box::pin(async move {
            let report = extenddb_storage::policy_migration::migrate_resource_tag_condition_keys(
                self.mgmt.as_ref(),
                false,
            )
            .await;
            let marker_present = self
                .settings
                .get_setting(extenddb_core::settings_keys::RESOURCE_TAG_MIGRATION)
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?
                .is_some();
            Ok((report, marker_present))
        })
    }

    fn rewrite(&self) -> StepFuture<'_, extenddb_storage::policy_migration::MigrationReport> {
        Box::pin(
            extenddb_storage::policy_migration::migrate_resource_tag_condition_keys(
                self.mgmt.as_ref(),
                true,
            ),
        )
    }

    fn record_marker(&self) -> StepFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            self.settings
                .set_setting(
                    extenddb_core::settings_keys::RESOURCE_TAG_MIGRATION,
                    extenddb_core::settings_keys::RESOURCE_TAG_MIGRATION_DONE,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))
        })
    }
}

/// A catalog written by a newer binary is not a pending upgrade. The runners
/// write the compiled version after every walk, so letting an older binary
/// "migrate" it would stamp the older version onto the newer schema and let
/// that binary start against tables it does not understand. A stored version
/// this binary cannot parse is refused for the same reason: it is the one
/// case the guard cannot rule out being newer. The rule is shared with the
/// runners (`extenddb_storage::util::check_stored_catalog_version`).
fn refuse_newer_catalog(current: Option<&str>, expected: &str) -> anyhow::Result<()> {
    use extenddb_core::version::CatalogVersion;
    let expected_v = expected.parse::<CatalogVersion>().map_err(|e| {
        anyhow::anyhow!("this binary's expected catalog version {expected:?} is invalid: {e}")
    })?;
    extenddb_storage::util::check_stored_catalog_version(current, expected_v).map_err(|e| {
        anyhow::anyhow!(
            "{e} extenddb migrate does not downgrade a catalog or overwrite one it cannot read."
        )
    })
}

#[cfg(test)]
mod preview_tests {
    //! The preview's exit status and that it changes nothing (it used to
    //! exit 1 with "Error: --yes is required", which stops a
    //! scripted upgrade under `set -e`).
    use std::sync::Mutex;

    use super::{ResourceTagStep, StepFuture};

    use async_trait::async_trait;
    use extenddb_storage::bootstrapper::{AdminBootstrapResult, Bootstrapper, KeyDefinitionRepair};
    use extenddb_storage::management_store::{OpError, OpResult};

    use super::{EXIT_PENDING, MigrateArgs, apply_migrations, op_err, storage_err};

    struct Fake {
        version: Mutex<Option<String>>,
        data_pending: Mutex<Vec<String>>,
        catalog_runs: Mutex<u32>,
        data_runs: Mutex<u32>,
    }

    impl Fake {
        fn new(version: Option<&str>, data_pending: &[&str]) -> Self {
            Self {
                version: Mutex::new(version.map(str::to_owned)),
                data_pending: Mutex::new(data_pending.iter().map(|s| (*s).to_owned()).collect()),
                catalog_runs: Mutex::new(0),
                data_runs: Mutex::new(0),
            }
        }
        fn runs(&self) -> (u32, u32) {
            (
                *self.catalog_runs.lock().unwrap(),
                *self.data_runs.lock().unwrap(),
            )
        }
    }

    fn unused<T>() -> OpResult<T> {
        Err(OpError::Internal("not used by migrate".to_owned()))
    }

    #[async_trait]
    impl Bootstrapper for Fake {
        async fn ensure_app_user(&self) -> OpResult<()> {
            unused()
        }
        async fn grant_app_role_to_admin(&self) -> OpResult<()> {
            unused()
        }
        async fn create_catalog_db(&self) -> OpResult<()> {
            unused()
        }
        async fn create_data_db(&self) -> OpResult<()> {
            unused()
        }
        async fn run_catalog_migrations(&self) -> OpResult<()> {
            *self.catalog_runs.lock().unwrap() += 1;
            *self.version.lock().unwrap() = Some(self.expected_catalog_version());
            Ok(())
        }
        async fn run_data_migrations(&self) -> OpResult<()> {
            *self.data_runs.lock().unwrap() += 1;
            self.data_pending.lock().unwrap().clear();
            Ok(())
        }
        async fn repair_lost_sort_key_definitions(
            &self,
            _apply: bool,
        ) -> OpResult<KeyDefinitionRepair> {
            Ok(KeyDefinitionRepair::default())
        }
        async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
            Ok(self.data_pending.lock().unwrap().clone())
        }
        async fn record_data_connection(&self) -> OpResult<()> {
            unused()
        }
        async fn bootstrap_encryption_key(&self) -> OpResult<()> {
            unused()
        }
        async fn bootstrap_default_account(&self) -> OpResult<()> {
            unused()
        }
        async fn bootstrap_admin_user(
            &self,
            _env_user: Option<&str>,
            _env_password: Option<&str>,
        ) -> OpResult<AdminBootstrapResult> {
            unused()
        }
        async fn is_catalog_initialized(&self) -> OpResult<bool> {
            Ok(true)
        }
        async fn list_table_names(&self) -> OpResult<Vec<String>> {
            unused()
        }
        async fn get_data_db_name(&self) -> OpResult<Option<String>> {
            unused()
        }
        async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
            unused()
        }
        async fn read_catalog_version(&self) -> OpResult<Option<String>> {
            Ok(self.version.lock().unwrap().clone())
        }
        fn expected_catalog_version(&self) -> String {
            "0.0.4".to_owned()
        }
        fn catalog_database_name(&self) -> String {
            "fake".to_owned()
        }
        fn endpoint_info(&self) -> String {
            "fake".to_owned()
        }
        fn catalog_connection_url(&self) -> String {
            "fake://".to_owned()
        }
        fn generate_backend_config_section(&self) -> String {
            String::new()
        }
    }

    /// A catalog whose policies need no resource-tag rewrite and that already
    /// records the completion marker.
    struct NoResourceTags;

    impl ResourceTagStep for NoResourceTags {
        fn detect(
            &self,
        ) -> StepFuture<
            '_,
            anyhow::Result<(extenddb_storage::policy_migration::MigrationReport, bool)>,
        > {
            Box::pin(async { Ok((Default::default(), true)) })
        }
        fn rewrite(&self) -> StepFuture<'_, extenddb_storage::policy_migration::MigrationReport> {
            Box::pin(async { Default::default() })
        }
        fn record_marker(&self) -> StepFuture<'_, anyhow::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn args(yes: bool) -> MigrateArgs {
        MigrateArgs {
            config: "extenddb.toml".to_owned(),
            pg_user: None,
            pg_pass: None,
            data_db: None,
            cassandra_user: None,
            cassandra_pass: None,
            yes,
        }
    }

    #[tokio::test]
    async fn a_preview_with_pending_work_exits_3_and_changes_nothing() {
        for (version, data) in [
            (Some("0.0.3"), &[][..]),
            (Some("0.0.4"), &["rebuild backup_items (2 rows)"][..]),
            (None, &["004_x.sql"][..]),
        ] {
            let fake = Fake::new(version, data);
            let code = apply_migrations(&fake, &args(false), &NoResourceTags)
                .await
                .expect("preview");
            assert_eq!(code, EXIT_PENDING, "{version:?} {data:?}");
            assert_eq!(fake.runs(), (0, 0), "the preview applied something");
            assert_eq!(
                fake.read_catalog_version().await.unwrap().as_deref(),
                version
            );
        }
    }

    #[tokio::test]
    async fn up_to_date_exits_0_with_or_without_yes() {
        for yes in [false, true] {
            let fake = Fake::new(Some("0.0.4"), &[]);
            assert_eq!(
                apply_migrations(&fake, &args(yes), &NoResourceTags)
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(fake.runs(), (0, 0));
        }
    }

    #[tokio::test]
    async fn yes_applies_and_exits_0_and_a_second_preview_is_up_to_date() {
        let fake = Fake::new(Some("0.0.3"), &["rebuild backup_items (2 rows)"]);
        assert_eq!(
            apply_migrations(&fake, &args(true), &NoResourceTags)
                .await
                .unwrap(),
            0
        );
        assert_eq!(fake.runs(), (1, 1));
        assert_eq!(
            apply_migrations(&fake, &args(false), &NoResourceTags)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn a_newer_catalog_is_an_error_not_a_preview() {
        let fake = Fake::new(Some("0.0.5"), &[]);
        let err = apply_migrations(&fake, &args(false), &NoResourceTags)
            .await
            .expect_err("newer");
        assert!(err.to_string().contains("does not downgrade"), "{err}");
        assert_eq!(fake.runs(), (0, 0));
    }

    #[test]
    fn errors_print_their_message_without_the_variant_name() {
        let shown = format!(
            "{:?}",
            op_err(OpError::Internal("Cannot connect to PostgreSQL".to_owned()))
        );
        assert_eq!(shown, "Cannot connect to PostgreSQL");
        let shown = format!(
            "{:?}",
            storage_err(extenddb_storage::error::StorageError::Internal(
                "Cannot connect to PostgreSQL".to_owned()
            ))
        );
        assert_eq!(shown, "Cannot connect to PostgreSQL");
    }
}

#[cfg(test)]
mod backend_args_tests {
    use super::backend_args;

    fn argv(extra: &[&str]) -> Vec<String> {
        ["extenddb", "migrate"]
            .iter()
            .chain(extra)
            .map(|s| (*s).to_owned())
            .collect()
    }

    #[test]
    fn passwords_come_from_the_environment_when_the_flag_is_absent() {
        let args =
            backend_args(argv(&[]), Some("pgsecret".into()), Some("cass".into())).expect("args");
        assert_eq!(
            args,
            argv(&["--pg-pass", "pgsecret", "--cassandra-pass", "cass"])
        );
    }

    #[test]
    fn a_flag_on_the_command_line_wins_over_the_environment() {
        let given = argv(&["--cassandra-pass=fromflag", "--pg-pass", "pgflag"]);
        let args = backend_args(given.clone(), Some("pgenv".into()), Some("cassenv".into()))
            .expect("args");
        assert_eq!(args, given);
    }

    #[test]
    fn no_environment_adds_nothing() {
        assert_eq!(
            backend_args(argv(&[]), None, None).expect("args"),
            argv(&[])
        );
    }
}

#[cfg(test)]
mod newer_catalog_tests {
    use super::refuse_newer_catalog;

    #[test]
    fn older_or_equal_or_absent_is_allowed() {
        assert!(refuse_newer_catalog(None, "0.0.4").is_ok());
        assert!(refuse_newer_catalog(Some("0.0.3"), "0.0.4").is_ok());
        assert!(refuse_newer_catalog(Some("0.0.4"), "0.0.4").is_ok());
    }

    #[test]
    fn an_unparseable_stored_version_is_refused() {
        for stored in ["garbage", "", "0.0", "0.0.4-rc1"] {
            let err = refuse_newer_catalog(Some(stored), "0.0.4").expect_err(stored);
            assert!(err.to_string().contains("refusing to overwrite"), "{err}");
        }
    }

    #[test]
    fn newer_is_refused() {
        let err = refuse_newer_catalog(Some("0.0.5"), "0.0.4").expect_err("newer catalog");
        assert!(err.to_string().contains("does not downgrade"), "{err}");
        assert!(refuse_newer_catalog(Some("0.1.0"), "0.0.4").is_err());
    }
}
