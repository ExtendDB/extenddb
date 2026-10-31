// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What a runner and the status read from the database about other
//! sessions, about the data connection, and about who owns the objects
//! migrations alter, and the stops and notes built from it.
//!
//! - A migrator that dies while a statement runs leaves that statement
//!   running: the database notices a client is gone only when it next reads
//!   from or writes to the connection. Its lock connection is idle in its
//!   transaction, so that session ends at once and frees the migration lock,
//!   while a body session blocked on a lock or building an index goes on.
//!   [`wait_for_earlier_runners`] makes the next migrator wait until no
//!   earlier runner's session runs a statement in either database, looking
//!   at each database through the runner's own connection to it, so a data
//!   database on another server or reached as another role is seen too; and
//!   the runner's own transactions set `client_connection_check_interval`,
//!   so a statement whose client died ends within a second.
//! - [`served_data`] reaches the data database with the connection string
//!   the server connects with; [`served_data_options`] reads it for a caller
//!   outside the runner, such as the pgvector attempt.
//! - [`ownership_note`] names the objects the application role does not
//!   own and the statements that give it them; [`ownership_stop`] prints the
//!   same after a body failed with SQLSTATE 42501, and [`denied_with_notes`]
//!   after a status read did. [`lock_timeout_stop`] names the tables other
//!   sessions hold locks on after a body's last lock timeout.

use std::time::{Duration, Instant};

use extenddb_storage::migrations::{MigrationError, MigrationId, StatusNote, StoreKind, StoreRef};
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;

use super::PostgresTarget;
use super::connect::{RUNNER_APPLICATION_PREFIX, one_connection, relation_exists, sql_error};

/// How often the wait for an earlier runner looks again.
const POLL: Duration = Duration::from_millis(200);

/// The setting the runner's transactions send so that a statement whose
/// client is gone ends instead of running on.
pub(crate) const CONNECTION_CHECK: &str = "SET LOCAL client_connection_check_interval = '1s'";

/// Whether the server accepts [`CONNECTION_CHECK`]: PostgreSQL 14 and later,
/// on a platform that can watch a socket for the peer closing it.
pub(crate) async fn connection_check_supported(pool: &PgPool) -> bool {
    let probe = sqlx::raw_sql(&format!("BEGIN; {CONNECTION_CHECK}; ROLLBACK"))
        .execute(pool)
        .await;
    if probe.is_err() {
        let _ = sqlx::raw_sql("ROLLBACK").execute(pool).await;
    }
    probe.is_ok()
}

/// Wait until no session of another runner runs a statement in any of
/// `databases`, each a database and the runner's connection to it, up to
/// `timeout`; `own` is this runner's `application_name`. Each database is
/// looked at through its own connection, which reaches its server as the
/// role an earlier runner's sessions there used. An idle session is not
/// waited for: a dead client's idle session has already ended, and a
/// pooler's idle backend keeps the last client's name.
///
/// # Errors
///
/// [`MigrationError::Blocked`] (exit 6) naming the sessions when they
/// outlast `timeout`.
pub(crate) async fn wait_for_earlier_runners(
    databases: &[(&PgPool, &str)],
    own: &str,
    timeout: Duration,
) -> Result<(), MigrationError> {
    type Row = (i32, String, Option<String>, Option<String>, Option<String>);
    let started = Instant::now();
    let mut announced = false;
    loop {
        let mut rows: Vec<Row> = Vec::new();
        for (pool, database) in databases {
            let found: Vec<Row> = sqlx::query_as(
                "SELECT pid, datname::text, state, wait_event_type, left(query, 60) \
                 FROM pg_stat_activity \
                 WHERE datname = $1 AND starts_with(application_name, $2) \
                   AND application_name <> $3 AND coalesce(state, '') <> 'idle' \
                 ORDER BY pid",
            )
            .bind(database)
            .bind(RUNNER_APPLICATION_PREFIX)
            .bind(own)
            .fetch_all(*pool)
            .await
            .map_err(|e| sql_error("look for an earlier migrator's sessions", &e))?;
            rows.extend(found);
        }
        if rows.is_empty() {
            return Ok(());
        }
        let sessions = rows
            .iter()
            .map(|(pid, database, state, wait, query)| {
                let waiting = wait
                    .as_deref()
                    .map_or_else(String::new, |w| format!(", waiting on a {w}"));
                format!(
                    "pid {pid} in {database} ({}{waiting}: {})",
                    state.as_deref().unwrap_or("state not visible to this role"),
                    query.as_deref().unwrap_or("").trim()
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        if started.elapsed() >= timeout {
            return Err(MigrationError::Blocked(format!(
                "statements of an earlier migrator that stopped are still running, still after \
                 {} s: {sessions}. They do not end by themselves; end them with SELECT \
                 pg_cancel_backend(<pid>), as their role or a superuser, then run again",
                timeout.as_secs()
            )));
        }
        if !announced {
            println!(
                "--- Waiting up to {} s for the statements of an earlier migrator that stopped: \
                 {sessions}",
                timeout.as_secs()
            );
            announced = true;
        }
        tokio::time::sleep(POLL).await;
    }
}

/// The catalog's `data_database_connection_string` row; `None` when the
/// catalog has no settings table or no such row.
async fn connection_row(catalog: &PgPool) -> Result<Option<String>, MigrationError> {
    if !relation_exists(catalog, "settings").await? {
        return Ok(None);
    }
    sqlx::query_scalar("SELECT value FROM settings WHERE key = 'data_database_connection_string'")
        .fetch_optional(catalog)
        .await
        .map_err(|e| sql_error("read data_database_connection_string", &e))
}

/// A status session (read-only, named `extenddb status`) on `options`.
pub(crate) async fn status_session(
    options: &PgConnectOptions,
    timeout: Duration,
) -> Result<PgPool, MigrationError> {
    one_connection(options.clone(), true, timeout)
        .await
        .map_err(|e| {
            sql_error(
                &format!(
                    "connect to the database {}",
                    options.get_database().unwrap_or("postgres")
                ),
                &e,
            )
        })
}

/// Roll a status session's transaction back and close it.
pub(crate) async fn close_status(pool: PgPool) {
    let _ = sqlx::raw_sql("ROLLBACK").execute(&pool).await;
    pool.close().await;
}

/// [`connection_row`] on a status session of its own.
async fn read_connection_row(target: &PostgresTarget) -> Result<Option<String>, MigrationError> {
    let pool = status_session(&target.catalog, target.connect_timeout).await?;
    let row = connection_row(&pool).await;
    close_status(pool).await;
    row
}

/// The data database's connect options: those of the catalog's
/// `data_database_connection_string` row, which the server connects with,
/// when the catalog has the row; otherwise the catalog's server and
/// credentials with the target's data database. A target whose data
/// database was given on the command line keeps that name on the row's
/// server.
///
/// # Errors
///
/// The row is not a PostgreSQL URL, or it names another data database than
/// the target's, which was not given on the command line.
pub(crate) async fn served_data(
    target: &PostgresTarget,
    catalog: &PgPool,
) -> Result<PgConnectOptions, MigrationError> {
    served_from(target, connection_row(catalog).await?)
}

/// [`served_data`] from the row's value.
fn served_from(
    target: &PostgresTarget,
    row: Option<String>,
) -> Result<PgConnectOptions, MigrationError> {
    let Some(row) = row.filter(|r| !r.is_empty()) else {
        return Ok(target.data());
    };
    let served: PgConnectOptions = row.parse().map_err(|e| {
        MigrationError::Refused(format!(
            "the catalog's data_database_connection_string row is not a PostgreSQL URL ({e}); \
             the server cannot connect with it either; correct the row"
        ))
    })?;
    let served_name = served.get_database().unwrap_or("postgres").to_owned();
    if served_name == target.data_database {
        Ok(served)
    } else if target.data_database_given {
        Ok(served.database(&target.data_database))
    } else {
        Err(MigrationError::Refused(format!(
            "the catalog's data_database_connection_string row names the data database \
             {served_name}, which the server serves, and its data_database_name row (or the \
             catalog database's name, when that row is missing) gives {}; make the \
             data_database_name row name {served_name}, or correct the connection string, then \
             run again",
            target.data_database
        )))
    }
}

/// [`served_data`] on a status session of its own, for a caller outside the
/// runner: the connect options of the data database the server serves.
///
/// # Errors
///
/// As [`served_data`], or the catalog cannot be reached.
pub async fn served_data_options(
    target: &PostgresTarget,
) -> Result<PgConnectOptions, MigrationError> {
    served_from(target, read_connection_row(target).await?)
}

/// For the server: refuse a catalog with no `data_database_connection_string`
/// row. The engine would then keep table data in the catalog database, while
/// the status read the ledger of the data database `target` names.
///
/// # Errors
///
/// The row is missing or empty, or the catalog cannot be read.
pub async fn require_data_connection_row(target: &PostgresTarget) -> Result<(), MigrationError> {
    match read_connection_row(target).await? {
        Some(value) if !value.is_empty() => Ok(()),
        _ => Err(MigrationError::Refused(format!(
            "the catalog database {} has no data_database_connection_string row, which \
             extenddb init writes; without it the server would keep table data in the catalog \
             database instead of the data database {}, whose migrations the status read. Record \
             the data database's connection string in that row, then start the server",
            target.catalog_database(),
            target.data_database
        ))),
    }
}

/// The stop after a body failed with SQLSTATE 42501: the database's
/// [`ownership_note`], which names the objects the application role does
/// not own and the statements that give them to it. Runs after the body's
/// transaction rolled back.
pub(crate) async fn ownership_stop(
    pool: &PgPool,
    store: &StoreRef,
    id: MigrationId,
    message: &str,
) -> MigrationError {
    let committed = format!(
        "{message}. Nothing of migration {id} was committed in the {} database {}",
        store.kind.as_str(),
        store.name
    );
    match ownership_note(pool, store.kind, &store.name).await {
        Ok(Some(note)) => MigrationError::Store(format!(
            "{committed}. Migrations run as the application role, which must own every table, \
             view and sequence in the public schema of the catalog and data databases and may \
             create tables there: {}",
            note.text
        )),
        _ => MigrationError::Store(committed),
    }
}

/// How many objects a note names before it counts the rest.
const NAMED: usize = 5;

/// What [`not_owned`] reads in one database.
struct NotOwned {
    /// The session's role, the application role.
    role: String,
    /// Whether the role may create objects in the public schema.
    may_create: bool,
    /// The role that owns the public schema (the database's owner when the
    /// schema belongs to `pg_database_owner`), and whether it is a superuser.
    schema_owner: (String, bool),
    /// (owner, object, whether the owner is a superuser), in order.
    objects: Vec<(String, String, bool)>,
}

/// The objects in the public schema of the database `pool` reaches whose
/// owner's privileges the session's role does not have, which is the check
/// PostgreSQL makes before it lets a role alter an object; whether the role
/// may create objects in the public schema; and who owns that schema.
/// Objects that follow another's owner (a column's sequence, an extension's
/// members) are left out, as the statements that give the others to the
/// role leave them out.
async fn not_owned(pool: &PgPool) -> Result<NotOwned, MigrationError> {
    let (role, may_create, schema_owner, schema_owner_super): (String, bool, String, bool) =
        sqlx::query_as(
            "WITH s AS (SELECT CASE WHEN pg_get_userbyid(n.nspowner) = 'pg_database_owner' \
             THEN (SELECT d.datdba FROM pg_database d WHERE d.datname = current_database()) \
             ELSE n.nspowner END AS owner, has_schema_privilege(n.oid, 'CREATE') AS may_create \
             FROM pg_namespace n WHERE n.nspname = 'public') \
             SELECT current_user::text, coalesce((SELECT may_create FROM s), true), \
             coalesce((SELECT r.rolname::text FROM s JOIN pg_roles r ON r.oid = s.owner), ''), \
             coalesce((SELECT r.rolsuper FROM s JOIN pg_roles r ON r.oid = s.owner), false)",
        )
        .fetch_one(pool)
        .await
        .map_err(|e| sql_error("read the role's privileges", &e))?;
    let objects: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT pg_get_userbyid(c.relowner)::text, c.relname::text, coalesce(r.rolsuper, false) \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         LEFT JOIN pg_roles r ON r.oid = c.relowner \
         WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'S', 'v', 'm') \
           AND NOT pg_has_role(c.relowner, 'USAGE') \
           AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass \
                           AND d.objid = c.oid AND d.deptype IN ('a', 'i', 'e')) \
         ORDER BY 1, 2",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| sql_error("read the owners of the objects", &e))?;
    Ok(NotOwned {
        role,
        may_create,
        schema_owner: (schema_owner, schema_owner_super),
        objects,
    })
}

/// `name` as a quoted SQL identifier.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The statements that give `role` the objects of each of `owners` (name,
/// whether a superuser) in the public schema and, unless `may_create`, the
/// right to create objects there, in the order they must run, each with the
/// role that runs it. PostgreSQL lets a role that is not a superuser give
/// an object it owns only to a role it may `SET ROLE` to, and only when that
/// role may create objects in the object's schema, so for such an owner a
/// role that may grant `role` (one with its ADMIN option, which the
/// administrator that created it holds; a role cannot grant itself) first
/// runs `GRANT "<role>" TO "<owner>"`, and the schema grant, run by the
/// schema's owner, comes before the `DO` blocks. A superuser needs neither
/// and gets the same result in this order. Each owner's `DO` block changes
/// the owner of that owner's objects with `ALTER ... OWNER TO` and of
/// nothing else: not its objects in other schemas or databases, which
/// `REASSIGN OWNED` would also move. The membership is needed only by the
/// `DO` block, so the last steps take it back with `REVOKE "<role>" FROM
/// "<owner>"`; an owner that was a member before keeps it by skipping that
/// step.
fn give_to(
    role: &str,
    owners: &[(&str, bool)],
    may_create: bool,
    schema_owner: (&str, bool),
) -> Vec<(String, String)> {
    let ident = quoted(role);
    let literal = role.replace('\'', "''");
    let mut steps = Vec::new();
    for (owner, _) in owners.iter().filter(|(_, superuser)| !superuser) {
        steps.push((
            format!("an administrator that may grant {role}"),
            format!("GRANT {ident} TO {};", quoted(owner)),
        ));
    }
    if !may_create {
        let who = match schema_owner {
            (owner, false) if !owner.is_empty() => {
                format!("{owner}, which owns the public schema, or a superuser")
            }
            _ => "a superuser".to_owned(),
        };
        steps.push((who, format!("GRANT CREATE ON SCHEMA public TO {ident};")));
    }
    for (owner, superuser) in owners {
        let who = if *superuser {
            "a superuser".to_owned()
        } else {
            format!("{owner}, or a superuser")
        };
        let owner = owner.replace('\'', "''");
        steps.push((
            who,
            format!(
                "DO $$DECLARE o record; BEGIN FOR o IN SELECT c.relname, CASE c.relkind WHEN \
                 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE \
                 'TABLE' END AS kind FROM pg_class c JOIN pg_namespace n ON n.oid = \
                 c.relnamespace WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'S', 'v', \
                 'm') AND pg_get_userbyid(c.relowner) = '{owner}' AND NOT \
                 pg_has_role('{literal}', c.relowner, 'USAGE') AND NOT EXISTS (SELECT 1 FROM \
                 pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND \
                 d.deptype IN ('a', 'i', 'e')) LOOP EXECUTE format('ALTER %s public.%I OWNER TO \
                 %I', o.kind, o.relname, '{literal}'); END LOOP; END $$;"
            ),
        ));
    }
    for (owner, _) in owners.iter().filter(|(_, superuser)| !superuser) {
        steps.push((
            format!(
                "an administrator that may grant {role}, unless {owner} was a member of {role} \
                 before these steps"
            ),
            format!("REVOKE {ident} FROM {};", quoted(owner)),
        ));
    }
    steps
}

/// The status note for the database `pool` reaches when the session's role
/// does not own some of the tables, views and sequences in its public
/// schema, or may not create objects in it: the objects are named with
/// their owners, with the statements that fix both, numbered in the order
/// they run and each with the role that runs it ([`give_to`]). Every
/// migration that alters such an object fails with SQLSTATE 42501, and so
/// does one that creates a table where the role may not, so the note
/// refuses `migrate --yes` before it takes the lock.
///
/// # Errors
///
/// The catalog of objects cannot be read.
pub(crate) async fn ownership_note(
    pool: &PgPool,
    kind: StoreKind,
    name: &str,
) -> Result<Option<StatusNote>, MigrationError> {
    let NotOwned {
        role,
        may_create,
        schema_owner,
        objects,
    } = not_owned(pool).await?;
    if objects.is_empty() && may_create {
        return Ok(None);
    }
    let mut owners: Vec<(&str, Vec<&str>)> = Vec::new();
    let mut superusers: Vec<(&str, bool)> = Vec::new();
    for (owner, object, superuser) in &objects {
        match owners.last_mut() {
            Some((last, list)) if last == owner => list.push(object),
            _ => {
                owners.push((owner, vec![object]));
                superusers.push((owner, *superuser));
            }
        }
    }
    let held = owners
        .iter()
        .map(|(owner, list)| {
            let mut shown = list
                .iter()
                .take(NAMED)
                .copied()
                .collect::<Vec<_>>()
                .join(", ");
            if list.len() > NAMED {
                shown.push_str(&format!(" and {} more", list.len() - NAMED));
            }
            format!("{} owned by {owner} ({shown})", list.len())
        })
        .collect::<Vec<_>>()
        .join("; ");
    let kind = kind.as_str();
    let mut found = Vec::new();
    if !objects.is_empty() {
        found.push(format!(
            "the application role {role} does not own these tables, views or sequences in the \
             public schema of the {kind} database {name}: {held}"
        ));
    }
    if !may_create {
        found.push(format!(
            "the application role {role} may not create tables in the public schema of the \
             {kind} database {name}"
        ));
    }
    let steps = give_to(
        &role,
        &superusers,
        may_create,
        (schema_owner.0.as_str(), schema_owner.1),
    )
    .iter()
    .enumerate()
    .map(|(i, (who, statement))| format!("({}) as {who}: {statement}", i + 1))
    .collect::<Vec<_>>()
    .join(" ");
    Ok(Some(StatusNote {
        text: format!(
            "{}. Migrations run as {role}, so migrate --yes refuses to apply any while this \
             holds. Connected to {name}, run these statements in this order, each as the role \
             named before it: {steps}",
            found.join("; "),
        ),
        refuses_runs: true,
    }))
}

/// A status read that failed with SQLSTATE 42501, such as `permission
/// denied for table settings` on a catalog another role restored without
/// grants, with the ownership note of each database `databases` reaches:
/// the note reads only the system catalogs, which every role may read. When
/// the data database's name could not be read (`data_unnamed`), a line says
/// so after the catalog database's note, since that name is never guessed.
/// The error is returned as it is when no note explains it.
pub(crate) async fn denied_with_notes(
    error: MigrationError,
    databases: &[(StoreKind, PgConnectOptions)],
    data_unnamed: bool,
    timeout: Duration,
) -> MigrationError {
    let mut notes = Vec::new();
    for (kind, options) in databases {
        let Ok(pool) = status_session(options, timeout).await else {
            continue;
        };
        let name = options.get_database().unwrap_or("postgres");
        if let Ok(Some(note)) = ownership_note(&pool, *kind, name).await {
            notes.push(note.text);
        }
        close_status(pool).await;
    }
    if notes.is_empty() {
        return error;
    }
    if data_unnamed {
        notes.push(
            "the data database's name is in the catalog's settings, which the application role \
             cannot read, so the data database has no note here: run the catalog database's \
             statements first, or pass migrate --data-db <name>, and the next run prints its \
             note"
                .to_owned(),
        );
    }
    MigrationError::Refused(format!(
        "{error}. The status reads these objects as the application role, which has to own \
         them:\n{}",
        notes
            .iter()
            .map(|n| format!("Note: {n}"))
            .collect::<Vec<_>>()
            .join("\n")
    ))
}

/// The stop after the last lock timeout: which tables other sessions
/// hold locks on in that database.
pub(crate) async fn lock_timeout_stop(
    pool: &PgPool,
    store: &StoreRef,
    id: MigrationId,
    last: &str,
    lock_timeout_secs: u64,
    retries: u32,
) -> MigrationError {
    let holders: Vec<(String, i32, String)> = sqlx::query_as(
        "SELECT DISTINCT c.relname::text, l.pid, coalesce(a.application_name, '') \
         FROM pg_locks l JOIN pg_class c ON c.oid = l.relation \
         LEFT JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.granted AND l.pid <> pg_backend_pid() \
           AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
           AND c.relkind IN ('r', 'p') \
         ORDER BY 1, 2",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let held = if holders.is_empty() {
        "no other session holds a table lock now".to_owned()
    } else {
        holders
            .iter()
            .map(|(table, pid, app)| format!("{table} (pid {pid}, {app:?})"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    MigrationError::Store(format!(
        "migration {id} could not lock a table in the {} database {} within \
         {lock_timeout_secs} s, {} times ({last}); tables other sessions hold locks on: \
         {held}. Nothing of migration {id} was committed there; stop those sessions and run \
         again",
        store.kind.as_str(),
        store.name,
        retries + 1
    ))
}
