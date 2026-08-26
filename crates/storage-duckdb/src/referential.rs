// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Explicit referential integrity for the catalog.
//!
//! DuckDB accepts `FOREIGN KEY` declarations but supports neither `ON DELETE
//! CASCADE` nor updates to rows that a foreign key still references, so the
//! catalog schema declares no foreign keys at all (`schema.rs`). The
//! parent/child relationships the PostgreSQL and SQLite backends enforce with
//! constraints are enforced here instead: every parent delete removes its
//! children in the same transaction, and every child insert that would have
//! failed with a foreign-key violation first checks its parent exists.
//!
//! The existence checks are not atomic with the insert that follows in the
//! autocommit paths. Both backends this one mirrors report a missing parent
//! as `NotFound`, and that is what a lost race produces here too, just from a
//! later read rather than from the constraint.

use extenddb_storage::management_store::{OpError, OpResult};

use crate::db;

fn internal(context: &str, e: db::Error) -> OpError {
    tracing::error!("{context}: {e}");
    OpError::Internal("Database error".to_owned())
}

/// Delete everything owned by a DynamoDB table: index and vector-index
/// metadata, stream shards, and stream records. The per-table data tables are
/// dropped separately by the caller (`drop_data_table` / `drop_index_data_table`).
pub(crate) async fn delete_table_children(
    tx: &mut db::Transaction,
    table_id: &str,
) -> Result<(), db::Error> {
    for sql in [
        "DELETE FROM stream_records WHERE table_id = ?",
        "DELETE FROM stream_shards WHERE table_id = ?",
        "DELETE FROM indexes WHERE table_id = ?",
        "DELETE FROM vector_indexes WHERE table_id = ?",
    ] {
        db::query(sql).bind(table_id).execute(&mut **tx).await?;
    }
    Ok(())
}

/// Delete every IAM object in an account. Tables are the caller's concern (an
/// account with tables is refused before this runs).
pub(crate) async fn delete_account_children(
    tx: &mut db::Transaction,
    account_id: &str,
) -> Result<(), db::Error> {
    for sql in [
        "DELETE FROM iam_permissions_boundaries WHERE account_id = ?",
        "DELETE FROM iam_policies WHERE account_id = ?",
        "DELETE FROM iam_sessions WHERE account_id = ?",
        "DELETE FROM iam_role_tags WHERE account_id = ?",
        "DELETE FROM iam_roles WHERE account_id = ?",
        "DELETE FROM iam_group_members WHERE account_id = ?",
        "DELETE FROM iam_groups WHERE account_id = ?",
        "DELETE FROM access_keys WHERE account_id = ?",
        "DELETE FROM iam_user_tags WHERE account_id = ?",
        "DELETE FROM iam_users WHERE account_id = ?",
    ] {
        db::query(sql).bind(account_id).execute(&mut **tx).await?;
    }
    Ok(())
}

/// Delete a user's tags, access keys, and group memberships.
pub(crate) async fn delete_user_children(
    tx: &mut db::Transaction,
    account_id: &str,
    user_name: &str,
) -> Result<(), db::Error> {
    for sql in [
        "DELETE FROM iam_user_tags WHERE account_id = ? AND user_name = ?",
        "DELETE FROM access_keys WHERE account_id = ? AND user_name = ?",
        "DELETE FROM iam_group_members WHERE account_id = ? AND user_name = ?",
    ] {
        db::query(sql)
            .bind(account_id)
            .bind(user_name)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// Delete a group's memberships.
pub(crate) async fn delete_group_children(
    tx: &mut db::Transaction,
    account_id: &str,
    group_name: &str,
) -> Result<(), db::Error> {
    db::query("DELETE FROM iam_group_members WHERE account_id = ? AND group_name = ?")
        .bind(account_id)
        .bind(group_name)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Delete a role's tags and sessions.
pub(crate) async fn delete_role_children(
    tx: &mut db::Transaction,
    account_id: &str,
    role_name: &str,
) -> Result<(), db::Error> {
    for sql in [
        "DELETE FROM iam_role_tags WHERE account_id = ? AND role_name = ?",
        "DELETE FROM iam_sessions WHERE account_id = ? AND role_name = ?",
    ] {
        db::query(sql)
            .bind(account_id)
            .bind(role_name)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// Delete a parent row and its children in one transaction on `pool`.
///
/// `children` runs first, then `parent_sql` bound with `binds`. Returns
/// `NotFound(not_found)` when the parent row did not exist.
pub(crate) async fn delete_with_children<F>(
    pool: &db::Pool,
    context: &str,
    children: F,
    parent_sql: &str,
    binds: &[&str],
    not_found: &str,
) -> OpResult<()>
where
    F: for<'a> AsyncFnOnce(&'a mut db::Transaction) -> Result<(), db::Error>,
{
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| internal(&format!("{context} begin"), e))?;
    children(&mut tx)
        .await
        .map_err(|e| internal(&format!("{context} children"), e))?;
    let mut q = db::query(parent_sql);
    for b in binds {
        q = q.bind(*b);
    }
    let result = q
        .execute(&mut *tx)
        .await
        .map_err(|e| internal(context, e))?;
    if result.rows_affected() == 0 {
        return Err(OpError::NotFound(not_found.to_owned()));
    }
    tx.commit()
        .await
        .map_err(|e| internal(&format!("{context} commit"), e))?;
    Ok(())
}

async fn exists<E: db::Executor>(exec: E, sql: &str, binds: &[&str]) -> Result<bool, db::Error> {
    let mut q = db::query_scalar::<bool>(sql);
    for b in binds {
        q = q.bind(*b);
    }
    q.fetch_one(exec).await
}

/// `NotFound("Account not found")` unless the account exists.
pub(crate) async fn ensure_account_exists<E: db::Executor>(
    exec: E,
    account_id: &str,
) -> OpResult<()> {
    match exists(
        exec,
        "SELECT EXISTS(SELECT 1 FROM accounts WHERE account_id = ?)",
        &[account_id],
    )
    .await
    {
        Ok(true) => Ok(()),
        Ok(false) => Err(OpError::NotFound("Account not found".to_owned())),
        Err(e) => Err(internal("ensure_account_exists", e)),
    }
}

/// `NotFound("IAM user not found")` unless the user exists.
pub(crate) async fn ensure_user_exists<E: db::Executor>(
    exec: E,
    account_id: &str,
    user_name: &str,
) -> OpResult<()> {
    match exists(
        exec,
        "SELECT EXISTS(SELECT 1 FROM iam_users WHERE account_id = ? AND user_name = ?)",
        &[account_id, user_name],
    )
    .await
    {
        Ok(true) => Ok(()),
        Ok(false) => Err(OpError::NotFound("IAM user not found".to_owned())),
        Err(e) => Err(internal("ensure_user_exists", e)),
    }
}

/// `NotFound("IAM group not found")` unless the group exists.
pub(crate) async fn ensure_group_exists<E: db::Executor>(
    exec: E,
    account_id: &str,
    group_name: &str,
) -> OpResult<()> {
    match exists(
        exec,
        "SELECT EXISTS(SELECT 1 FROM iam_groups WHERE account_id = ? AND group_name = ?)",
        &[account_id, group_name],
    )
    .await
    {
        Ok(true) => Ok(()),
        Ok(false) => Err(OpError::NotFound("IAM group not found".to_owned())),
        Err(e) => Err(internal("ensure_group_exists", e)),
    }
}

/// `NotFound("IAM role not found")` unless the role exists.
pub(crate) async fn ensure_role_exists<E: db::Executor>(
    exec: E,
    account_id: &str,
    role_name: &str,
) -> OpResult<()> {
    match exists(
        exec,
        "SELECT EXISTS(SELECT 1 FROM iam_roles WHERE account_id = ? AND role_name = ?)",
        &[account_id, role_name],
    )
    .await
    {
        Ok(true) => Ok(()),
        Ok(false) => Err(OpError::NotFound("IAM role not found".to_owned())),
        Err(e) => Err(internal("ensure_role_exists", e)),
    }
}
