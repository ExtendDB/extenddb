use super::check_idempotency_token_in_tx;
use extenddb_storage::error::StorageError;
use sqlx::sqlite::SqlitePool;

async fn pool_with_schema() -> SqlitePool {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::query(
        "CREATE TABLE idempotency_tokens (\
             account_id  TEXT NOT NULL,\
             token       TEXT NOT NULL,\
             fingerprint TEXT NOT NULL,\
             created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),\
             PRIMARY KEY (account_id, token)\
         )",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool
}

async fn check(
    pool: &SqlitePool,
    account: &str,
    token: &str,
    fp: &str,
) -> Result<(), StorageError> {
    let mut tx = pool.begin().await.unwrap();
    let r = check_idempotency_token_in_tx(&mut tx, account, token, fp).await;
    tx.commit().await.unwrap();
    r
}

#[tokio::test]
async fn idempotency_is_scoped_per_account() {
    let pool = pool_with_schema().await;

    // First use of (acctA, tok) records it.
    check(&pool, "acctA", "tok", "fpX").await.unwrap();

    // Same account + token + fingerprint = idempotent replay.
    assert!(matches!(
        check(&pool, "acctA", "tok", "fpX").await,
        Err(StorageError::IdempotentReplay)
    ));

    // Same account + token, different fingerprint = mismatch.
    assert!(matches!(
        check(&pool, "acctA", "tok", "fpY").await,
        Err(StorageError::IdempotentMismatch)
    ));

    // Regression: a DIFFERENT account reusing the same token value must be
    // independent — not a replay and not a mismatch.
    check(&pool, "acctB", "tok", "fpX").await.unwrap();
}

/// Set the stored `created_at` of `(account, token)` to `minutes` ago.
async fn age(pool: &SqlitePool, account: &str, token: &str, minutes: i64) {
    let then = crate::sqlite_util::format_timestamp(
        time::OffsetDateTime::now_utc() - time::Duration::minutes(minutes),
    );
    sqlx::query("UPDATE idempotency_tokens SET created_at = ? WHERE account_id = ? AND token = ?")
        .bind(then)
        .bind(account)
        .bind(token)
        .execute(pool)
        .await
        .unwrap();
}

async fn stored(pool: &SqlitePool, account: &str, token: &str) -> (String, String) {
    sqlx::query_as(
        "SELECT fingerprint, created_at FROM idempotency_tokens \
         WHERE account_id = ? AND token = ?",
    )
    .bind(account)
    .bind(token)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// The service keeps a cancelled token used 9 minutes after the
/// cancellation and treats it as new 11 minutes after it (measured
/// 2026-11-05, Q1b and Q1a).
#[tokio::test]
async fn a_cancelled_token_is_used_for_ten_minutes() {
    let pool = pool_with_schema().await;
    super::record_cancelled_token(&pool, "acct", "tok", "fpX")
        .await
        .unwrap();
    age(&pool, "acct", "tok", 9).await;
    assert!(matches!(
        check(&pool, "acct", "tok", "fpY").await,
        Err(StorageError::IdempotentMismatch)
    ));
    age(&pool, "acct", "tok", 11).await;
    check(&pool, "acct", "tok", "fpY").await.unwrap();
    assert_eq!(stored(&pool, "acct", "tok").await.0, "fpY");
}

/// A cancellation of the same request sent again starts the window again
/// (measured 2026-11-05, Q1c: used 5 minutes after the second
/// cancellation, 11 minutes after the first).
#[tokio::test]
async fn a_cancelled_rerun_starts_the_window_again() {
    let pool = pool_with_schema().await;
    super::record_cancelled_token(&pool, "acct", "tok", "fpX")
        .await
        .unwrap();
    age(&pool, "acct", "tok", 6).await;
    let (_, aged) = stored(&pool, "acct", "tok").await;
    super::record_cancelled_token(&pool, "acct", "tok", "fpX")
        .await
        .unwrap();
    let (fingerprint, renewed) = stored(&pool, "acct", "tok").await;
    assert_eq!(fingerprint, "cancelled:fpX");
    assert!(renewed > aged, "{renewed} is not later than {aged}");
    age(&pool, "acct", "tok", 5).await;
    assert!(matches!(
        check(&pool, "acct", "tok", "fpY").await,
        Err(StorageError::IdempotentMismatch)
    ));
}

/// A token another request stored less than 10 minutes ago stays; one
/// stored longer ago is replaced.
#[tokio::test]
async fn a_cancellation_keeps_another_requests_live_token() {
    let pool = pool_with_schema().await;
    check(&pool, "acct", "tok", "fpY").await.unwrap();
    super::record_cancelled_token(&pool, "acct", "tok", "fpX")
        .await
        .unwrap();
    assert_eq!(stored(&pool, "acct", "tok").await.0, "fpY");
    age(&pool, "acct", "tok", 11).await;
    super::record_cancelled_token(&pool, "acct", "tok", "fpX")
        .await
        .unwrap();
    assert_eq!(stored(&pool, "acct", "tok").await.0, "cancelled:fpX");
}
