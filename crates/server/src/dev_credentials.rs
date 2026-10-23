// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Developer-mode credential vending.
//!
//! Dev mode verifies `SigV4` exactly like production, so the server must know
//! every credential an SDK may sign with. The contract is "always the example
//! pair, plus at most one explicit extra":
//!
//! * The built-in credential is AWS's documented example pair. It is seeded on
//!   every dev-mode boot, unconditionally, so tools that hardcode it (`NoSQL
//!   Workbench`'s `ExtendDB` connection type, the `@extenddb/dev` launchers, the
//!   docs and CI) keep working regardless of operator configuration. An SDK
//!   must know a credential up front (it cannot read a printed banner), which
//!   is why the default is well-known rather than generated. The pair is
//!   public AWS documentation, recognised by secret scanners as non-live, so
//!   it is safe in banners, fixtures, and syslog.
//! * `EXTENDDB_DEV_ACCESS_KEY_ID` / `EXTENDDB_DEV_SECRET_ACCESS_KEY` (both or
//!   neither) seed one additional credential. The dedicated names follow the
//!   `EXTENDDB_DEV_ALLOW_ANY_BIND` precedent: dev-only levers get dev-only
//!   variables, read only when `dev-mode` is compiled in.
//! * The standard `AWS_*` variables are never read for seeding. Adopting them
//!   meant a shell holding real long-lived credentials silently copied them
//!   into the dev catalog, a shell holding SSO session credentials (`ASIA*`)
//!   failed the boot, and a dev credential could authenticate against real
//!   AWS when an endpoint was misconfigured. When `AWS_ACCESS_KEY_ID` is set
//!   and matches no seeded key, [`ignored_aws_env_key`] lets callers warn
//!   that requests signed with it will be rejected.
//!
//! Seeding is env-authoritative per boot: if a key id already exists with a
//! different secret (a file-backed store whose operator rotated the variable),
//! the stored secret is replaced. Tolerating `AlreadyExists` silently, as
//! earlier versions did, pinned the first-ever secret forever and turned a
//! rotation into per-request `InvalidSignatureException` with no boot-time
//! indication.

use extenddb_storage::CatalogStore;
use extenddb_storage::management_store::OpError;

/// Access key id of AWS's documented example credential. Always seeded in dev
/// mode; public contract for every tool that hardcodes the pair.
pub const EXAMPLE_ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
/// Secret half of AWS's documented example credential. Public documentation,
/// not a secret: safe to print and to commit in fixtures.
pub const EXAMPLE_SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

const ENV_ACCESS_KEY_ID: &str = "EXTENDDB_DEV_ACCESS_KEY_ID";
const ENV_SECRET_ACCESS_KEY: &str = "EXTENDDB_DEV_SECRET_ACCESS_KEY";

/// A credential the dev server seeds and verifies `SigV4` against.
pub struct DevCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// True for the built-in example pair. Callers use this to decide whether
    /// the secret may be echoed: the example secret is public documentation,
    /// an operator-supplied secret must never reach a banner or syslog.
    pub is_builtin: bool,
}

// Manual impl so an operator-supplied secret cannot leak through debug
// formatting; the built-in secret is public documentation and prints as-is.
impl std::fmt::Debug for DevCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevCredential")
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                if self.is_builtin {
                    &self.secret_access_key
                } else {
                    &"<redacted>"
                },
            )
            .field("is_builtin", &self.is_builtin)
            .finish()
    }
}

/// Resolve the dev-mode credential set from the process environment.
///
/// Element 0 is always the built-in example pair; element 1, when present, is
/// the operator's `EXTENDDB_DEV_*` pair.
///
/// # Errors
///
/// Returns an error when only one of the two variables is set, when the key id
/// is not AKIA-shaped, or when it collides with the built-in key id.
pub fn resolve() -> anyhow::Result<Vec<DevCredential>> {
    resolve_from(
        non_empty_env(ENV_ACCESS_KEY_ID),
        non_empty_env(ENV_SECRET_ACCESS_KEY),
    )
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn resolve_from(
    custom_key_id: Option<String>,
    custom_secret: Option<String>,
) -> anyhow::Result<Vec<DevCredential>> {
    let mut credentials = vec![DevCredential {
        access_key_id: EXAMPLE_ACCESS_KEY_ID.to_owned(),
        secret_access_key: EXAMPLE_SECRET_ACCESS_KEY.to_owned(),
        is_builtin: true,
    }];

    let (key_id, secret) = match (custom_key_id, custom_secret) {
        (None, None) => return Ok(credentials),
        (Some(k), Some(s)) => (k, s),
        (Some(_), None) => anyhow::bail!(
            "{ENV_ACCESS_KEY_ID} is set but {ENV_SECRET_ACCESS_KEY} is not; \
             set both to seed an additional dev credential, or neither."
        ),
        (None, Some(_)) => anyhow::bail!(
            "{ENV_SECRET_ACCESS_KEY} is set but {ENV_ACCESS_KEY_ID} is not; \
             set both to seed an additional dev credential, or neither."
        ),
    };

    // The access-key prefix is a credential-type discriminator across the auth
    // layer: `AKIA*` keys are long-lived IAM user keys, `ASIA*` are temporary
    // role session credentials (session token, expiry, role-scoped
    // invalidation), and anything else is unknown to credential lookup. The
    // dev credential is a long-lived key on a user, so it must be AKIA-shaped.
    // This also matches real AWS, which rejects fabricated key ids with
    // UnrecognizedClientException rather than authenticating them.
    if !key_id.starts_with("AKIA") {
        anyhow::bail!(
            "{ENV_ACCESS_KEY_ID} must be AKIA-shaped (got '{key_id}'). The built-in \
             example pair {EXAMPLE_ACCESS_KEY_ID} is always seeded; clients migrating \
             from DynamoDB Local dummy credentials can sign with it instead."
        );
    }
    if key_id == EXAMPLE_ACCESS_KEY_ID {
        anyhow::bail!(
            "{ENV_ACCESS_KEY_ID} matches the built-in example key id, which is always \
             seeded with its documented secret; choose a different key id."
        );
    }

    credentials.push(DevCredential {
        access_key_id: key_id,
        secret_access_key: secret,
        is_builtin: false,
    });
    Ok(credentials)
}

/// The value of `AWS_ACCESS_KEY_ID` when it is set, non-empty, and matches no
/// seeded credential. Dev mode used to adopt the `AWS_*` pair, so a caller
/// should warn: requests signed with this key will get
/// `UnrecognizedClientException`.
#[must_use]
pub fn ignored_aws_env_key(credentials: &[DevCredential]) -> Option<String> {
    ignored_aws_env_key_from(non_empty_env("AWS_ACCESS_KEY_ID"), credentials)
}

fn ignored_aws_env_key_from(
    aws_key_id: Option<String>,
    credentials: &[DevCredential],
) -> Option<String> {
    aws_key_id.filter(|id| credentials.iter().all(|c| c.access_key_id != *id))
}

/// Seed the resolved credentials onto the `dev` user of the default account.
///
/// Idempotent, and env-authoritative for the secret: an existing key whose
/// stored secret differs from the resolved one is deleted and re-imported.
///
/// # Errors
///
/// Returns an error when the catalog has no default account (not
/// bootstrapped), or when any create/lookup/import/delete step fails.
pub async fn seed(
    catalog_store: &dyn CatalogStore,
    credential_store: &dyn extenddb_auth::CredentialStore,
    credentials: &[DevCredential],
) -> anyhow::Result<()> {
    // Attach the dev user to the deployment's recorded default account (set at
    // bootstrap) rather than inferring it from account-list ordering.
    let account_id = catalog_store
        .default_account_id()
        .await
        .map_err(|e| anyhow::anyhow!("dev mode: failed to read default account: {e:?}"))?
        .ok_or_else(|| {
            anyhow::anyhow!("dev mode: no default account recorded (catalog not bootstrapped)")
        })?;

    match catalog_store.create_user(&account_id, "dev", None).await {
        Ok(()) | Err(OpError::AlreadyExists(_)) => {}
        Err(e) => anyhow::bail!("dev mode: failed to create dev user: {e:?}"),
    }

    for credential in credentials {
        let key_id = credential.access_key_id.as_str();
        match catalog_store
            .import_access_key(&account_id, "dev", key_id, &credential.secret_access_key)
            .await
        {
            Ok(()) => {}
            Err(OpError::AlreadyExists(_)) => {
                let stored = credential_store
                    .lookup_credential(key_id)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "dev mode: failed to look up existing credential {key_id}: {e:?}"
                        )
                    })?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "dev mode: credential {key_id} reported as existing but not found"
                        )
                    })?;
                if stored.secret_key != credential.secret_access_key {
                    // Rotated secret: the environment is authoritative.
                    // Keeping the stored secret, as earlier versions did,
                    // meant the old secret kept verifying and the new one
                    // failed with InvalidSignatureException on every request.
                    catalog_store
                        .delete_access_key(&account_id, "dev", key_id)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "dev mode: failed to delete stale credential {key_id}: {e:?}"
                            )
                        })?;
                    catalog_store
                        .import_access_key(
                            &account_id,
                            "dev",
                            key_id,
                            &credential.secret_access_key,
                        )
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "dev mode: failed to re-import rotated credential {key_id}: {e:?}"
                            )
                        })?;
                    tracing::info!(
                        "dev credential {key_id}: stored secret differed from the environment; replaced"
                    );
                }
            }
            Err(e) => anyhow::bail!("dev mode: failed to import dev credential {key_id}: {e:?}"),
        }
    }
    Ok(())
}

/// One-line description of the seeded credentials for banners and logs.
///
/// The built-in pair is printed in full (public documentation); a custom
/// credential contributes its key id only, so an operator-supplied secret
/// never reaches stdout or syslog.
#[must_use]
pub fn describe(credentials: &[DevCredential]) -> String {
    credentials
        .iter()
        .map(|c| {
            if c.is_builtin {
                format!(
                    "{} / {} (AWS documented example pair, always seeded)",
                    c.access_key_id, c.secret_access_key
                )
            } else {
                format!(
                    "{} (from {ENV_ACCESS_KEY_ID}, secret not shown)",
                    c.access_key_id
                )
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_env_yields_builtin_only() {
        let creds = resolve_from(None, None).unwrap();
        assert_eq!(creds.len(), 1);
        assert!(creds[0].is_builtin);
        assert_eq!(creds[0].access_key_id, EXAMPLE_ACCESS_KEY_ID);
        assert_eq!(creds[0].secret_access_key, EXAMPLE_SECRET_ACCESS_KEY);
    }

    #[test]
    fn custom_pair_is_added_after_builtin() {
        let creds = resolve_from(
            Some("AKIADEVTEAMKEY000001".into()),
            Some("team-secret".into()),
        )
        .unwrap();
        assert_eq!(creds.len(), 2);
        assert!(creds[0].is_builtin);
        assert!(!creds[1].is_builtin);
        assert_eq!(creds[1].access_key_id, "AKIADEVTEAMKEY000001");
        assert_eq!(creds[1].secret_access_key, "team-secret");
    }

    #[test]
    fn half_set_pair_is_rejected_naming_the_missing_var() {
        let err = resolve_from(Some("AKIADEVTEAMKEY000001".into()), None).unwrap_err();
        assert!(
            err.to_string().contains("EXTENDDB_DEV_SECRET_ACCESS_KEY"),
            "{err}"
        );
        let err = resolve_from(None, Some("team-secret".into())).unwrap_err();
        assert!(
            err.to_string().contains("EXTENDDB_DEV_ACCESS_KEY_ID"),
            "{err}"
        );
    }

    #[test]
    fn non_akia_key_is_rejected_pointing_at_the_builtin_pair() {
        let err = resolve_from(Some("test".into()), Some("test".into())).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("AKIA-shaped"), "{msg}");
        assert!(msg.contains(EXAMPLE_ACCESS_KEY_ID), "{msg}");
    }

    #[test]
    fn builtin_key_id_collision_is_rejected() {
        let err = resolve_from(
            Some(EXAMPLE_ACCESS_KEY_ID.into()),
            Some("different-secret".into()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("always seeded"), "{err}");
    }

    #[test]
    fn aws_env_key_is_flagged_only_when_foreign() {
        let creds = resolve_from(
            Some("AKIADEVTEAMKEY000001".into()),
            Some("team-secret".into()),
        )
        .unwrap();
        // Unset or empty: nothing to warn about.
        assert_eq!(ignored_aws_env_key_from(None, &creds), None);
        // Matches a seeded credential (either of them): no warning.
        assert_eq!(
            ignored_aws_env_key_from(Some(EXAMPLE_ACCESS_KEY_ID.into()), &creds),
            None
        );
        assert_eq!(
            ignored_aws_env_key_from(Some("AKIADEVTEAMKEY000001".into()), &creds),
            None
        );
        // Foreign key, including ASIA session keys: flagged for the advisory.
        assert_eq!(
            ignored_aws_env_key_from(Some("ASIAFOREIGNSESSION01".into()), &creds),
            Some("ASIAFOREIGNSESSION01".into())
        );
    }

    #[test]
    fn describe_prints_builtin_secret_but_never_custom_secret() {
        let creds = resolve_from(
            Some("AKIADEVTEAMKEY000001".into()),
            Some("team-secret".into()),
        )
        .unwrap();
        let text = describe(&creds);
        assert!(text.contains(EXAMPLE_SECRET_ACCESS_KEY), "{text}");
        assert!(text.contains("AKIADEVTEAMKEY000001"), "{text}");
        assert!(!text.contains("team-secret"), "{text}");
    }
}
