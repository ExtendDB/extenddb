// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Developer-mode credential vending.
//!
//! Dev mode verifies `SigV4` exactly like production, so the server must know
//! every credential an SDK may sign with. The contract is "the example pair
//! unless explicitly disabled, plus at most one explicit extra":
//!
//! * The built-in credential is AWS's documented example pair. It is seeded on
//!   every dev-mode boot by default, so tools that hardcode it (`NoSQL
//!   Workbench`'s `ExtendDB` connection type, the `@extenddb/dev` launchers, the
//!   docs and CI) keep working with no operator configuration. An SDK
//!   must know a credential up front (it cannot read a printed banner), which
//!   is why the default is well-known rather than generated. The pair is
//!   public AWS documentation, recognised by secret scanners as non-live, so
//!   it is safe in banners, fixtures, and syslog.
//! * `EXTENDDB_DEV_ACCESS_KEY_ID` / `EXTENDDB_DEV_SECRET_ACCESS_KEY` (both or
//!   neither) seed one additional credential. The dedicated names follow the
//!   `EXTENDDB_DEV_ALLOW_ANY_BIND` precedent: dev-only levers get dev-only
//!   variables, read only when `dev-mode` is compiled in.
//! * `EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL=1` drops the example pair, for a
//!   dev server that is reachable by others (a shared host, a CI runner with a
//!   routable address): the public key would otherwise open every table to
//!   anyone who can reach the port. It requires the `EXTENDDB_DEV_*` pair, so
//!   the server always has a credential to verify, and it removes the example
//!   key from whichever user an earlier boot or an operator stored it on in a
//!   file-backed catalog, so the key stops verifying rather than lingering.
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

/// Access key id of AWS's documented example credential. Seeded in dev mode
/// unless `EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL` is set; public contract for
/// every tool that hardcodes the pair.
pub const EXAMPLE_ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
/// Secret half of AWS's documented example credential. Public documentation,
/// not a secret: safe to print and to commit in fixtures.
pub const EXAMPLE_SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

const ENV_ACCESS_KEY_ID: &str = "EXTENDDB_DEV_ACCESS_KEY_ID";
const ENV_SECRET_ACCESS_KEY: &str = "EXTENDDB_DEV_SECRET_ACCESS_KEY";
/// Environment variable that drops the built-in example pair.
pub const ENV_DISABLE_EXAMPLE: &str = "EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL";

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
/// Element 0 is the built-in example pair unless
/// `EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL` drops it; the operator's
/// `EXTENDDB_DEV_*` pair, when present, follows it.
///
/// # Errors
///
/// Returns an error when only one of the two variables is set, when the key id
/// is not AKIA-shaped, when it collides with the built-in key id, when the
/// disable switch holds an unrecognised value, or when the example pair is
/// disabled without an `EXTENDDB_DEV_*` pair to replace it.
pub fn resolve() -> anyhow::Result<Vec<DevCredential>> {
    resolve_with(|name| std::env::var_os(name))
}

/// [`resolve`], reading each variable through `env` (`None` when unset), so
/// tests can supply the environment.
fn resolve_with(
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> anyhow::Result<Vec<DevCredential>> {
    let non_empty = |name: &str| {
        env(name)
            .and_then(|v| v.into_string().ok())
            .filter(|s| !s.is_empty())
    };
    // Read as-is, not through `non_empty`: an empty value is refused rather
    // than read as unset (see `parse_switch`), and so is a value that is not
    // UTF-8, which `std::env::var(..).ok()` would also have read as unset,
    // leaving the example pair active with nothing printed.
    let switch = match env(ENV_DISABLE_EXAMPLE) {
        None => None,
        Some(value) => Some(value.into_string().map_err(|_| {
            anyhow::anyhow!(
                "{ENV_DISABLE_EXAMPLE} is not valid UTF-8; set it to 1, true, 0 or false."
            )
        })?),
    };
    resolve_from(
        non_empty(ENV_ACCESS_KEY_ID),
        non_empty(ENV_SECRET_ACCESS_KEY),
        parse_switch(ENV_DISABLE_EXAMPLE, switch)?,
    )
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

/// Parse an on/off switch. Unset is off. An unrecognised value is an error
/// rather than "off": this switch removes a public credential, so a typo such
/// as `yes` must not leave the server quietly accepting it. An empty value is
/// refused for the same reason: `-e NAME=$VAR` with `VAR` unset sets it empty,
/// and reading that as "off" would leave the public credential active when
/// the operator asked to remove it.
fn parse_switch(name: &str, value: Option<String>) -> anyhow::Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    match value.trim() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        "" => anyhow::bail!(
            "{name} is set but empty; set it to 1 or true to remove the example \
             credential, 0 or false to keep it, or unset it."
        ),
        other => anyhow::bail!("{name} must be 1, true, 0 or false (got '{other}')."),
    }
}

fn resolve_from(
    custom_key_id: Option<String>,
    custom_secret: Option<String>,
    disable_example: bool,
) -> anyhow::Result<Vec<DevCredential>> {
    let mut credentials = Vec::with_capacity(2);
    if !disable_example {
        credentials.push(DevCredential {
            access_key_id: EXAMPLE_ACCESS_KEY_ID.to_owned(),
            secret_access_key: EXAMPLE_SECRET_ACCESS_KEY.to_owned(),
            is_builtin: true,
        });
    }

    // With the switch set, "neither" is refused above, so the half-set
    // messages must not offer it.
    let set_both = if disable_example {
        format!("set both, as {ENV_DISABLE_EXAMPLE} requires.")
    } else {
        "set both to seed an additional dev credential, or neither.".to_owned()
    };
    let (key_id, secret) = match (custom_key_id, custom_secret) {
        (None, None) if disable_example => anyhow::bail!(
            "{ENV_DISABLE_EXAMPLE} is set, which removes the built-in example credential, \
             but no replacement is configured; set {ENV_ACCESS_KEY_ID} and \
             {ENV_SECRET_ACCESS_KEY} so requests have a credential to sign with."
        ),
        (None, None) => return Ok(credentials),
        (Some(k), Some(s)) => (k, s),
        (Some(_), None) => anyhow::bail!(
            "{ENV_ACCESS_KEY_ID} is set but {ENV_SECRET_ACCESS_KEY} is not; {set_both}"
        ),
        (None, Some(_)) => anyhow::bail!(
            "{ENV_SECRET_ACCESS_KEY} is set but {ENV_ACCESS_KEY_ID} is not; {set_both}"
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
        // With the switch set the example pair is not seeded, so it is no
        // alternative to point at.
        if disable_example {
            anyhow::bail!("{ENV_ACCESS_KEY_ID} must be AKIA-shaped (got '{key_id}').");
        }
        anyhow::bail!(
            "{ENV_ACCESS_KEY_ID} must be AKIA-shaped (got '{key_id}'). The built-in \
             example pair {EXAMPLE_ACCESS_KEY_ID} is seeded by default; clients migrating \
             from DynamoDB Local dummy credentials can sign with it instead."
        );
    }
    if key_id == EXAMPLE_ACCESS_KEY_ID {
        anyhow::bail!(
            "{ENV_ACCESS_KEY_ID} matches the built-in example key id, which is reserved \
             for its documented secret; choose a different key id."
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

/// The sentence the ignored-`AWS_ACCESS_KEY_ID` advisories add when that key
/// is the example key and the switch removed it, so the operator learns why a
/// key the docs hand out is rejected. `None` in every other case.
#[must_use]
pub fn ignored_key_switch_note(ignored: &str, credentials: &[DevCredential]) -> Option<String> {
    (ignored == EXAMPLE_ACCESS_KEY_ID && !example_pair_active(credentials))
        .then(|| format!("The example key is not accepted because {ENV_DISABLE_EXAMPLE} is set."))
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

    // The example pair is disabled: remove it if an earlier boot, or an
    // operator through the management API, stored it in a file-backed catalog.
    // Seeding only adds keys, so without this the public key would keep
    // verifying on every restart after the switch was set.
    if !example_pair_active(credentials) {
        remove_example_key(catalog_store, credential_store).await?;
    }
    Ok(())
}

/// Delete the example access key from whichever user holds it.
///
/// The key id is public, so a copy on any user opens the same door: the one
/// dev mode seeds lives on the `dev` user, but an operator can import the pair
/// for another user with `extenddb manage`. The owner comes from the
/// credential store, which looks keys up by id alone. It is the uncached
/// store: the auth cache is built after seeding, so this lookup leaves nothing
/// cached behind the delete.
async fn remove_example_key(
    catalog_store: &dyn CatalogStore,
    credential_store: &dyn extenddb_auth::CredentialStore,
) -> anyhow::Result<()> {
    let stored = credential_store
        .lookup_credential(EXAMPLE_ACCESS_KEY_ID)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "dev mode: failed to look up the example credential {EXAMPLE_ACCESS_KEY_ID}: {e:?}"
            )
        })?;
    let Some(stored) = stored else {
        return Ok(());
    };
    let (account_id, user_name) = (&stored.account_id, &stored.principal_name);
    match catalog_store
        .delete_access_key(account_id, user_name, EXAMPLE_ACCESS_KEY_ID)
        .await
    {
        Ok(()) => tracing::info!(
            "dev credential {EXAMPLE_ACCESS_KEY_ID}: removed from user \
             {account_id}/{user_name} ({ENV_DISABLE_EXAMPLE} is set)"
        ),
        // The lookup above just read this row, and nothing else deletes access
        // keys while the server starts, so `NotFound` means the delete missed
        // the row and the public key still verifies. Refuse to start rather
        // than run with the switch set and the key active.
        Err(OpError::NotFound(_)) => anyhow::bail!(
            "dev mode: the example credential {EXAMPLE_ACCESS_KEY_ID} is held by user \
             {account_id}/{user_name}, but deleting it from that user removed nothing, so \
             it would keep verifying; refusing to start with {ENV_DISABLE_EXAMPLE} set."
        ),
        Err(e) => anyhow::bail!(
            "dev mode: failed to remove the example credential {EXAMPLE_ACCESS_KEY_ID} \
             from user {account_id}/{user_name}: {e:?}"
        ),
    }
    Ok(())
}

/// True when the built-in example pair is among the resolved credentials.
#[must_use]
pub fn example_pair_active(credentials: &[DevCredential]) -> bool {
    credentials.iter().any(|c| c.is_builtin)
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
                    "{} / {} (AWS documented example pair; public, set {ENV_DISABLE_EXAMPLE}=1 \
                     to remove it)",
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

    const TEAM_KEY: &str = "AKIADEVTEAMKEY000001";

    fn team_pair() -> (Option<String>, Option<String>) {
        (Some(TEAM_KEY.into()), Some("team-secret".into()))
    }

    #[test]
    fn no_env_yields_builtin_only() {
        let creds = resolve_from(None, None, false).unwrap();
        assert_eq!(creds.len(), 1);
        assert!(creds[0].is_builtin);
        assert_eq!(creds[0].access_key_id, EXAMPLE_ACCESS_KEY_ID);
        assert_eq!(creds[0].secret_access_key, EXAMPLE_SECRET_ACCESS_KEY);
        assert!(example_pair_active(&creds));
    }

    #[test]
    fn custom_pair_is_added_after_builtin() {
        let (k, s) = team_pair();
        let creds = resolve_from(k, s, false).unwrap();
        assert_eq!(creds.len(), 2);
        assert!(creds[0].is_builtin);
        assert!(!creds[1].is_builtin);
        assert_eq!(creds[1].access_key_id, TEAM_KEY);
        assert_eq!(creds[1].secret_access_key, "team-secret");
    }

    #[test]
    fn disabling_the_example_pair_leaves_only_the_custom_pair() {
        let (k, s) = team_pair();
        let creds = resolve_from(k, s, true).unwrap();
        assert_eq!(creds.len(), 1);
        assert!(!creds[0].is_builtin);
        assert_eq!(creds[0].access_key_id, TEAM_KEY);
        assert!(!example_pair_active(&creds));
    }

    #[test]
    fn disabling_the_example_pair_without_a_replacement_is_rejected() {
        let err = resolve_from(None, None, true).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(ENV_DISABLE_EXAMPLE), "{msg}");
        assert!(msg.contains("EXTENDDB_DEV_ACCESS_KEY_ID"), "{msg}");
        assert!(msg.contains("EXTENDDB_DEV_SECRET_ACCESS_KEY"), "{msg}");
    }

    #[test]
    fn disabling_the_example_pair_with_a_half_set_pair_names_the_missing_var() {
        let err = resolve_from(Some(TEAM_KEY.into()), None, true).unwrap_err();
        assert!(
            err.to_string().contains("EXTENDDB_DEV_SECRET_ACCESS_KEY"),
            "{err}"
        );
    }

    #[test]
    fn with_the_switch_set_a_half_set_pair_does_not_offer_neither() {
        // "Neither" is refused with the switch set, so the message must not
        // suggest it.
        for (k, s) in [
            (Some(TEAM_KEY.into()), None),
            (None, Some("team-secret".into())),
        ] {
            let msg = resolve_from(k, s, true).unwrap_err().to_string();
            assert!(!msg.contains("neither"), "{msg}");
            assert!(msg.contains(ENV_DISABLE_EXAMPLE), "{msg}");
        }
        let msg = resolve_from(Some(TEAM_KEY.into()), None, false)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("or neither"), "{msg}");
    }

    #[test]
    fn with_the_switch_set_a_non_akia_key_does_not_point_at_the_example_pair() {
        let msg = resolve_from(Some("test".into()), Some("test".into()), true)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("AKIA-shaped"), "{msg}");
        assert!(!msg.contains("seeded by default"), "{msg}");
        assert!(!msg.contains(EXAMPLE_ACCESS_KEY_ID), "{msg}");
    }

    #[test]
    fn disable_switch_accepts_on_and_off_values() {
        for on in ["1", "true", " 1 "] {
            assert!(
                parse_switch(ENV_DISABLE_EXAMPLE, Some(on.into())).unwrap(),
                "{on}"
            );
        }
        for off in ["0", "false", " 0 "] {
            assert!(
                !parse_switch(ENV_DISABLE_EXAMPLE, Some(off.into())).unwrap(),
                "{off}"
            );
        }
        assert!(!parse_switch(ENV_DISABLE_EXAMPLE, None).unwrap());
    }

    #[test]
    fn disable_switch_rejects_an_empty_value() {
        // `-e NAME=$VAR` with VAR unset: the operator asked for the switch,
        // so an empty value must not quietly leave the public key active.
        for empty in ["", " "] {
            let err = parse_switch(ENV_DISABLE_EXAMPLE, Some(empty.into())).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(ENV_DISABLE_EXAMPLE), "{msg}");
            assert!(msg.contains("empty"), "{msg}");
        }
    }

    /// Resolve with `vars` as the whole environment.
    fn resolve_env(vars: &[(&str, &str)]) -> anyhow::Result<Vec<DevCredential>> {
        resolve_with(|name| {
            vars.iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| (*v).into())
        })
    }

    #[cfg(unix)]
    #[test]
    fn a_non_utf8_switch_refuses_to_resolve() {
        use std::os::unix::ffi::OsStringExt;
        // `std::env::var(..).ok()` reads this as unset, which would leave the
        // example pair active with nothing printed.
        let err = resolve_with(|name| match name {
            ENV_ACCESS_KEY_ID => Some(TEAM_KEY.into()),
            ENV_SECRET_ACCESS_KEY => Some("team-secret".into()),
            ENV_DISABLE_EXAMPLE => Some(std::ffi::OsString::from_vec(vec![b'1', 0xff])),
            _ => None,
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(ENV_DISABLE_EXAMPLE), "{msg}");
        assert!(msg.contains("UTF-8"), "{msg}");
    }

    #[test]
    fn an_empty_switch_in_the_environment_refuses_to_resolve() {
        // Reached through the environment reader, so reading the switch
        // through the empty-as-unset filter would fail this test.
        for empty in ["", " "] {
            let err = resolve_env(&[
                (ENV_ACCESS_KEY_ID, TEAM_KEY),
                (ENV_SECRET_ACCESS_KEY, "team-secret"),
                (ENV_DISABLE_EXAMPLE, empty),
            ])
            .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(ENV_DISABLE_EXAMPLE), "{msg}");
            assert!(msg.contains("empty"), "{msg}");
        }
    }

    #[test]
    fn the_environment_reader_resolves_the_switch_and_the_pair() {
        let creds = resolve_env(&[
            (ENV_ACCESS_KEY_ID, TEAM_KEY),
            (ENV_SECRET_ACCESS_KEY, "team-secret"),
            (ENV_DISABLE_EXAMPLE, "1"),
        ])
        .unwrap();
        assert_eq!(creds.len(), 1);
        assert_eq!(creds[0].access_key_id, TEAM_KEY);
        assert!(!example_pair_active(&creds));

        // Empty key variables still count as unset.
        let creds = resolve_env(&[(ENV_ACCESS_KEY_ID, ""), (ENV_SECRET_ACCESS_KEY, "")]).unwrap();
        assert_eq!(creds.len(), 1);
        assert!(creds[0].is_builtin);
    }

    #[test]
    fn disable_switch_rejects_an_unrecognised_value() {
        // A typo must not leave the public credential quietly active.
        for bad in ["yes", "on", "TRUE", "2"] {
            let err = parse_switch(ENV_DISABLE_EXAMPLE, Some(bad.into())).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(ENV_DISABLE_EXAMPLE), "{msg}");
            assert!(msg.contains(bad), "{msg}");
        }
    }

    #[test]
    fn half_set_pair_is_rejected_naming_the_missing_var() {
        let err = resolve_from(Some(TEAM_KEY.into()), None, false).unwrap_err();
        assert!(
            err.to_string().contains("EXTENDDB_DEV_SECRET_ACCESS_KEY"),
            "{err}"
        );
        let err = resolve_from(None, Some("team-secret".into()), false).unwrap_err();
        assert!(
            err.to_string().contains("EXTENDDB_DEV_ACCESS_KEY_ID"),
            "{err}"
        );
    }

    #[test]
    fn non_akia_key_is_rejected_pointing_at_the_builtin_pair() {
        let err = resolve_from(Some("test".into()), Some("test".into()), false).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("AKIA-shaped"), "{msg}");
        assert!(msg.contains(EXAMPLE_ACCESS_KEY_ID), "{msg}");
    }

    #[test]
    fn builtin_key_id_collision_is_rejected_whether_or_not_it_is_disabled() {
        for disable in [false, true] {
            let err = resolve_from(
                Some(EXAMPLE_ACCESS_KEY_ID.into()),
                Some("different-secret".into()),
                disable,
            )
            .unwrap_err();
            assert!(err.to_string().contains("reserved"), "{err}");
        }
    }

    #[test]
    fn aws_env_key_is_flagged_only_when_foreign() {
        let (k, s) = team_pair();
        let creds = resolve_from(k, s, false).unwrap();
        // Unset or empty: nothing to warn about.
        assert_eq!(ignored_aws_env_key_from(None, &creds), None);
        // Matches a seeded credential (either of them): no warning.
        assert_eq!(
            ignored_aws_env_key_from(Some(EXAMPLE_ACCESS_KEY_ID.into()), &creds),
            None
        );
        assert_eq!(
            ignored_aws_env_key_from(Some(TEAM_KEY.into()), &creds),
            None
        );
        // Foreign key, including ASIA session keys: flagged for the advisory.
        assert_eq!(
            ignored_aws_env_key_from(Some("ASIAFOREIGNSESSION01".into()), &creds),
            Some("ASIAFOREIGNSESSION01".into())
        );
    }

    #[test]
    fn aws_env_example_key_is_flagged_once_the_example_pair_is_disabled() {
        let (k, s) = team_pair();
        let creds = resolve_from(k, s, true).unwrap();
        assert_eq!(
            ignored_aws_env_key_from(Some(EXAMPLE_ACCESS_KEY_ID.into()), &creds),
            Some(EXAMPLE_ACCESS_KEY_ID.into())
        );
    }

    #[test]
    fn the_switch_note_names_the_switch_only_for_a_removed_example_key() {
        let (k, s) = team_pair();
        let disabled = resolve_from(k.clone(), s.clone(), true).unwrap();
        let note = ignored_key_switch_note(EXAMPLE_ACCESS_KEY_ID, &disabled).unwrap();
        assert!(note.contains(ENV_DISABLE_EXAMPLE), "{note}");
        // A foreign key is rejected for another reason.
        assert_eq!(
            ignored_key_switch_note("ASIAFOREIGNSESSION01", &disabled),
            None
        );
        // Not reached in practice (the example key is not ignored while the
        // pair is active), but the note must not claim the switch is set.
        let active = resolve_from(k, s, false).unwrap();
        assert_eq!(
            ignored_key_switch_note(EXAMPLE_ACCESS_KEY_ID, &active),
            None
        );
    }

    #[test]
    fn describe_prints_builtin_secret_but_never_custom_secret() {
        let (k, s) = team_pair();
        let creds = resolve_from(k, s, false).unwrap();
        let text = describe(&creds);
        assert!(text.contains(EXAMPLE_SECRET_ACCESS_KEY), "{text}");
        assert!(text.contains(ENV_DISABLE_EXAMPLE), "{text}");
        assert!(text.contains(TEAM_KEY), "{text}");
        assert!(!text.contains("team-secret"), "{text}");
    }

    #[test]
    fn describe_omits_the_example_pair_once_disabled() {
        let (k, s) = team_pair();
        let creds = resolve_from(k, s, true).unwrap();
        let text = describe(&creds);
        assert!(!text.contains(EXAMPLE_ACCESS_KEY_ID), "{text}");
        assert!(!text.contains(EXAMPLE_SECRET_ACCESS_KEY), "{text}");
        assert!(text.contains(TEAM_KEY), "{text}");
    }
}

#[cfg(test)]
#[path = "dev_credentials_seed_tests.rs"]
mod seed_tests;
