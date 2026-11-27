# RFC-0000: Encrypt stored secrets under a key kept outside the catalog, and hold less key material in memory

- Status: Draft
- Author: @LeeroyHannigan
- Created: 2026-11-11
- FCP ends: not set
- Tracking issue: #707

## Summary

ExtendDB encrypts only stored secrets: the secret of each access key and the secret of each AssumeRole session. On PostgreSQL this RFC adds one more stored secret, the `data_database_connection_string` row (section 12). ExtendDB does not encrypt table data, and nothing in this RFC changes that.

Today one AES-256-GCM key encrypts the stored secrets. The catalog stores that key in plaintext, in the same database as the secrets. A copy of the catalog therefore opens every stored secret.

This RFC makes four changes:

1. A new setting, `key_file`, gives the path of a key-encryption key (KEK) file on the server host. The KEK encrypts the data encryption key (DEK). The DEK encrypts the stored secrets. The catalog then holds only the encrypted DEK. Any tool that delivers a secret to a service as a file can hold the KEK (section 3). Examples are systemd credentials sealed to a Trusted Platform Module (TPM), and Kubernetes secret volumes.
2. The server holds less key material in memory. It caches one Signature Version 4 (SigV4) signing key per access key and day instead of the plaintext secret. It keeps the DEK and the signing keys in locked memory that core dumps exclude (section 6).
3. Each stored secret is bound to its row through the additional authenticated data (AAD) of AES-GCM (section 5). A writer to the catalog who has no key cannot reassign a secret to another principal or extend a session.
4. Six commands under `extenddb encryption-key`, each named for what it does: `status`, `switch`, `rewrap`, `rotate`, `reset` and `set-data-connection` (section 8). A KEK change rewrites one catalog row and needs no outage. The switch to a KEK file and a DEK replacement rewrite every stored secret and run while every server is stopped.

Without `key_file`, the stored format and the catalog key stay as they are today. Change 2 applies to the catalog key as well. No part of the design uses a network key service.

## Motivation

A production deployment must not let a holder of a catalog dump hold both the encryption key and the secrets it encrypts. Today it does. On main at 95425f5e8:

- `extenddb init` writes the key as the `encryption_key` row of the catalog `settings` table, in base64, on all four backends (`crates/storage-postgres/src/bootstrapper.rs:226-254` and the matching bootstrappers). The security model states the result: read access to the catalog database "yields every stored access key secret" (`docs/manuals/10-security-model.md:22`).
- The stored format is `nonce(12) || ciphertext || tag(16)`, with the access key ID as the AAD (`crates/storage/src/secret_cipher.rs:4-16`). No stored byte records which key encrypted a secret.
- Each server reads the key once at startup and keeps it in memory until it exits (`crates/storage-postgres/src/lib.rs:958-972` and the matching factories). `extenddb settings set` refuses to change the key (`crates/app/src/cmd_settings.rs:179-194`).
- The credential cache keeps up to 10,000 plaintext secrets and session tokens (section 1). A heap leak, a core dump or swap gives the secret of every active key.
- The only rotation is the manual procedure at `docs/manuals/10-security-model.md:66-70`. The operator replaces the row, restarts every server, then deletes and re-creates every access key. The comment on #707 describes the result as an outage for every signed request until each client has new credentials.

On #228 (2026-08-05), a code owner wrote that the current handling "is probably not sufficient for many production uses". The same review asked for other mechanisms, such as a hardware security module (HSM) or a passcode. ExtendDB runs with no internet connection (the paragraph of `README.md` after its trademark notice, `docs/manuals/11-deployment-guide.md:148-154`), so no key source may depend on a network key service.

## Detailed design

### Terms

The rest of this document uses each term below with this meaning only.

- Stored secret: the encrypted secret of one access key or one AssumeRole session, in the `access_keys` or `iam_sessions` table of the catalog. On PostgreSQL, after section 12, also the `data_database_connection_string` row. Table data is never a stored secret.
- Catalog key: the AES-256-GCM key in the `encryption_key` row of the catalog `settings` table. Every deployment uses it today.
- DEK (data encryption key): the 32-byte AES-256-GCM key that encrypts stored secrets in a deployment that uses a KEK file. The catalog key is not a DEK.
- KEK (key-encryption key): a 32-byte key, kept in a file outside the catalog, that encrypts the DEK. Encrypting a key with another key is called wrapping.
- Key ring: the catalog `settings` row `encryption_key_ring`. It holds each wrapped DEK, the id of the KEK that wraps them, the KEK source, and the state of an unfinished switch (section 4). The KEK authenticates every field of the ring other than the wrapped values.
- AAD (additional authenticated data): an input to AES-GCM that is authenticated but not encrypted. Decryption fails when the AAD at decrypt time differs from the AAD at encrypt time. The AAD therefore binds a ciphertext to a context, such as its row.
- Opens: a key opens a ciphertext when it decrypts the ciphertext and the AES-GCM authentication passes. A wrong key, a changed byte or a changed AAD fails that check.
- The file rule: the ownership, mode and access control list (ACL) check of a KEK file (section 2). Every command applies it before it reads the key.
- Delivery route: the tool that gives the server its KEK file at start: systemd, Kubernetes, Docker swarm, Podman or sops (section 3).
- Switching to a KEK file, or the switch: the one-time change of a deployment from the catalog key to a KEK file. It rewrites every stored secret under a new DEK and deletes the catalog key (section 8.1).
- Older release: a release of ExtendDB without this change.
- Unit: the systemd service unit that runs the ExtendDB server, as in the deployment guide (`docs/manuals/11-deployment-guide.md:253-285`).
- Scope date: the UTC date in a SigV4 credential scope, for example `20261114`.
- Signing key: the SigV4 key derived from a secret, a scope date, a region and a service (`crates/auth/src/sigv4/signing_key.rs:24-29`). A request signature is an HMAC under this key.
- Locked key store: the memory region of section 6 that holds the DEK and the signing keys. Its pages stay out of swap and out of core dumps.
- Catalog copy: any copy of the catalog database: a dump, a backup, a replica, a snapshot or a copied file.
- Catalog writer: anyone who can write rows into the catalog database without an ExtendDB server.

### 1. Threat model

The problem: a reviewer must be able to see what each attacker gets today and after this change.

| Attacker | Today | After this RFC |
|---|---|---|
| A reader of a catalog copy | Every stored secret, because the copy holds the catalog key | No secret, unless the copy predates the switch (it holds the catalog key) or the reader also holds the KEK. A physical copy of PostgreSQL, MongoDB or Cassandra taken after the switch can still hold the deleted `encryption_key` row. Examples: a write-ahead log archive, an SSTable before compaction, an oplog entry. Section 11 says what to do about that |
| A reader of the server's memory (heap leak, core dump, swap) | The catalog key, in two long-lived copies per process, one never wiped (`crates/storage-sqlite/src/catalog_store.rs:29`, `crates/storage-sqlite/src/credential_store.rs:20-24` and the matching stores). Up to 10,000 plaintext secrets and session tokens in the credential cache (`crates/config/src/lib.rs:352-354`, `crates/auth/src/lib.rs:55-83`) | The DEK, and the signing keys valid for one scope date, in locked pages that core dumps exclude (section 6). No plaintext secret outside the moment of a cache miss |
| Root on the running host | The catalog key and every secret | The DEK, under every delivery route. A later key process narrows this (section 6, item 3) |
| A catalog writer | Admin control, with no key (below) | The same. Out of scope |

A catalog writer is out of scope. With no key at all, a writer can today:

- take admin or user control: replace `admin_users.password_hash` (`crates/storage-sqlite/migrations/0001_baseline.sql:169-171`, checked by `crates/server/src/management/auth.rs:180-224`) or `iam_users.password_hash` (`0001_baseline.sql:176-180`). An admin creates access keys for any user and calls AssumeRole.
- grant any permission or change any role trust: write `iam_policies.policy_document` (`0001_baseline.sql:268-273`, read at `crates/storage/src/authorization_store.rs:22-98`) or `iam_roles.trust_policy` (`0001_baseline.sql:230-234`, evaluated at `crates/server/src/management/assume_role.rs:137-181`).
- reassign any access key or session to another principal, or extend a session. The AAD today is the access key ID alone, and the identity and `expires_at` come from the row (`crates/storage-sqlite/src/credential_store.rs:39-41`, `:61-64`, `:95-103`, `0001_baseline.sql:261`).
- deny service by deleting or replacing the `encryption_key` row.

Encryption cannot protect against any of these. Integrity protection of the catalog would be its own RFC. The rules of section 8.5 still stop a catalog key that a writer chose from becoming valid through the commands that the server's messages name. Section 5 binds each stored secret to its row. That ends the reassignment and session extension items for a writer who holds no key.

Some plaintext stays in memory as today, and this RFC does not cover it. That is the TLS private key inside rustls, the database passwords in the configuration and in the sqlx pools, and the console session tokens. The response of one CreateAccessKey or AssumeRole request holds a secret for the life of that request (`crates/storage/src/management_store/types.rs:110-113`, `crates/server/src/management/assume_role.rs:358-366`). The catalog also keeps each session token in plaintext as the primary key of `iam_sessions` (`0001_baseline.sql:252-254`). Hashing it there changes a primary key on four backends and is out of scope.

### 2. The KEK file and the file rule

The problem: the server needs a key outside the catalog, and it must refuse a key file that another user can read or replace.

A new `[auth.encryption]` section of `extenddb.toml` gives the path of the KEK file:

```toml
[auth.encryption]
key_file = "/etc/extenddb/kek"
```

| `key_file` | Key the server uses | Where the key is kept |
|---|---|---|
| not set (the default) | the catalog key | the `encryption_key` row of the catalog `settings` table, as today |
| set | the DEK in the key ring, unwrapped with the KEK | the KEK in `key_file`, the wrapped DEK in the key ring |

An upgrade changes nothing until the operator switches the deployment to a KEK file. A later key source, such as a PKCS#11 token, gets its own setting and its own `source` value in the key ring (section 4). The server refuses a configuration that sets two key sources at once.

File format. The file holds exactly 32 random bytes in standard base64 with padding, which is 44 characters, and at most one trailing newline. The server accepts nothing else: no comment, no blank line, no leading or trailing space. `head -c 32 /dev/urandom | base64 > kek`, run under `umask 077`, writes a file that the server accepts. A leading `~` in `key_file` expands through `expand_tilde`, as in the TLS key path (`crates/app/src/serve_helpers.rs:164`). After that expansion the path must be absolute, because the daemon runs from `/`. The configuration refuses a relative `key_file`.

The KEK must be new random bytes. A catalog copy taken before the switch holds the catalog key. A KEK equal to it would open the key ring of every later copy. `switch` refuses a KEK that equals the catalog key or a DEK of the ring (section 8.1).

The file rule. Every command that reads a KEK file applies these checks, in this order, and refuses the file when one fails:

1. Resolve the path to its canonical form. The files of a Kubernetes secret volume are symlinks, and the check must see the target.
2. Check every parent directory of the canonical path. Root or the effective user must own each one. None may be writable by group or others, unless its sticky bit is set. This is the ancestor half of the backup store's directory check (`check_private_tree`, `crates/backup-store/src/fs.rs:893-938`), made a public function that both callers use.
3. Open the canonical path with `O_NOFOLLOW`, then `fstat` the open descriptor. The check runs on the descriptor, not on the path, so no one can swap the file between the check and the read. The descriptor must be a regular file that root or the effective user owns. Others must have no permission bits. Check 4 judges the group bits, because their meaning depends on the ACL.
4. On Linux, read the POSIX ACL of the descriptor (`fgetxattr` on `system.posix_acl_access`) before any judgement of the group bits. With an ACL that has a mask entry, the group class bits of `st_mode` are the mask, not the permission of the owning group. acl(5) states this under "Correspondence between ACL entries and file permission bits" ([acl(5)](https://man7.org/linux/man-pages/man5/acl.5.html)). In that case the `ACL_GROUP_OBJ` entry stands for the owning group: it must grant nothing unless root owns the file and the file's group is the server's effective group. Each named user entry must name the effective user. Each named group entry must name the effective group, and it must grant nothing unless root owns the file. The entry for others must grant nothing. Without an ACL, the rule allows the mode's group bits only when root owns the file and the file's group is the server's effective group. A file that the server's user owns needs no group access. On macOS the rule checks owner and mode only, and the startup log says so.

The order matters for the systemd tmpfs case. systemd's `write_credential` (`src/core/exec-credential.c`, [source](https://raw.githubusercontent.com/systemd/systemd/main/src/core/exec-credential.c)) creates the file as root at mode 0400 and adds the user's ACL entry with `fd_add_uid_acl_permission`. It calls `fchown` only when the file system refuses ACLs. Measured on a tmpfs in a container on the shared host: a root:root file at mode 0400 with a named-user entry reports `st_mode` 0440. A rule that judged the group bits before the ACL would refuse that file for group permission that no group has.

The result: the server's user or root owns the file, and no one else can read it. The rule cannot see which users hold a group, so the server's effective group must have no other members. The two delivery routes that give the file a root owner pass. These are the systemd tmpfs case (root owner, an ACL entry for the service user) and the Kubernetes `fsGroup` case (root owner, group 10001, mode 0440). The second needs a server whose effective group is 10001.

The TLS key check (`crates/app/src/serve_helpers.rs:193-207`) stays as it is, because existing deployments depend on it. It narrows the mode and warns. The file rule refuses instead. A new setting has no deployments that a refusal can break. A user who could read the file before may already hold a copy of the KEK. The file rule is its own function and may share the mode check with the TLS key check.

The server reads the KEK file once at startup into a `Zeroizing` buffer, unwraps the DEK into the locked key store, and drops the buffer. That wipes the KEK from the server's memory. The file is unchanged, and the KEK stays in it. `rewrap` reads the file again when it runs (section 8.2).

### 3. Delivering the KEK file

The problem: the KEK must reach the server at each start as a file that the file rule accepts, with no operator typing it. Any tool that gives a service a file at start can deliver it. The routes below need no ExtendDB code. The server runs as the user `extenddb` in the unit, and as uid 10001 in the container image (`docs/manuals/11-deployment-guide.md:253-285`, `Dockerfile:57-58`, `:78`). Each route gives that user the file with an owner and a mode that the file rule accepts.

systemd. Recommended for the systemd deployment of the deployment guide, on a host with systemd 250 or later and a TPM 2.0 chip. The operator writes the KEK to a file, keeps a copy away from the host, and seals the file to the TPM with `systemd-creds encrypt --with-key=tpm2`. `LoadCredentialEncrypted=` decrypts it at each start of the unit into `/run/credentials/<unit>/`. systemd unmounts that directory when the unit stops ([systemd-creds(1)](https://www.freedesktop.org/software/systemd/man/latest/systemd-creds.html), [systemd.exec(5)](https://man7.org/linux/man-pages/man5/systemd.exec.5.html)). A copy of the host disk then holds no KEK that opens without that TPM. Root on the running host can still decrypt it.

```sh
systemd-creds encrypt --with-key=tpm2 --name=extenddb-kek kek /etc/credstore.encrypted/extenddb-kek
```

```ini
[Service]
LoadCredentialEncrypted=extenddb-kek:/etc/credstore.encrypted/extenddb-kek
Environment=EXTENDDB__AUTH__ENCRYPTION__KEY_FILE=%d/extenddb-kek
LimitMEMLOCK=16M
```

systemd gives the file to the unit's user in one of two ways. On a ramfs, the user owns the file at mode 0400. systemd 254 or later on Linux 6.4 or later uses a tmpfs. There root owns the file and a POSIX ACL entry lets the user read it, so the mode reads as 0440 (section 2). The file rule accepts both. `LimitMEMLOCK=` is for the locked key store (section 6).

Kubernetes. Use a Secret volume with `defaultMode: 0400`, and `fsGroup: 10001` in the pod's `securityContext`. kubelet then gives the file to root with group 10001 at mode 0440. Its `changeFilePermission` in `pkg/volume/volume_linux.go` calls `os.Lchown(filename, -1, int(*fsGroup))`, which changes the group and never the owner ([volume_linux.go](https://github.com/kubernetes/kubernetes/blob/master/pkg/volume/volume_linux.go)). Without `fsGroup`, root owns the file with group root, and uid 10001 cannot open it. The file rule accepts the group bits only when root owns the file and its group is the server's effective group (section 2). The server must therefore run with group 10001. The image's `USER 10001:10001` (`Dockerfile:78`) gives that group. A pod that sets `runAsUser` also sets `runAsGroup: 10001`, so that the group does not depend on the container runtime. Measured on a tmpfs: a process with effective group 0 can read the file when it holds 10001 as a supplementary group. The file rule refuses that process. Secrets are unencrypted in etcd unless the cluster enables encryption at rest ([Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)). Set the memlock limit through the container runtime (section 6). The line to use: `securityContext: {runAsUser: 10001, runAsGroup: 10001, fsGroup: 10001}` with `defaultMode: 0400` on the volume.

Docker swarm. Create the service with `--secret source=extenddb-kek,target=kek,uid=10001,gid=10001,mode=0400` ([docker service create](https://docs.docker.com/reference/cli/docker/service/create/)). The default, root with mode 0444, fails the file rule. Swarm keeps every secret in the Raft log of its managers, and each manager keeps the key of that log unencrypted on its disk. A copy of a manager's disk therefore gives the KEK. If that copy must not give the KEK, enable `--autolock`. Each manager restart then waits for an operator to type the unlock key ([Lock your swarm](https://docs.docker.com/engine/swarm/swarm_manager_locking/)). ExtendDB restarts do not wait while the managers are up. After a manager's Docker restarts, no service starts or restarts until an operator unlocks it. Compose file secrets are bind mounts that ignore `uid`, `gid` and `mode`, so uid 10001 must own the host file at mode 0400. The line to use: the `--secret` option above, with `--autolock` when a manager disk copy must not give the KEK.

Podman. Use `--secret extenddb-kek,type=mount,uid=10001,gid=10001,mode=0400`. The default, root with mode 0444, fails the file rule. The line to use: the `--secret` option above.

sops with age. sops keeps the encrypted KEK in configuration management, and `sops decrypt` in an `ExecStartPre=` writes the file under `umask 077`. Use this route only with an age identity on a hardware token, such as a YubiKey or a TPM through an age plugin. An age identity file on the same disk as the KEK protects no more than a plain KEK file. A PIN or touch policy on the token stops unattended restarts.

The offline commands of section 8 need the KEK too. They run only while every server is stopped, and never by themselves: an operator starts them. Each runs as the server's user, with the same delivery route as the server. On systemd that is a transient service with the same `LoadCredentialEncrypted=` line, started with `systemd-run --wait --pipe`. On Kubernetes it is a Job that mounts the same Secret while the Deployment has 0 replicas. On swarm it is a one-off service with the same secret, and on Podman a `podman run` with the same secret.

None of these copies is a backup of the KEK. A TPM-sealed file opens only with that TPM, so it is lost with the host. Keep a copy of each KEK away from the host, made before the KEK is sealed or stored as a secret. Every route gives root on the running host the DEK, because the server holds it in memory (section 6).

### 4. The key ring

The problem: with a KEK file, the catalog must hold the DEK in a form that only the KEK opens. It must also record which KEK that is, and the state of an unfinished switch or rotation.

With `key_file` set, the catalog holds the DEK in one settings row, `encryption_key_ring`:

```json
{"version":1,"kek":{"source":"file","id":"3f9a0c51e2d47b86"},"keys":[{"id":2,"wrapped":"<base64>"}],"switch_from_catalog_key":false,"retiring_catalog_key_id":null}
```

- `kek.source` records where the KEK comes from. This RFC defines `file`. A later source, such as PKCS#11, gets its own value. The configuration needs no type field, because each source has its own setting (section 2). The ring records the source so that ids of two sources never compare equal.
- `kek.id` is the first 8 bytes, in hex, of HMAC-SHA256 keyed with the 32 decoded bytes of the KEK, over the text `extenddb key-encryption-key id`. It identifies the KEK and does not reveal it. `aws_lc_rs::hmac` computes HMAC-SHA256 for SigV4 already (`crates/auth/src/sigv4/signing_key.rs:37-43`).
- `wrapped` is the DEK encrypted under the KEK with the existing cipher (`secret_cipher::encrypt_secret`, `crates/storage/src/secret_cipher.rs:37`), then base64 encoded. Its AAD covers every field of the ring other than the wrapped values. After the fixed prefix `extenddb-dek-v1` come `version`, `catalog_id`, the KEK source, the KEK id, the ids of every DEK in the ring, this DEK's id, `switch_from_catalog_key` and `retiring_catalog_key_id`. The list of ids is in increasing order, in decimal, with commas between them, for example `1,2`. The KEK therefore authenticates the ring's state and its set of DEKs. Each of these changes makes a DEK fail to unwrap: a flipped flag, a changed id, an added or deleted DEK entry. So does a wrapped DEK copied from another ring. The server and every command then refuse with the damaged-ring message, which names no write command (section 7). `catalog_id` is the random universally unique identifier (UUID) that migration 6 writes into `settings` on every backend (`crates/storage-sqlite/src/migrations/m0006_backup_catalog_index.rs:162-170` and the matching migrations).
- Every write of the ring re-seals each DEK, because the AAD changes with the state. That is one `encrypt_secret` call per DEK, 745 ns each in the measurement of section 8.4.
- A new DEK comes from `helpers::generate_encryption_key`, which `init` uses for the catalog key (`crates/storage/src/bootstrapper.rs:257`).
- DEK ids start at 1, and each DEK replacement uses the next id. The DEK with the highest id is the active one. The order of the entries in `keys` carries no meaning.
- The parser refuses a field that the example above does not show, and a value of another type. `version` and each DEK id are JSON integers. `kek.id` is 16 lowercase hex characters, and `kek.source` is a source that this release knows. `switch_from_catalog_key` is a JSON boolean. `retiring_catalog_key_id` is null or 16 lowercase hex characters, never an empty string, because null and an empty string have the same AAD encoding.
- The ring holds one DEK. It holds two only during an unfinished DEK replacement (section 8.3). It never grows past two, so the row cannot grow large and nothing needs pruning.
- `switch_from_catalog_key` is true from the start of a switch until the switch finishes. `switch` sets `retiring_catalog_key_id` at the same time (section 8.1). It is the first 8 bytes, in hex, of HMAC-SHA256 keyed with the KEK over the 32 decoded bytes of the catalog key. This is the construction of `kek.id` with a different input. A resumed switch accepts only a catalog key whose id matches. A catalog key row is an old key only while the flag is true and its id matches (section 8.5). Both fields are under the KEK's authentication, so a catalog writer cannot change them. A writer can still write back a whole ring row from a catalog copy (section 8.5).
- One row holds the whole ring. Each change to the ring is one row write, which is atomic on all four backends, Cassandra included.
- The row name contains `encryption_key`, so `extenddb settings list`, the management API and the console mask its value as they mask the catalog key (`crates/config/src/display.rs:13-36`). `extenddb settings set` refuses it, as it refuses every key outside `KNOWN_KEYS` (`crates/app/src/cmd_settings.rs:174-194`).

AAD encoding. Every AAD in this RFC has the same form. After the fixed prefix, each field follows a `|` as its length in bytes in decimal, a colon, then the bytes. For example, take a ring that holds only a DEK with id 2, under the KEK above, in a catalog whose id is `0f3c...` (36 characters). The AAD of that wrapped DEK is `extenddb-dek-v1|1:1|36:0f3c...|4:file|16:3f9a0c51e2d47b86|1:2|1:2|5:false|0:`. No field value can change where the next field starts. The reason is the import route of access keys. It checks only the length and the `AKIA` prefix of an access key ID (`crates/server/src/management/iam_user_self.rs:270-273`), so a `|` can appear in the 16 characters after the prefix. With a plain separator, two rows could share one AAD.

A DEK entry deleted from the ring by hand changes the list of DEK ids. Each remaining DEK then fails to unwrap, and the server refuses with the damaged-ring message (section 7). A stored secret that no key opens fails as a damaged secret fails today, with HTTP 500 `InternalServerError` (`crates/storage-sqlite/src/credential_store.rs:54-60` and the matching stores). `encryption-key status` lists such secrets by access key ID (section 8). Without `key_file`, the catalog has no key ring.

Why a DEK at all, instead of encrypting the secrets with the file key directly? A KEK change then rewrites one row, the ring, and touches no stored secret (section 8.2, `rewrap`). A DEK replacement changes no file on any host. A later hardware source unwraps the DEK once at startup instead of on each cache miss (Alternatives).

### 5. Binding each stored secret to its row

The problem: today the AAD of a stored secret is the access key ID alone (`crates/storage/src/secret_cipher.rs:9-10`). The identity of the principal comes from the row (`crates/storage-sqlite/src/credential_store.rs:39-41`, `:61-64`, `crates/auth/src/lib.rs:237-247`). A catalog writer with no key can therefore reassign a stored secret to another principal, extend a session, or carry a secret into another deployment.

Stored secrets written under a ring DEK use this AAD, in the field encoding of section 4:

```text
extenddb-secret-v1|<catalog_id>|<table>|<access_key_id>|<account_id>|<user or role name>|<session name or empty>|<expires_at as whole Unix seconds, or empty>
```

Secrets under the catalog key keep today's AAD, the access key ID. No stored byte marks which form a secret uses: the key that opens a secret decides it. A server with `key_file` uses the new form for every stored secret, and the switch writes every stored secret in it (section 8.1). The `data_database_connection_string` row of section 12 is the one stored secret with its own form, `extenddb-setting-v1|<catalog_id>|data_database_connection_string`, because it has none of the fields above.

Facts that make this possible at 95425f5e8:

- Every encrypt and decrypt site has these fields available after the row read, except `catalog_id`. The sites are CreateAccessKey (`crates/storage-sqlite/src/management_store/access_keys.rs:35-44`), ImportAccessKey (`:131-140`), AssumeRole (`crates/server/src/management/assume_role.rs:284-337`), the user lookup (`crates/storage-sqlite/src/credential_store.rs:35-71`), the session lookup (`:73-122`), and the matching stores. The server reads `catalog_id` once at startup and passes it to the stores as it passes the key.
- AssumeRole computes `expires_at` after its encrypt call today (`assume_role.rs:315`, `:323-324`). It must compute it first.
- The backends store `expires_at` at different precisions: SQLite as RFC 3339 text, PostgreSQL in microseconds, MongoDB in whole seconds, Cassandra in milliseconds (`crates/storage-mongodb/src/management_store.rs:2141`, `crates/storage-cassandra/src/management_store/access_keys.rs:252`). The AAD therefore carries whole Unix seconds. The code truncates the value before the encrypt and before the write.
- No code renames a user or role, extends a session, or updates an access key row. The only change to a credential row is `iam_sessions.session_policy` (`crates/storage-sqlite/src/management_store/mod.rs:669` and the matching stores). A row's AAD fields therefore never change after the write.
- The AAD does not bind `is_active` (`crates/storage-sqlite/src/credential_store.rs:39-41`). No store runs an update of `access_keys`, so no code changes that column today. The omission is a stated choice.

What this stops: a writer without the DEK reassigning an existing secret to another principal, extending a session, or carrying a secret into another deployment. The AWS Encryption SDK binds an encryption context to each message for the same reason ([Concepts](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/concepts.html)). What it does not stop: a writer who also holds a key, who can build any ciphertext with any AAD. Nor does it stop a writer who holds a catalog copy from writing back a row deleted after that copy. The row's AAD fields did not change, so its secret opens again: a deleted access key works again, and a session works again until its `expires_at`. The routes to admin control in section 1 need no key and stay open.

Access key IDs start with `AKIA` or `ASIA` (`crates/server/src/management/iam_user_self.rs:269-276`, `crates/server/src/dev_credentials.rs:197-205`, `crates/server/src/management/crypto.rs:28-39`). The three AAD prefixes are `extenddb-dek-v1|` for a wrapped DEK, `extenddb-secret-v1|` for a stored secret under a DEK, and `extenddb-setting-v1|` for the connection string row. No two kinds of ciphertext share an AAD, and the length encoding keeps that true for any field value.

### 6. Less key material in memory

The problem: envelope encryption lowers the risk from a copy of the catalog. It does not lower the risk from the server's memory. After the switch alone, the server would hold the DEK for its life. The credential cache would hold the plaintext secret and session token of each active key. A heap leak, a core dump or swap would then give the secret of every active key, and the DEK, which opens the rest.

Requirement: the server holds the smallest amount of key material it can, in locked memory. Three items meet it. Items 1 and 2 are in the 1.0 scope of this RFC, as are the rules of section 8.5. Item 3 is planned work.

#### 6.1 Cache signing keys, not secrets

Today the credential cache holds a `StoredCredential` per access key, with the plaintext `secret_key` and, for a session, the plaintext `session_token` (`crates/auth/src/lib.rs:55-83`). SigV4 reads the secret only to derive a signing key (`crates/auth/src/sigv4/verify.rs:145-146`). Each request derives that key again with four HMAC-SHA256 calls (`crates/auth/src/sigv4/signing_key.rs:24-29`).

After this change, the cache entry for an access key holds:

- the principal fields, the session flag, `expires_at`, and the SHA-256 of the session token.
- the SigV4 signing key for each scope date in use, in a slot of the locked key store (6.2).

It holds no plaintext secret and no plaintext token. The server hashes a presented session token and compares it with the stored hash in constant time, as it compares the plaintext today (`crates/auth/src/lib.rs:196-206`).

The rules of the cache, and the facts behind them:

- One region and one service per server. `serve` sets the region from `server.region` (`crates/server/src/serve.rs:531-535`, default `us-east-1` at `crates/config/src/lib.rs:380-382`). The service must be `dynamodb` (`verify.rs:66-68`). A signing key therefore depends only on the secret and the scope date, so one key per access key and day is enough.
- The server accepts an `X-Amz-Date` within 900 s of its clock (`verify.rs:17-18`, `:166-190`). The scope date must equal the date of `X-Amz-Date` (`verify.rs:25-31`). From 23:45 to 00:15 UTC server time, one access key can therefore need two scope dates. The cache keeps at most two slots per access key. Deriving a signing key for a new scope date frees the slot of a stale date. A date is stale when it is neither the current UTC date nor the adjacent date inside the 900 s window.
- A cache miss decrypts the secret and derives the signing key into a slot. It then wipes the secret, the `"AWS4"` prefix string and the three intermediate HMAC keys. Today none of these is wiped (`signing_key.rs:25-28`). A stale-while-revalidate refresh of the cache (`crates/auth/src/credential_cache.rs:7-11`) is one decrypt and one derive.
- The cache evicts in maintenance tasks, so live entries can exceed `max_entries` for a short time and a miss can find no free slot. The miss then derives the signing key into a temporary locked buffer, serves the request, wipes the buffer and caches nothing. The server logs one warning per minute at most for that case. Startup reserves these buffers, one per runtime worker thread, inside the store's size (6.2). Decrypt, derive and signature check run in one synchronous section, so each worker thread uses one buffer at a time. If no buffer is free, that request fails with HTTP 500 `InternalServerError`, with a warning in the log. It never derives into unlocked memory.
- Measured on the shared 16-CPU host, as the median of 7 runs of 100,000 calls, at a load average of 4.7 to 5.3. `derive_signing_key` takes 1.72 to 1.76 µs, which a cache hit no longer pays. `compute_signature` takes 0.65 µs and stays. SHA-256 of a 64-character token takes 0.09 µs.
- A leaked signing key signs only requests whose scope date is its date. The server accepts such a key for 24.5 hours of server time, from 23:45 UTC the day before to 00:15 UTC the day after. A leaked token hash is not a token. After that window, a leak of signing keys needs no access key re-created.
- The hash protects the server's memory, not the catalog, which keeps each session token in plaintext (section 1).
- The dev-mode seeding check compares the stored secret with the configured one once at startup through the raw store (`crates/server/src/dev_credentials.rs:292-305`). It keeps working: it decrypts on purpose, then wipes.

#### 6.2 The locked key store

One module in `crates/auth` calls `libc` directly, which is already a workspace dependency (`Cargo.toml:184`). No new dependency.

- On Linux it first tries `memfd_secret`, which the kernel has since 5.14 and enables by default since 6.5 ([memfd_secret(2)](https://man7.org/linux/man-pages/man2/memfd_secret.2.html)). Its pages leave the kernel direct map, never reach swap, and stay out of core dumps. On `ENOSYS`, `EPERM` or `EAGAIN` it uses an anonymous private mapping with `mlock`, `MADV_DONTDUMP` and `MADV_WIPEONFORK`. Measured on the shared host: a page marked `MADV_DONTDUMP` was absent from the core dump. One locked page cost about 6 µs to set up. That figure is the sum of `mmap`, `mlock` and `madvise`, as the mean of 20,000 calls at a load average of 3.5. The module wipes each slot when it frees the slot.
- The store holds the DEK, signing-key slots and the temporary buffers of 6.1. Its size follows `[auth.cache] max_entries`: two 32-byte slots per entry, so slots for 10,000 entries need 640,000 bytes, which is 157 pages. Each runtime worker thread adds one 32-byte buffer.
- The kernel's default memlock limit is 8 MiB on kernel 5.16 and later, and 64 KiB before ([commit 9dcc38e2813e](https://github.com/torvalds/linux/commit/9dcc38e2813e)). Hosts on a kernel before 5.16, for example RHEL 9 and Amazon Linux 2, therefore need `LimitMEMLOCK=` in the unit, `--ulimit memlock=` in Docker, or a smaller `max_entries`. The documented unit gains `LimitMEMLOCK=16M` (section 3). Measured in a container on this host, as uid 10001 under a 64 KiB limit: `mlock` of 32 KiB succeeded, and `mlock` of 1 MiB failed with `ENOMEM`.
- When locking fails under a configuration that sets `key_file`, startup refuses, naming both fixes and how much locked memory it needs. `[auth.encryption] require_locked_memory = false` turns the refusal into a warning. Vault's `disable_mlock` is the precedent ([Vault configuration](https://developer.hashicorp.com/vault/docs/configuration)). Without `key_file`, the server warns and starts, so an upgrade breaks no deployment.
- The startup log line names the protection in force: `memfd_secret`, locked pages, or none.
- The server does not call `prctl(PR_SET_DUMPABLE, 0)` by default. That call stops every core dump, which operators need for crash diagnosis. `MADV_DONTDUMP` keeps the core and excludes the key pages. An operator who wants no core at all sets `LimitCORE=0` in the unit.
- macOS: `mlock`, with no per-range core exclusion through libc. Windows: `VirtualLock`. The dev npm binaries warn when they cannot lock.
- One key holder per process. Each backend holds the key once, in the store, and both of its stores reference that slot. This deletes the unwiped copies that exist today. The first is the catalog store's `Arc<str>` (`crates/storage-sqlite/src/catalog_store.rs:29`, `:64` and the matching stores). The second is the per-call copies in CreateAccessKey, ImportAccessKey and AssumeRole (`crates/storage-sqlite/src/management_store/access_keys.rs:22-24`, `crates/server/src/management/assume_role.rs:293-295`, `crates/storage-mongodb/src/management_store.rs:2728-2745`). The third is the plaintext read of the raw key row in the admin settings routes (`crates/server/src/management/settings.rs:44-56`, `:76-82`).

#### 6.3 A later key process

Planned, not in 1.0. A local key process holds the DEK. The server passes it a stored ciphertext and its AAD, and receives the signing key for (access key, scope date). The server then never holds the DEK. With 6.1 that is one local call per access key per day. OpenSSH privilege separation and Vault Transit are the models ([README.privsep](https://raw.githubusercontent.com/openssh/openssh-portable/master/README.privsep), [Transit secrets engine](https://developer.hashicorp.com/vault/docs/secrets/transit)). The interface of 6.1 is designed for it: the cache asks one function for a signing key and never sees the DEK.

### 7. Startup

The problem: a server must find its key, and refuse a catalog whose key state does not match its configuration. Each refusal names the command that fixes it. With `key_file` set, no refusal may send the operator to a write command for a catalog key that appeared after a finished switch (section 8.5).

One function in `crates/storage` loads the key, beside the key check that #863 adds there. It reads the `encryption_key` row and the `encryption_key_ring` row through the existing `SettingsStore::get_setting` (`crates/storage/src/management_store/mod.rs:56`). Each backend factory calls it once, in place of its own key query (`crates/storage-sqlite/src/lib.rs:427-434` and the matching factories). It returns the one key that the server uses, or a reason to refuse. Checks run in table order, and whichever row matches first decides. With `key_file` set, it compares KEK ids first and unwraps every DEK second. Until all of them unwrap, it reads no other ring field. Without `key_file` the server has no KEK to check a ring with, so it names only the next step. That command checks the ring before writing (section 8).

| The catalog holds | `key_file` | Result |
|---|---|---|
| the catalog key, no ring | not set | Start with the catalog key, as today. The log line says that a copy of the catalog decrypts every stored secret. |
| neither row | not set | Refuse with `MissingEncryptionKey`, as today (`crates/storage/src/server_components.rs:74-77`). |
| a ring with `switch_from_catalog_key` true | not set | Refuse: the ring says that a switch did not finish. Name `switch --key-file <path> --yes`, which checks the ring and resumes the switch (section 8.1). If no switch of yours stopped, read section 11 first. |
| a ring with `switch_from_catalog_key` false | not set | Refuse. Name `key_file` and the KEK id. |
| any state | set, the file is missing or the file rule refuses it | Refuse. Name the path and the check that failed. |
| the catalog key, no ring | set | Refuse. In the documented order, `key_file` is set only after a finished switch, so this state means a deleted ring or a configuration set too early. Name `encryption-key status` and the restore of section 11. Name no write command. |
| neither row | set | Refuse. Name the missing `encryption_key_ring` row and the restore of section 11. |
| a ring whose KEK id is not the id of the configured KEK | set | Refuse, and print both KEK ids. After a `rewrap`, set `key_file` to the new file. Otherwise find the KEK that the ring names (section 11). |
| a ring in which any DEK fails to unwrap | set | Refuse: the ring has damage or a changed field. Name `encryption-key status` and section 11. Name no write command. |
| a ring with `switch_from_catalog_key` true | set | Refuse. In the documented order, the operator sets `key_file` only after a switch finishes. This state means that the operator set it while a stopped switch was unfinished. It can also mean a ring row written back from a catalog copy taken during the switch (section 8.5). Name `encryption-key status` and section 11. Name no write command. |
| a ring and a catalog key | set | Refuse: a catalog key appeared after the switch finished. Name `encryption-key status`, which lists the stored secrets under that key. Name no write command. |
| a ring with two DEKs | set | Refuse: a DEK replacement did not finish. Name `rotate`, whose preview lists the stored secrets under the older DEK, and `rotate --yes`, which resumes it (section 8.3). |
| a ring with one DEK, and no catalog key | set | Start with that DEK. |
| any state, and the locked key store cannot be locked | set | Refuse, unless `require_locked_memory = false`. Name the memlock limit needed, `LimitMEMLOCK=` or `--ulimit memlock=`, and `max_entries`. |
| the catalog key, and the locked key store cannot be locked | not set | Warn and start. |

- A damaged catalog key, KEK file or key ring also stops the server. The catalog key keeps `MissingEncryptionKey` and the `InvalidEncryptionKey` of #863. Each other refusal carries its text in the existing `InitializationFailed(String)` (`crates/storage/src/server_components.rs:55`), so the change adds no error variant.
- A KEK file that does not decode gets the reason that `cipher_key` gives for a damaged catalog key (`crates/storage/src/secret_cipher.rs:100-113`). For a bad character the decoder's text contains that character (`DecodeError::InvalidByte` of `base64` 0.22.1), which can be part of the key. The change drops the character from the reason and keeps the kind of error and its offset.
- The factory takes the key to load as a new argument: the catalog key, a KEK that the caller read, or none. `serve` passes it, so no code in `crates/storage` reads a file. On PostgreSQL the key loads inside the registration that runs first in the factory (`crates/storage-postgres/src/lib.rs:861-866`). That registration records the server, runs this function with the KEK on its target, and only then reads the catalog status (`crates/storage-postgres/src/migrations/registrar.rs:109-112`). One DEK serves the status read, the data pool and the stores, and this call gives the refusals of the table (section 12).
- The code that reads the `encryption_key` row when its store holds no key is deleted (section 6.2). A store built without the key refuses to create, import or store a credential. The `settings` and `migrate` commands and the migration bodies use such stores, and none of them writes a credential.
- The SQLite dev bootstrap runs at each start of an in-memory or dev-build server (`crates/storage-sqlite/src/lib.rs:292-301`). Today it writes the `encryption_key` row when that row is absent (`crates/storage-sqlite/src/store.rs:266-280`). This RFC adds the ring to that check: the bootstrap writes the row only when the catalog has neither the row nor a ring. Without this rule a dev build would write a new catalog key into a switched catalog at each start.
- An in-memory SQLite catalog refuses `key_file`. It is new at each start (`crates/storage-sqlite/src/lib.rs:278-290`), so no command can reach it from another process.
- `init` writes bootstrap rows only into a catalog that has no admin user (`crates/storage/src/bootstrapper.rs:128-147`, `:159-170`), so it needs no new rule.

### 8. The encryption-key commands

The problem: an operator needs to switch to a KEK file, change the KEK, replace the DEK, and recover from a lost KEK. The operator also needs to see the key state. One command for all of these is unclear. Each command below does one thing. Each previews by default and writes only with `--yes`. Each takes `--config extenddb.toml`.

| Command | What it does | Servers | Pass over stored secrets |
|---|---|---|---|
| `extenddb encryption-key status` | prints the key source, the KEK id, the DEK ids and the count of stored secrets per key. For each stored secret under a key other than the target it prints the access key ID, owner and creation time, never a secret | may run | reads each secret and wipes the plaintext at once |
| `extenddb encryption-key switch --key-file <path>` | switches from the catalog key to a KEK file | stopped | yes, offline |
| `extenddb encryption-key rewrap --key-file <new path>` | wraps the same DEK under a new KEK, and rewrites the ring row only | may run | no |
| `extenddb encryption-key rotate` | replaces the DEK | stopped | yes, offline |
| `extenddb encryption-key reset --key-file <path> --discard-secrets` | recovers a restored catalog whose KEK is lost (section 11) | stopped | deletes every stored secret |
| `extenddb encryption-key set-data-connection` | PostgreSQL only. Reads a connection string from standard input and writes the `data_database_connection_string` row encrypted under the DEK (section 12) | may run | no |

Common rules:

- Each command reads the KEK file through the file rule (section 2). It runs as the server's user, with the same delivery route as the server (section 3).
- Every command except `status` and `reset` checks the ring before it writes anything, in the order of section 7. For a ring under another KEK, the refusal prints both KEK ids. For a ring in which any DEK fails to unwrap, it names `encryption-key status` and section 11. `status` reports such a ring: it prints its fields, marked as not authenticated, and which DEK ids fail to unwrap. `reset` is for a lost KEK, which leaves rings in that state (section 8.6).
- Before its first write, `switch --yes` and `rotate --yes` open every `access_keys` and `iam_sessions` secret with whatever keys the run holds at that point. If a secret opens under none of them, the run stops and writes nothing, unless the operator passes `--keep-unopenable`. Its refusal prints how many such secrets exist and their access key IDs. Section 12 gives a matching check for the connection string row.
- Without `--yes`, a command previews: it prints what `--yes` would do and writes nothing. `status` and the previews decrypt each stored secret only to find its key. They wipe each plaintext at once. They print counts. For each stored secret under a key other than the target they print the access key ID, owner and creation time, never a secret. They follow the same user and host rules as `--yes`.
- An offline command (`switch`, `rotate`, `reset`) opens its stores as `migrate --yes` does, through the migration backend, and never through `create_server_components`. It takes the migration lock through `CatalogMigrations::lock` and frees it with `release` (`crates/app/src/cmd_migrate_runner.rs:301-318`). The lock waits for registered servers to stop. The command takes the `--lock-timeout` option of `migrate --yes`, waits up to that limit, and then exits 6 (`docs/adr/0023-catalog-migrations.md:246-248`). On Cassandra and MongoDB the lock is a lease, which the stores of the migration backend check before each request (`crates/storage-cassandra/src/migrations/guarded.rs:4-17`, `crates/storage-mongodb/src/migrations/backend.rs:6-10`).
- Unlike `migrate --yes` on SQLite, no command takes a copy of the database. A copy beside the database would hold the catalog key next to every secret, which is the exposure that the switch ends. The pass resumes after a stop, so it needs no copy.
- A command with nothing to do exits 0. It prints that the ring is already in the target state, with its KEK id and DEK id. Each command makes this check right after it reads its KEK file and unwraps the ring, before any other refusal. A second `switch` with the current KEK file therefore reports success and not an error. So does a `rewrap` to the file the ring already names.
- Every command that writes prints a summary at the end: the KEK id, the DEK id and the counts per table. On PostgreSQL it says whether the `data_database_connection_string` row was rewritten. No command prints the connection string or its password. The summary lists the access key IDs of the stored secrets that no key opens, and never a secret. It also prints the `key_file` line for the configuration, and this notice: "This does not revoke any credential. Each access key keeps its secret. A secret that leaked before this run works until its access key is re-created."

#### 8.1 `switch --key-file <path>`

The one-time change from the catalog key to a KEK file. The deployment runs with the catalog key before it and with `key_file` after it.

1. It reads the target KEK from `--key-file` through the file rule, and the ring if one exists. If that ring holds one DEK under this KEK, with the flag false and no catalog key row, there is nothing to do (common rules). In any other case it refuses a configuration that sets `key_file`, and no option overrides that. In the documented order, operators set `key_file` after the switch. An operator who set it first unsets it for the run. Next the run reads the catalog key from the `encryption_key` row. Refused: a KEK equal to the catalog key or to a DEK of the ring, migrations that are not current, and a missing `catalog_id` row. A ring with `switch_from_catalog_key` true means an earlier run stopped, and this run resumes (below). A ring with the flag false and a catalog key row is the case of section 8.5.
2. With `--yes`, it checks the stored secrets with the catalog key alone, as the common rules say. The run writes nothing before this check.
3. It writes the key ring: a new DEK wrapped under the KEK, `switch_from_catalog_key` true, and `retiring_catalog_key_id` (section 4). That id is the id of the catalog key it read. It writes one audit line with the DEK id and the KEK id, as `extenddb settings set` writes one (`crates/app/src/cmd_settings.rs:204-208`).
4. It runs the pass (8.4) with the catalog key as the old key and the DEK as the target. On PostgreSQL the pass also encrypts the connection string row (section 12).
5. It deletes the `encryption_key` row.
6. It writes the ring with `switch_from_catalog_key` false and `retiring_catalog_key_id` null.
7. It prints the summary.

Resume. A run that finds the flag true and a catalog key row accepts that row only when its id equals `retiring_catalog_key_id`. It checks stored secrets with both keys, then runs steps 4 to 7. With the flag true and no catalog key row, a run checks stored secrets with the DEK alone, then runs steps 6 and 7. That is the only state in which the command expects a missing catalog key row: the run stopped between steps 5 and 6. During an unfinished switch, a catalog key row whose id does not match appeared after the switch started. Such a row is refused, and no option accepts it. Its refusal names `encryption-key status` and section 11.

The operator then sets `key_file` and starts the server. A new deployment that wants a KEK file runs `init`, then `switch --key-file <path> --yes`, then `serve`. `init` creates no access key (`crates/storage/src/bootstrapper.rs:159-170`), so that deployment never has a stored secret under the catalog key.

#### 8.2 `rewrap --key-file <new path>`

The routine KEK change. It needs no outage and touches no stored secret. MongoDB's KMIP master key rotation and InnoDB's master key rotation rewrap the inner keys the same way (Prior art).

1. It opens the ring with the configured KEK and reads the new KEK from `--key-file` through the file rule. Refused: a new KEK that equals the current KEK, a DEK of the ring, or a catalog key row present in the catalog.
2. It writes the ring with the same DEK wrapped under the new KEK, and the new KEK id. It writes the ring only while the ring row still holds the text that step 1 read. A `rotate` that wrote the ring in between makes `rewrap` refuse, with nothing written.
3. It prints the summary, with the `key_file` line for the new file.

Running servers keep their DEK and read the ring only at startup. Until `key_file` names the new file, a restart refuses and prints both KEK ids (section 7), so operators change `key_file` right after the command. Whoever holds both an old KEK and a catalog copy from before its rewrap still holds the DEK. Only `rotate` ends that for a reader of catalog copies. A catalog writer can still roll the ring back (section 8.5).

#### 8.3 `rotate`

The DEK replacement. It rewrites every stored secret, so it runs while every server is stopped.

1. It opens the ring with the configured KEK, and refuses a ring whose `switch_from_catalog_key` is true: a switch finishes first. It refuses a catalog key row unless the operator passes `--accept-catalog-key` (section 8.5). Two DEKs mean that an earlier run stopped, and this run resumes with the highest id as its target. With `--yes`, it checks stored secrets with each DEK of the ring, as the common rules say. A fresh run then adds a DEK with the next id and writes a ring holding both. This write happens only while the ring row holds the ring that the command read, as in `rewrap` (section 8.2). The write of step 3 happens only while the row holds the two-DEK ring of step 1, or on a resumed run the ring that the run read.
2. It runs the pass (8.4) with the older DEK as the old key and the DEK of the highest id as the target.
3. It writes the ring with the target DEK alone.
4. It prints the summary.

The ring holds two DEKs only between steps 1 and 3. A DEK replacement does not protect a secret whose ciphertext and key leaked together: re-create that access key.

#### 8.4 The pass

The problem that the pass solves: after an interrupted run, each stored secret can be under an old key or under the target. No stored byte records which. Trial decryption finds each secret's key: target first, then each old key in turn. A wrong key fails AES-GCM authentication with the same error as changed bytes (`crates/storage/src/secret_cipher.rs:54-62`, `:83`). A secret that opens under the target is done. The pass re-encrypts a secret that opens under an old key. Secrets that open under neither stop the run before its first write (common rules). With `--keep-unopenable` they stay as they are, and the summary lists their access key IDs. Such secrets fail today on every request, and fail the same way after the run. A `rotate` with `--accept-catalog-key` has two old keys, the older DEK and the catalog key (section 8.5).

A benchmark on the shared 16-CPU host, at a load average near 12, measured the median of 7 runs of 200,000 calls. `decrypt_secret` took 389 ns with either key, and `encrypt_secret` 745 ns. A first pass spends about 1.5 µs of cipher time per secret, and a resumed pass 0.4 µs per secret that is done.

The steps:

1. It opens each `access_keys` row and each `iam_sessions` row by trial, expired sessions included. On PostgreSQL it also opens the `data_database_connection_string` row, by the rules of section 12. The pass does not skip expired sessions, so the command host's clock does not matter. Step 3 reads again each row that this step read. The cost: SQLite, PostgreSQL and Cassandra never delete expired sessions (#971), so the pass rewrites those rows too.
2. It re-encrypts under the target each secret that opens under an old key, with the AAD of section 5. It writes in batches of rows found by their primary keys. No other process writes these rows during the pass. Only a server writes a credential, the import CLI posts to a server (`crates/app/src/manage_http.rs:311-319`), and the lock keeps every server out.
3. It reads every row of step 1 again, and stops with every key kept if a secret still opens under an old key. A row that is gone passes: on MongoDB the time-to-live (TTL) index deletes expired sessions during the pass. It also reads the KEK file again, and stops if the file is gone or its id is not the ring's. This step guards against a write that reports success and changes nothing. A store method that matches no row reports success, and so does a Cassandra update whose write timestamp is not above the row's (below). Without this step, such a secret would be lost when the old key goes.

A run that stops at any step runs again with the same arguments. Until a run finishes, the server refuses to start (section 7).

The pass is one driver in `crates/storage`, as the pass of migration 2 is (`crates/storage/src/policy_migration.rs`, `crates/storage/src/migrations/m0002_resource_tag_condition_keys.rs:13-18`). Each backend adds three `ManagementStore` methods. These are a paged read of the `access_keys` and `iam_sessions` rows, the write of a batch of ciphertexts, and the delete of one settings row. No store has the delete today. The ring row and, on PostgreSQL, the connection string row use the existing `SettingsStore::get_setting` and `set_setting` (`crates/storage/src/management_store/mod.rs:56`, `:59`), except in `set-data-connection` (section 8.7). The conditional ring writes of `rewrap`, `rotate` and `reset` use a new `SettingsStore::replace_setting`, which writes a row only while it holds a given value.

Per backend:

- SQLite: the migration lock is the `flock` that `serve` also holds (`docs/adr/0023-catalog-migrations.md:216`). Each batch is one transaction of single-row updates. The command sets `PRAGMA secure_delete = ON` before the first write. `secure_delete` zeroes only the cells that it frees, and rows that moved between pages before the command leave old copies in free pages. `switch`, `rotate` and `reset` therefore end with `VACUUM` under `secure_delete` and `temp_store = MEMORY`, then `PRAGMA wal_checkpoint(TRUNCATE)`, so a file copy taken after the run holds neither the deleted catalog key nor a replaced ciphertext. The admin guide copies the SQLite files for a backup (`docs/manuals/05-admin-guide.md:1108`), and no code sets `secure_delete` today.
- PostgreSQL: each batch is one `UPDATE ... FROM (VALUES ...)` statement by primary key. Behind PgBouncer, or for up to 30 s after a failover of the catalog's server, a server can serve beside the holder of the lock (`docs/adr/0023-catalog-migrations.md:231-244`). The operator therefore stops every server first.
- MongoDB: each row is one `update_one` on the document `_id`, with no upsert, because nothing makes `access_key_id` unique in `access_keys` (`crates/storage-mongodb/src/migrations/m0001_baseline.rs:102`). A TTL index deletes expired sessions during the pass (`:106`), and an update with no upsert leaves a deleted document deleted. The writes use the existing majority write concern (`crates/storage-mongodb/src/lib.rs:569`).
- Cassandra: each row is one `UPDATE ... WHERE <primary key> = ?`, with no lightweight transaction, because nothing deletes a row during the pass and session rows have no TTL (`crates/storage-cassandra/src/management_store/access_keys.rs:240-246`). Today the server reads credentials and settings at `ONE`, the driver default (`crates/storage-cassandra/src/credential_store.rs:104-107`, `crates/storage-cassandra/src/catalog_store.rs:290-313`), and #770 makes the server name its consistency on each statement. The pass names `ALL` on every statement it sends, and every write of the ring runs at `ALL` too, so every replica holds the new ciphertexts and the new ring before the old key goes. The commands read the ring, the catalog key and `catalog_id` at `ALL`. The server's startup read stays at `ONE`: a ring write that returned at `ALL` is on every replica. The conditional ring write is a lightweight transaction committed at `ALL`. A conditional update takes no client timestamp, so the command reads the row back at `ALL` and stops when it does not hold the new ring. Each paged read also selects `WRITETIME(secret_key_encrypted)`, and each update sets `USING TIMESTAMP` above that value. CreateAccessKey and ImportAccessKey write with `INSERT ... IF NOT EXISTS` (`crates/storage-cassandra/src/management_store/access_keys.rs:56`, `:202`), and a Paxos timestamp can be ahead of any later plain update's clock. Cassandra acknowledges an update whose write timestamp is not above a row's, and keeps its old value. Without an explicit timestamp, step 3 would stop on each rerun until clocks passed that old timestamp. The command stops when a replica of the catalog keyspace is down (section 15, PR 3).

A proxy measurement of the database work ran on the same host. A Python script read 100,000 rows in pages of 1,000 and wrote each page back with random bytes in place of the cipher. A local PostgreSQL 16 container took 3.85 s and a SQLite file 1.44 s, reads included. These are proxy figures: no cipher, a local database, and a conditional write that this design drops. Step 3 adds one more read of every row, and the cipher adds about 0.15 s per 100,000 secrets. MongoDB, Cassandra and a PostgreSQL server across a network were not measured.

#### 8.5 A catalog key that appears after the switch

The problem: a catalog writer can insert a catalog key of its own choice. It can also insert an access key row for an admin principal, encrypted under that key. Running servers hold only the DEK, so the forged key fails with HTTP 500. Suppose the next server start told the operator to run a command that treats any catalog key as an old key. That command would re-encrypt the forged secret under the DEK and delete the evidence. The forged key would then be a valid credential.

The rules:

- A catalog key row is an old key only while `switch_from_catalog_key` is true and the row's id equals `retiring_catalog_key_id`. Both fields are inside the AAD of every wrapped DEK (section 4), so a writer who changes either one makes the DEK fail to unwrap. The server and every command then refuse with the damaged-ring message, which names no write command (section 7). A writer cannot make a key of its own choice into an old key. During an unfinished switch, no option makes a catalog key whose id does not match into an old key.
- After a finished switch, a catalog key row has three possible writers. These are an older release, an older dev image (section 10), and a catalog writer. The server refuses to start, says so, and names `encryption-key status`. `status` lists each stored secret under that key with its access key ID, its owner and its creation time, and never a secret. The refusal names no write command.
- `switch` and `rotate` refuse such a key unless the operator passes `--accept-catalog-key`, after reading that list. The option applies only to a ring whose switch finished. The older dev image case of section 10 is the documented reason to pass it. With the option, the run treats the catalog key as an old key, re-encrypts the listed secrets under the ring's DEK, and deletes the row. `switch` then writes no new DEK: it checks stored secrets and runs steps 4, 5 and 7 of 8.1. `--accept-catalog-key` accepts the listed rows as written. It is not a check of them. A writer who inserted the key chose every field of those rows. A writer who holds a catalog copy from before the switch can give them old IDs, owners and creation times.

These rules do not cover a catalog writer who can also read the catalog during an unfinished switch. That writer holds the catalog key and can forge rows under it, which is the exposure every deployment has before its switch. A catalog copy keeps that state: the ring row of an unfinished switch stays authentic while its KEK and its DEK are current. A writer with such a copy can write that ring back, with its catalog key and rows under that key. With `key_file` set, the server then names no write command (section 7). An operator who resumes a switch that they did not start re-encrypts those rows under the DEK. A ring row from a copy taken during a `rotate` holds two authentic DEKs, and its write-back gets the refusal that names `rotate`. A resumed `rotate` then re-encrypts any rows that a holder of the older DEK forged under it. A one-DEK ring row from a copy taken before the last `rotate` is authentic too while its KEK is current. Written back alone, it makes every stored secret fail, because `rotate` re-encrypted them all. Written back with the copy's credential rows, it returns the credentials to the copy, access keys deleted since then included. The server starts with it and prints no message, on PostgreSQL once the copy's connection string row is written back too. A writer who holds the older DEK can then add rows under it. No AAD content detects the write-back of a state that was once authentic. The commands read no state outside the catalog other than their configuration. Section 11 says what to do. A catalog writer stays out of scope (section 1). These rules stop a catalog key that a writer chose from becoming valid through the commands that the server's messages name.

#### 8.6 `reset --key-file <path> --discard-secrets`

Recovery when a restored catalog's KEK is lost (section 11). Every server must be stopped. Every user, role, policy, group, account and table stays. Every access key and every session goes. Without `--discard-secrets` the command refuses, so the preview and the flag name both say what is lost.

`reset` is the one command that writes beside a ring that fails to unwrap, because a lost KEK leaves rings in that state. Two cases are refused. The first is a catalog with a catalog key row and no ring. With `key_file` unset, that catalog uses the catalog key, and the refusal names `switch`. With `key_file` set, it names `encryption-key status` and section 11, as section 7 does. A catalog writer can add a catalog key row beside a deleted ring. The second is a ring whose KEK id equals the given KEK's id while a DEK fails to unwrap. That ring has damage. Its KEK is not lost, because the given KEK has its id. Section 11 gives its restore. When every DEK unwraps under the given KEK, the KEK is not lost: `reset` writes nothing and exits 0. A catalog with neither a ring nor a catalog key row lost its DEK with the ring. `reset` runs on it as for a lost KEK.

The steps:

1. On PostgreSQL it reads the data database connection string on standard input, by the rules of section 8.7. It opens its stores and reads the catalog status with that string in place of the row. No key of the run opens the row (section 12).
2. It deletes every `access_keys` row and every `iam_sessions` row in batches, and the `encryption_key` row if one exists. It tries no key on them. A catalog key row is never a key for `reset`, because a catalog writer can put one there.
3. On PostgreSQL it writes the `data_database_connection_string` row, encrypted under a new DEK.
4. It writes a new ring: that DEK under the given KEK, `switch_from_catalog_key` false and `retiring_catalog_key_id` null. The DEK takes the id after the highest id that the old ring lists, or 1 when no ring exists. When a ring exists, the write happens only while the ring row still holds the ring that the run read, so a `rewrap` in between stops the run before this write.
5. It prints the summary, with the deleted access key IDs and whether it deleted an `encryption_key` row.

The old ring stays until step 4, which is one row write. A run that stops earlier leaves the old ring. A rerun with the same arguments and the same standard input then starts again at step 1.

#### 8.7 `set-data-connection`

PostgreSQL only. The command takes a connection string on standard input, encrypts it under the DEK with section 12's AAD, and writes the `data_database_connection_string` row. For catalogs with a ring, this replaces the `psql UPDATE` that the admin guide and troubleshooting guide give for a restore to another server (`docs/manuals/05-admin-guide.md:1137`, `docs/troubleshooting.md:45`). Catalogs without a ring keep their plaintext row and that `psql UPDATE`. The settings API keeps the row read-only (`crates/server/src/management/ops_settings.rs:52-56`).

- Refused: a catalog without a ring, a ring with `switch_from_catalog_key` true, and a ring with two DEKs.
- It parses the string as a PostgreSQL connection string before it writes, and refuses one that does not parse.
- Through a PostgreSQL method of its own, it reads the ring with `SELECT ... FOR SHARE` and writes the row in the same transaction. `SettingsStore` has no transaction (`crates/storage/src/management_store/mod.rs:53-67`). A ring write of `rotate` then waits until the command commits, or the command sees the ring with two DEKs and refuses.
- Servers read this row only at startup, so the command may run beside a running server. That server keeps its data pool until it restarts (`crates/storage-postgres/src/lib.rs:390-413`), so a wrong string shows at the next start. `verify` tests the stored string (`crates/app/src/cmd_verify.rs:97-103`).
- Standard input, here and for `reset`: the command reads it to the end and strips one trailing newline, as the KEK file allows one. It refuses empty input. It also refuses a terminal as standard input, so it never waits with no prompt. A preview of `set-data-connection` reads no standard input. `reset` reads it in its preview too, because it opens its stores with the string. To keep the password out of the shell history, write the string to a file under `umask 077`, run `extenddb encryption-key set-data-connection --yes < conn.txt`, and delete the file after the run.

### 9. Several servers

In 1.0, ExtendDB supports one server per catalog (`README.md`, "What ExtendDB is not"). Only SQLite enforces the limit (`docs/manuals/11-deployment-guide.md`, "Multi-Instance Considerations"). The offline commands assume that one server. They run while no server is registered, and the one server loads the new key at its next start. 1.0 does not support more than one server on a catalog. The commands add nothing for it: no reload signal and no poll of the catalog. `rewrap` and `set-data-connection` run beside the running server, because the server reads the ring and the connection string row only at startup.

### 10. Upgrade and rollback

The problem: an upgrade must change nothing for a deployment that keeps the catalog key, and a rollback must be possible after the switch.

- The change adds no catalog migration. It changes no stored row of a deployment that does not switch to a KEK file. Such a deployment needs no `migrate --yes` run for this change, and the change adds nothing to its catalog that an older release refuses.
- After the switch, the catalog has no `encryption_key` row. An older release then refuses to start with `MissingEncryptionKey` (`crates/storage/src/server_components.rs:74-77`). To run an older release again, restore a catalog copy taken before the switch. That copy holds the catalog key, and every catalog write since the copy is lost. On Cassandra the upgrade manual has no tested rollback (`docs/manuals/07-upgrade-manual.md:240`).
- A SQLite dev image of an older release writes a new `encryption_key` row into a switched catalog at startup, and serves with that key. Every request signed with a credential under the DEK then fails with HTTP 500. A server with this change refuses to start on that catalog, and `switch --key-file <path> --accept-catalog-key --yes` re-encrypts those secrets under the DEK (sections 7 and 8.5). It runs with a configuration that does not set `key_file` (section 8.1).
- A rollback also deletes `[auth.encryption]` and any `EXTENDDB__AUTH__ENCRYPTION__KEY_FILE` override from the configuration. The previous release refuses an unknown field in `[auth]` (`crates/config/src/lib.rs:275-287`), as the upgrade manual says for `[backup]` (`docs/manuals/07-upgrade-manual.md:85`).
- Section 6 changes no stored byte, so it needs no rollback step.

### 11. Backups and restores

The problem: an operator who restores a catalog copy must know which key it needs, and must have a path when that key is lost.

The catalog and the table data are already separate. Table backups (`CreateBackup`) hold one table's items and definition, and no credentials (`docs/manuals/05-admin-guide.md:1103`). Restoring table data therefore never needs a KEK, and a table restores into a deployment with any key state. Only a catalog copy holds stored secrets.

- A catalog copy taken after the switch holds the key ring and its KEK id. A restore needs the KEK that the id names. The server refuses a catalog whose ring names another KEK, and prints both ids (section 7).
- Keep each KEK for as long as a catalog copy wrapped under it is kept. `rewrap` and `rotate` change no older copy. MySQL's keyring, Kubernetes encryption at rest and DataStax Enterprise document the same rule ([InnoDB data-at-rest encryption](https://dev.mysql.com/doc/refman/8.4/en/innodb-data-encryption.html), [Encrypting Confidential Data at Rest](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/), [DSE rekey](https://docs.datastax.com/eol/en/security/6.7/security/secRekeySystem.html)).
- A catalog copy taken before the switch holds the catalog key. It needs a configuration without `key_file`.
- A physical copy of a PostgreSQL, MongoDB or Cassandra catalog taken after the switch can still hold the deleted `encryption_key` row until its pages are overwritten. Examples: a write-ahead log archive, an SSTable before compaction, an oplog entry. With it, the copy opens each secret that existed before the switch. To end that exposure, re-create the access keys that existed before the switch. Sessions expire within 12 hours. On SQLite the command overwrites the row (section 8.4).
- A restore into another deployment needs the KEK file on the new host and the same `[auth.encryption]` section. On PostgreSQL, when the new server's connection string differs, `set-data-connection` writes the new string (section 8.7).
- If the KEK of a copy is lost, run `reset --key-file <new> --discard-secrets` (section 8.6). It keeps every user, role, policy and table, deletes every access key and session, and writes a new ring. The operator then re-creates the access keys, or imports them under their old IDs from the secrets that their clients hold (`docs/manuals/14-backup-and-restore.md:549-616`). Clients re-create sessions with AssumeRole.
- A ring that fails to unwrap under the KEK whose id it names has damage or a changed field (section 7). A ring missing beside a configuration that sets `key_file` was deleted. In both cases, restore a catalog copy taken before the change, and check it with `status`. `reset` refuses a damaged ring, because the KEK is not lost.
- A ring can say that a switch or a DEK replacement did not finish when no run of yours stopped. A catalog writer then wrote that ring row back from a copy (section 8.5). Do not resume it, because the resumed run would re-encrypt the rows that the writer chose. Restore a catalog copy taken after your last run finished, and check it with `status`. When a run of yours stopped, resume it. For a switch, unset `key_file` for the run (section 8.1). A ring under an older KEK can also be a ring row written back from a copy. So can a ring whose DEK id is lower than the one that the last command to write the ring printed. `status` prints both ids.
- On SQLite the KEK file and the database file can be on one disk, so one snapshot holds both. The server warns at startup when the two files are on the same file system (same `st_dev`).

### 12. The PostgreSQL connection string

The problem: on PostgreSQL the catalog holds `data_database_connection_string`, with the password of the application role (`crates/storage-postgres/src/bootstrapper.rs:96-106`, written at `:200-212`). That password opens the catalog database and the data database (`docs/manuals/10-security-model.md:22`). A catalog copy gives it, with or without the switch.

After the switch, the DEK encrypts the row. The AAD is `extenddb-setting-v1|<catalog_id>|data_database_connection_string`, in the field encoding of section 4. The alternative that stores the row without its password is in Alternatives. The rules for the row:

- Before the switch the row holds the plaintext string, as `init` and migration 1 write it (`crates/storage-postgres/src/bootstrapper.rs:200-212`, `crates/storage-postgres/src/migrations/backend.rs:347-360`). During a switch, a row that does not open under the target is in that plaintext form. The pass parses it as a connection string, encrypts it under the target and writes it. Step 3 of the pass checks that it opens (section 8.4). Before its first write, `switch` or `rotate` checks the row with the stored secrets (section 8). The run stops when the row does not open, unless a switch finds it in the plaintext form. `--keep-unopenable` does not apply to this row.
- Every other reader, and the pass of `rotate`, refuses a row that does not open under a DEK of the ring, plaintext included. A catalog writer therefore cannot put in a string of its own.
- With a ring, a missing row, a failed read or a row that does not open is a refusal. The engine falls back to the catalog pool when the row is missing or empty (`crates/storage-postgres/src/lib.rs:390-413`). No factory caller reaches that fallback, because the registration that each one runs first refuses such a catalog (`crates/storage-postgres/src/lib.rs:864`, `crates/storage-postgres/src/migrations/sessions.rs:246-257`). The change leaves the fallback as it is.

The cost. Every reader of the row needs the DEK after the switch. On PostgreSQL that includes the migration backend. `PostgresMigrations::open_with` parses the row as a URL to find the data database and checks its name (`crates/storage-postgres/src/migrations/backend.rs:206`, `crates/storage-postgres/src/migrations/sessions.rs:197-222`). The catalog status opens that backend, and `migrate` and the registration of every factory caller read the status first (`crates/storage-postgres/src/migrations/mod.rs:314-324`, `crates/storage-postgres/src/migrations/registrar.rs:112`). With a ring, these paths decrypt the row with the DEK before they parse it, and the target carries the KEK that its caller read. The backend that `switch` opens accepts the plaintext form while the ring's flag is true. Every other caller of the backend refuses it. Four commands read this row today without a key and would need the KEK file: `migrate`, with or without `--yes` (`crates/storage-postgres/src/migrations/sessions.rs:233-237`), `catalog-check` (`crates/app/src/cmd_catalog_check.rs:72-84`), `verify` (`crates/app/src/cmd_verify.rs:97-103`) and the `extenddb backup` commands. The backup commands open the catalog through the same factory as `serve` (`crates/app/src/cmd_backup.rs:649-672`). `serve`'s registration unwraps the DEK before it reads the catalog status (section 7), and that read comes before the data pool (`crates/storage-postgres/src/lib.rs:390-413`). `switch`, `rotate` and `reset` open the migration backend with the KEK they read. The admin guide and the troubleshooting guide tell the operator to edit the row with `psql UPDATE` after a restore to another server (`docs/manuals/05-admin-guide.md:1137`, `docs/troubleshooting.md:45`). An encrypted row needs a command that writes it: `extenddb encryption-key set-data-connection` (section 8.7). `reset` takes the string on standard input for the same reason (section 8.6).

`init` builds both connection strings from the one application role and its password (`bootstrapper.rs:80-100`, `:114-121`, `:200-212`, `docs/manuals/05-admin-guide.md:1137`). The stored row and the configured `[storage.postgres] connection_string` therefore name the same role today.

MongoDB stores its data connection string with the password redacted, and Cassandra stores none. Neither needs this change.

### 13. What operators do differently

- Create the KEK file under `umask 077`, readable only by the server's user, or deliver it by a route of section 3. Copy it to a place away from the host before `switch --yes` or `rewrap --yes`. From the ring write on, the ring opens only with it.
- Switch: run `encryption-key switch --key-file <path>` to preview. Stop every server, run it with `--yes` as the server's user, set `key_file`, and start the server. Then re-create the access keys that existed before the switch, to end the exposure of older catalog copies (Drawbacks).
- Change the KEK: run `encryption-key rewrap --key-file <new path> --yes` beside the running server, then set `key_file` to the new file.
- Replace the DEK: stop every server, run `encryption-key rotate --yes`, start the server.
- Give the KEK file to `serve`, and on PostgreSQL to `migrate`, `catalog-check`, `verify` and the `backup` commands (section 12).
- On PostgreSQL, after a restore to a server with another connection string, run `encryption-key set-data-connection` with the new string on standard input (section 8.7). It replaces the `psql UPDATE` of the admin guide. Give `reset` the string on standard input.
- Set `LimitMEMLOCK=` in the unit, or `--ulimit memlock=` for a container, on hosts whose kernel is older than 5.16 or whose limit is lowered (section 6).
- Keep each old KEK while a catalog copy wrapped under it is kept (section 11).
- After `init --overwrite`, put `[auth.encryption]` back. That command keeps only the storage settings (`docs/manuals/05-admin-guide.md:177`). Delete `[auth.encryption]` before a rollback (section 10).
- Keep catalog dumps under `umask 077`. A dump taken before the switch holds the catalog key (`docs/manuals/07-upgrade-manual.md:111`).
- A switch or rotation that did not finish stops the server at its next start. Section 7 gives the message for each state. The change adds no metric.

### 14. Docs to change

Line numbers are on main at 95425f5e8.

- Statements that this change makes false:
  - `docs/manuals/10-security-model.md:22`, `:23`, `:29` and `:62-70`
  - `docs/manuals/02-design-guide.md`, and `docs/design/08-component-config.md`, whose environment variable table needs `auth.encryption.key_file` and `require_locked_memory`
  - `docs/manuals/05-admin-guide.md:336-339` and `:1112`
  - `docs/manuals/07-upgrade-manual.md:111` and `docs/manuals/14-backup-and-restore.md:34-41`
  - `docs/design/04-component-storage.md`, `docs/design/05-component-auth-v3.md`, `docs/design/06-component-server.md` and `docs/design/13-storage-mongodb.md`
  - `docs/adr/0026-credential-crypto-on-aws-lc.md:85-86`
- The credential cache description in `docs/manuals/10-security-model.md` (Credential and Policy Caching): it caches signing keys and token hashes, not secrets.
- Code comments: `crates/storage/src/secret_cipher.rs:7-9`, `crates/storage-postgres/src/catalog_store.rs:24-25`, `crates/storage-cassandra/src/catalog_store.rs:46-47`.
- New text:
  - the threat model of section 1, in the security model
  - the rollback step beside `docs/manuals/07-upgrade-manual.md:85`
  - the restore needs and `reset` of section 11, beside `docs/manuals/14-backup-and-restore.md:549-616`
  - the KEK file in the air-gapped transfer list (`docs/manuals/11-deployment-guide.md:150-154`) and in the Kubernetes mount
  - the `systemd-creds` command, the two unit lines and `LimitMEMLOCK=`, beside the unit file (`:253-285`)
  - the container steps (`docker/README.md`) with `uid`, `gid`, `mode` and `--ulimit memlock`
  - the command run for each route of section 3, and the command reference in the admin guide
  - a troubleshooting entry for each refusal of section 7
  - `set-data-connection` for a catalog with a ring, beside the `psql UPDATE` of the connection string row (`docs/manuals/05-admin-guide.md:1137`, `docs/troubleshooting.md:45`). That `psql UPDATE` stays right for a catalog without a ring. With a ring, the row in a dump no longer gives the password.
- #972 covers the false statements that this change does not touch. #863 changes `docs/manuals/10-security-model.md:67`.

### 15. Implementation order

Three pull requests, each with its own docs, ship in one release:

1. The locked key store, one key holder per backend with the unwiped copies deleted, and the signing-key cache (section 6). This helps catalog key deployments too. No stored format changes. Crates: `auth`, `storage`, the four backend crates, `server` and `app`. Estimated from the design before the code exists: about 500 to 600 production lines and about 250 test lines.
2. `key_file`, the file rule, the key ring and the startup function of section 7. The commands `status`, `switch`, `rewrap`, `rotate`, `reset` and `set-data-connection` on SQLite and PostgreSQL, with the rules of 8.5, the AAD of section 5 and the encrypted connection string of section 12. Crates: `config`, `storage`, `storage-sqlite`, `storage-postgres` and `app`. Estimated: about 800 to 1,000 production lines and about 350 test lines.
3. The store methods of MongoDB and Cassandra, and two `cassandra_util` functions that take the consistency as an argument. `query_rows_quorum` and `execute_quorum` become wrappers of them (`crates/storage-cassandra/src/cassandra_util.rs:121-135`, `:227-248`), and their 72 callers do not change. Crates: `storage-mongodb` and `storage-cassandra`. Estimated: about 200 to 300 production lines and about 100 test lines.

Tests: one check in `crates/storage` holds the command and startup cases as one table, and each backend's tests run it with their own store. `metrics_conformance` is the precedent (`crates/storage/src/metrics_conformance.rs:44`). The cases:

- a secret under the catalog key, a secret under the old DEK, and a secret that no key opens
- a write that changed nothing
- a catalog key written beside a finished ring, with and without `--accept-catalog-key`
- a ring with a hand-edited `switch_from_catalog_key` or `retiring_catalog_key_id`, and a ring with a wrapped DEK copied from another ring
- a ring with a DEK entry added or deleted, and a two-DEK ring with its entries in either order
- the ring row of an unfinished switch written back after the switch finished, with `key_file` set
- a `reset` stopped before its ring write, then run again
- on PostgreSQL, a plaintext connection string row during a switch, and after it
- on Cassandra, a row whose write timestamp is ahead of the command's clock
- a switch stopped between the delete of the catalog key and the final ring write, then resumed
- an expired session, re-encrypted like any other
- a KEK equal to the catalog key
- a run resumed after a stop, and a rerun with nothing to do
- each refusal of section 7

On SQLite, a test reads the database files after a run and finds neither the catalog key nor an old ciphertext. The file rule has a test for a root-owned file with a named-user ACL entry. It has one for a group-readable file whose group is not the server's, and one for a group-readable file that the server's user owns. The locked key store has a test for each kernel case: no `memfd_secret`, `memfd_secret` disabled, and a memlock limit below the store.

Size, estimated from the design before the code exists: about 1,500 to 1,900 production lines and about 700 test lines in total, split as above. Each pull request reports its counts.

## Libraries considered

aws-lc-rs 1.18.2 already provides the credential cipher (`docs/adr/0026-credential-crypto-on-aws-lc.md`). It also provides the wrap and unwrap, the KEK id (HMAC-SHA256) and the new DEK (`helpers::generate_encryption_key`). The locked region is about a hundred lines over `libc`, a workspace dependency already (`Cargo.toml:184`). Two surveys, on 2026-11-12 and 2026-11-14, checked each crate below. The checks were the licence gate (`about.toml:6-17`), the minimum supported Rust version (MSRV) of 1.88 (`Cargo.toml:30`), and the offline requirement.

| Crate | What it offers | Why this RFC does not use it |
|---|---|---|
| `tink-core` and `tink-aead` 0.3.0 | Key sets with rotation. | No Federal Information Processing Standards (FIPS) module, 21 added crates, and the 8 crates that ADR-0026 dropped. Last release 2024-11-28. |
| `kms-aead` 0.32.0 | Envelope encryption under a versioned local KEK. | MSRV 1.99, one maintainer, 34,752 lines. It checks a key file's length, not its mode or owner. |
| `aws-esdk` 1.3.0 | The raw AES keyring of the AWS Encryption SDK. | It links the AWS Key Management Service (AWS KMS) and DynamoDB clients and a second hyper and rustls stack. |
| `envelopers`, `ironcore-alloy`, `aws-mpl-legacy` | Envelope encryption. | They fail the licence gate. |
| `age` 0.12.1, `cocoon` 0.5.0, `keyring` 4.2.0 | A passphrase or identity file format, and operating-system key stores. | No key ring. This RFC rejects a passphrase source, and the operating-system stores need D-Bus or are lost at reboot. |
| `cryptoki` 0.12.1, `tss-esapi` 7.7.0 | PKCS#11 and TPM 2.0 access. | A later key source can use one (Alternatives). |
| `memsec` 0.7.0 | `mlock`, `MADV_DONTDUMP`, `memfd_secret`, guard pages. | Last release June 2024, no declared MSRV, it ignores the `madvise` result, and it adds a fourth `windows-sys` version. |
| `secrets` 1.3.0 | libsodium's guarded allocations. | libsodium becomes a native dependency of every image and npm binary, and `RLIMIT_CORE` goes to 0 by default. |
| `region` 4.0.1, `secrecy` 0.10.3 | `mlock` guards, and a zeroizing wrapper. | Neither covers `MADV_DONTDUMP` or `memfd_secret`. |
| `secmem-proc` 0.3.9 | `PR_SET_DUMPABLE`, `RLIMIT_CORE` 0, `PT_DENY_ATTACH`. | This RFC keeps the process dumpable (section 6). |

## Answers to the questions on #707

| Question on #707 | Answer |
|---|---|
| Which providers come first, where configured? | A KEK file through `key_file` (section 2). No plugin interface. |
| Provider unreachable at startup? | Refuse, name the path and the check (section 7). |
| Unwrapped key kept in memory? | The DEK, in the locked key store (section 6). The server wipes the KEK. |
| How does a deployment switch? | `switch --key-file <path> --yes` (section 8.1). |
| Secrets written during a rotation? | None: no server runs, and `rewrap` writes no secret (section 8). |
| Backup and restore? | Section 11. |
| Record the key on each secret? | No. Trial decryption (section 8.4). |
| How do servers learn of a new key? | By a restart (section 8.2). |
| Flush the caches? | A restart empties them. A rotation changes no secret, so cached signing keys stay valid (section 6.1). |

## Drawbacks

- `switch` and `rotate` stop every server for the length of the pass. The proxy measurement of section 8.4 rewrote 100,000 rows in 3.85 s on PostgreSQL 16 and 1.44 s on SQLite. It ran with no cipher and a local database. MongoDB and Cassandra were not measured.
- On Cassandra a pass needs every replica of the catalog keyspace up.
- On PostgreSQL, four commands that need no key today need the KEK file after the switch (section 12). A restore to another server needs `set-data-connection` in place of a `psql UPDATE`.
- A lost KEK loses every stored secret of each catalog copy wrapped under it. `reset` recovers everything else (section 11).
- Hosts whose memlock limit is below the locked key store need a limit change, a smaller cache, or `require_locked_memory = false` (section 6).
- SQLite, PostgreSQL and Cassandra never delete expired sessions (#971), so the pass reads and rewrites every expired session row too.
- After the switch, an older release runs only from a catalog copy taken before the switch (section 10).
- The switch does not protect the secrets that older catalog copies hold. A copy taken before the switch holds the catalog key and each secret that existed then. A physical copy taken after the switch can hold the deleted row (section 11). To end that exposure, re-create the access keys that existed before the switch.
- Every catalog still holds the bcrypt hashes of console passwords and the session tokens of `iam_sessions` in plaintext.
- The project owns about a hundred lines of `unsafe` code for the locked key store, with a branch per platform.

## Alternatives

- An online DEK replacement, inside the server. Every surveyed system that rewrites data does it online, because its readers accept both keys during the pass. Kerberos, Vault Transit, Kubernetes KMS v2 and Grafana do (Prior art). The ring already holds two DEKs during a replacement, so the stored format does not block it. The costs in ExtendDB:
  - Each server holds both DEKs during the pass and tries both on each decrypt.
  - On Cassandra an `UPDATE` is an upsert. A pass write after a concurrent DeleteAccessKey recreates a partial row, so each pass write would need a lightweight transaction (`IF EXISTS`) and its Paxos round. SQL `UPDATE` by key and MongoDB `update_one` without upsert have no such case.
  - The old DEK can go only after the server loads the ring with the new DEK. That needs a handshake through the catalog, where the command waits until the server records the ring it loaded. The other way is a pass inside the server with a resume at startup.
  - A concurrency test on four backends.
  Recommendation: in 1.0 the DEK replacement is offline. The routine KEK change (`rewrap`) is online. The offline pass took 3.85 s per 100,000 rows on PostgreSQL in the proxy measurement (no cipher, local database). A later RFC can add an online pass on the two-DEK ring.
- The PostgreSQL connection string without its password (option B). The stored row would carry no password, as MongoDB's does (`crates/storage-mongodb/src/bootstrapper.rs:177-187`). The server would take the password from the configured `[storage.postgres] connection_string`, which names the same application role. No command needs the KEK for it, and the catalog holds no password at all. The migration backend keeps parsing the row as it does today, with the password from the configured string. The row stays unauthenticated. A catalog writer can then point the data pool at a server of its choice, and the server logs in there with the configured password. It holds while the stored row and the configured string name one role, which `init` does today (section 12). This RFC encrypts the row under the DEK instead. The data database credentials then do not depend on the configured catalog string naming the same role.
- One key from the file, and no DEK. Every KEK change would then rewrite every stored secret and need a new key file on every host (section 4).
- A key id on each stored secret, in a new column or as a prefix of the stored bytes. The server holds one DEK, and the pass finds the key of a secret by trial, so neither form has a reader today. A column needs a breaking catalog migration on three backends, whose rollback is a restore (`docs/adr/0023-catalog-migrations.md:366-369`). A later online pass can add a prefix.
- Caching the keyed HMAC object instead of the 32-byte signing key. Measured, it would leave about 0.46 µs of HMAC work per request instead of 0.65 µs. The precomputed HMAC states let their holder sign any message ([RFC 2104, section 4](https://www.rfc-editor.org/rfc/rfc2104#section-4)). The object therefore needs the same secrecy as the key, and it is larger. The 32-byte key fits the slot design.
- `mlockall` of the whole process, as Vault does. It compares the process's whole virtual size with the memlock limit, which a multi-threaded server exceeds under the 8 MiB default. Measured: `mlockall` failed under a 64 KiB limit, and `--cap-add IPC_LOCK` alone did nothing for uid 10001 in a container.
- `prctl(PR_SET_DUMPABLE, 0)` by default, as ssh-agent does. It stops every core dump and every same-user debugger. `MADV_DONTDUMP` keeps the core and excludes the key pages (section 6).
- An `env` source. A file covers the same deployments, and the same user can read a process's environment through `/proc/<pid>/environ`.
- A `passphrase` source, with PBKDF2. For an unattended restart the passphrase sits in a file, where it protects no more than 32 random bytes. Anyone with a catalog copy can test guesses offline.
- A network key service: it fails the offline requirement.
- A built-in PKCS#11 source, through the `cryptoki` crate (0.12.1, Apache-2.0). It is the one source that gives a key that root cannot export. It costs a vendor module loaded at run time, a PIN file under the file rule, and a SoftHSM2 job in continuous integration. The mechanisms differ by token: tpm2-pkcs11 has no `C_UnwrapKey`, the YubiHSM 2 has no AES-GCM, and the Nitrokey HSM 2 has no AES. A later RFC can add this source with its own threat model.
- A TPM source in Rust, through `tss-esapi`. It does what `systemd-creds --with-key=tpm2` does (section 3), and adds the tpm2-tss C library to every build.
- AES key wrap (RFC 3394), from `aws_lc_rs::key_wrap`. The GCM cipher already binds each wrapped DEK to its context through the AAD, so key wrap adds a second primitive for nothing.
- No change. Main documents the trust boundary (`docs/manuals/10-security-model.md:22`) and the manual rotation (`:66-70`). The catalog still decrypts every stored secret, and the cache still holds every active secret in plaintext.

## Unresolved questions

- Is the downtime of an offline `switch` acceptable for the largest deployment that reviewers expect? MongoDB and Cassandra were not measured.
- Should `switch` also take a deployment back to the catalog key, so that an older release can run after the switch without a restore?

## Prior art

- Kerberos is the closest analog: a credential store, a master key in a stash file beside the database, and a per-entry key version. Its rotation adds a key, activates it, re-encrypts every entry online and purges the old key ([Updating the master key](https://web.mit.edu/kerberos/krb5-latest/doc/admin/database.html)). The stash file has the same weakness as `key_file`: root on the host and a copy of the host disk get it.
- MongoDB's encrypted storage engine with KMIP and MySQL InnoDB rotate the master key by rewrapping the inner keys. Neither passes over the data ([Rotate encryption keys](https://www.mongodb.com/docs/manual/tutorial/rotate-encryption-key/), [InnoDB data-at-rest encryption](https://dev.mysql.com/doc/refman/8.4/en/innodb-data-encryption.html)). `rewrap` works the same way.
- Vault separates `rotate` (a new barrier key, old versions kept) from `rekey` (new unseal keys, the root key rewrapped), and neither rewrites data ([operator rotate](https://developer.hashicorp.com/vault/docs/commands/operator/rotate), [operator rekey](https://developer.hashicorp.com/vault/docs/commands/operator/rekey)). Vault refuses to start when `mlock` fails unless `disable_mlock` is set ([Vault configuration](https://developer.hashicorp.com/vault/docs/configuration)). Vault Transit's `rewrap` re-encrypts a ciphertext under the newest key version while the service runs ([Transit secrets engine](https://developer.hashicorp.com/vault/docs/secrets/transit)).
- The AWS Encryption SDK and AWS KMS bind an encryption context to each ciphertext as AAD ([Encryption context](https://docs.aws.amazon.com/kms/latest/developerguide/encrypt_context.html)). Section 5 does the same.
- OpenSSH checks the open descriptor with `fstat` and every parent directory of the canonical path in `safe_path_fd` ([misc.c](https://raw.githubusercontent.com/openssh/openssh-portable/master/misc.c)). It keeps the host key out of the network-facing process through privilege separation ([README.privsep](https://raw.githubusercontent.com/openssh/openssh-portable/master/README.privsep)). The file rule and section 6.3 follow both.
- Kubernetes KMS v2 rotates the KEK with no API server restart, and its storage migration rewrites objects online ([KMS provider](https://kubernetes.io/docs/tasks/administer-cluster/kms-provider/)).
- Grafana encrypts data source secrets under DEKs wrapped by a KEK. It offers the rewrap, the DEK rotation and the re-encrypt pass as separate commands ([Database encryption](https://grafana.com/docs/grafana/latest/setup-grafana/configure-security/configure-database-encryption/)). Section 8 takes the same split.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
