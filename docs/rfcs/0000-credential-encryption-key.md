# RFC-0000: Wrap the credential encryption key with a key kept outside the catalog, and rotate it

- Status: Draft
- Author: @LeeroyHannigan
- Created: 2026-11-11
- FCP ends: not set
- Tracking issue: #707

## Summary

ExtendDB encrypts each stored access key secret and each AssumeRole session secret with one AES-256-GCM key. The catalog stores that key unwrapped, in the same database as the secrets it protects.

This RFC adds one setting, `key_file`. With it, a key-encryption key (KEK) in a file on the server host wraps the data encryption key (DEK) that encrypts the secrets. The catalog then holds only the wrapped DEK. Tools that deliver a secret to a service as a file can hold the KEK (section 2). Examples are systemd credentials sealed to a TPM, and Kubernetes secret volumes.

A new offline command, `extenddb encryption-key rotate`, moves a deployment to a KEK file and later replaces the DEK or the KEK. It re-encrypts each stored secret that it can open, except expired sessions, and changes no access key ID or secret. Clients therefore keep their credentials. The stored format of a secret does not change, and the change adds no catalog migration.

Without `key_file`, the server keeps today's behavior and today's protection. No part of the design uses a network key service.

## Motivation

On main at ce3a51a4b:

- `extenddb init` writes the key as the `encryption_key` row of the catalog `settings` table, in base64, on all four backends (`crates/storage-postgres/src/bootstrapper.rs:226-254` and the matching bootstrappers). The security model states the result: read access to the catalog database "yields every stored access key secret" (`docs/manuals/10-security-model.md:22`).
- The stored format is `nonce(12) || ciphertext || tag(16)`, with the access key ID as the additional authenticated data (AAD) (`crates/storage/src/secret_cipher.rs:4-16`). No byte records which key encrypted a row.
- Each server reads the key once at startup and keeps it until it exits (`crates/storage-postgres/src/lib.rs:958-972` and the matching factories). `extenddb settings set` refuses the key (`crates/app/src/cmd_settings.rs:179-194`).
- The only rotation is the manual procedure at `docs/manuals/10-security-model.md:66-70`. The operator replaces the row, restarts every server, then deletes and re-creates every access key. The comment on #707 describes the result as an outage for every signed request, until each client has new credentials.

On #228 (2026-08-05), a code owner wrote that the current handling "is probably not sufficient for many production uses". The same review asked for other mechanisms, such as an HSM or a passcode. ExtendDB runs with no internet connection (`README.md:17`, `docs/manuals/11-deployment-guide.md:148-154`), so no key source may depend on a network key service.

## Detailed design

### Terms

- DEK (data encryption key): the 32-byte AES-256-GCM key that encrypts stored secrets.
- KEK (key-encryption key): a 32-byte key that encrypts (wraps) a DEK.
- Catalog key: the key in the `encryption_key` row. A server without `key_file` uses it, as today.
- Key ring: the settings row `encryption_key_ring`, which holds the wrapped DEKs and the id of the KEK that wraps them.
- File rule: the mode check of section 1, which `serve` and `rotate` apply to a KEK file.

### 1. The KEK file

A new `[auth.encryption]` section of `extenddb.toml` names the file:

```toml
[auth.encryption]
key_file = "/etc/extenddb/kek"
```

| `key_file` | Key the server uses | Where the key is kept |
|---|---|---|
| not set (the default) | the catalog key | the `encryption_key` row of the catalog `settings` table, as today |
| set | the newest DEK in the key ring, unwrapped with the KEK | the KEK in `key_file`, the wrapped DEK in the key ring |

An upgrade changes nothing until the operator moves the deployment. A later key source, such as a hardware module, adds its own field.

The KEK file holds 32 random bytes in standard base64. The server strips one trailing newline, so `head -c 32 /dev/urandom | base64 > kek`, run under `umask 077`, writes a file that the server accepts. A leading `~` in `key_file` expands through `expand_tilde`, as in the TLS key path (`crates/app/src/serve_helpers.rs:164`).

The KEK must be new random bytes. A catalog copy taken before the move holds the catalog key, so a KEK equal to the catalog key would open every later copy. `rotate` refuses such a KEK (section 6).

`serve` and `rotate` read the KEK file in `crates/app`, and apply the file rule to it before they use it. The file rule is the check that `serve` applies to the TLS private key (`crates/app/src/serve_helpers.rs:193-207`), moved into one function for both callers. It accepts a file whose mode gives the group and others no access. It also accepts a file that another user, such as root, owns, when its mode gives others no access. It reads the mode through `std::fs::metadata`, which follows symlinks, so the files of a Kubernetes secret volume pass.

The file rule reads the mode bits only. It does not see POSIX ACL entries, and on a file with an ACL the group bits show the ACL mask. The operator therefore keeps the group of the file, and any ACL entry, to the server's user.

Where the TLS check narrows the mode and warns, the file rule refuses. A new setting has no deployments that a refusal can break. Users who could read the file before may hold a copy of the KEK, and narrowing the mode later does not change that.

The server reads the KEK file once at startup into a `Zeroizing` buffer, unwraps the DEK, and then drops the buffer, which wipes the KEK.

### 2. Delivering the KEK file

The server reads the KEK from a file, so any tool that gives a service a file at start can hold the KEK. The routes below need no ExtendDB code. The server runs as the user `extenddb` in the systemd unit of the deployment guide, and as uid 10001 in the container image (`docs/manuals/11-deployment-guide.md:253-284`, `Dockerfile:57-58`). Each route therefore gives that user the file at a mode that the file rule accepts.

- systemd 250 or later, on a host with a TPM 2.0 chip. The operator writes the KEK to a file, keeps a copy away from the host, and seals the file to the TPM with `systemd-creds encrypt --with-key=tpm2`. `LoadCredentialEncrypted=` decrypts it at each start of the unit into `/run/credentials/<unit>/`, and systemd unmounts that directory when the unit stops ([systemd-creds(1)](https://www.freedesktop.org/software/systemd/man/latest/systemd-creds.html), [systemd.exec(5)](https://man7.org/linux/man-pages/man5/systemd.exec.5.html)). A copy of the host disk then holds no KEK that opens without that TPM. Root on the running host can still decrypt it. The route applies only to the systemd deployment of the deployment guide.

  ```sh
  systemd-creds encrypt --with-key=tpm2 --name=extenddb-kek kek /etc/credstore.encrypted/extenddb-kek
  ```

  ```ini
  [Service]
  LoadCredentialEncrypted=extenddb-kek:/etc/credstore.encrypted/extenddb-kek
  Environment=EXTENDDB__AUTH__ENCRYPTION__KEY_FILE=%d/extenddb-kek
  ```

  systemd gives the file to the user of the unit in one of two ways. On a ramfs, the user owns the file at mode 0400. systemd 254 or later uses a tmpfs on Linux 6.4 or later. There root owns the file, and a POSIX ACL entry lets the user read it. Its mode then reads as 0440. That case comes from the systemd source and acl(5), and was not observed on a host. The file rule accepts both.
- Kubernetes: a Secret volume with `defaultMode: 0400`, and either `securityContext.runAsUser: 10001` or `fsGroup: 10001`. With `runAsUser`, uid 10001 owns the file. With `fsGroup`, root owns it, with group 10001 and mode 0440. With `defaultMode` alone, root owns the file, and uid 10001 can read it only at a mode that the file rule refuses. Secrets are unencrypted in etcd unless the cluster enables encryption at rest ([Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)).
- Docker swarm: `--secret source=extenddb-kek,target=kek,uid=10001,gid=10001,mode=0400` ([docker service create](https://docs.docker.com/reference/cli/docker/service/create/)). The default, root with mode 0444, fails the file rule. Without `--autolock`, each manager keeps the key of its Raft log unencrypted on disk. With it, a manager that restarts waits for an operator to type the unlock key ([Lock your swarm](https://docs.docker.com/engine/swarm/swarm_manager_locking/)). Compose file secrets are bind mounts that ignore `uid`, `gid` and `mode`, so uid 10001 must own the host file, at mode 0400.
- Podman: `--secret extenddb-kek,type=mount,uid=10001,gid=10001,mode=0400`. The default is root with mode 0444.
- sops with age keeps the encrypted KEK in configuration management. `sops decrypt` in an `ExecStartPre=` writes the file under `umask 077`. With an age identity file on the same disk, the two files protect no more than the KEK file alone. With an age plugin for a YubiKey or a TPM, a PIN or touch policy stops unattended restarts.

`rotate` needs the KEK too, and it runs while every server is stopped. It runs as the server's user, with the delivery that the server has. On systemd that is a transient service with the same `LoadCredentialEncrypted=` line, started with `systemd-run --wait --pipe`. On Kubernetes it is a Job that mounts the same Secret while the Deployment has 0 replicas. On swarm it is a one-off service with the same secret, and on Podman a `podman run` with the same secret.

None of these copies is a backup of the KEK. A TPM-sealed file opens only with that TPM, so it is lost with the host. Keep a copy of each KEK away from the host. Make that copy before the operator seals the KEK or stores it as a secret.

Every route gives root on the running host the DEK, because the server holds it in memory for each credential lookup (Answers). The TPM route protects a copy of the host disk: with a catalog copy, that disk copy opens no secret without the TPM.

### 3. The key ring

With `key_file` set, the catalog holds the DEK in one settings row, `encryption_key_ring`:

```json
{"kek_id":"3f9a0c51e2d47b86","keys":[{"id":2,"wrapped":"<base64>"}]}
```

- `wrapped` is the DEK, in base64, sealed with the existing cipher: `secret_cipher::encrypt_secret(dek, kek, "dek:<id>")` (`crates/storage/src/secret_cipher.rs:37`). The AAD binds the wrapped DEK to its id, so a wrapped value copied to another id does not open.
- Every access key ID starts with `AKIA` or `ASIA`. Created IDs start with `AKIAEXTENDDB`, and imported and dev IDs must start with `AKIA` (`crates/server/src/management/iam_user_self.rs:269-276`, `crates/server/src/dev_credentials.rs:129-135`). Session IDs start with `ASIAEXTENDDB` (`crates/server/src/management/crypto.rs:28-39`). No stored secret therefore has the AAD of a wrapped DEK.
- A new DEK comes from `helpers::generate_encryption_key`, which `init` uses for the catalog key (`crates/storage/src/bootstrapper.rs:257`).
- `kek_id` is the first 8 bytes, in hex, of HMAC-SHA256 keyed with the 32 decoded bytes of the KEK, over the text `extenddb key-encryption-key id`. It names the KEK and does not reveal it. `aws_lc_rs::hmac` computes HMAC-SHA256 for SigV4 already (`crates/auth/src/sigv4/signing_key.rs:37-43`).
- DEK ids start at 1. Each rotation adds the next id, so no two DEKs share an id. The newest DEK is the active one.
- One row holds the whole ring. Each change to the ring is a write of one row, which is atomic on all four backends, Cassandra included.
- The row name contains `encryption_key`, so `extenddb settings list`, the management API and the console mask its value as they mask the catalog key (`crates/config/src/display.rs:13-36`). `extenddb settings set` refuses it, as it refuses every key outside `KNOWN_KEYS` (`crates/app/src/cmd_settings.rs:174-194`).

Without `key_file`, the catalog has no key ring.

Every rotation re-encrypts every row (section 6), so the DEK does not make a KEK change cheaper. A rotation that replaces only the DEK changes no file on any host, so the operator keeps one KEK file across such rotations. A later hardware source can also unwrap the DEK once, at startup. Without a DEK, it would call the module on each credential lookup that misses the cache (Alternatives).

### 4. How a row's key is found

A stored secret records no key id, and its format does not change. The server holds one key (section 5), so every row that it can open is under that key. A row under another key fails AES-GCM authentication, as a damaged row fails today, with HTTP 500 `InternalServerError` (`crates/storage-sqlite/src/credential_store.rs:54-60` and the matching stores).

The rotation command and its preview find the key of a row by trial. They open the row with the target DEK first, then with each other key that the catalog holds. These are the other DEK of an unfinished ring, and the catalog key while the `encryption_key` row exists. A wrong key fails authentication with the same error as changed bytes (`crates/storage/src/secret_cipher.rs:54-62`, `:83`). A row that opens under the target is done. The command re-encrypts a row that opens under another key. A row that opens under no key stays as it is, and the command lists it (section 6).

A benchmark measured the cost of these calls. It ran a copy of `secret_cipher` with the aws-lc-rs version and features that main locks, on a shared 16-CPU host with a load average near 12. Each figure is the median of 7 runs of 200,000 calls:

| Call | Time per call |
|---|---|
| `decrypt_secret`, the right key | 389 ns |
| `decrypt_secret`, a wrong key, which fails | 389 ns |
| `encrypt_secret` | 745 ns |

Each call includes the base64 decode and the key setup that the stores repeat on every call. A first pass therefore spends about 1.5 µs of cipher time on a row, and 0.4 µs of that is the failed trial. A resumed pass spends 0.4 µs on a row that is done.

AES-GCM does not commit to its key, so a ciphertext built with two keys can open under both. Only a holder of both keys can build one, and such a holder can already write any secret.

The catalog schema does not change, so the change needs no catalog migration (section 8). The Alternatives section gives the reasons against a key id on each row.

### 5. Startup

One function in `crates/storage` loads the key, beside the key check that #863 adds there. It reads the `encryption_key` row and the `encryption_key_ring` row through the existing `SettingsStore::get_setting` of the backend (`crates/storage/src/management_store/mod.rs:56`). Each backend factory calls it once, in place of its own key query (`crates/storage-sqlite/src/lib.rs:427-434` and the matching factories) and its call to the check of #863. The function returns the one key that the server uses, or a reason to refuse:

| The catalog holds | `key_file` | Result |
|---|---|---|
| the `encryption_key` row, no key ring | not set | Start with the catalog key, as today. |
| the `encryption_key` row, no key ring | set | Refuse. The catalog key is still in the catalog. Name `rotate --to-key-file`. |
| a key ring with one DEK, no `encryption_key` row | set, to the KEK that the ring names | Start with that DEK. |
| a key ring with one DEK, no `encryption_key` row | set, to another KEK | Refuse, and name both KEK ids. |
| a key ring with one DEK, no `encryption_key` row | not set | Refuse, and name `key_file` and the KEK id. |
| a key ring with more than one DEK, or a key ring and the `encryption_key` row | either | Refuse. A rotation did not finish, or a release without this change wrote a catalog key after the move. Name the KEK id of the ring, and `rotate --yes`, which finishes both cases (section 6). When `key_file` is not set, name `--to-key-file` for the file of that id. |
| neither row | not set | Refuse with `MissingEncryptionKey`, as today. |
| neither row | set | Refuse. Name the missing `encryption_key_ring` row and the restore of section 9. |

- A damaged catalog key, KEK file or key ring also stops the server. The catalog key keeps `MissingEncryptionKey` and the `InvalidEncryptionKey` of #863. Each other refusal carries its text in the existing `InitializationFailed(String)` (`crates/storage/src/server_components.rs:55`), so the change adds no error variant.
- A KEK file that does not decode gets the reason that `cipher_key` gives for a damaged catalog key (`crates/storage/src/secret_cipher.rs:100-113`), so one rule covers both. For a bad character, the decoder's text contains that character, which can be part of the stored key (`DecodeError::InvalidByte` and `InvalidLastSymbol` of `base64` 0.22.1). PR 1 leaves the character out of the reason, and keeps the kind of error and its offset. No message, log line or settings response then contains key material.
- At startup the server logs where its key comes from. Without `key_file`, the line says that a copy of the catalog decrypts every stored secret.
- The factory gives the key that the function returns to the store fields that hold the catalog key today (`crates/storage-sqlite/src/lib.rs:443-450` and the matching factories). The call sites that encrypt and decrypt a secret do not change.
- Today some code reads the `encryption_key` row when its store holds no key. This change deletes that code: `crates/server/src/management/assume_role.rs:292-308`, `crates/storage-sqlite/src/management_store/access_keys.rs:20-33`, `crates/storage-postgres/src/management_store/access_keys.rs:21-34` and `:120-133`, `crates/storage-mongodb/src/management_store.rs:2724-2739`, and `crates/storage-cassandra/src/management_store/access_keys.rs:25-44` and `:173-192`. A store built without the key refuses to create, import or store a credential. The `settings` and `migrate` commands and the migration bodies use such stores, and none of them writes a credential.
- The factory takes the key to load as a new argument: the catalog key, a KEK that the caller read, or none. The argument is not a field of the `Copy` options struct (`crates/storage/src/server_components.rs:86-135`). `serve` passes the catalog key, or the KEK from `key_file` when that is set, so no code in `crates/storage` reads a file. The `extenddb backup` commands pass none (`crates/app/src/cmd_backup.rs:649-672`). They read no credential, so they read neither key row and do not need the KEK file.
- The SQLite dev bootstrap runs at each start of an in-memory or dev-build server (`crates/storage-sqlite/src/lib.rs:292-301`). It writes the `encryption_key` row only when the catalog has neither that row nor a key ring (`crates/storage-sqlite/src/store.rs:266-280`). Without this rule, a dev build would write a new catalog key into a moved catalog at each start, and then refuse to start.
- An in-memory SQLite catalog refuses `key_file`. It is new at each start, so it holds no key ring, and `rotate` cannot reach it from another process.
- `init` needs no bootstrap rule. It writes bootstrap rows only into a catalog that has no admin user (`crates/storage/src/bootstrapper.rs:128-147`). If a catalog key appears in a moved catalog anyway, the server refuses to start. `rotate --yes` then re-encrypts the rows under that key and deletes it (section 6).

### 6. Rotation

```text
extenddb encryption-key rotate --config extenddb.toml [--to-key-file <path>] [--yes]
```

Without `--yes`, the command previews. It reads the catalog and opens every stored secret in memory with the keys that the catalog holds (section 4). It prints the newest DEK id and the KEK id of the ring. It also prints the rows per key, the rows that no key opens, and what `--yes` would do. It writes nothing, so it can run beside a server, as the `migrate` preview does (`docs/adr/0023-catalog-migrations.md:249-250`).

With `--yes`, it opens its stores as `migrate --yes` does, through the migration backend, and never through `create_server_components`. It takes the migration lock through `CatalogMigrations::lock` and frees it with `release` (`crates/app/src/cmd_migrate_runner.rs:301-318`). The lock waits for registered servers to stop. The command takes the `--lock-timeout` option of `migrate --yes`, waits up to that limit, and then exits 6 (`docs/adr/0023-catalog-migrations.md:246-250`). On Cassandra and MongoDB the lock is a lease, and the stores of the migration backend check it before each request (`crates/storage-cassandra/src/migrations/guarded.rs:4-17`, `crates/storage-mongodb/src/migrations/backend.rs:6-10`).

Unlike `migrate --yes` on SQLite, the command takes no copy of the database. A copy beside the database would hold the catalog key next to every secret, which is the exposure that the move ends. The pass resumes after a stop, so it needs no copy.

Then it takes these steps:

1. It reads the keys that the catalog holds: the catalog key from the `encryption_key` row, and the DEKs of the key ring. It opens the ring with the KEK that the ring names, which must be the configured KEK or the target KEK. It reads the target KEK from `--to-key-file`, or from `key_file` when the option is absent. When neither is set, it refuses, because a KEK kept in the catalog protects nothing from a reader of the catalog. It also refuses a target file that fails the file rule. It refuses a target KEK that equals the catalog key or a DEK of the ring.
2. It writes the key ring wrapped under the target KEK, with a new DEK as the newest entry. It writes one audit line with the DEK id and the KEK id, as `extenddb settings set` writes one (`crates/app/src/cmd_settings.rs:204-208`). A catalog in one of the unfinished states of section 5 shows that an earlier run stopped. This run then adds no DEK, and resumes with the newest DEK of the ring as the target.
3. It opens each `access_keys` row, and each `iam_sessions` row that has not expired, by trial (section 4). It re-encrypts under the target DEK each row that opens under another key.
4. It reads every row again, and stops with every key kept if a row still opens under another key. It also reads the target file again, and stops if the file is gone or its KEK id is not the one of the ring. A second run with the same arguments then finishes the work.
5. It writes the key ring with the target DEK alone.
6. It deletes the `encryption_key` row, if the catalog has one.
7. It prints the target KEK id, the number of rows it re-encrypted per table, the rows it left, and the `key_file` line for the configuration.

The operator then sets `key_file` to the target file, if it changed, and starts the server.

Step 4 guards against a write that reports success and changes nothing. Cassandra acknowledges an update whose write timestamp is older than the timestamp of the row, and keeps the value that the row had. That happens, for example, when the clock of the coordinator is behind the clock that wrote the row. A store method that matches no row also reports success. Without step 4, such a row would lose its secret at step 5 or 6.

In step 3:

- Expired sessions keep their old key. Each credential store checks the expiry of a session before it decrypts its secret (`crates/storage-sqlite/src/credential_store.rs:95-103` and the matching stores). A session lasts at most 43,200 s (`crates/server/src/management/assume_role.rs:98`). The pass skips only sessions that expired more than 15 minutes before the run started. A rotate host whose clock runs up to 15 minutes ahead of a server's then skips no session that the server still accepts.
- Each write replaces the ciphertexts of a batch of rows, found by their primary keys. No other process writes these rows during the pass. Only a server writes a credential, and the CLI that imports an access key posts to the management API of a server (`crates/app/src/manage_http.rs:311-319`). The lock keeps every server out (section 7).
- The command leaves a row that no key opens as it is, and lists it by access key ID. Such a row fails today on every request, and it fails in the same way after the rotation.

A run that stops at any step can run again with the same arguments. The rows that open under an older key are the remaining work. Until a run finishes, the server refuses to start (section 5).

The pass is one driver in `crates/storage`, as the pass of migration 2 is (`crates/storage/src/policy_migration.rs`, `crates/storage/src/migrations/m0002_resource_tag_condition_keys.rs:13-18`). Each backend adds three `ManagementStore` methods:

- a paged read of the `access_keys` and `iam_sessions` rows
- the write of the ciphertexts of a batch of rows
- the delete of one settings row, which no store has today

The notes below describe those methods.

Per backend:

- SQLite: the migration lock is the `flock` that `serve` also holds, so no server runs during the pass (`docs/adr/0023-catalog-migrations.md:216`). Each batch is one transaction of single-row updates. The connections of the command set `PRAGMA secure_delete = ON` before the first write, and the run ends with `PRAGMA wal_checkpoint(TRUNCATE)`. SQLite then overwrites the deleted catalog key and each replaced ciphertext, so a file copy taken after the run holds neither. The admin guide copies the SQLite files for a backup (`docs/manuals/05-admin-guide.md:1107`), and no code sets `secure_delete` today.
- PostgreSQL: each batch is one statement, `UPDATE ... SET secret_key_encrypted = v.new FROM (VALUES ...) AS v(id, new) WHERE <primary key> = v.id`. Behind PgBouncer, or for up to 30 s after a failover of the catalog's server, a server can serve beside the holder of the lock (`docs/adr/0023-catalog-migrations.md:231-244`). As before an upgrade, the operator stops every server first (section 10).
- MongoDB: each row is one `update_one` on the `_id` of the document, with no upsert, because nothing makes `access_key_id` unique in `access_keys` (`crates/storage-mongodb/src/migrations/m0001_baseline.rs:102`). A TTL index deletes expired sessions during the pass (`crates/storage-mongodb/src/migrations/m0001_baseline.rs:106`), and an update with no upsert leaves a deleted document deleted. The writes use the existing majority write concern (`crates/storage-mongodb/src/lib.rs:569`).
- Cassandra: each row is one `UPDATE ... SET secret_key_encrypted = ? WHERE <primary key> = ?`, with no lightweight transaction. Nothing deletes a row during the pass, and session rows have no TTL (`crates/storage-cassandra/src/management_store/access_keys.rs:240-246`), so an update creates no partial row. The server reads credentials and settings at `ONE`, the default of the driver (`crates/storage-cassandra/src/credential_store.rs:104-107`, `crates/storage-cassandra/src/catalog_store.rs:290-313`). The command therefore reads and writes at `ALL`: the rows, the ring writes of steps 2 and 5, and the delete of step 6. Every replica then holds the new ciphertexts and the new ring before the old key goes. The command needs every replica of the catalog keyspace up, and it stops when one is down. Two new `cassandra_util` functions take the consistency as an argument, and `query_rows_quorum` and `execute_quorum` become wrappers of them (`crates/storage-cassandra/src/cassandra_util.rs:121-135`, `:227-248`). Their 72 callers do not change.

A proxy measurement of the database work of the pass ran on the same host. A Python script read 100,000 rows in pages of 1,000 and wrote each page back, with random bytes in place of the cipher. On PostgreSQL each page was one `UPDATE ... FROM (VALUES ...)` statement, and on SQLite one transaction of single-row updates. Each write was conditional on the old ciphertext, and it also set a key id column that this design drops. A local PostgreSQL 16 container took 3.85 s, and a SQLite file took 1.44 s, reads included. The design writes by primary key alone. Step 4 adds one more read of every row, and the cipher adds about 0.15 s per 100,000 rows (section 4). MongoDB, Cassandra and a PostgreSQL server across a network were not measured.

What a rotation protects, and what it does not:

- After a move to a KEK file, a logical copy of the catalog holds no unwrapped key. Examples are `pg_dump`, `mongodump`, an SQLite `.dump` and a CQL export.
- A copy taken before the move holds the catalog key and the ciphertext of each secret that existed then. It decrypts those secrets with nothing else.
- A physical copy of a PostgreSQL, MongoDB or Cassandra catalog can hold the deleted `encryption_key` row too, until its pages are overwritten. Examples are a PostgreSQL write-ahead log archive or base backup, a Cassandra SSTable or snapshot before compaction, and a MongoDB oplog entry. To end that exposure, re-create the access keys that existed before the move. Sessions expire within 12 hours. On SQLite the command overwrites the row (Per backend).
- A DEK rotation protects the secrets from a holder of the old DEK alone who gets a new copy of the catalog. A KEK change also protects them from a holder of the old KEK. Neither protects a secret whose ciphertext and key leaked together: re-create that access key.
- `init` creates no access key (`crates/storage/src/bootstrapper.rs:159-170`). A deployment that moves before its first `serve` therefore has no secret under the catalog key. A new deployment that wants a KEK file runs `init`, then `rotate --to-key-file <path> --yes`, then `serve`.
- On PostgreSQL a catalog copy also holds `data_database_connection_string`. It carries the password of the application role, which opens the catalog database and the data database (`docs/manuals/10-security-model.md:22`, `crates/storage-postgres/src/bootstrapper.rs:96-120`).
- Every catalog also holds the bcrypt hashes of console passwords, and the AssumeRole session tokens of `iam_sessions` in plaintext. This RFC changes neither.

### 7. Several servers

In 1.0, running more than one server on a catalog is unsupported, and only SQLite refuses a second one (`README.md:32`, `docs/manuals/11-deployment-guide.md:300`). The rotation command runs only while no server is registered. Every server therefore starts again after the rotation and loads the new key. No server needs a reload signal or a poll of the catalog.

### 8. Upgrade and rollback

- The change adds no catalog migration, and it changes no stored row of a deployment that does not move. Such a deployment needs no `migrate --yes` run for this change, and the change adds nothing to its catalog that a release without it refuses.
- After a move, the catalog has no `encryption_key` row. A release without this change then refuses to start with `MissingEncryptionKey` (`crates/storage/src/server_components.rs:74-77`). To run such a release again, restore a catalog copy taken before the move. That copy holds the catalog key again, and every catalog write since the copy is lost. On Cassandra the upgrade manual has no tested rollback (`docs/manuals/07-upgrade-manual.md:230`).
- A SQLite dev image of a release without this change writes a new `encryption_key` row into a moved catalog at startup, and serves with that key. Every request signed with a credential under the DEK then fails with HTTP 500. A server with this change refuses to start on that catalog until `rotate --yes` runs again (section 5).
- A rollback also deletes `[auth.encryption]` and any `EXTENDDB__AUTH__ENCRYPTION__KEY_FILE` override from the configuration. The previous release refuses an unknown field in `[auth]` (`crates/config/src/lib.rs:275-287`), as the upgrade manual says for `[backup]` (`docs/manuals/07-upgrade-manual.md:83`).

### 9. Backups and restores

- Table backups (`CreateBackup`) hold no credentials and do not change (`docs/manuals/05-admin-guide.md:1102`).
- With a KEK file, a catalog copy holds the key ring and its KEK id. A restore needs the KEK that the id names. The server refuses a catalog whose ring names another KEK, and prints both ids (section 5).
- Keep each KEK for as long as any catalog copy wrapped under it is kept. A rotation to a new KEK does not change older copies.
- A copy taken before the move needs a configuration without `key_file`.
- A restore into another deployment needs the KEK file on the new host and the same `[auth.encryption]` section.
- A lost KEK loses every stored secret of each catalog copy wrapped under it. Each access key is then re-created, or imported under its old ID from the secret that its client holds (`docs/manuals/14-backup-and-restore.md:549-614`). Keep a copy of the KEK apart from the catalog and away from the host (section 2).

### 10. What operators do differently

- Create the KEK file under `umask 077`, readable only by the server's user, or deliver it by a route of section 2.
- Copy the target KEK file to a place away from the host before `rotate --yes`. From step 2 of the rotation on, the key ring opens only with it. A target KEK lost after step 2 loses every row of a KEK change, and the rewritten rows of a move.
- Run `extenddb encryption-key rotate --to-key-file <path>` without `--yes` to preview. Stop every server, run it with `--yes` as the server's user (section 2), set `key_file`, and start the servers.
- Give the KEK file to `serve`. The `extenddb backup` commands do not need it.
- Keep each old KEK while a catalog copy wrapped under it is kept.
- Re-create the access keys that existed before the move, to end the exposure of older copies (section 6).
- After `init --overwrite`, put `[auth.encryption]` back. That command keeps only the storage settings (`docs/manuals/05-admin-guide.md:175`).
- Delete `[auth.encryption]` from the configuration before a rollback (section 8).
- Keep catalog dumps under `umask 077`. On PostgreSQL they still hold the password of the application role (`docs/manuals/07-upgrade-manual.md:109`).
- A rotation that did not finish stops the server at its next start, with a message that names the command. The preview reports the rows per key and the newest DEK id. The change adds no metric.

### 11. Docs to change

Line numbers are on main at ce3a51a4b.

- Statements that this change makes false:
  - `docs/manuals/10-security-model.md:22`, `:23`, `:29`, `:62-70`, `:352` and `:378`
  - `docs/manuals/02-design-guide.md:26`, `:109`, `:129`, `:131` and `:193`
  - `docs/design/08-component-config.md:123-124`, and the table of environment variables at `:47-58`, which needs `auth.encryption.key_file`
  - `docs/manuals/05-admin-guide.md:334-341` and `:1111`, `docs/manuals/07-upgrade-manual.md:109`, `docs/manuals/14-backup-and-restore.md:34-41`
  - `docs/design/04-component-storage.md:195-198`, `docs/design/05-component-auth-v3.md:404-410`, `docs/design/06-component-server.md:474`, `docs/design/13-storage-mongodb.md:112-115` and `:145-147`
  - `docs/getting-started.md:90`, `docs/manuals/04-quickstart-setup-guide.md:141`, `docs/manuals/08-install-linux.md:86`, `docs/manuals/09-install-macos.md:70`
  - `docs/adr/0026-credential-crypto-on-aws-lc.md:85-86`
- Statements already false on main, in paragraphs that this change rewrites: `docs/troubleshooting.md` at `:601-617`, `:661-663`, `:803-805` and `:903-907`.
- #972 covers the false statements that this change does not touch: `docs/troubleshooting.md:149` and the text that says the config file may contain the key, and `docs/design/05-component-auth.md:37` and `:1036-1039`.
- Code comments: `crates/storage/src/secret_cipher.rs:7-9`, `crates/storage/src/lib.rs:784-786`, `crates/storage-postgres/src/catalog_store.rs:24-25`, `crates/storage-cassandra/src/catalog_store.rs:46-47` and `crates/storage-cassandra/src/bootstrapper.rs:552`.
- New text:
  - the rollback step beside `docs/manuals/07-upgrade-manual.md:83`
  - the restore needs of section 9, beside the catalog restore of `docs/manuals/14-backup-and-restore.md:549-614`
  - the KEK file in the air-gapped transfer list (`docs/manuals/11-deployment-guide.md:150-154`), and in the Kubernetes mount (`:146`) with `runAsUser` or `fsGroup`
  - the `systemd-creds` command and the two unit lines of section 2, beside the unit file (`docs/manuals/11-deployment-guide.md:253-284`)
  - the container steps (`docker/README.md:124-143`), with the `uid`, `gid` and `mode` options of swarm and Podman
  - the `rotate` run for each route of section 2
  - the configuration and command reference in the admin guide
  - a troubleshooting entry for each new startup refusal
- `docs/rfcs/0000-mongodb-backend.md:179` is a record and stays as it is. #863 changes `docs/manuals/10-security-model.md:67`.

### 12. Implementation order

Three pull requests, each with its own docs, ship in one release:

1. The shared startup function, for the catalog key only, with the new factory argument and the deleted fallback reads. It also leaves the bad character out of the decode reason (section 5). No request or response changes. Crates: `storage`, `storage-sqlite`, `storage-postgres`, `storage-mongodb`, `storage-cassandra`, `server` and `app`.
2. `key_file`, the file rule shared with the TLS key check, the key ring and the KEK id. It also adds the dev bootstrap rule, the in-memory refusal and the startup refusals of section 5. It adds `extenddb encryption-key rotate` on SQLite and PostgreSQL, with their three store methods. Crates: `config`, `storage`, `storage-sqlite`, `storage-postgres` and `app`.
3. `rotate` on MongoDB and Cassandra, with their three store methods, and the two Cassandra functions that take a consistency. Crates: `storage-mongodb` and `storage-cassandra`.

Tests: one check in `crates/storage` holds the rotation and startup cases as one table, and each backend's tests run it with their own store. `metrics_conformance` is the precedent (`crates/storage/src/metrics_conformance.rs:44`). The tests of PostgreSQL, MongoDB and Cassandra call it, and so does a SQLite test file (`crates/storage-sqlite/src/migrations/released_tests.rs:411`). The cases include a row under the catalog key, a row under the old DEK, and a row that no key opens. They also include a write that changed nothing, and a catalog key written beside a key ring. They also cover a target KEK equal to the catalog key, a run resumed after a stop, and each refusal of section 5. On SQLite, a test reads the database files after a run and finds neither the catalog key nor an old ciphertext.

Size: each pull request reports its production and test line counts. A rough estimate from the design, before the code exists, is about 900 production lines added and 115 deleted, and about 400 test lines.

## Libraries considered

Beyond the credential cipher, the design needs a wrap and unwrap of a DEK, a KEK id and a new DEK. aws-lc-rs 1.18.2, which ExtendDB already uses for the credential cipher (`docs/adr/0026-credential-crypto-on-aws-lc.md`), provides each of them. The wrap is AES-256-GCM through `encrypt_secret` and `decrypt_secret`, the KEK id is `aws_lc_rs::hmac`, and a new DEK comes from `helpers::generate_encryption_key`. aws-lc-rs also has AES key wrap, AES-256-GCM-SIV, HKDF and PBKDF2.

The parts that a key-management crate could replace come to about 100 production lines, estimated before the code exists:

- the key ring and its checks, about 40
- the wrap and unwrap, about 20
- the KEK id, about 10
- the read of the KEK file, about 30

No crate replaces the rest: the rotation pass on four backends, the startup function, or the file rule. kms-aead, for example, checks that a key file is a regular file of the right length, and does not check its mode or owner.

A survey on 2026-11-12 checked the crates below against the licence gate (`about.toml:6-17`), the MSRV of 1.88 (`Cargo.toml:30`) and the offline requirement.

| Crate | What it offers | Why this RFC does not use it |
|---|---|---|
| `tink-core` and `tink-aead` 0.3.0 | Key sets with rotation, and a wrapped key set. A RAW AES-GCM key reads today's rows unchanged. | Its RustCrypto backend has no FIPS module, and brings back the 8 crates that ADR-0026 removed. It adds 21 crates, 6 more at build time, and 9,147 lines. Its last release was on 2024-11-28. |
| `kms-aead` 0.32.0, feature `aws-lc` | Envelope encryption under a versioned local KEK. | MSRV 1.99, one maintainer, and 34,752 lines. Its stored formats changed at 0.30. Its fit for today's rows was not checked. |
| `aws-esdk` 1.3.0 | The raw AES keyring of the AWS Encryption SDK. | It links the KMS and DynamoDB clients and a second hyper and rustls stack. It rewrites every stored secret into its message format. |
| `envelopers` 0.8.4, `ironcore-alloy` 0.16.3, `aws-mpl-legacy` 0.2.0 | Envelope encryption. | They fail the licence gate. envelopers has its own licence. ironcore-alloy is AGPL-3.0 and needs MPL-2.0 crates. aws-mpl-legacy pulls a second `aws-lc-sys`, whose licence includes the OpenSSL licence, and an MPL-2.0 crate. |
| `age` 0.12.1, `cocoon` 0.5.0 | A file format keyed by a passphrase or an identity. | No key ring, and this RFC rejects a passphrase source (Alternatives). |
| `aes-kw`, `aes-gcm-siv`, `argon2`, `scrypt` | One primitive each. | aws-lc-rs has AES key wrap, AES-256-GCM-SIV and PBKDF2. |
| `keyring` 4.2.0 | Operating-system key stores. | It needs a Secret Service on D-Bus, and its kernel store is lost at reboot ([linux-keyutils-keyring-store](https://docs.rs/linux-keyutils-keyring-store/latest/)). |
| `cryptoki` 0.12.1, `tss-esapi` 7.7.0 | PKCS#11 and TPM 2.0 access. | A later key source can use one (Alternatives). |
| `secret-keeper` 0.3.5 | Envelope encryption. | It does not resolve, because its `aes-soft` dependency is yanked. |

## Answers to the questions on #707

- Which providers come first, and where is a provider configured? A KEK file comes first, named by `key_file` in the `[auth.encryption]` section of `extenddb.toml` (section 1). There is no plugin interface, and a later source adds its own field. A host with systemd 250 or later and a TPM 2.0 can keep the KEK sealed to the TPM with no ExtendDB code (section 2). A built-in PKCS#11 source can come in a later RFC (Alternatives).
- What does the server do when the provider cannot be reached at startup? It refuses to start, and it names the file and the reason (section 5).
- Does it keep the unwrapped key in memory? Yes. Each credential lookup that misses the cache needs the DEK. The server therefore keeps it until it exits, in the store fields that hold the catalog key today. It wipes the KEK after startup. The credential cache keeps decrypted secrets as it does today (`crates/auth/src/lib.rs:55-83`).
- How does an existing deployment move? `rotate --to-key-file` re-encrypts every secret under a new DEK and deletes the catalog key (section 6). Wrapping the catalog key in place would let the catalog key from any older copy decrypt the secrets in every later copy.
- How does a rotation treat secrets written while it runs, on each backend? No server writes a secret while it runs, because the command runs only while no server is registered (section 6). On PostgreSQL behind PgBouncer, or for up to 30 s after a failover, a server can serve beside the lock. The operator therefore stops every server first (section 6).
- What do backup and restore carry, and what does a restore into another deployment need? Section 9.
- Should each stored secret record which key encrypted it? No. The server holds one key, and the rotation command finds the key of each row by trial (section 4). The stored format stays as it is, and the change needs no catalog migration.
- How does each running server learn of the new key? By a restart. The command runs only while every server is stopped (section 7).
- Should the decrypted secrets in each credential cache be flushed? A restart starts with empty caches (`docs/manuals/10-security-model.md:126`). A rotation also changes no plaintext secret, so a cached secret stays valid.

## Drawbacks

- A rotation stops every server for the length of the pass. In the proxy measurement of section 6, a Python script with no cipher rewrote 100,000 rows. It took 3.85 s on a local PostgreSQL 16 container and 1.44 s on a SQLite file. The cipher adds about 0.15 s, and step 4 adds one more read of every row. MongoDB and Cassandra were not measured.
- On Cassandra a rotation needs every replica of the catalog keyspace up.
- The operator keeps the KEK readable only by the server's user, and keeps a copy of it apart from the catalog. A lost KEK loses every stored secret.
- SQLite, PostgreSQL and Cassandra never delete expired sessions (#971). The pass therefore reads every session row to find the ones that have not expired.
- After a move, a release without this change can run only from a catalog copy taken before the move (section 8).
- The move does not protect the secrets that older catalog copies hold (section 6).

## Alternatives

- Online rotation, while the servers serve. Each server would hold both DEKs of an unfinished ring and try the newest first. The pass would race with create, import, AssumeRole and delete on four backends. The old DEK could go only after every server loaded the new ring. The servers poll settings rows every 30 s already (`crates/server/src/workers.rs:39`), so a server could load the ring that way. In the proxy measurement of section 6, 100,000 rows took 3.85 s on PostgreSQL and 1.44 s on SQLite. The key ring can hold two DEKs, so a later RFC can add an online pass.
- One key from the file, and no DEK. The file key would encrypt the secrets, and the catalog would hold only its id. Every rotation of this RFC re-encrypts every row either way. Without a DEK, each rotation needs a new key file on every server host. The operator then keeps one file per rotation, for as long as a catalog copy under it is kept. A resumed KEK change would also need both key files. With a DEK it needs only the target file, because step 2 wraps both DEKs under it. A later hardware source would call the module on each credential lookup that misses the cache. At run time the DEK costs one call to wrap it and one to unwrap it, and in code about 60 lines (Libraries considered).
- A key id on each stored secret, in a new column or as a prefix of the stored bytes. The server holds one key, and the rotation command finds the key of a row by trial (section 4), so neither form has a reader. A column needs a breaking catalog migration on three backends, and the rollback of a breaking migration is a restore (`docs/adr/0023-catalog-migrations.md:366-369`). A prefix adds a second byte layout. With a prefix, a reader that tries the catalog key when the named key fails can tell old rows from new ones. Trial decryption gives the same result with no prefix. A later online rotation, where a server holds two DEKs, can add one.
- An `env` source, such as `EXTENDDB_ENCRYPTION_KEK`. A file covers the same deployments, for example Kubernetes secret volumes, Docker bind mounts and systemd credentials. The same user can read the environment of a process through `/proc/<pid>/environ`, and child processes inherit it.
- A `passphrase` source, with PBKDF2. For an unattended restart, the passphrase sits in a file or the environment, where it protects no more than 32 random bytes in a file. A catalog copy holds the salt and the wrapped DEK, so anyone with a copy can test guesses offline. A passphrase typed at each start rules out unattended restarts.
- A network key service: it fails the offline requirement.
- A built-in PKCS#11 source, through the `cryptoki` crate (0.12.1, Apache-2.0). It is the one source that gives what a file cannot: a key that root cannot export from the token. It costs a vendor module loaded at run time, a PIN file under the same file rule, and a SoftHSM2 job in CI. The mechanisms differ by token. tpm2-pkcs11 has no `C_UnwrapKey`, the YubiHSM 2 has no AES-GCM and no plain AES key wrap, and the Nitrokey HSM 2 has no AES. RSA-OAEP through `C_Decrypt` works on SoftHSM2, the YubiHSM 2 and tpm2-pkcs11, with the DEK wrapped under the public key of the token. A later RFC can add this source with its own threat model. The server would still unwrap the DEK once at startup, and the stored secrets would not change.
- A TPM source in Rust, through `tss-esapi`. It does what `systemd-creds --with-key=tpm2` does (section 2), and it adds the tpm2-tss C library, 4.1.3 or later, to every build.
- AES key wrap (RFC 3394), from `aws_lc_rs::key_wrap`. The existing GCM cipher already binds each wrapped DEK to its id through the AAD. Using it adds no second primitive.
- No change. Main documents the trust boundary (`docs/manuals/10-security-model.md:22`) and the manual rotation (`:66-70`). The catalog still decrypts every stored secret.

## Unresolved questions

- Is the downtime of an offline rotation acceptable for the largest deployment that reviewers expect? MongoDB and Cassandra were not measured.
- Should `rotate` also move a deployment back to the catalog key, so that a release without this change can run after a move without a restore?
- Should a follow-up encrypt the PostgreSQL connection string, with its password, under the DEK?

## Prior art

- Kubernetes documents envelope encryption for its KMS providers: data encryption keys encrypt the data, and key encryption keys outside etcd encrypt the data keys. Its key rotation adds the new key, rewrites every Secret, and then deletes the old key ([Encrypting Confidential Data at Rest](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)). Section 6 takes the same order.
- The Vault Transit secrets engine keeps a key ring with a version for each key. Each ciphertext names its key version, and `rewrap` re-encrypts a ciphertext under the newest version ([Transit secrets engine](https://developer.hashicorp.com/vault/docs/secrets/transit)). This RFC keeps the ring and leaves the version out of the ciphertext, because each server holds one key.
- PostgreSQL refuses a TLS private key that the group or other users can read. It allows group read when root owns the key ([Secure TCP/IP Connections with SSL](https://www.postgresql.org/docs/current/ssl-tcp.html)). The TLS key check of `serve`, which the file rule shares, allows group read when any other user owns the key.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
