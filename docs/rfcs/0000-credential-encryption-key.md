# RFC-0000: Wrap the credential encryption key with a key kept outside the catalog, and rotate it

- Status: Draft
- Author: @LeeroyHannigan
- Created: 2026-11-11
- FCP ends: not set
- Tracking issue: #707

## Summary

ExtendDB encrypts each stored access key secret and each AssumeRole session secret with one AES-256-GCM key. The catalog stores that key unwrapped, in the same database as the secrets it protects.

This RFC adds a second key source, `file`. Under it, a key-encryption key (KEK) in a file on the server host wraps the data encryption key (DEK) that encrypts the secrets. The catalog then holds only the wrapped DEK.

A new offline command, `extenddb encryption-key rotate`, moves a deployment to the `file` source and later replaces the DEK. It re-encrypts every stored secret and changes no access key ID or secret, so clients keep their credentials.

The default source, `catalog`, keeps today's behavior and today's protection. No key source uses a network key service.

## Motivation

On main at 870962647:

- `extenddb init` writes the key as the `encryption_key` row of the catalog `settings` table, in base64, on all four backends (`crates/storage-postgres/src/bootstrapper.rs:226-254` and the matching bootstrappers). The security model states the result: read access to the catalog database "yields every stored access key secret" (`docs/manuals/10-security-model.md:22`).
- The stored format is `nonce(12) || ciphertext || tag(16)`, with the access key ID as the additional authenticated data (AAD) (`crates/storage/src/secret_cipher.rs:4-16`). No byte records which key encrypted a row.
- Each server reads the key once at startup and keeps it until it exits (`crates/storage-postgres/src/lib.rs:953-967` and the matching factories). `extenddb settings set` refuses the key (`crates/app/src/cmd_settings.rs:179-194`).
- The only rotation is the manual procedure at `docs/manuals/10-security-model.md:66-70`. The operator replaces the row, restarts every server, then deletes and re-creates every access key. The comment on #707 describes the result as an outage for every signed request, until each client has new credentials.

On #228 (2026-08-05), a code owner wrote that the current handling "is probably not sufficient for many production uses". The same review asked for other mechanisms, such as an HSM or a passcode. ExtendDB runs with no internet connection (`README.md:17`, `docs/manuals/11-deployment-guide.md:148-154`), so no key source may depend on a network key service.

## Detailed design

### Terms

- DEK (data encryption key): the 32-byte AES-256-GCM key that encrypts stored secrets.
- KEK (key-encryption key): a 32-byte key that encrypts (wraps) a DEK.
- Catalog key: the key in the `encryption_key` row. It is the only key of the `catalog` source.
- Key source: where the server gets its key at startup.
- Key ring: the settings row `encryption_key_ring`, which holds the wrapped DEKs and the id of the KEK that wraps them.

### 1. Key sources

A new `[auth.encryption]` section of `extenddb.toml` names the source:

```toml
[auth.encryption]
# "catalog" (default) or "file"
key_source = "file"
key_file = "/etc/extenddb/kek"
```

| Source | Key the server uses | Where the key is kept |
|---|---|---|
| `catalog` | the catalog key | the `encryption_key` row of the catalog `settings` table, as today |
| `file` | the newest DEK in the key ring, unwrapped with the KEK | the KEK in `key_file`, the wrapped DEK in the key ring |

The `catalog` source is the default, so an upgrade changes nothing until the operator moves the deployment.

The KEK file holds 32 random bytes in standard base64. The server strips one trailing newline, so `head -c 32 /dev/urandom | base64 > kek`, run under `umask 077`, writes a file that the server accepts.

The server refuses to start when other users can read the KEK file. It accepts mode 0600 or 0400 when the server's user owns the file, and mode 0640 or 0440 when root owns it. PostgreSQL applies the same rule to its TLS private key. A Kubernetes secret volume uses mode 0644 unless `defaultMode` is set ([Kubernetes volume reference](https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/volume/)). The deployment docs therefore give a `defaultMode` that meets the rule.

This rule is stricter than the rule for the TLS private key, which narrows the mode and warns (`crates/app/src/serve_helpers.rs:185-187`). A new setting has no deployments that a refusal can break. Users who could read the file before may hold a copy of the KEK, and narrowing the mode later does not change that.

The server reads the KEK file once at startup, unwraps the DEK, and then drops the KEK from memory. A KEK file that does not decode gets a fixed message. The base64 decoder's own error text names the bad character and its position, so the message leaves it out.

### 2. The key ring

Under the `file` source the catalog holds the DEK in one settings row, `encryption_key_ring`:

```json
{"kek_id":"3f9a0c51e2d47b86","keys":[{"id":2,"wrapped":"<base64>"}]}
```

- `wrapped` is the DEK, in base64, sealed with the existing cipher: `secret_cipher::encrypt_secret(dek, kek, "dek:<id>")` (`crates/storage/src/secret_cipher.rs:37`). The AAD binds the wrapped DEK to its id, so a wrapped value copied to another id does not open.
- Every access key ID starts with `AKIA` or `ASIA`: created IDs start with `AKIAEXTENDDB`, and imported and dev IDs must start with `AKIA` (`crates/server/src/management/iam_user_self.rs:269-276`, `crates/server/src/dev_credentials.rs:129-135`). Session IDs start with `ASIAEXTENDDB` (`crates/server/src/management/crypto.rs:28-39`). No stored secret therefore has the AAD of a wrapped DEK.
- `kek_id` is the first 8 bytes, in hex, of HMAC-SHA256 keyed with the KEK over the text `extenddb key-encryption-key id`. It names the KEK and does not reveal it. `aws_lc_rs::hmac` computes HMAC-SHA256 for SigV4 already (`crates/auth/src/sigv4/signing_key.rs:37-43`).
- DEK ids start at 1. Each rotation adds the next id, so no two DEKs share an id. The newest DEK is the active one.
- One row holds the whole ring. Each change to the ring is a write of one row, which is atomic on all four backends, Cassandra included.
- The row name contains `encryption_key`, so `extenddb settings list`, the management API and the console mask its value as they mask the catalog key (`crates/config/src/display.rs:13-36`). `extenddb settings set` refuses it, as it refuses every key outside `KNOWN_KEYS` (`crates/app/src/cmd_settings.rs:174-194`).

Under the `catalog` source the catalog has no key ring.

### 3. A key id on each stored secret

Migration 11 adds a nullable integer column, `key_id`, to `access_keys` and `iam_sessions` on SQLite, PostgreSQL and Cassandra. On MongoDB a document takes a `key_id` field with no schema change, because the catalog collections have no validator (`crates/storage-mongodb/src/migrations/mod.rs:175-178`).

- A null `key_id` means the catalog key. Every row written before the upgrade has a null `key_id`, so no existing row changes.
- Under the `catalog` source, writers keep `key_id` null. Under the `file` source, writers store the id of the active DEK.
- A reader decrypts a row with the key that the row names. The AAD stays the access key ID, so no existing ciphertext changes.
- A row can name a key that the server does not hold. Such a row fails as a decrypt failure fails today, with HTTP 500 `InternalServerError` (`crates/storage-sqlite/src/credential_store.rs:54-60` and the matching stores). The log line that names the access key ID also names the key id. Only a row that the rotation command could not decrypt names such a key (section 5).

With the column, a reader finds the key of a row without trial decryption. A prefix in the stored bytes cannot do that. The current bytes start with a random nonce, so no prefix tells an old row from a new one.

Migration 11 changes the schema only, so it needs no key. That holds on a new catalog too, where `init` writes the catalog key after the migrations (`crates/app/src/cmd_init.rs:357`, `:368`). It follows migration 9, which adds a nullable column in the same way:

- SQLite: a code body that adds the column only when it is missing (`crates/storage-sqlite/src/migrations/m0009_warm_throughput.rs`).
- PostgreSQL and Cassandra: file bodies (`crates/storage-postgres/migrations/0009_warm_throughput.sql`, `crates/storage-cassandra/migrations/catalog/0009_warm_throughput.cql`).
- MongoDB: no step.

Migration 11 is convergent. It is `Kind::Breaking`, because declaring a migration additive needs the old-binary job, which does not exist yet (`CONTRIBUTING.md:288-299`).

### 4. Startup

Each backend factory reads the `encryption_key` row and the `encryption_key_ring` row. It passes both, with the configured source, to one function in `crates/storage`, beside the key check that #863 adds there. The function returns the one key that the server uses, or a reason to refuse:

| The catalog holds | Configured source | Result |
|---|---|---|
| the `encryption_key` row, no key ring | `catalog` | Start with the catalog key, as today. |
| the `encryption_key` row, no key ring | `file` | Refuse. The catalog key is still in the catalog. Name `rotate --to-key-file`. |
| a key ring with one DEK, no `encryption_key` row | `file`, with the KEK that the ring names | Start with that DEK. |
| a key ring with one DEK, no `encryption_key` row | `file`, with another KEK | Refuse, and name both KEK ids. |
| a key ring with one DEK, no `encryption_key` row | `catalog` | Refuse, and name the `file` source and the KEK id. |
| a key ring with more than one DEK, or a key ring and the `encryption_key` row | either | Refuse. A rotation did not finish. Name the command that finishes it. |
| neither row | either | Refuse with `MissingEncryptionKey`, as today. |

- A damaged catalog key, KEK file or key ring also stops the server. The refusals reuse `MissingEncryptionKey` and the `InvalidEncryptionKey` of #863 with the source in the text. One new error variant carries the reason for a mismatch. No message, log line or settings response contains key material.
- The server holds one key, in one zeroizing buffer. The catalog store, the credential store and AssumeRole share it by `Arc`. Today each of them holds its own copy (`crates/storage-sqlite/src/lib.rs:443-450`, `crates/server/src/management/assume_role.rs:292-308`).
- Today some code reads the `encryption_key` row when its store holds no key. This change deletes that code: `crates/server/src/management/assume_role.rs:292-308`, `crates/storage-sqlite/src/management_store/access_keys.rs:20-33`, `crates/storage-postgres/src/management_store/access_keys.rs:21-34` and `:120-133`, `crates/storage-mongodb/src/management_store.rs:2484-2499`, and `crates/storage-cassandra/src/management_store/access_keys.rs:25-44` and `:173-192`. A store built without the key refuses to create, import or store a credential. The `settings` and `migrate` commands and the migration bodies use such stores, and none of them writes a credential.
- The factory takes the key source as a new argument, not as a field of the `Copy` options struct (`crates/storage/src/server_components.rs:86-135`). `serve` passes it. The `extenddb backup` commands pass none (`crates/app/src/cmd_backup.rs:602-625`). They read no credential, so they read neither key row and do not need the KEK file.
- `init` and the SQLite dev bootstrap write the `encryption_key` row only when the catalog has neither that row nor a key ring (`crates/storage/src/bootstrapper.rs:159-170`, `crates/storage-sqlite/src/store.rs:266-280`). Without this rule, a dev build would write a new catalog key into a catalog that moved to the `file` source.

### 5. Rotation

```text
extenddb encryption-key rotate --config extenddb.toml [--to-key-file <path>] [--yes]
```

Without `--yes`, the command previews. It reads the catalog, decrypts every stored secret in memory with the key that the row names, and prints what `--yes` would do. It writes nothing, so it can run beside a server, as the `migrate` preview does. It exits 3 when there is work (`docs/adr/0023-catalog-migrations.md:249-250`, `:356`).

With `--yes`, it takes the migration lock and waits for registered servers to stop, as `migrate --yes` does. It takes the `--lock-timeout` option of `migrate --yes`, waits up to that limit, and then exits 6 (`docs/adr/0023-catalog-migrations.md:246-250`). Then it takes these steps:

1. It reads the current keys with the configured source. It reads the target KEK from `--to-key-file`, or from `key_file` when the option is absent. It refuses a `catalog` target, because a KEK kept in the catalog protects nothing from a reader of the catalog.
2. It writes the key ring wrapped under the target KEK, with a new DEK as the newest entry. A catalog in one of the unfinished states of section 4 shows that an earlier run stopped. This run then resumes with the newest DEK of the ring as the target. It opens that ring with the KEK that the ring names, which must be the configured KEK or the target KEK.
3. It re-encrypts every `access_keys` row, and every `iam_sessions` row that has not expired, under the new DEK.
4. It writes the key ring with the new DEK alone.
5. It deletes the `encryption_key` row, if the catalog has one.
6. It prints the target KEK id, the number of rows it re-encrypted per table, the rows it left, and the `[auth.encryption]` lines for the configuration. It writes one audit line with the DEK id and the KEK id, as `extenddb settings set` writes one (`crates/app/src/cmd_settings.rs:204-208`).

The operator then changes `[auth.encryption]`, if the source changed, and starts the server.

In step 3:

- Expired sessions keep their old key. Each credential store checks the expiry of a session before it decrypts its secret (`crates/storage-sqlite/src/credential_store.rs:95-103` and the matching stores). A session lasts at most 43,200 s (`crates/server/src/management/assume_role.rs:98`).
- Each write is conditional on the ciphertext that the command read. The command leaves and counts a row that changed or was deleted meanwhile.
- The command leaves a row that does not decrypt as it is, and lists it by access key ID. Such a row fails today on every request, and it fails in the same way after the rotation.
- On SQLite, PostgreSQL and MongoDB the writes go in batches, with one transaction per batch.

A run that stops at any step can run again with the same arguments. Each row records its key id, so the rows that name an older key are the remaining work. Until a run finishes, the server refuses to start (section 4).

Per backend:

- SQLite: the migration lock is the `flock` that `serve` also holds, so no server runs during the pass (`docs/adr/0023-catalog-migrations.md:216`).
- PostgreSQL: each update is `UPDATE ... SET secret_key_encrypted = $1, key_id = $2 WHERE <primary key> = $3 AND secret_key_encrypted = $4`.
- MongoDB: each update filters on the `_id` of the document and on its old ciphertext, because nothing makes `access_key_id` unique in `access_keys` (`crates/storage-mongodb/src/migrations/m0001_baseline.rs:102`). The writes use the existing majority write concern (`crates/storage-mongodb/src/lib.rs:568`).
- Cassandra: each row is a lightweight transaction, `UPDATE ... IF secret_key_encrypted = ?`. A row that was deleted cannot meet that condition, so the update creates no partial row. The server reads credentials at `ONE` (`crates/storage-cassandra/src/credential_store.rs:85-88`). The command therefore reads at `ALL` and commits at `ALL`, so every replica holds the new ciphertext before the old key goes. It needs every replica of the catalog keyspace up, and it stops when one is down. The `cassandra_util` helpers take the consistency as an argument, instead of one more copy per level (`crates/storage-cassandra/src/cassandra_util.rs:121-135`, `:401-432`).

A proxy measurement of the pass ran on a shared 16-CPU host. A Python script rewrote 100,000 rows in batches of 1,000, each write conditional on the old ciphertext, with random bytes in place of the cipher. On PostgreSQL each batch was one `UPDATE` statement, and on SQLite one transaction of single-row updates. A local PostgreSQL 16 container took 3.85 s, and a SQLite file took 1.44 s. MongoDB, Cassandra and a PostgreSQL server across a network were not measured.

What a rotation protects, and what it does not:

- After a move to the `file` source, a logical copy of the catalog holds no unwrapped key. Examples are `pg_dump`, `mongodump`, an SQLite `.dump` and a CQL export.
- A copy taken before the move holds the catalog key and the ciphertext of each secret that existed then. It decrypts those secrets with nothing else.
- A physical copy can hold the deleted `encryption_key` row too. On PostgreSQL that is a write-ahead log archive, or a page that `VACUUM` has not reclaimed. On the other backends it is SQLite free pages, a Cassandra SSTable or snapshot before compaction, or a MongoDB oplog entry. To end that exposure, re-create the access keys that existed before the move. Sessions expire within 12 hours.
- A later rotation protects the secrets from a holder of an older DEK or KEK who gets a new copy of the catalog. It does not protect a secret whose ciphertext and key leaked together: re-create that access key.
- `init` creates no access key (`crates/storage/src/bootstrapper.rs:159-170`). A deployment that moves before its first `serve` therefore has no secret under the catalog key. A new deployment that wants the `file` source runs `init`, then `rotate --to-key-file <path> --yes`, then `serve`.
- On PostgreSQL a catalog copy also holds `data_database_connection_string`. It carries the password of the application role, which opens the catalog database and the data database (`docs/manuals/10-security-model.md:22`, `crates/storage-postgres/src/bootstrapper.rs:96-120`).
- Every catalog also holds the bcrypt hashes of console passwords and the session tokens in plaintext. This RFC changes neither.

### 6. Several servers

In 1.0, running more than one server on a catalog is unsupported, and only SQLite refuses a second one (`README.md:32`, `docs/manuals/11-deployment-guide.md:300`). The rotation command runs only while no server is registered. Every server therefore starts again after the rotation and loads the new key. No server needs a reload signal or a poll of the catalog.

### 7. Upgrade and rollback

- `extenddb migrate --yes` applies migration 11. The deployment keeps the `catalog` source and today's protection until the operator moves it.
- A 1.x binary without migration 11 refuses the migrated catalog with exit 4, because migration 11 is breaking (`CONTRIBUTING.md:283-286`).
- Releases before 1.0 refuse a 1.0 catalog or leave a trace in it (`docs/adr/0023-catalog-migrations.md:370-384`). If one starts anyway under the `catalog` source, it reads and writes as before, because a null `key_id` means the catalog key. After a move, it finds no `encryption_key` row and refuses to start.
- A SQLite dev image from before 1.0 may write a new `encryption_key` row at startup. A server with this change then refuses to start until `rotate --yes` runs again.
- A rollback restores the pre-upgrade backup of every store that the backend uses (`docs/adr/0023-catalog-migrations.md:366-369`). Every catalog write since the upgrade is lost, and the restored catalog holds the catalog key again.
- A rollback also deletes `[auth.encryption]` and any `EXTENDDB__AUTH__ENCRYPTION__*` override from the configuration. The previous binary refuses an unknown field in `[auth]` (`crates/config/src/lib.rs:275-287`), as the upgrade manual says for `[backup]` (`docs/manuals/07-upgrade-manual.md:79`).
- The upgrade manual has no tested rollback for Cassandra (`docs/manuals/07-upgrade-manual.md:224`).

### 8. Backups and restores

- Table backups (`CreateBackup`) hold no credentials and do not change (`docs/manuals/05-admin-guide.md:1083`).
- Under the `file` source, a catalog copy holds the key ring and its KEK id. A restore needs the KEK that the id names. The server refuses a catalog whose ring names another KEK, and prints both ids (section 4).
- Keep each KEK for as long as any catalog copy wrapped under it is kept. A rotation to a new KEK does not change older copies.
- A copy taken before the move needs `key_source = "catalog"`.
- A restore into another deployment needs the KEK file on the new host and the same `[auth.encryption]` section.
- A lost KEK loses every stored secret of each catalog copy wrapped under it. Each access key is then re-created, or imported under its old ID from the secret that its client holds (`docs/manuals/14-backup-and-restore.md:548-612`). Back up the KEK apart from the catalog.

### 9. What operators do differently

- Create the KEK file under `umask 077` (section 1), readable only by the server's user. On Kubernetes, mount the secret with a `defaultMode` that meets the file rule.
- Run `extenddb encryption-key rotate --to-key-file <path>` without `--yes` to preview. Stop every server, run it with `--yes`, set `[auth.encryption]`, and start the servers.
- Give the KEK file to `serve`. The `extenddb backup` commands do not need it.
- Keep each old KEK while a catalog copy wrapped under it is kept.
- Re-create the access keys that existed before the move, to end the exposure of older copies (section 5).
- After `init --overwrite`, put `[auth.encryption]` back. That command keeps only the storage settings (`docs/manuals/05-admin-guide.md:175`).
- Delete `[auth.encryption]` from the configuration before a rollback (section 7).
- Keep catalog dumps under `umask 077`. On PostgreSQL they still hold the password of the application role (`docs/manuals/07-upgrade-manual.md:105`).
- A rotation that did not finish stops the server at its next start, with a message that names the command. The preview reports the rows per key id. The change adds no metric.

### 10. Docs to change

Line numbers are on main at 870962647.

- Statements that this change makes false:
  - `docs/manuals/10-security-model.md:22`, `:23`, `:29`, `:62-70`, `:331` and `:357`
  - `docs/manuals/02-design-guide.md:26`, `:109`, `:129`, `:131` and `:193`
  - `docs/design/08-component-config.md:123-124`, and the table of environment variables at `:47-58`, which needs the `auth.encryption` keys
  - `docs/manuals/05-admin-guide.md:318-325` and `:1092`, `docs/manuals/07-upgrade-manual.md:105`, `docs/manuals/14-backup-and-restore.md:33-40`
  - `docs/design/04-component-storage.md:195-198`, `docs/design/05-component-auth-v3.md:404-410`, `docs/design/06-component-server.md:474`, `docs/design/13-storage-mongodb.md:112-115` and `:145-147`
  - `docs/getting-started.md:90`, `docs/manuals/04-quickstart-setup-guide.md:141`, `docs/manuals/08-install-linux.md:86`, `docs/manuals/09-install-macos.md:70`
  - `docs/adr/0026-credential-crypto-on-aws-lc.md:85-86`
- Statements already false on main, in paragraphs that this change rewrites: `docs/troubleshooting.md:149`, `:595-611`, `:655-657`, `:797-799` and `:897-901`, and `docs/design/05-component-auth.md:37` and `:1036-1039`.
- Code comments: `crates/storage/src/secret_cipher.rs:7-9`, `crates/storage/src/lib.rs:781-783`, `crates/storage-postgres/src/catalog_store.rs:24-25`, `crates/storage-cassandra/src/catalog_store.rs:46-47` and `crates/storage-cassandra/src/bootstrapper.rs:552`.
- New text:
  - migration 11 beside `docs/manuals/07-upgrade-manual.md:7-11`, and the rollback step beside `:79`
  - the KEK file in the air-gapped transfer list (`docs/manuals/11-deployment-guide.md:150-154`) and in the Kubernetes mount (`:146`)
  - the container steps (`docker/README.md:124-143`)
  - the configuration and command reference in the admin guide
  - a troubleshooting entry for each new startup refusal
- `docs/rfcs/0000-mongodb-backend.md:179` is a record and stays as it is. #863 changes `docs/manuals/10-security-model.md:67`.

### 11. Implementation order

Three pull requests, each with its own docs, ship in one release:

1. Migration 11, the shared startup function and the one shared key, for the `catalog` source only. The pull request also deletes the fallback reads and adds the new factory argument. The released-upgrade tests and `docs/schema/<backend>.txt` change with the migration (`CONTRIBUTING.md:329-341`). No request or response changes. Crates: `storage`, `storage-sqlite`, `storage-postgres`, `storage-mongodb`, `storage-cassandra`, `server` and `app`.
2. The `file` source, its file rule and the KEK id. The bootstrap rule of section 4, the startup refusals, and `extenddb encryption-key rotate` on SQLite and PostgreSQL. Crates: `config`, `storage`, `storage-sqlite`, `storage-postgres` and `app`.
3. `rotate` on MongoDB and Cassandra, and the consistency argument of the Cassandra helpers. Crates: `storage-mongodb` and `storage-cassandra`.

## Answers to the questions on #707

- Which providers come first, and where is a provider configured? The `file` source comes first, in the `[auth.encryption]` section of `extenddb.toml` (section 1). There is no plugin interface, and the sources are part of the binary. A source wraps and unwraps a DEK, and it does not give its KEK to the rest of the server. A later source can therefore keep its KEK inside a hardware module, for example through PKCS#11, with no change to the stored format.
- What does the server do when the provider cannot be reached at startup? It refuses to start, and it names the source and the reason (section 4).
- Does it keep the unwrapped key in memory? Yes. Each credential lookup that misses the cache needs the DEK, so the server holds one copy, in a zeroizing buffer, until it exits. It drops the KEK after startup. The credential cache keeps decrypted secrets as it does today (`crates/auth/src/lib.rs:55-83`).
- How does an existing deployment move? `rotate --to-key-file` re-encrypts every secret under a new DEK and deletes the catalog key (section 5). Wrapping the catalog key in place would let the catalog key from any older copy decrypt the secrets in every later copy.
- How does a rotation treat secrets written while it runs, on each backend? No server writes a secret while it runs. The command runs only while no server is registered, and each write is conditional on the ciphertext that it read (section 5).
- What do backup and restore carry, and what does a restore into another deployment need? Section 8.
- Should each stored secret record which key encrypted it? Yes, in the `key_id` column (section 3).
- How does each running server learn of the new key? By a restart. The command runs only while every server is stopped (section 6).
- Should the decrypted secrets in each credential cache be flushed? A restart starts with empty caches (`docs/manuals/10-security-model.md:126`). A rotation also changes no plaintext secret, so a cached secret stays valid.

## Drawbacks

- A rotation stops every server for the length of the pass. In the proxy measurement of section 5, 100,000 rows took 3.85 s on PostgreSQL and 1.44 s on SQLite. MongoDB and Cassandra were not measured.
- On Cassandra a rotation needs every replica of the catalog keyspace up.
- The operator keeps the KEK readable only by the server's user, and backs it up apart from the catalog. A lost KEK loses every stored secret.
- SQLite, PostgreSQL and Cassandra never delete expired sessions. The pass therefore reads every session row to find the ones that have not expired.
- Migration 11 is breaking, so its rollback is a restore.
- The move does not protect the secrets that older catalog copies hold (section 5).

## Alternatives

- Online rotation, while the server serves: the servers keep serving during the pass. It needs a server that holds several DEKs, and a management endpoint with its authorization. It also needs conditional writes that handle a concurrent create, import, AssumeRole and delete on four backends. On Cassandra it also needs a consistency rule for the pass and for the credential reads. In the proxy measurement of section 5, the offline pass rewrote 100,000 rows in 3.85 s on PostgreSQL and in 1.44 s on SQLite. The `key_id` column and the key ring let a later RFC add an online pass.
- One KEK from the file, and no DEK: the file key encrypts the secrets directly, and the catalog holds no key ring. A later hardware source then needs a call into the module on each credential lookup that misses the cache. With a DEK, the server calls the module once, at startup.
- An `env` source, such as `EXTENDDB_ENCRYPTION_KEK`: a file covers the same deployments, for example Kubernetes secret volumes, Docker bind mounts and systemd credentials. The environment of a process is readable through `/proc/<pid>/environ` by the same user, and child processes inherit it. A later release can add it.
- A `passphrase` source, with PBKDF2: for an unattended restart, the passphrase sits in a file or the environment. There it protects no better than 32 random bytes in a file. A catalog copy holds the salt and the wrapped DEK, so anyone with a copy can test guesses offline. A passphrase typed at each start rules out unattended restarts.
- A network key service: it fails the offline requirement. A hardware module on the host can be a later source (Answers).
- A key id prefix in the stored bytes: no column changes, but no prefix can tell an old row from a new one (section 3).
- AES key wrap (RFC 3394), from `aws_lc_rs::key_wrap`: the existing GCM cipher already binds each wrapped DEK to its id through the AAD. Using it adds no second primitive.
- No change: main documents the trust boundary (`docs/manuals/10-security-model.md:22`) and the manual rotation (`:66-70`). The catalog still decrypts every stored secret.

## Unresolved questions

- Which release carries migration 11 and the three pull requests? `RELEASED_THROUGH` is 0, and the 1.0 release pull request raises it to 10 (`crates/storage/src/migrations/registry.rs:108-111`).
- Is the downtime of an offline rotation acceptable for the largest deployment that reviewers expect? On Cassandra each row is a lightweight transaction, and its rate was not measured.
- Should a follow-up encrypt the PostgreSQL connection string, with its password, under the DEK?

## Prior art

- Kubernetes documents envelope encryption for its KMS providers: data encryption keys encrypt the data, and key encryption keys outside etcd encrypt the data keys. Its key rotation adds the new key, rewrites every Secret, and then deletes the old key ([Encrypting Confidential Data at Rest](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)). Section 5 takes the same order.
- The Vault Transit secrets engine keeps a key ring with a version for each key. Each ciphertext names its key version, and `rewrap` re-encrypts a ciphertext under the newest version ([Transit secrets engine](https://developer.hashicorp.com/vault/docs/secrets/transit)). The `key_id` column has the role of that version.
- PostgreSQL refuses a TLS private key that the group or other users can read. A key that root owns may also give its group read access ([Secure TCP/IP Connections with SSL](https://www.postgresql.org/docs/current/ssl-tcp.html)). The KEK file rule follows it.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is
a trademark of Amazon.com, Inc.
