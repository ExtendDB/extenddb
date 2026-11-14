# ExtendDB Cassandra Container

This directory documents the registry-neutral ExtendDB Cassandra container
image. The image contains the ExtendDB executable compiled with the
`cassandra` backend on a Debian bookworm-slim base. It does **not** contain
Cassandra, `cqlsh` or a Java runtime.

> **Experimental.** The Cassandra backend is experimental. Read the known
> limitations in [`docs/cassandra-setup.md`](../docs/cassandra-setup.md) and
> validate it against your workload before you rely on it.

Provide Apache Cassandra 4.1 or newer separately. `docs/cassandra-setup.md`
covers the cluster requirements (password authentication, the `datacenter1`
datacenter name, replication factor) and the backend's known limitations.

## Image contents and security defaults

The image:

* runs as UID/GID `10001:10001`;
* supports a read-only root filesystem;
* uses `tini` to forward SIGTERM;
* publishes only port 18443;
* stores configuration and TLS state under `/var/lib/extenddb`;
* uses `extenddb healthcheck` for liveness;
* contains no Cassandra server or client.

The healthcheck verifies that ExtendDB is alive. It does not continuously
verify Cassandra availability after startup; use the orchestrator's readiness
checks and Cassandra monitoring for that. `serve` exits if it cannot reach
Cassandra when it starts.

ExtendDB connects to Cassandra over plain TCP: the driver is built without
TLS, so credentials and item data cross the network in clear text. Run the
ExtendDB container and the Cassandra cluster on a private network, and do not
route Cassandra traffic across an untrusted one.

## Build locally

Generate the notices for the Cassandra feature set, then build the image:

```bash
devtools/generate-software-license-notices --backend cassandra
docker build \
  --file Dockerfile.cassandra \
  --build-arg VERSION="$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')" \
  --build-arg VCS_REF="$(git rev-parse --short=12 HEAD)" \
  --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t extenddb-cassandra:dev .
```

## Connect to an existing Cassandra cluster

The cluster must run `PasswordAuthenticator`, and its local datacenter must be
named `datacenter1`; `docs/cassandra-setup.md` explains why. The contact point
must be resolvable and reachable from the ExtendDB container.

**Run `CassandraAuthorizer`.** Under it, `init` grants the application role
only what ExtendDB needs: SELECT, MODIFY, CREATE and ALTER on the catalog
keyspace, those four and DROP on the default account's keyspace, and CREATE ON
ALL KEYSPACES, which CreateAccount needs and which also lets the role create
tables in any keyspace on the cluster. Under `AllowAllAuthorizer`, Cassandra's
default, `init` grants nothing, because every login role, including the
application role, can then read and drop any keyspace and change any role's
password, the superuser's included; use such a cluster for ExtendDB alone.
Anyone who can read `extenddb.toml` holds the application role's password.
A deployment created before 1.0 under `CassandraAuthorizer` needs the GRANT
statements in `docs/manuals/07-upgrade-manual.md` before it is upgraded.

Run `extenddb init` once against a writable state volume. It connects as a
Cassandra superuser, creates the catalog keyspace, the default account's
keyspace and an application role (`extenddb` by default), and writes
`/var/lib/extenddb/extenddb.toml` with the application role's credentials.
Pass the passwords through the environment rather than as arguments:

* `EXTENDDB_CASSANDRA_PASSWORD`: the superuser password `init` connects with
  (and `migrate` when it is given `--cassandra-user`).
* `EXTENDDB_APP_PASSWORD`: the password `init` gives the application role when
  it creates it. Always set it; without it the role gets the fixed password
  `extenddb-local-dev`. It may contain only ASCII letters, digits, space and
  ``-_.,!@#$%^&*()+=~` ``. If the role already exists, `init` does not change
  or check its password and writes the value you supplied into the config, so
  a wrong value surfaces only when `serve` fails to connect.
* `EXTENDDB_ADMIN_USER` and `EXTENDDB_ADMIN_PASSWORD`: the first ExtendDB
  administrator. Set both; with only the password set, `init` prints that
  password in its output, and so in the container's logs.

`init` writes only the first of the `--cassandra-contact-points` into
`extenddb.toml`; add the others to `contact_points` there afterwards. The file
is in the volume and the image has no editor, so edit it with `sed` from the
image itself, which runs as the file's owner (UID 10001) and keeps its 0600
mode:

```bash
docker run --rm -v extenddb-state:/v --entrypoint sed \
  extenddb/extenddb-cassandra:X.Y.Z -i \
  's/^contact_points = .*/contact_points = ["cassandra-1.example.internal:9042", "cassandra-2.example.internal:9042", "cassandra-3.example.internal:9042"]/' \
  /v/extenddb.toml
```

The examples use a named volume, `extenddb-state`. Docker creates it owned by
the image's `extenddb` user (UID 10001), which runs both commands. To use a
host directory instead, make it owned by UID 10001 first
(`sudo chown 10001:10001 <dir>`); `init` otherwise fails with
`Failed to create TLS directory ... Permission denied`.

```bash
docker run --rm \
  -v extenddb-state:/var/lib/extenddb \
  -e EXTENDDB_CASSANDRA_PASSWORD \
  -e EXTENDDB_APP_PASSWORD \
  -e EXTENDDB_ADMIN_USER=admin \
  -e EXTENDDB_ADMIN_PASSWORD \
  extenddb/extenddb-cassandra:X.Y.Z \
  init --backend cassandra \
  --config /var/lib/extenddb/extenddb.toml \
  --cassandra-contact-points cassandra.example.internal:9042 \
  --cassandra-user <superuser> \
  --bind-addr 0.0.0.0 \
  --tls-san localhost --tls-san 127.0.0.1

docker run -d --name extenddb-cassandra \
  -p 127.0.0.1:18443:18443 \
  -v extenddb-state:/var/lib/extenddb \
  --read-only --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --cap-drop ALL --security-opt no-new-privileges:true \
  extenddb/extenddb-cassandra:X.Y.Z
```

Clients need the self-signed certificate `init` created. Copy it out of the
volume with:

```bash
docker run --rm -v extenddb-state:/v:ro --entrypoint cat \
  extenddb/extenddb-cassandra:X.Y.Z /v/.extenddb/tls/cert.pem > extenddb-cert.pem
```

### Replication factor

`init` creates its keyspaces with replication factor 1 and has no flag to
change that. On a multi-node cluster, follow "Keyspace replication" in
[`docs/cassandra-setup.md`](../docs/cassandra-setup.md#keyspace-replication):
set `replication_factor` in `extenddb.toml` and restart the serving
container, raise the replication of `system_auth` and of the keyspaces `init`
created, then repair. The config edit, with the volume and image from above:

```bash
docker run --rm -v extenddb-state:/v --entrypoint sed \
  extenddb/extenddb-cassandra:X.Y.Z -i \
  's/^replication_factor = .*/replication_factor = 3/' /v/extenddb.toml
docker restart extenddb-cassandra
```

### Production

Run one serving ExtendDB instance per catalog. Every container binds
`0.0.0.0:18443`, from which ExtendDB derives its stream node id, so two
replicas would share one; see "Multi-Instance Considerations" in
[`docs/manuals/11-deployment-guide.md`](../docs/manuals/11-deployment-guide.md).

Inject credentials through the orchestrator's secret store, pin a version or
digest, and run `init` and `migrate` as separate one-shot containers or Jobs.
The serving container needs only the generated `extenddb.toml` and TLS files.
Do not give it the superuser password. Under `AllowAllAuthorizer` this keeps
the password out of the serving container but does not limit what the
application role can do (see above).

### Upgrade

Stop the serving container, then run one migration container with the new
image. `migrate` connects as the application role in `extenddb.toml`.
`migrate --yes` waits up to `--lock-timeout` seconds (60 by default) for every
running server to stop, then exits 6 naming them, and a server does not start
while a migration runs:

```bash
extenddb migrate --config /var/lib/extenddb/extenddb.toml --yes
```

Without `--yes` it lists what is pending and changes nothing; it exits 0 when
the catalog is up to date and 3 when migrations are pending. Pass
`--cassandra-user <role>` with `EXTENDDB_CASSANDRA_PASSWORD` to run it as
another role.

## Local reference stack

For users who want Cassandra and ExtendDB launched together locally, the
repository provides an optional reference stack, `docker-compose.cassandra.yml`.

This stack is for local development only. It runs one Cassandra node with
replication factor 1 and a 1 GB heap (about 1.6 GB of memory in use), so give
Docker at least 3 GB. Cassandra publishes no port on the host's interfaces;
ExtendDB is published on `127.0.0.1` only. The stack enables
`PasswordAuthenticator` and `CassandraAuthorizer`, replaces the stock
`cassandra` superuser password on first start, and refuses to start until you
set all three passwords:

```bash
export CASSANDRA_SUPERUSER_PASSWORD='choose-a-password'
export EXTENDDB_CASSANDRA_APP_PASSWORD='choose-another-password'
export EXTENDDB_ADMIN_PASSWORD='choose-a-third-password'
export EXTENDDB_VERSION="$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')"
export VCS_REF="$(git rev-parse --short=12 HEAD)"
export BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
docker compose -f docker-compose.cassandra.yml up -d --build
```

`EXTENDDB_CASSANDRA_APP_PASSWORD` has the character limits of
`EXTENDDB_APP_PASSWORD` above. `CASSANDRA_SUPERUSER_PASSWORD` must not be
`cassandra`, contain a line break, or start or end with whitespace.

To run a published image instead of building one, set
`EXTENDDB_CASSANDRA_IMAGE=extenddb/extenddb-cassandra:X.Y.Z` and use
`up -d --no-build`. Other settings you can override: `EXTENDDB_PORT` (default
18443), `EXTENDDB_ADMIN_USER` (default `admin`), `CASSANDRA_MAX_HEAP_SIZE`,
`CASSANDRA_HEAP_NEWSIZE` and `CASSANDRA_IMAGE`.

Each `up` runs `extenddb init` if the state volume has no config yet.
Otherwise it runs the `extenddb migrate` preview and, only when migrations are
pending, `extenddb migrate --yes`. That waits for a running server, so to move
the stack to a new image that has migrations, run
`docker compose -f docker-compose.cassandra.yml stop extenddb` first; an `up`
with ExtendDB still running fails after 60 s and changes nothing.

Keep the same `CASSANDRA_SUPERUSER_PASSWORD` across starts: the stock password
is replaced only once, and later starts log in with yours. Cassandra takes
about a minute to accept logins on first start. If a one-shot service fails,
`docker compose ... logs cassandra-auth-init extenddb-bootstrap` shows why.
Compose resolves the three variables for every command run with `-f`, `down`,
`ps` and `logs` included. Rather than exporting them in every shell, put them
in an untracked `.env` file next to the compose file (`.gitignore` already
lists it), with mode 0600:

```
CASSANDRA_SUPERUSER_PASSWORD=...
EXTENDDB_CASSANDRA_APP_PASSWORD=...
EXTENDDB_ADMIN_PASSWORD=...
```

`down -v` removes both volumes and starts the stack over.

## Automated smoke test

The smoke test builds the image, starts the reference stack with random
passwords, verifies Cassandra authentication and that Cassandra is not
published, checks image identity and hardening, performs a DynamoDB API round
trip, and verifies persistence after restarting ExtendDB and after recreating
the stack, where the bootstrap runs the migrate preview against the existing
catalog:

```bash
ci/smoke-test-cassandra-container.sh
```

The `Cassandra image smoke` workflow runs the same script, in build mode, on
every pull request that changes the Dockerfile, the compose file or the
script, on an amd64 and an arm64 runner. `release-image` runs it again against
the image it is about to publish.

To test an already-built image without rebuilding it:

```bash
EXTENDDB_CASSANDRA_IMAGE=extenddb-cassandra:dev \
  ci/smoke-test-cassandra-container.sh
```
