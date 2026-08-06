# ExtendDB PostgreSQL Container

This directory documents the registry-neutral Phase B container image. Registry
publishing, signing, provenance, and release-tag automation are separate work.

## Image contents

The image contains the ExtendDB executable compiled with the `postgres` backend,
CA certificates, `tini`, the project license/notice, and generated Rust dependency
notices. It does **not** contain a PostgreSQL server or PostgreSQL data directory.

Provide PostgreSQL 14 or newer separately through RDS, Aurora PostgreSQL,
self-managed PostgreSQL, or the local Compose service.

## Security defaults

* Runs as UID/GID `10001:10001`.
* Supports a read-only root filesystem.
* Uses an exec-form entrypoint and forwards SIGTERM through `tini`.
* Removes setuid/setgid bits from runtime executables.
* Publishes only port 18443; the local Compose mapping binds to `127.0.0.1`.
* Writes configuration and TLS state only under `/var/lib/extenddb`, which must
  be a writable volume for `init` and read/write state management.
* Sends foreground logs to the container runtime.
* Uses `extenddb healthcheck` for liveness. This does not verify PostgreSQL
  readiness after startup.

## Build locally

Generate notices first if dependency metadata changed:

```bash
devtools/generate-third-party-notices
```

Build with explicit source metadata:

```bash
docker build \
  --build-arg VERSION="$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')" \
  --build-arg VCS_REF="$(git rev-parse --short=12 HEAD)" \
  --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -t extenddb-postgres:dev .
```

The Dockerfile pins both build and runtime images by digest. Update those digests
only in a reviewed change, and rebuild when security updates are available.

## One-command local development

The Compose file starts four roles:

1. `postgres`: a separate PostgreSQL 16.10 service.
2. `extenddb-volume-init`: gives UID 10001 ownership of the named state volume.
3. `extenddb-bootstrap`: runs `init` once, or `migrate --yes` on later starts.
4. `extenddb`: runs only `serve --foreground` without PostgreSQL admin secrets.

Start the stack:

```bash
export VCS_REF="$(git rev-parse --short=12 HEAD)"
export BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
docker compose up -d --build
```

Check status and logs:

```bash
docker compose ps
docker compose logs extenddb-bootstrap
docker compose logs -f extenddb
```

Stop the stack but keep data:

```bash
docker compose down
```

Delete all local data:

```bash
docker compose down -v
```

The passwords in `docker-compose.yml` are development defaults. Override them
through shell variables or an untracked `.env` file. Never use them in a shared
or production environment.

## Automated local smoke test

The smoke test builds one uniquely tagged Compose image, confirms artifact
identity across init/bootstrap/serve, verifies non-root/read-only/capability
hardening, initializes PostgreSQL separately, provisions temporary ExtendDB API
credentials, and executes CreateTable/PutItem/GetItem. It also verifies restart
persistence, graceful SIGTERM shutdown, and a full Compose down/up cycle through
the existing-config migration path. Every run uses an isolated Compose project,
Docker-assigned loopback port, and disposable volumes and credentials.

Requirements: Docker, Docker Compose v2 (or `docker-compose`), AWS CLI, and
Python 3.

```bash
ci/smoke-test-container.sh
```

To test an already-built local image without rebuilding or retagging it:

```bash
EXTENDDB_IMAGE=extenddb-postgres:dev ci/smoke-test-container.sh
```

The prebuilt image must carry non-empty OCI version, revision, and creation-time
labels. The smoke test compares those labels with the binary's embedded build
metadata and leaves the supplied image untouched.

## Production lifecycle

Do not use the local Compose bootstrap behavior as the production control plane.
Use the same image for three explicit operations.

### Fresh deployment

Run exactly one bootstrap Job with PostgreSQL bootstrap credentials. Inject
`EXTENDDB_PG_PASSWORD` and `EXTENDDB_APP_PASSWORD` from the orchestrator's
secret store; do not put either value in command arguments.

```bash
extenddb init \
  --config /var/lib/extenddb/extenddb.toml \
  --backend postgres \
  --pg-host <postgres-host> \
  --pg-port 5432 \
  --pg-user <bootstrap-user> \
  --extenddb-user <application-user> \
  --bind-addr 0.0.0.0 \
  --tls-san <service-dns-name>
```

Persist the generated config and TLS files securely. The config and private key
are written mode 0600.

### Upgrade

Back up PostgreSQL, stop or scale down incompatible serving versions, then run
one migration Job with `EXTENDDB_PG_PASSWORD` injected from the orchestrator's
secret store:

```bash
extenddb migrate \
  --config /var/lib/extenddb/extenddb.toml \
  --yes \
  --pg-user <bootstrap-user>
```

The generated config stores the application connection but not the PostgreSQL
bootstrap password, so the migration Job must receive that elevated credential
separately. Do not give it to the serving container.

### Steady state

Run only:

```bash
extenddb serve \
  --config /var/lib/extenddb/extenddb.toml \
  --foreground
```

Mount configuration/TLS state at `/var/lib/extenddb`, set the root filesystem
read-only, drop all capabilities, use the runtime default seccomp profile, and
apply CPU/memory limits in the orchestrator.

## Known Phase B limitations

* The image is not yet published to ECR Public or GHCR.
* The repository has no backend-aware readiness endpoint; health is liveness.
* Generated third-party notices cover Rust dependencies. Debian package notices
  remain in `/usr/share/doc/*/copyright` with `dpkg` metadata intact.
* Local Compose uses a self-signed certificate and development passwords.
