# The `extenddb-dev` Image

> **Local development and CI only.** The dev image serves plain HTTP, its
> authorization is open, and by default it accepts AWS's public example
> credential. Anyone who can reach its port can read, change and delete every
> table. Always publish it on loopback, `-p 127.0.0.1:18080:18080`; a bare
> `-p 18080:18080` publishes it on every network the host is on. Never run it
> on a shared network or keep real data in it. On a shared host or CI runner,
> also [remove the example credential](#a-shared-host-or-ci-runner).

`extenddb/extenddb-dev` is a single-container, DynamoDB-compatible endpoint over
SQLite, built for local development and CI. There is no external database, no
init step, and no certificate to trust:

```bash
docker run -d -p 127.0.0.1:18080:18080 -v extenddb:/var/lib/extenddb \
  extenddb/extenddb-dev
```

```bash
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
aws dynamodb list-tables \
  --region us-east-1 --endpoint-url http://127.0.0.1:18080
```

It is **not the production image**. For a durable, TLS-terminated deployment use
[`extenddb/extenddb-postgres`](local-dev-docker-compose.md).

## What "dev-mode" means

The image is compiled with the `dev-mode` feature, which trades hardening for
zero-friction local use:

- **Plain HTTP.** No TLS, so SDKs need no custom trust store.
- **A seeded, well-known credential.** The server seeds AWS's documented
  example key pair (`AKIAIOSFODNN7EXAMPLE` /
  `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`) on every startup by default and
  prints it in the logs; sign requests with it. It is recognised by secret
  scanners as an example credential, so committing it in test fixtures is
  safe. To seed one additional key pair, pass `EXTENDDB_DEV_ACCESS_KEY_ID` and
  `EXTENDDB_DEV_SECRET_ACCESS_KEY` to the container (the key id must start
  with `AKIA`; its secret is never printed). The example pair keeps working
  alongside it, so tools that hardcode it (NoSQL Workbench, the launchers)
  are unaffected by the override, unless you remove the example pair (see
  below). The standard `AWS_*` variables are ignored by the server, so real
  or SSO credentials in the environment are never copied into the dev
  catalog; a startup notice calls out an `AWS_ACCESS_KEY_ID` that would not
  verify. Authorization is open: whoever holds a seeded credential can do
  everything.

Because of that, the server must not be reachable from other machines. The
image binds `0.0.0.0` **inside** the container (required for port publishing to
work at all; this is what `EXTENDDB_DEV_ALLOW_ANY_BIND=1` in the image enables),
and containment moves to the publish flag: always bind the host side to
loopback, `-p 127.0.0.1:18080:18080`, as every example here does. Do not
publish this port on a shared or routable interface. The server cannot see the
host's port mapping, so whenever it binds a non-loopback address, as it does in
the image, it prints a warning block at startup with the loopback publish
command.

### A shared host or CI runner

Where other people or machines can reach the host (a shared build machine, a
CI runner with a routable address), remove the public example credential and
use your own. The switch below is read by images released after 0.1.14;
releases through 0.1.14 ignore it and always accept the example credential.

```bash
docker run -d -p 127.0.0.1:18080:18080 \
  -e EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL=1 \
  -e EXTENDDB_DEV_ACCESS_KEY_ID=AKIA<your key id> \
  -e EXTENDDB_DEV_SECRET_ACCESS_KEY=<your secret> \
  extenddb/extenddb-dev
```

With `EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL=1` (or `true`), the server stops
accepting the example pair:

- It does not seed the example pair, and on a file-backed database it deletes
  the example key from whichever user holds it: the `dev` user an earlier
  start seeded, or a user you imported it for with `extenddb manage`. Unset
  the variable to seed it again on the next start.
- It refuses to start unless both `EXTENDDB_DEV_*` variables are set, so it
  never runs with no credential to sign with.
- It refuses to start for a value other than `1`, `true`, `0` or `false`,
  including an empty value, so a typo or a `-e
  EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL=$VAR` with `VAR` unset does not
  leave the example pair active.
- Tools that hardcode the example pair (NoSQL Workbench, the launchers) must
  be pointed at your credential instead.

Pass the switch as `-e EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL=1`. A bare
`-e EXTENDDB_DEV_DISABLE_EXAMPLE_CREDENTIAL`, without `=1`, passes the host's
value and leaves the switch unset when the host has not exported it, and a
misspelled name is never read; both start with the example pair active. Check
the `Sign requests with:` line the server prints at startup, which lists the
credentials it seeded: `AKIAIOSFODNN7EXAMPLE` must not be on it.

The switch removes only the example key. Other keys a file-backed database
holds keep working: an `EXTENDDB_DEV_*` key from an earlier start that you
have since changed, and keys added with `extenddb manage`. To start with only
the current `EXTENDDB_DEV_*` pair, recreate the volume.

This narrows who can sign requests; it does not add TLS or permissions. Keep
the loopback publish.

## Storage modes

One image serves both modes; the mode is chosen at run time.

**File-backed (default).** The database lives at
`/var/lib/extenddb/extenddb.sqlite`. Mount a volume there and data survives
container restarts and upgrades:

```bash
docker run -d -p 127.0.0.1:18080:18080 -v extenddb:/var/lib/extenddb \
  extenddb/extenddb-dev
```

**In-memory.** Everything lives in process memory and vanishes when the
container stops — useful for test suites that want a pristine database per run:

```bash
docker run -d -p 127.0.0.1:18080:18080 \
  -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: \
  extenddb/extenddb-dev
```

`docker-compose.dev.yml` at the repository root defines both as services
(`extenddb-dev`, and `extenddb-dev-memory` under the `memory` profile).

## Health

The image carries a Docker `HEALTHCHECK` (the binary's own `healthcheck`
subcommand over plain HTTP), so `docker ps` and compose `depends_on:
condition: service_healthy` work with no extra wiring.

## Image properties

- Runtime base is distroless (`gcr.io/distroless/cc-debian12:nonroot`): no
  shell, no package manager, runs as uid 65532. `docker exec` into it is not
  possible by design.
- The binary is built with the `release-slim` profile (stripped, LTO).
- Licence notices for this image's exact dependency set ship at
  `/usr/share/doc/extenddb/SOFTWARE-LICENSE-NOTICES.html`
  (generated by `devtools/generate-dev-license-notices`).
- Signed with Sigstore cosign; verify as described on the image's Docker Hub
  overview.

## Limitations

- Single node, single process. Throughput settings are accepted and tracked
  but not enforced as capacity.
- No TLS and no access control (see dev-mode above): never expose it beyond
  the local machine, and never store real data in it.
