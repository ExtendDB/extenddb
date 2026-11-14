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

## Browser requests

The server treats a request as coming from a browser when it carries an
`Origin` header or a `Sec-Fetch-Site` header. A browser sends `Origin` on every
request whose method is not `GET` or `HEAD` and on every cross-origin CORS
request. It sends `Sec-Fetch-Site` and the other `Sec-Fetch-*` headers only to
`https` URLs and to loopback URLs (a loopback IP address, `localhost` or a
`.localhost` name). A page script cannot remove these headers. The AWS SDKs and
the CLI send neither. Node's built-in `fetch` sends `Sec-Fetch-Mode` on its own,
which the server does not count.

In dev mode the server serves a browser request only when its host is an IP
address, `localhost`, a name ending in `.localhost`, or a name you list in
`server.dev_allowed_hosts` (environment variable
`EXTENDDB__SERVER__DEV_ALLOWED_HOSTS`). The host is read from the `Host`
header. Any other browser request gets HTTP 403 before authentication. A call
to the DynamoDB API and a console login are `POST`s, which carry `Origin`, so a
web page whose name resolves to the loopback address cannot use the seeded
credential or log in to the console. The allowance for `.localhost` names
assumes a browser that resolves them to the loopback address itself. Chrome,
Firefox and Edge do; Safari on macOS hands them to the system resolver.

The management routes (`/management` and below) and `/metrics` answer `GET`
with the admin password, and a page's same-origin `GET` carries neither header.
The server therefore checks every request to these routes against the host
rule, whether or not it carries a browser header. A client on the same host
that uses `127.0.0.1` or `localhost` needs no change. A client in another
container that calls these routes by the Compose service name, such as
`extenddb manage --endpoint http://extenddb-dev:18080` or a metrics poller,
gets HTTP 403. Such a client needs no listed name if it uses an IP address,
which the rule always serves. Point a metrics poller at the dev container's IP
address, and run `extenddb manage` inside the dev container, where `127.0.0.1`
reaches the server. The image has no config file, so give the endpoint:

```bash
docker exec <container> extenddb manage --endpoint http://127.0.0.1:18080 \
  --user admin --password <pw> list-accounts
```

The setting is a single string of comma-separated bare host names, with no
port, scheme or wildcard, such as `app.test,api.test`. A TOML array stops the
server at startup. The startup banner states the rule and the names in force,
and names each entry it dropped.

List a name when a browser reaches the server through it, for example a
development proxy that forwards the browser's `Host`:

```bash
# A development proxy at http://app.test:3000 forwards the browser's requests,
# with their Host header unchanged, to this dev server:
docker run -d -p 127.0.0.1:18080:18080 \
  -e EXTENDDB__SERVER__DEV_ALLOWED_HOSTS=app.test \
  extenddb/extenddb-dev
```

A name your hosts file maps to the loopback address, such as
`localhost.localdomain`, `localhost4` or your machine's own name, has to be
listed too when a browser uses it, and so does the host name of the URL a cloud
development environment forwards to the server, if it passes the browser's
`Host` through unchanged.

The server sends no CORS headers, so a page on another origin cannot call it
directly, whether or not its name is listed. For a page at
`http://app.test:3000` that calls `http://localhost:18080`, the browser's
preflight `OPTIONS` request gets 405 and the browser does not send the call.

A listed name is trusted for browser requests as `localhost` is, so a
page served under it can call the server with the seeded credential. List a
name that resolves to the loopback address only when you control that
resolution on the machine where the browser runs, as with an entry in its hosts
file or the name your own development proxy forwards.

A browser on the host reaches the dev container through its published port as
`localhost` or `127.0.0.1` and needs no listed name. Do not list a single-label
Compose service name such as `extenddb-dev` for that browser. The host resolves
such a name through its DNS search domains and, on Windows, through Link-Local
Multicast Name Resolution (LLMNR) and NetBIOS, which another machine on the
network can answer. That machine can answer with its own address and serve a
page under the name, then answer with the loopback address, so the page's
requests reach the dev server under a listed name.

A browser that has to reach the server by a Compose service name or
`host.docker.internal`, for example one in a test container, sends its requests
under that name, so the name has to be listed in the dev service's environment:

```yaml
services:
  extenddb-dev:
    environment:
      EXTENDDB__SERVER__DEV_ALLOWED_HOSTS: extenddb-dev
```

The listed name is then trusted for browser requests from the host too.

A reverse proxy in front of the dev server decides which `Host` the server
sees:

- A proxy that rewrites `Host` to `localhost` or to a listed name hides the
  browser's host, so the check does not protect a server behind it. That proxy
  has to check `Host` itself.
- A proxy that forwards the browser's `Host` unchanged keeps the check, and the
  name the browser uses has to be listed.

The check does not cover these cases:

- **A same-origin `GET` or `HEAD` from a page served over plain HTTP.** It
  carries neither header, so the server serves it as it serves a non-browser
  client. Outside the management routes and `/metrics`, which are checked on the
  host alone, it reaches `/health` and the console's login and documentation
  pages. Every other console page needs a session, which only a login `POST`
  creates.
- **Firefox before 70 and EdgeHTML.** They send no `Origin` on a `POST` and no
  `Sec-Fetch-*` headers, so in one of them a page whose name resolves to the
  loopback address can still reach the DynamoDB API with the seeded credential,
  and, if it knows a console password, log in to the console and use it. The
  management routes and `/metrics` are still refused, since they are checked on
  the host alone.
- **Clients that are not browsers.** The SDKs, the CLI and any other
  non-browser client send no browser header, so outside the management routes
  and `/metrics` they are served whatever their `Host`, as before. A web page
  cannot drive those clients.

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

**Export files.** The image configures no import or export root. When you set
one, for example with `-e EXTENDDB__IMPORT_EXPORT_ROOT=/exports` and a bind
mount at `/exports`, each export file is created with mode 0600 and owned by
the container's uid, 65532, so a host user cannot read it through the mount:
copy it out with `docker cp <container>:/exports/<account id>/<file> .`, or
run the container as the host user (`--user "$(id -u):$(id -g)"`) in the
in-memory mode, since the image's state directory belongs to uid 65532.

## Health

The image carries a Docker `HEALTHCHECK` (the binary's own `healthcheck`
subcommand over plain HTTP), so `docker ps` and compose `depends_on:
condition: service_healthy` work with no extra wiring.

## Image properties

- Runtime base is distroless (`gcr.io/distroless/cc-debian12:nonroot`): no
  shell, no package manager, runs as uid 65532. `docker exec` can run the
  `extenddb` binary in it but cannot open a shell, by design.
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

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is an open
source project maintained by AWS. It is not Amazon DynamoDB or an AWS service.
"DynamoDB" is a trademark of Amazon.com, Inc. or its affiliates.
