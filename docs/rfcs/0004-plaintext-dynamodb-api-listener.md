# RFC-0004: Plaintext DynamoDB API Listener and `aws:SecureTransport`

- Status: Draft
- Author: @LukeKennedy
- Created: 2026-10-26
- FCP ends: (set when entering Final Comment Period)

## Summary

This RFC adds an optional, disabled-by-default second listener to `extenddb serve` that accepts plaintext HTTP on its own bind address and port. The plaintext listener serves only the DynamoDB API (`POST /`, covering every `DynamoDB_20120810` and `DynamoDBStreams_20120810` operation, including table-level operations such as `CreateTable` and `DeleteTable`) and the unauthenticated liveness endpoint (`/health`). It does not serve `/management`, `/console`, or `/metrics`. The existing TLS listener is unchanged.

ExtendDB resolves the `aws:SecureTransport` global condition key from a server-wide value: whether its one listener serves TLS. This RFC makes the key a per-request value derived from the listener and connection the request arrived on, so operators can write policies that distinguish requests by transport. On the plaintext listener the key is `true` only for requests whose TCP peer address is in a configured list of secure transport sources: proxies that accept client traffic only over TLS. Finally, the RFC requires a prominent warning on every surface where an operator would notice that plaintext serving is enabled.

## Motivation

ExtendDB requires TLS on its only listener. That is the right default, but it blocks a common deployment pattern: terminating TLS outside the process. Operators deploying ExtendDB behind a load balancer (AWS ALB with TLS listeners, nginx, HAProxy) or inside a service mesh (Istio, Linkerd, Consul Connect, ECS Service Connect) want the proxy to own certificates, rotation, and mTLS, and forward plaintext HTTP to the backend over a link they already trust (inside a pod, a private subnet, or a mesh sidecar).

Today those operators have two options, both poor:

1. Configure the proxy to re-encrypt to ExtendDB's self-signed certificate. This requires distributing ExtendDB's certificate into the proxy's trust store, duplicates TLS cost on every request, and gives no security benefit when the proxy and ExtendDB share a host or pod.
2. Build with `dev-mode`, which serves plaintext but also relaxes authorization and binds loopback unless `EXTENDDB_DEV_ALLOW_ANY_BIND` is set for container use. It is not usable for anything beyond local development, and the `postgres` and `mongodb` builds cannot enable it.

A dedicated plaintext listener serves this pattern without weakening the defaults. Keeping it separate from the TLS listener, and keeping the administrative surfaces off it, limits the plaintext exposure to SigV4-authenticated DynamoDB API traffic. Administrative credentials (HTTP Basic passwords for `/management` and the console login) never cross a plaintext link.

`aws:SecureTransport` is how an operator expresses "this principal may only be used over TLS" or "deny writes that arrive over plaintext," because IAM policies are the only per-principal control ExtendDB offers. Today the key is resolved from `AppState::tls_enabled`, which is `true` in every production build. A second listener that shares `AppState` would therefore report `true` for plaintext requests unless the value is made per-request, which would silently defeat every policy that relies on the key.

## Detailed design

### Configuration

A new optional `[server.plaintext]` section configures the listener. It is a dedicated section rather than an entry in a generic listener list, so that a plaintext listener is distinct and obvious in any config file: searching for `plaintext` finds it, and it cannot be enabled by changing a field on an entry that otherwise looks like the TLS listener.

```toml
[server.plaintext]
# Off unless explicitly enabled.
enabled = true
# Defaults to loopback. Binding a routable address exposes unencrypted traffic
# to the network between the client (or proxy) and ExtendDB.
bind_addr = "127.0.0.1"
# Required when enabled. There is no default port.
port = 18080
# Peer addresses (CIDR) of proxies that accept client traffic only over TLS.
# Requests from these peers report aws:SecureTransport = true; all other peers
# report false. Defaults to empty: every peer reports false.
secure_transport_sources = ["127.0.0.1/32", "::1/128"]
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Starts the plaintext listener. |
| `bind_addr` | string | `"127.0.0.1"` | Address the plaintext listener binds: an IPv4 literal, an IPv6 literal without brackets (`::`, `fd00::5`), or `localhost`, which is resolved to `127.0.0.1`. Independent of `server.bind_addr`. |
| `port` | u16 | none | Port the plaintext listener binds. Required when `enabled = true`. |
| `secure_transport_sources` | array of CIDR strings | `[]` | Peer addresses whose requests report `aws:SecureTransport = true`. A bare address is accepted as a single-host prefix (`/32` or `/128`). |

The keys follow the existing environment override convention, for example `EXTENDDB__SERVER__PLAINTEXT__ENABLED=true` and `EXTENDDB__SERVER__PLAINTEXT__PORT=18080`. The config loader does not split environment values into lists, so `secure_transport_sources` uses a custom deserializer that accepts either a TOML array or a comma-separated string, and `EXTENDDB__SERVER__PLAINTEXT__SECURE_TRANSPORT_SOURCES=127.0.0.6/32,::6/128` works. This avoids enabling `try_parsing` on the environment source, which would change how every other string-valued override is parsed. No new CLI flags are added. `extenddb serve --port` continues to apply to the TLS listener only.

Validation at `extenddb serve` startup, before the process daemonizes. Each failure is an error that names the key and the offending value.

- `enabled = true` without `port`, or with `port = 0`, is an error.
- `port` equal to the effective TLS port (`server.port`, or `--port` when given) is an error regardless of the two bind addresses. On Linux a conflicting bind fails with `EADDRINUSE`, but on macOS and the BSDs `SO_REUSEADDR` (which Rust's `TcpListener::bind` sets on Unix) can allow `127.0.0.1:P` to bind while `0.0.0.0:P` is bound, silently diverting loopback clients from the TLS listener to the plaintext one. Other bind failures are reported by the operating system and the error names the plaintext listener.
- `bind_addr` must be an IP literal or `localhost` (resolved to `127.0.0.1`). The socket address is built from the parsed address and port, not by string formatting, so IPv6 literals bind correctly.
- Each `secure_transport_sources` entry must parse as an IPv4 or IPv6 address or CIDR prefix. Entries are rejected if they:
  - have host bits set (`10.0.1.20/16`), because the intended range is ambiguous;
  - are written in IPv4-mapped IPv6 form (any prefix within `::ffff:0:0/96`), because peer addresses are normalized to IPv4 before matching (see Peer classification) and such an entry would never match; the error suggests the IPv4 form;
  - carry an IPv6 zone identifier (`fe80::1%eth0`);
  - have a zero-length prefix (`0.0.0.0/0`, `::/0`), which would match every peer.
- A `dev-mode` build refuses `enabled = true`. Its main listener already serves plaintext, and a second plaintext listener would bypass the dev-mode bind confinement.
- `server.tls.enabled = false` remains an error in non-dev builds. The plaintext listener is additive and is not a way to disable TLS.

The section is absent from configs generated by `extenddb init`. It is documented, commented out, in `extenddb.sample.toml`.

### Secure transport sources

The secure transport decision is made from the TCP peer address of each connection, which ExtendDB observes directly. On an established TCP connection, a host that is not on the network path cannot forge its peer address. 

A listed address also covers every process that can send from it. Listing a loopback address, or any address inside the pod's network namespace, extends `aws:SecureTransport = true` to everything that shares the namespace with ExtendDB:

- other containers in the same pod or ECS task;
- `kubectl port-forward` and `kubectl exec` sessions;
- containers run with `--network host`;
- on a VM or bare-metal host, every local user and process.

### Listener and routing

Both listeners are bound in the synchronous startup path in `cmd_serve`, before daemonizing, so port conflicts on either are reported to the invoking terminal. Both are set non-blocking before conversion to tokio listeners. Both share one `AppState`, one storage engine, one set of auth caches, and one set of background workers. A single process serves both.

The plaintext listener serves this route table:

| Route | Plaintext listener | TLS listener |
|-------|--------------------|--------------|
| `POST /` (all DynamoDB and DynamoDB Streams operations) | Served | Served |
| `GET /health`, `HEAD /health` | Served | Served |
| Other methods on `/` | 405 | 405 |
| `GET /metrics` | 404 | Served |
| `/management/*` | 404 | Served |
| `/console`, `/console/*` | 404 | Served |

Unserved paths fall through to axum's default 404, so the plaintext listener does not advertise that administrative routes exist. At the request layer, handling on `POST /` is identical on both listeners: same body limits, same SigV4 verification, same authorization, same throttling, same metrics. The only per-request difference is the value of `aws:SecureTransport`.

The connection layer differs. The TLS listener runs on `axum-server` with rustls and negotiates HTTP/1.1 or HTTP/2 by ALPN. The plaintext listener runs on `axum::serve`, whose hyper-util automatic builder accepts HTTP/1.1 and HTTP/2 with prior knowledge (h2c). HTTP/2 via the HTTP/1.1 `Upgrade: h2c` header is not supported. Envoy and other proxies that use h2c to their upstreams must be configured for prior knowledge. Neither listener sets a request header read timeout today; this RFC does not change that.

Response headers on the plaintext listener include the existing `X-Content-Type-Options`, `X-Frame-Options`, and `Referrer-Policy` headers. `Strict-Transport-Security` is not sent, because HSTS on a plaintext response is ignored by clients and misleading in captures. The TLS listener keeps its HSTS header and its plain-HTTP-to-HTTPS redirect acceptor unchanged; a plaintext request sent to the TLS port is still redirected.

Plaintext connections set `TCP_NODELAY` using the existing `nodelay_listener` wrapper.

### Peer classification

The plaintext listener keeps axum's standard `ConnectInfo<SocketAddr>`. A middleware on the plaintext router reads the peer address from it on each request, matches it against `secure_transport_sources`, and inserts the result as a `Transport` request extension. The peer address is fixed for the life of a connection, so classifying per request gives the same result as classifying per connection, without a custom listener type. The match is a linear scan over a handful of prefixes and costs less than SigV4 verification by orders of magnitude.

Before matching, the peer address is normalized with `IpAddr::to_canonical()`, which converts IPv4-mapped IPv6 addresses (`::ffff:10.0.0.5`, as reported by a dual-stack socket) to IPv4 and leaves every other address unchanged. `Ipv6Addr::to_ipv4()` is not used, because it also converts IPv4-compatible addresses and would turn `::1` into `0.0.0.1`. Configuration entries are not normalized, so an entry in mapped form would silently never match a normalized peer; such entries are rejected at startup with a suggestion to use the IPv4 form (see Configuration).

Every request is served regardless of the result. Classification determines only the value of `aws:SecureTransport`.

### `aws:SecureTransport`

`aws:SecureTransport` is a single-valued boolean global condition key. It is present on every request, matching AWS IAM, and resolves to `"true"` or `"false"` for use with the `Bool` operator.

ExtendDB resolves it today in both identity-policy evaluation (`RequestContext`) and trust-policy evaluation (`AssumeRoleContext`). The data-plane value comes from `RequestParams::secure_transport`, which the `base_params()` closure in `authorize_request` (`crates/server/src/request_helpers.rs`) sets from `state.tls_enabled` for every check, including each batch and transaction sub-operation. The trust-policy value comes from `ManagementState::secure_transport`, set from the same flag. Both are server-wide. This RFC keeps the key's type, presence, and evaluation semantics and changes only where the data-plane value comes from:

| Request path | Value |
|--------------|-------|
| `POST /` on the TLS listener | `true` |
| `POST /` on the plaintext listener from a peer in `secure_transport_sources` | `true` |
| `POST /` on the plaintext listener from any other peer | `false` |
| `AssumeRole` trust policy evaluation (`/management`, TLS listener only) | `true` |

The values marked `true` for the TLS listener and `AssumeRole` are not written as literals. In production builds that is always `true`. In a `dev-mode` build, whose main listener serves plaintext, it is `false`, which is the truthful value.

For `AssumeRole`, the key describes the connection that made the management API call. `AssumeRole` is admin-only, authenticated with HTTP Basic, and the caller ARN is supplied in the request body, so the key reflects the administrator's connection to `/management` rather than anything about the principal that will use the resulting credentials. Since `/management` is served only over TLS, the value is constant, and conditions on it in trust policies have no practical use.

ExtendDB cannot verify that a listed source actually used TLS on the client side of the proxy. `secure_transport_sources` is an operator statement that the listed hosts accept client traffic only over TLS (see Secure transport sources); ExtendDB enforces that only those hosts receive `true`. The result is only as accurate as the list and the proxies' own configuration.

The plaintext listener serves the full DynamoDB API, including table-level and administrative operations such as `CreateTable`, `DeleteTable`, `UpdateTable`, `ImportTable`, `ExportTableToPointInTime`, `TagResource`, and `UpdateTimeToLive`. Policies intended to block plaintext access should therefore deny broadly rather than list write operations. Denying all DynamoDB actions over plaintext:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyPlaintext",
      "Effect": "Deny",
      "Action": "dynamodb:*",
      "Resource": "*",
      "Condition": { "Bool": { "aws:SecureTransport": "false" } }
    }
  ]
}
```

Allowing only reads over plaintext, by denying every action outside a read list:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PlaintextReadOnly",
      "Effect": "Deny",
      "NotAction": [
        "dynamodb:GetItem", "dynamodb:BatchGetItem", "dynamodb:Query", "dynamodb:Scan",
        "dynamodb:TransactGetItems", "dynamodb:DescribeTable", "dynamodb:ListTables",
        "dynamodb:DescribeStream", "dynamodb:GetShardIterator", "dynamodb:GetRecords", "dynamodb:ListStreams"
      ],
      "Resource": "*",
      "Condition": { "Bool": { "aws:SecureTransport": "false" } }
    }
  ]
}
```

With either policy attached, requests through a listed proxy and requests on the TLS listener are unaffected, and a client that reaches the plaintext port directly is denied.

Batch and transaction requests are authorized per sub-operation. Every sub-operation check is built from `base_params()` and carries the enclosing request's request-level keys, including `aws:SecureTransport`. Under this RFC, `base_params()` takes its value from the request's `Transport` extension instead of `state.tls_enabled`, so sub-operations inherit the per-request value with no change to the batch and transaction paths. A params value built without `base_params()` gets `secure_transport = false` from `RequestParams::default()`. That value can only over-deny: a `Deny` on `false` applies and an `Allow` requiring `true` does not grant.

The management API's own routes authenticate with HTTP Basic and do not evaluate identity policies, so `aws:SecureTransport` has no effect there beyond the `AssumeRole` trust policy case above.

### Warnings

Plaintext serving is a deliberate reduction in transport security, and an operator who inherits a deployment must be able to tell that it is on. Whenever `server.plaintext.enabled = true`, ExtendDB emits a warning on each of the following surfaces. None of them can be suppressed by configuration.

1. **Startup banner.** A boxed block printed immediately after the existing startup banner, on the same stream (stdout in daemon mode, stderr in `--foreground`), before the process daemonizes. Color follows the existing banner rules: applied only on an interactive terminal, honoring `NO_COLOR` and `TERM=dumb`. The block for a non-loopback bind:

   ```text
   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   !!  WARNING: PLAINTEXT LISTENER ENABLED                                !!
   !!                                                                     !!
   !!  DynamoDB API requests are accepted WITHOUT TLS on 0.0.0.0:18080.   !!
   !!  This address is reachable from other hosts.                        !!
   !!                                                                     !!
   !!  Request and response bodies, including item data, can be read and  !!
   !!  responses can be altered by anyone on the network path. Captured   !!
   !!  signed requests can be replayed for up to 30 minutes.              !!
   !!                                                                     !!
   !!  Use only behind a TLS-terminating proxy on a trusted network.      !!
   !!  Set server.plaintext.enabled = false to disable.                   !!
   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   ```

   For a loopback bind, the "reachable from other hosts" line is omitted. The banner does not list the configured sources.

2. **Log at startup.** A `WARN` record with structured fields `listener = "plaintext"`, `bind`, `loopback`, and `secure_transport_sources` (the resolved list), written to syslog or stderr according to the configured log target. This survives daemon mode, where the banner is seen only by the invoking terminal, and gives the operator the one parameter that determines `aws:SecureTransport` in the same record as the warning.

3. **Periodic log reminder.** The same `WARN` record repeats every 24 hours while the listener is running, so log retention and rotation do not remove the only record that plaintext serving is on.

4. **CLI.**
   - `extenddb status` probes the plaintext listener with a TCP connect check. The TLS check connects to `127.0.0.1:<port>`, which does not answer for a listener bound to a specific non-loopback address, so the plaintext probe uses a separate target rule, shared with `extenddb healthcheck --plaintext`: `127.0.0.1` when `bind_addr` is an IPv4 loopback or unspecified address, `::1` when it is an IPv6 loopback or unspecified address, and `bind_addr` itself otherwise. It prints a warning line when the plaintext port answers, and a separate line when the loaded configuration enables the listener but the port does not answer. The probe uses the configured address and port, so it detects a running listener only where the configuration `status` loads matches the running process.
   - `extenddb healthcheck --plaintext` probes `GET /health` on the plaintext listener over plain HTTP, using the target rule above, with the same 3-second deadline, response size limit, and exit codes (0 healthy, 1 not) as the existing TLS probe. It exits 1 with an error naming the key when the loaded configuration does not enable the plaintext listener. `--port` overrides the plaintext port when combined with `--plaintext`. `--endpoint` accepts an `http://` URL, which selects the plaintext probe without `--plaintext`; combining `--plaintext` with an `https://` endpoint is an error. Without `--plaintext` or an `http://` endpoint, the subcommand probes the TLS listener as it does today. A container image whose orchestrator health-checks the plaintext port can use `--plaintext` so the `HEALTHCHECK` exercises the same listener.
   - `extenddb verify` validates the deployment's databases and does not contact the server. It prints a warning line when the loaded configuration enables the plaintext listener, and it runs the startup validation rules for `[server.plaintext]`, reporting violations as failures. It does not probe either listener.

5. **`/metrics`.** The metrics response reports the listener state, described in the next section, so monitoring can alert on it without access to the config file or logs.

Nothing is added to the `/health` response body. `/health` is unauthenticated, and on the plaintext listener its response is visible to anyone on the path, so it should not describe the server's configuration.

### Metrics

Two handlers return `MetricsResponse`:

- `GET /metrics`, authenticated with admin HTTP Basic, served only on the TLS listener, in `crates/server/src/metrics_endpoint.rs`;
- `GET /console/metrics-data`, authenticated with a console session, in `crates/server/src/console/pages/metrics_pages.rs`.

Both build the response through `query_metrics_from_store`, and `/metrics` also has an in-memory fallback branch. A new top-level `listeners` field is added to `MetricsResponse`, defaulting to `None` and skipped when `None`, and both handlers set it after the response is built. Every `200` response from either handler includes it, regardless of query parameters or whether the plaintext listener is enabled. Error responses, such as the `400 ValidationException` for an invalid time range, are unchanged.

```json
{
  "metrics": [ ... ],
  "buckets": [ ... ],
  "segments": [ ... ],
  "source": "database",
  "listeners": {
    "tls": { "bind": "0.0.0.0:18443" },
    "plaintext": {
      "enabled": true,
      "bind": "10.0.1.20:18080",
      "loopback": false
    }
  }
}
```

With the listener disabled, `plaintext` is `{"enabled": false}` and the other fields are omitted. A scraper can then alert on `listeners.plaintext.enabled == true` without distinguishing an absent field from a disabled listener.

The values come from the resolved configuration at startup. They are held in a shared `ListenerInfo` value referenced from both `AppState` and `ConsoleState`, since the console handler does not have access to `AppState`. The console dashboard ignores the field.

The per-table CloudWatch-style metrics are unchanged. Requests on either listener contribute to them identically; this RFC does not add a listener dimension to them.

### Migration

Existing configuration files are unaffected; the listener is off unless `[server.plaintext]` is added. There are no on-disk format, catalog schema, or DynamoDB wire-format changes.

Policy evaluation is unchanged for deployments that do not enable the plaintext listener. `aws:SecureTransport` resolves to `true` on the TLS listener and continues to. With the plaintext listener enabled, the only new values are `false` for requests from unlisted peers on that listener.

### Operational requirements

Operators enabling the plaintext listener need to:

- Open or publish the new port only to the proxy or sidecar, and keep it closed to everything else. `secure_transport_sources` limits which peers receive `aws:SecureTransport = true`, but it does not stop other peers from connecting and does not protect traffic in transit; network placement still determines who can reach the port and observe the plaintext hop.
- List only proxies that accept client traffic exclusively over TLS, as described under Secure transport sources, and confirm that ExtendDB sees each proxy's address as the TCP peer.
- Configure the proxy to forward the client's original `Host` header unchanged. SigV4 signs `host`, and a rewritten `Host` produces `SignatureDoesNotMatch`. ALB and Envoy forward it by default. nginx's `proxy_pass` replaces it with the upstream address unless configured otherwise; `proxy_set_header Host $http_host;` forwards the client's header exactly, including any port. HAProxy forwards it by default in HTTP mode. NLB operates at layer 4 and does not interpret HTTP headers.
- Point load balancer and mesh health checks at `GET /health` on the plaintext port. `/health` is not authorized against IAM policy, so the health checker's address does not need to be listed. For a container `HEALTHCHECK` that should exercise the same path as the load balancer, use `extenddb healthcheck --plaintext` (see CLI under Warnings).
- Keep the TLS listener reachable for administrators, including any administrator or automation that calls `AssumeRole` to issue credentials for workloads. `AssumeRole` is served only under `/management`.

Unchanged by this RFC, and documented alongside it:

- Client source addresses seen by ExtendDB are the proxy's, so the original client address is not available for logging or policy. PROXY protocol support would be needed to recover it.
- `DescribeEndpoints` continues to return `localhost:<port>` for the effective TLS port, which is not reachable through a proxy. AWS SDKs do not enable endpoint discovery for DynamoDB by default.
- Running multiple ExtendDB instances against one PostgreSQL or MongoDB catalog behind a load balancer remains unsupported, as described in the deployment guide.

### Implementation sketch

Changes land in dependency order. Each step builds and passes tests on its own.

1. **`extenddb-auth`: condition key.**
   - `RequestContext` and `AssumeRoleContext` already carry `secure_transport` and resolve `aws:SecureTransport` to `"true"` or `"false"`. No resolution change is needed.
   - Update the `RequestParams::secure_transport` doc comment, which currently states the value is server-level rather than per-connection. The field, `RequestContext::build`, and `AssumeRoleContext` are otherwise unchanged.
   - Extend the existing `context.rs` unit tests, which cover resolution of both values, to cover `Bool`, `Null`, and `BoolIfExists` against both values.

2. **`extenddb-config`: configuration.**
   - Add `PlaintextListenerConfig` with `#[serde(deny_unknown_fields)]` as `ServerConfig::plaintext: Option<PlaintextListenerConfig>`, with the custom `secure_transport_sources` deserializer that accepts an array or a comma-separated string.
   - Add a `validate` step covering the rules in Configuration, taking the effective TLS port as an argument so a `--port` override is checked. Add the keys to `build_config_entries` in `crates/config/src/display.rs`.
   - Parse `secure_transport_sources` into a `SecureTransportSources` type holding `(IpAddr, prefix_len)` pairs, with `contains(IpAddr) -> bool` that applies `to_canonical()` before matching. Prefix matching is implemented directly on the address bytes, which is about 20 lines plus tests. `ipnet` is in `Cargo.lock` only as a transitive dependency of the MongoDB driver and is not compiled into the PostgreSQL or SQLite builds, so using it would add a dependency to those builds.
   - Move the loopback test used by the dev-mode bind check in `cmd_serve` into this crate, so the dev-mode check, the `loopback` metrics and log field, and the banner share one definition.

3. **`extenddb-server`: routing, transport plumbing, shutdown.**
   - Define `Transport { secure: bool }`. A transport middleware reads `ConnectInfo<SocketAddr>` and inserts `Transport` as a request extension: for the TLS listener `secure = state.tls_enabled`; for the plaintext listener `secure = sources.contains(peer.ip())`. Both listeners serve with `into_make_service_with_connect_info::<SocketAddr>()`, so no custom listener or connect-info type is needed.
   - `handle_request` extracts `Extension<Transport>` and passes it to `authorize_request`. `base_params()` sets `secure_transport` from it in place of `state.tls_enabled`. Every existing check, including the batch and transaction sub-operation checks, already goes through `base_params()`, so `check_authorization` is unchanged.
   - The `Authorization denied` warning in `crates/server/src/authorization.rs` gains `secure_transport` and `peer` fields. This is the log operators use to confirm the peer address a proxy presents before listing it.
   - `ManagementState::secure_transport` and `AssumeRoleContext::secure_transport` are unchanged. `/management` is served only on the TLS listener.
   - Split router construction in `crates/server/src/lib.rs` into an API router builder (`POST /`, `/health`, body limits, security headers) that takes the listener's transport classifier and applies the transport middleware, and the full router, which merges the TLS-classified API router with `/metrics`, `/management`, `/console`, and HSTS. Each listener therefore gets its own instance of the API router with its own classifier.
   - Replace the per-branch shutdown handling in `start_server`:
     - Add a new HTTP shutdown `CancellationToken`, separate from the existing worker shutdown token in `serve.rs`. Workers continue to be cancelled only after both servers have returned, so the metrics flush worker keeps running while requests drain.
     - One signal task awaits `shutdown_signal()` and cancels the HTTP token.
     - The TLS server runs on `axum-server`, and a task that awaits the token calls `Handle::graceful_shutdown(Some(5s))`. The plaintext server runs on `axum::serve(...).with_graceful_shutdown(token.cancelled_owned())`.
     - Each server runs in its own spawned task, which cancels the HTTP token when it exits for any reason, so an error on one listener drains the other. `start_server` awaits both with `tokio::join!` and returns the first error after both have finished. `tokio::try_join!` is not used, because it returns on the first error before the other server has drained, so worker drain and PID cleanup would run early.
     - A single watchdog replaces the two existing PID-file timeout tasks (10 seconds on the TLS branch, 5 seconds on the plaintext branch). It starts when the HTTP token is cancelled and removes the PID file if both servers have not returned within 10 seconds. The normal path removes the PID file once, after both servers return.
   - `ServeParams` gains `plaintext_listener: Option<std::net::TcpListener>` and the resolved plaintext configuration.
   - Add a shared `ListenerInfo` referenced from `AppState` and `ConsoleState`, add the optional `listeners` field to `MetricsResponse` in `crates/core/src/metrics/types.rs`, and set it in the `/metrics` handler (both branches) and the `/console/metrics-data` handler.
   - Add the startup and periodic `WARN` records and the console banner. The periodic reminder runs as a background worker on the worker shutdown token so it drains with the others.

4. **`extenddb-app`: CLI.**
   - In `cmd_serve`, validate the section against the effective TLS port, refuse it in `dev-mode` builds, bind the plaintext socket next to the TLS socket from a parsed `SocketAddr`, set it non-blocking, and print the boxed warning after the startup banner.
   - Add the plaintext probe target rule to `extenddb-config` so `cmd_status` and `cmd_healthcheck` share it. `cmd_status` probes the plaintext port and prints the warning lines described under Warnings.
   - `cmd_healthcheck` gains `--plaintext` and accepts `http://` endpoints, sending the existing `GET /health` request over a plain `TcpStream` instead of the rustls client, under the same deadline and response limit.
   - `cmd_verify` prints the warning line and runs the `[server.plaintext]` validation rules.

5. **Tests.**
   - Configuration (Rust): malformed entries, host bits set, IPv4-mapped entries, zone identifiers, zero-length prefixes, comma-separated environment values, `port = 0`, a port equal to `server.port`, and a port equal to a `--port` override.
   - Classification (Rust): IPv4, IPv6, IPv4-mapped IPv6 peers matching IPv4 prefixes, and `::1` not being converted.
   - Routing (Rust): the plaintext router serves `POST /` and `/health` and returns 404 for `/metrics`, `/management/accounts`, `/console`, and `/console/`. A plaintext request on the TLS port still receives the 301 redirect.
   - Transport (Rust): router-level tests with axum's `MockConnectInfo` inject a listed and an unlisted peer and assert the `aws:SecureTransport` value reaching the policy evaluator, including for a `BatchWriteItem` and a `TransactWriteItems` sub-operation. This covers the unlisted case, which the Python suite cannot reach from a single test host.
   - Shutdown and metrics (Rust): both listeners stop on one signal; an error on one listener drains the other; the PID file is removed once. `/metrics` and `/console/metrics-data` include `listeners` for both the enabled and disabled cases.
   - CLI (Rust): the plaintext probe target rule for loopback, unspecified, and specific bind addresses in both families; `healthcheck --plaintext` returning 0 against a running plaintext listener, 1 when the listener is disabled in the configuration or not answering, and an error for `--plaintext` with an `https://` endpoint.
   - Python:
     - The suite builds management URLs from `EXTENDDB_TEST_ENDPOINT`, so a new optional `EXTENDDB_TEST_DYNAMODB_ENDPOINT` is added for DynamoDB clients only, and `EXTENDDB_TEST_ENDPOINT` continues to point at the TLS listener for management calls.
     - `devtools/run-tests` gains a mode that enables the plaintext listener with the test host's address in `secure_transport_sources`, sets `EXTENDDB_TEST_DYNAMODB_ENDPOINT` to it, and runs the existing suite, which is then expected to pass without changes.
     - Existing tests that require a secure connection, including the `aws:SecureTransport` cases in `tests/test_abac.py` and `tests/test_auth_integration.py`, remain on the TLS listener.

6. **Documentation.**
   - Revise the "TLS is mandatory" statements in `README.md`, `AGENTS.md`, `docs/manuals/05-admin-guide.md`, and `docs/manuals/10-security-model.md`. TLS stays mandatory on the primary listener; the plaintext listener is an opt-in exception for DynamoDB API traffic only.
   - Add a "TLS termination behind a proxy" section to `docs/manuals/11-deployment-guide.md`. It covers the requirements under Secure transport sources and Operational requirements, with ALB, nginx, and Istio examples, including `STRICT` mTLS and the `127.0.0.6` source.
   - Update the `aws:SecureTransport` descriptions that call it the "listener TLS state" (`docs/manuals/05-admin-guide.md` global condition key resolution, ADR-0022) to describe per-listener and per-peer resolution, and add the key's `AssumeRole` semantics to the security model.
   - Document the `listeners` object in the `/metrics` reference.
   - Add troubleshooting entries for `SignatureDoesNotMatch` caused by `Host` rewriting and for `aws:SecureTransport` reporting `false` because the proxy's peer address is not listed.
   - Document the section in `extenddb.sample.toml` and the Docker READMEs.
   - Document `extenddb healthcheck --plaintext` in the README CLI reference, the admin guide, and the container `HEALTHCHECK` guidance in the Docker READMEs.

## Drawbacks

- **Confidentiality and integrity on the plaintext hop.** Item data in requests and responses is readable by anyone on the path between the proxy and ExtendDB. SigV4 covers the signed headers and the request body hash, which ExtendDB recomputes from the bytes it receives, so tampered requests are rejected. Responses have no integrity protection: an on-path attacker can alter returned items or forge errors such as `ConditionalCheckFailedException`.
- **Replay.** A captured signed request can be replayed while its timestamp is within the ±15-minute clock-skew window (`MAX_CLOCK_SKEW_SECS`). A request signed with a timestamp up to 15 minutes ahead of the server's clock remains valid for up to 30 minutes after capture. The design limits this to the DynamoDB API but cannot remove it.
- **`aws:SecureTransport` is only as accurate as the list and the listed proxies.** ExtendDB verifies the peer address, not that the peer is a TLS-terminating proxy, and not that the proxy refuses plaintext from its own clients. The following all report `true` for traffic that never used TLS:
  - a mesh in permissive mode;
  - an ALB with an HTTP listener forwarding to the target group;
  - a listed range that includes non-proxy hosts (a shared subnet, or node CIDRs under Kubernetes SNAT);
  - another process in the same network namespace as a listed loopback or sidecar address.
- **Some deployments cannot use the list as intended.** Where the peer address is not the proxy's (NLB client IP preservation), configuration changes on the proxy are needed.
- **Weaker default posture messaging.** "TLS is mandatory" is a simple, enforceable rule. "TLS is mandatory except on an optional DynamoDB API listener" is harder to audit, and a reviewer can no longer assume from the binary alone that traffic is encrypted.
- **Lifecycle complexity.** Two listeners require coordinated binding, shutdown, and error propagation in a startup path that is already subtle (bind-before-fork, daemonization, PID file cleanup).

## Alternatives

**A generic `[[server.listeners]]` array.** Each entry would carry its own address, port, TLS setting, and route set, generalizing to future cases such as a metrics-only listener. Rejected: a plaintext listener would be one field away from looking like the TLS listener, which makes it easy to enable by editing the wrong entry and harder to spot when reviewing a config. A dedicated `[server.plaintext]` section is distinct and obvious, and it leaves `server.bind_addr` and `server.port` unchanged.

**Allow `tls.enabled = false` on the existing listener.** This is the smallest code change, but it puts `/management` HTTP Basic passwords, console login, and console session cookies on a plaintext link.

**Serve plaintext and TLS on one port by sniffing the first byte.** `HttpsRedirectAcceptor` already peeks for a TLS `ClientHello`. Extending it to serve plaintext instead of redirecting would avoid a second port, but it cannot give the two transports different bind addresses or route tables, and it prevents firewalling the plaintext path separately.

**Re-encrypt from the proxy to ExtendDB's TLS listener.** This works today. It requires distributing ExtendDB's certificate to every proxy, pays for TLS twice per request, and adds nothing when the proxy is a sidecar on the same host. It remains the recommended configuration when the proxy-to-ExtendDB link is not trusted, or when no proxy can be guaranteed to refuse plaintext from its clients.

**Report `aws:SecureTransport = false` on the plaintext listener unconditionally.** This is strictly truthful about the connection ExtendDB received and needs no new configuration. It also means the standard AWS pattern, `Deny` when `aws:SecureTransport` is `false`, blocks all proxied traffic, so operators running behind a TLS-terminating proxy would have to delete the policies that protect their TLS listener. `secure_transport_sources` keeps those policies usable and, when empty, gives exactly this behavior.

**A per-listener boolean asserting secure transport.** A single `assume_secure_transport = true` would report `true` for every request on the plaintext listener, with an extra acknowledgment key required on routable bind addresses. It needs no address configuration, but it reports `true` to any client that bypasses the proxy, and ExtendDB has no way to tell the difference. Rejected in favor of classification by peer address, which ExtendDB can observe and which fails closed when the list is too narrow or empty.

**Refuse connections from peers outside `secure_transport_sources`.** Closing unlisted connections at accept time would make the listener restrictive without any IAM policy. Rejected for this version: it moves an access decision from IAM policy into listener configuration, and it complicates health checks and debugging, since a health checker or operator connecting from an unlisted address would see connections closed with no response. Network controls already provide connection-level restriction.

**Derive `aws:SecureTransport` per request from `X-Forwarded-Proto`, read only from `secure_transport_sources`.** HashiCorp Vault uses this model for `X-Forwarded-For`. It would handle a listed proxy that forwards both TLS and plaintext client traffic, which this RFC instead forbids. It is not proposed for the first version because proxies differ in whether they set, append to, or pass through the header, and a proxy that passes a client-supplied header through unchanged turns it into a client-controlled value. It can be added later as an opt-in refinement that reads the header only on requests from a listed source, without changing the configuration proposed here.

**PROXY protocol v2.** NLB and HAProxy can send PROXY protocol headers that carry the client address and, with the `PP2_TYPE_SSL` TLV, whether the client connection used TLS. ALB and most mesh sidecars do not send it, so it does not cover the motivating deployments. Out of scope; a candidate follow-up if client source addresses become needed.

## Unresolved questions

1. Should plaintext connections be limited to only addresses in the `secure_transport_sources` list?

## Prior art

- **AWS IAM `aws:SecureTransport`.** A global boolean condition key present on every request, most commonly used in `Deny` statements on S3 bucket policies to reject non-TLS requests. See the [AWS global condition context keys reference](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-securetransport). This RFC follows its type, presence, and evaluation semantics.
- **Apache Kafka named listeners.** Kafka brokers declare multiple listeners, each with its own address and security protocol (`PLAINTEXT`, `SSL`, `SASL_SSL`), commonly used to expose plaintext only on an internal network. See [`listeners`](https://kafka.apache.org/documentation/#brokerconfigs_listeners). The per-listener model was considered and rejected in Alternatives in favor of a dedicated section.
- **HashiCorp Vault TCP listener.** Vault supports multiple listeners with per-listener `tls_disable` and an `x_forwarded_for_authorized_addrs` allowlist for proxied deployments. See the [TCP listener documentation](https://developer.hashicorp.com/vault/docs/configuration/listener/tcp). Vault's documentation is explicit that disabling TLS is only appropriate behind a TLS-terminating proxy, which is the stance taken here.
- **Service mesh sidecars (Istio, Linkerd, Envoy).** The standard mesh model terminates mTLS in a sidecar and forwards plaintext to the application inside the pod. Since Istio 1.10 that delivery goes to the pod IP rather than loopback, as described in [Upcoming networking changes in Istio 1.10](https://istio.io/latest/blog/2021/upcoming-networking-changes/). This is the primary deployment this RFC targets, and the reason it requires enforced mTLS for listed mesh sources.
- **DynamoDB Local.** Serves plaintext HTTP only, with no TLS option. It is a development tool and is not deployed behind proxies in production, so it offers no precedent for the warning or condition-key design.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0. See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark of Amazon.com, Inc.
