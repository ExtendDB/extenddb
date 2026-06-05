# RFC-0001: Extract serve orchestration for external backend reuse

- Status: Draft
- Author: @LeeroyHannigan
- Created: 2026-06-05
- FCP ends: TBD
- Tracking issue: TBD

## Summary

Expose the server startup orchestration as a public function on `extenddb-server`
so that external storage backends can produce runnable server binaries without
forking the `extenddb` repository.

## Motivation

ExtendDB's storage layer is trait-based. Backends implement the storage traits,
register via `inventory::submit!`, and the server discovers them at link time. The
traits, engine, and HTTP server are cleanly separated into library crates.

However, an external backend author who implements these traits **cannot produce a
runnable server** without forking the binary crate. The reason: the server startup
orchestration — assembling `AppState` from storage components, constructing
auth/cache layers, spawning background workers, configuring TLS, and calling
`start_server()` — lives in `crates/bin/src/cmd_serve.rs` as a private 380-line
function. None of it is reusable.

Today, producing a backend-specific binary requires:
1. Fork the `extenddb` repo
2. Add the backend crate as a cargo feature
3. Rebuild the binary

Community backends already exist (TiDB #140, SQLite #109). The in-memory backend
runs separately. Each would benefit from shipping an independent distribution
against published crates instead of maintaining a fork.

## Detailed design

### New public API on `extenddb-server`

```rust
/// Configuration for server orchestration.
pub struct ServeConfig {
    pub region: String,
    pub tls: Option<ServerTlsConfig>,
    pub auth_cache: AuthCacheConfig,
    pub import_paths: Vec<PathBuf>,
    pub export_paths: Vec<PathBuf>,
    pub version_info: String,
    pub log_level: String,
}

/// Auth cache configuration matching the [auth.cache] TOML section.
pub struct AuthCacheConfig {
    pub enabled: bool,
    pub ttl_seconds: u64,
    pub soft_ttl_seconds: u64,
    pub negative_ttl_seconds: u64,
    pub max_entries: usize,
}

/// Start a fully-wired ExtendDB server.
///
/// Assembles AppState from the provided components, spawns background
/// workers, and runs the HTTP server until shutdown.
///
/// The caller is responsible for:
/// - Binding the TCP listener
/// - Setting up logging/tracing
/// - Process lifecycle (daemonize, PID files, signal handling)
///
/// The function is responsible for:
/// - Constructing auth provider and cache layers
/// - Assembling AppState
/// - Spawning background workers (metrics, settings polling, TTL)
/// - TLS termination
/// - Running the HTTP server until graceful shutdown
pub async fn serve(
    config: ServeConfig,
    components: ServerComponents,
    listener: tokio::net::TcpListener,
    pid_file: Option<PathBuf>,
) -> anyhow::Result<()>;
```

### External backend usage

```rust
// my-backend-server/src/main.rs
extern crate my_storage_backend; // ensures inventory links it

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let components = extenddb_storage::create_server_components(
        "my-backend",
        &my_config,
        "us-east-1",
    ).await?;

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await?;

    extenddb_server::serve(
        ServeConfig {
            region: "us-east-1".into(),
            tls: None,
            auth_cache: AuthCacheConfig { enabled: true, ..Default::default() },
            import_paths: vec![],
            export_paths: vec![],
            version_info: "my-backend 0.1.0".into(),
            log_level: "info".into(),
        },
        components,
        listener,
        None,
    ).await
}
```

### What stays in the binary crate

- Clap argument parsing and subcommand dispatch
- Config file parsing (`AppConfig` from `extenddb.toml`)
- Daemonization and PID file management
- Syslog vs stderr logging setup
- `env!("EXTENDDB_GIT_HASH")` version printing
- All other subcommands (`init`, `destroy`, `stop`, `status`, etc.)

### What moves into `extenddb_server::serve()`

- `create_server_components()` call (already public, but the wiring around it)
- Credential cache wrapping (`CachedCredentialStore`)
- Auth provider construction (`BuiltinAuthProvider`)
- Authz cache construction (`CachedAuthzStore`)
- Table key info cache construction
- Auth cache registry assembly
- Import/export path resolution
- AppState assembly
- Background worker spawning (metrics, log level polling, throttle polling, cleanup)
- Runtime hooks dispatch (`spawn_workers`)
- TLS config and `start_server()` call

### Implementation order

1. Define `ServeConfig` and `AuthCacheConfig` on `extenddb-server`
2. Extract the body of `serve_inner()` from `crates/bin/src/cmd_serve.rs` into
   `extenddb_server::serve()`
3. `cmd_serve.rs` becomes: parse config → setup logging → daemonize → call
   `extenddb_server::serve()`
4. Verify: all tests pass, integration tests pass, both feature configurations
   compile

### Behavioral changes

None. The HTTP server, wire protocol, auth behavior, and operational model are
unchanged. This is a code organization change only.

## Drawbacks

- **`extenddb-server`'s public API grows.** One function and two config structs.
  These become part of the contract that external backends depend on — changes
  require semver consideration.
- **Worker internals become coupled to the server crate.** Background workers
  (metrics polling, log level reloading) move from the binary into the library.
  Their implementation details become harder to change without a minor version bump.
- **Pre-1.0 churn.** The `ServeConfig` struct may need revision as we learn what
  external backends actually need. Early adopters will face breaking changes.

## Alternatives

1. **Extract the entire CLI into a library crate.** Rejected: the CLI (clap args,
   daemonize, syslog, version printing) is binary-specific. No external backend
   wants to reuse it — they want their own CLI. This over-extracts and creates a
   large, unmotivated public API surface.

2. **Do nothing — document the fork path.** Workable for one or two backends,
   unscalable for a community. Each fork must track upstream changes manually.

3. **Dynamic plugin loading.** Rejected: Rust has no stable ABI, `inventory` is
   link-time only, and plugins add attack surface for no benefit.

4. **Builder pattern on AppState.** Instead of one `serve()` function, expose an
   `AppStateBuilder` that external backends configure step by step. More flexible
   but more API surface, more ways to misconfigure, and more maintenance. The single
   function with a config struct is simpler and sufficient.

## Unresolved questions

1. Should `serve()` own logging setup (the tracing filter + reload handle), or
   should the caller set up tracing and pass a reload handle in? Owning it is
   simpler for external backends; passing it in is more flexible.

2. Should the `init`/`destroy`/`migrate` orchestration also be extracted, or is
   `serve()` sufficient? External backends that want their own init flow can call
   `create_bootstrapper()` directly (already public). Full lifecycle extraction can
   follow if there's demand.

3. Exact semver policy for `ServeConfig` — should fields be added via
   `#[non_exhaustive]` to allow future additions without breaking?
