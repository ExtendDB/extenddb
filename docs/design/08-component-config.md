# extenddb — Component Design: Configuration & Operations

**Version:** 1.0
**Date:** 2026-04-03
**Status:** Draft

## 1. Purpose

This document covers the configuration system, configurable limits, logging, metrics, health checks, and deployment considerations. Configuration is handled in the `bin` crate (startup wiring) with types defined in `core` (limits) and `server` (server config).

## 2. Configuration Layering

Precedence (highest to lowest):
1. **CLI flags** — `--config`, `--port`, `--version`, `--validate-config`
2. **Environment variables** — `EXTENDDB__SERVER__PORT=8000`
3. **Config file** — TOML format, path specified via `--config` or generated as `extenddb.toml`
4. **Defaults** — hardcoded in Rust structs via `Default` trait

### 2.1 CLI (clap)

```rust
#[derive(Parser)]
#[command(name = "extenddb", about = "DynamoDB-compatible API server")]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Override server port
    #[arg(short, long)]
    port: Option<u16>,

    /// Validate configuration and exit
    #[arg(long)]
    validate_config: bool,

    /// Print version and exit
    #[arg(short = 'V', long)]
    version: bool,
}
```

### 2.2 Environment Variable Convention

Environment variables use double-underscore (`__`) as a nesting separator, prefixed with `EXTENDDB`:

| Config Key | Environment Variable |
|-----------|---------------------|
| `server.port` | `EXTENDDB__SERVER__PORT` |
| `server.bind_addr` | `EXTENDDB__SERVER__BIND_ADDR` |
| `storage.backend` | `EXTENDDB__STORAGE__BACKEND` |
| `storage.tidb.connection_string` | `EXTENDDB__STORAGE__TIDB__CONNECTION_STRING` |
| `storage.tidb.resource_group` | `EXTENDDB__STORAGE__TIDB__RESOURCE_GROUP` |
| `storage.tidb.default_read_staleness_seconds` | `EXTENDDB__STORAGE__TIDB__DEFAULT_READ_STALENESS_SECONDS` |
| `auth.provider` | `EXTENDDB__AUTH__PROVIDER` |
| `auth.encryption_key` | `EXTENDDB__AUTH__ENCRYPTION_KEY` |
| `limits.max_item_size_bytes` | `EXTENDDB__LIMITS__MAX_ITEM_SIZE_BYTES` |

### 2.3 Loading

```rust
use config::{Config, Environment, File};

fn load_config(cli: &Cli) -> Result<AppConfig, anyhow::Error> {
    let config_path = cli.config.to_str()
        .ok_or_else(|| anyhow::anyhow!("Config path is not valid UTF-8: {:?}", cli.config))?;

    let config = Config::builder()
        .add_source(File::with_name(config_path).required(false))
        .add_source(Environment::with_prefix("EXTENDDB").separator("__"))
        .build()?;

    let mut app_config: AppConfig = config.try_deserialize()?;

    // CLI overrides
    if let Some(port) = cli.port {
        app_config.server.port = port;
    }

    Ok(app_config)
}
```

## 3. TOML Configuration File

```toml
# extenddb configuration

[server]
bind_addr = "0.0.0.0"
port = 8000
max_connections = 10000
request_timeout_secs = 30
shutdown_drain_secs = 30       # Time to wait for in-flight requests before force-closing
max_request_size_bytes = 16777216  # 16 MB

[server.tls]
enabled = true
cert_path = "/etc/extenddb/tls/cert.pem"
key_path = "/etc/extenddb/tls/key.pem"

[server.rate_limit]
global_rps = 0          # 0 = disabled
per_table_rps = 0       # 0 = disabled

[storage]
backend = "tidb"         # "tidb" | "postgres"

[storage.tidb]
connection_string = "mysql://extenddb:extenddb-local-dev@localhost:4000/extenddb_catalog"
pool_size = 20
catalog_pool_size = 20
# pool_size sizes the strong and default-read data pools. catalog_pool_size
# separately sizes catalog metadata, control-plane, authz, IAM, and console pools.
# TiDB default reads use native closest-adaptive follower read; ConsistentRead=true
# and writes use the strong data pool.
# default_read_staleness_seconds = 5  # optional bounded stale read for base-table default reads

[storage.tidb.backup]
pd_endpoint = "127.0.0.1:2379"
storage_uri = "local:///var/lib/extenddb/tidb-backups"
binary = "tiup"
component = "br"
send_credentials_to_tikv = false

[storage.postgres]
connection_string = "postgresql://localhost:5432/extenddb"  # Explicit alternate backend
pool_size = 20
connection_timeout_secs = 5
statement_timeout_secs = 30

[auth]
provider = "builtin"     # builtin SigV4 + local IAM; auth is mandatory
region = "us-east-1"     # Region for SigV4 validation
# encryption_key: Set via EXTENDDB__AUTH__ENCRYPTION_KEY env var (never in config files)
credential_cache_ttl_secs = 300
policy_cache_ttl_secs = 300
timestamp_skew_secs = 300

[limits]
# Item & attribute limits
max_item_size_bytes = 409600           # 400 KB
max_partition_key_size_bytes = 2048
max_sort_key_size_bytes = 1024
max_attribute_name_bytes = 65535
max_index_key_name_length = 255
max_nesting_depth = 32
max_expression_length_bytes = 4096
max_expression_attribute_names = 100
max_expression_attribute_values = 100
max_projected_attributes = 100

# Table & index limits
max_tables_per_account = 2500
max_gsis_per_table = 20
max_lsis_per_table = 5
max_item_collection_size_gb = 10

# Throughput limits
per_table_max_rcu = 40000
per_table_max_wcu = 40000
per_account_max_rcu = 80000
per_account_max_wcu = 80000
per_partition_max_rcu = 3000
per_partition_max_wcu = 1000

# Operation limits
batch_get_max_items = 100
batch_get_max_response_bytes = 16777216   # 16 MB
batch_write_max_items = 25
transact_get_max_items = 100
transact_write_max_items = 100
query_scan_max_response_bytes = 1048576   # 1 MB
list_tables_max_per_page = 100

[logging]
level = "info"           # trace, debug, info, warn, error
format = "json"          # "json" | "pretty" (human-readable)
request_logging = true   # Log every request
# Logging output is determined by launch mode:
#   extenddb serve → syslog (facility: daemon, ident: extenddb)
#   Read logs with: journalctl -t extenddb

[metrics]
enabled = true
endpoint = "/metrics"    # Prometheus scrape endpoint path

[ttl]
enabled = true
scan_interval_secs = 60
batch_size = 100         # Items to delete per scan cycle

[streams]
default_shard_count = 4
retention_hours = 24
cleanup_interval_secs = 3600
```

## 4. Configuration Types

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub limits: LimitsConfig,
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,
    pub ttl: TtlConfig,
    pub streams: StreamsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_shutdown_drain")]
    pub shutdown_drain_secs: u64,
    #[serde(default = "default_max_request_size")]
    pub max_request_size_bytes: usize,
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    // All fields have #[serde(default = "...")] with DynamoDB-compatible defaults
    pub max_item_size_bytes: usize,           // 409600
    pub max_partition_key_size_bytes: usize,   // 2048
    pub max_sort_key_size_bytes: usize,        // 1024
    pub max_tables_per_account: usize,         // 2500
    pub max_gsis_per_table: usize,             // 20
    pub max_lsis_per_table: usize,             // 5
    pub per_table_max_rcu: u64,                // 40000
    pub per_table_max_wcu: u64,                // 40000
    pub per_account_max_rcu: u64,              // 80000
    pub per_account_max_wcu: u64,              // 80000
    pub per_partition_max_rcu: u64,            // 3000
    pub per_partition_max_wcu: u64,            // 1000
    pub batch_get_max_items: usize,            // 100
    pub batch_write_max_items: usize,          // 25
    pub transact_get_max_items: usize,         // 100
    pub transact_write_max_items: usize,       // 100
    pub query_scan_max_response_bytes: usize,  // 1048576
    // ... all other limits from 01-requirements.md §5
}
```

## 5. Logging

### 5.1 Setup

```rust
use tracing_subscriber::{fmt, EnvFilter};

pub fn init_logging(config: &LoggingConfig) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.level));

    match config.format.as_str() {
        "json" => {
            fmt().json().with_env_filter(filter).init();
        }
        _ => {
            fmt().with_env_filter(filter).init();
        }
    }
}
```

### 5.2 Request Logging

Every request logs:
- Request ID
- Operation name
- Table name (if applicable)
- Response status code
- Latency (ms)
- Consumed capacity (if tracked)

```
{"timestamp":"2026-04-03T12:00:00Z","level":"INFO","request_id":"abc-123","operation":"PutItem","table":"Users","status":200,"latency_ms":3.2,"wcu":1.0}
```

## 6. Metrics

Using the `metrics` crate with `metrics-exporter-prometheus`:

```rust
use metrics::{counter, histogram};

pub fn record_request(operation: &str, status: u16, latency: Duration) {
    counter!("dynamodb_requests_total", "operation" => operation.to_string(), "status" => status.to_string()).increment(1);
    histogram!("dynamodb_request_duration_seconds", "operation" => operation.to_string()).record(latency.as_secs_f64());
}

pub fn record_capacity(table: &str, rcu: f64, wcu: f64) {
    // Track in milli-units to preserve fractional capacity (e.g., 0.5 RCU for eventually consistent reads)
    counter!("dynamodb_consumed_rcu_milli_total", "table" => table.to_string()).increment((rcu * 1000.0) as u64);
    counter!("dynamodb_consumed_wcu_milli_total", "table" => table.to_string()).increment((wcu * 1000.0) as u64);
}
```

## 7. Background Workers

Storage backends install runtime workers through backend-specific hooks alongside the HTTP server. Workers must use native
storage capabilities where available:

- PostgreSQL runs an application TTL cleanup worker because PostgreSQL has no table-native TTL.
- TiDB uses native table TTL for item expiration and must not run a parallel application TTL worker.

### 7.1 Stream Record Cleanup Worker

```rust
async fn stream_cleanup_worker(storage: Arc<dyn StorageEngine>, config: StreamsConfig) {
    let mut interval = tokio::time::interval(Duration::from_secs(config.cleanup_interval_secs));
    loop {
        interval.tick().await;
        // Delete stream records older than retention_hours
    }
}
```

## 8. Binary Entry Point

The `bin` crate wires everything together:

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.version {
        println!("extenddb {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let config = load_config(&cli)?;

    if cli.validate_config {
        println!("Configuration is valid.");
        return Ok(());
    }

    init_logging(&config.logging);

    // Initialize selected storage backend. `serve` validates catalog version
    // but never runs migrations; `init` and `migrate` are explicit lifecycle
    // commands.
    let backend = storage_registry::select(&config.storage.backend)?;
    let storage = backend.connect(&config).await?;
    storage.check_catalog_version().await?;

    // Initialize auth provider
    let credential_store = StorageCredentialAdapter::new(storage.clone());
    anyhow::ensure!(config.auth.provider == "builtin", "auth.provider must be builtin");
    let auth: Arc<dyn AuthProvider> = Arc::new(BuiltinAuthProvider::new(
        credential_store,
        config.auth.clone(),
    ));

    // Initialize metrics
    let metrics = init_metrics(&config.metrics);

    // Build app state and start backend runtime hooks. PostgreSQL may install
    // application TTL and statistics workers. TiDB relies on native table TTL
    // and information_schema statistics, so it installs only TiDB-specific
    // control-plane and pool-metrics workers.
    let state = AppState::new(storage.clone(), auth, metrics, config.clone());
    if let Some(hooks) = storage.runtime_hooks() {
        hooks.spawn_workers(&WorkerContext::from_config(&config)).await;
    }

    // Start HTTP server
    start_server(config.server, state).await
}
```

## 9. Deployment

### 9.1 VM Deployment

```bash
# Install
cp extenddb /usr/local/bin/
cp config.toml /etc/extenddb/config.toml

# systemd service
[Unit]
Description=extenddb
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
ExecStart=/usr/local/bin/extenddb serve --config /etc/extenddb/config.toml
ExecStop=/usr/local/bin/extenddb stop --config /etc/extenddb/config.toml
Restart=on-failure
RestartSec=5
Environment=EXTENDDB__AUTH__ENCRYPTION_KEY=<secret>

[Install]
WantedBy=multi-user.target
```

### 9.2 Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: extenddb
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: extenddb
        image: extenddb:latest
        ports:
        - containerPort: 8000
        env:
        - name: EXTENDDB__SERVER__BIND_ADDR
          value: "0.0.0.0"
        - name: EXTENDDB__STORAGE__POSTGRES__CONNECTION_STRING
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: connection-string
        - name: EXTENDDB__AUTH__ENCRYPTION_KEY
          valueFrom:
            secretKeyRef:
              name: auth-secrets
              key: encryption-key
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 3
          periodSeconds: 5
        resources:
          requests:
            cpu: 500m
            memory: 256Mi
          limits:
            cpu: 2000m
            memory: 1Gi
```

### 9.3 Docker Build

```dockerfile
# Static musl build for minimal container image (REQ-DEPLOY-006: < 50 MB)
FROM rust:1.85-alpine AS builder
RUN apk add --no-cache musl-dev
RUN rustup target add x86_64-unknown-linux-musl
WORKDIR /app
COPY . .
RUN cargo build -j12 --release --target x86_64-unknown-linux-musl -p extenddb

FROM scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/extenddb /extenddb
COPY extenddb.sample.toml /etc/extenddb/config.toml
EXPOSE 8000
ENTRYPOINT ["/extenddb"]
CMD ["serve", "--config", "/etc/extenddb/config.toml"]
```

## 10. SDK Client Configuration

extenddb serves both DynamoDB and DynamoDB Streams on a single port. An unmodified application that works against AWS must work against extenddb by changing only endpoint configuration.

### 10.1 Recommended Setup: Environment Variables

The simplest approach — set two service-specific endpoint variables and credentials:

```bash
# Endpoint overrides (both point to the same extenddb instance)
export AWS_ENDPOINT_URL_DYNAMODB=https://127.0.0.1:8000
export AWS_ENDPOINT_URL_DYNAMODB_STREAMS=https://127.0.0.1:8000

# Credentials (must match a pair registered in extenddb's credential store)
export AWS_ACCESS_KEY_ID=local-dev-key
export AWS_SECRET_ACCESS_KEY=local-dev-secret
export AWS_DEFAULT_REGION=us-east-1
```

This works with every AWS SDK (Python/boto3, Java v2, Rust, Go v2, .NET, Node.js) — all support the `AWS_ENDPOINT_URL_<SERVICE>` convention since late 2023.

**Alternative — single global override** (when the application only uses DynamoDB):

```bash
export AWS_ENDPOINT_URL=https://127.0.0.1:8000
```

This sends all AWS SDK calls to extenddb. Simpler, but breaks if the application also calls non-DynamoDB services.

### 10.2 Alternative Setup: AWS Config File

For teams that prefer file-based configuration or need to switch between extenddb and real DynamoDB:

```ini
# ~/.aws/config
[profile extenddb]
region = us-east-1
services = extenddb-services

[services extenddb-services]
dynamodb =
  endpoint_url = https://127.0.0.1:8000
dynamodb_streams =
  endpoint_url = https://127.0.0.1:8000
```

```ini
# ~/.aws/credentials
[extenddb]
aws_access_key_id = local-dev-key
aws_secret_access_key = local-dev-secret
```

Activate with `export AWS_PROFILE=extenddb`. Switch back to real DynamoDB with `export AWS_PROFILE=default` (or unset).

### 10.3 How It Works

AWS SDKs resolve endpoints in this order (highest priority first):

1. Service-specific env var (`AWS_ENDPOINT_URL_DYNAMODB`, `AWS_ENDPOINT_URL_DYNAMODB_STREAMS`)
2. Global env var (`AWS_ENDPOINT_URL`)
3. Service-specific setting in `~/.aws/config` `[services]` section
4. Global `endpoint_url` in `~/.aws/config` profile
5. SDK default (real AWS regional endpoint)

When any override is set, the SDK disables endpoint discovery (`DescribeEndpoints` is never called automatically). All requests go directly to the configured URL.

extenddb accepts both `DynamoDB_20120810.*` and `DynamoDBStreams_20120810.*` target prefixes on the same port. Both use `dynamodb` as the SigV4 signing name, so authentication is identical.

### 10.4 DescribeEndpoints Response

When called, `DescribeEndpoints` returns the server's own listen address:

```json
{
    "Endpoints": [
        {
            "Address": "localhost:8000",
            "CachePeriodInMinutes": 10
        }
    ]
}
```

The `Address` is derived from the server's configured `bind_addr` and `port`. If `bind_addr` is `0.0.0.0`, the response uses the `Host` header from the incoming request instead (so the client gets back the address it actually connected to).

### 10.5 Credential Pairing

The credentials in `~/.aws/credentials` (or env vars) are what the SDK uses to sign requests. extenddb validates these signatures against its own credential store. The access key and secret key must match on both sides:

1. Register credentials in extenddb's credential store (via admin API or database seeding)
2. Configure the same `aws_access_key_id` / `aws_secret_access_key` in the SDK client's environment

For development environments, a default credential pair can be seeded automatically on first startup (configurable via `auth.seed_credentials` in `config.toml`).

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
