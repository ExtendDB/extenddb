// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Configuration shapes for the backup store.
//!
//! These model the `[backup.store]` table of the server configuration. The
//! `[backup]` table itself (job knobs such as `data_file_target_bytes`) is
//! the server's; nesting the store under its own key lets the server keep
//! `deny_unknown_fields` on both without `serde(flatten)`, which would drop
//! it, and without a second copy of these fields:
//!
//! ```toml
//! [backup.store]
//! kind = "filesystem"                   # "filesystem" | "s3"; filesystem when absent
//!
//! [backup.store.filesystem]
//! path = "<data_dir>/backups"           # required when kind = "filesystem"; the server defaults it
//!
//! [backup.store.s3]
//! bucket = "my-extenddb-backups"        # required when kind = "s3"; must not be empty
//! prefix = "extenddb/"                  # optional; at most 512 bytes
//! region = "us-east-1"                  # optional; SDK chain otherwise
//! endpoint = "http://127.0.0.1:9000"    # optional; moto, Garage, LocalStack, or any S3-compatible endpoint
//! allow_insecure_endpoint = false       # an http:// endpoint must be loopback unless true
//! force_path_style = true               # false when absent; an http:// endpoint needs true
//! upload_part_size_bytes = 8388608      # 5 MiB to 5 GiB (S3's part size limits)
//! max_concurrent_uploads = 4            # at least 1
//! ```
//!
//! Endpoints. The rules below apply to every endpoint setting the SDK reads
//! for a client that sends requests for the store, and `open` checks each
//! before the S3 client is built and before any request is sent:
//!
//! - the S3 client's: `endpoint` when it is set, otherwise one the AWS SDK
//!   reads itself (`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, or
//!   `endpoint_url` in the shared config profile or its `services`
//!   section's `s3` entry);
//! - those of the STS, SSO and SSO OIDC clients the SDK's credential
//!   providers build (web identity, assume role, SSO): `AWS_ENDPOINT_URL_STS`,
//!   `AWS_ENDPOINT_URL_SSO`, `AWS_ENDPOINT_URL_SSO_OIDC`, `AWS_ENDPOINT_URL`,
//!   and the profile's `services` entries (`sts`, `sso`, `sso-oidc`) and
//!   `endpoint_url`. `endpoint` does not reach them;
//! - the general setting (`AWS_ENDPOINT_URL`, or the profile's
//!   `endpoint_url`), which any SDK client without its own setting falls
//!   back to;
//! - the IMDS client's (`AWS_EC2_METADATA_SERVICE_ENDPOINT`, or the
//!   profile's `ec2_metadata_service_endpoint`), with IMDS's own clear-text
//!   rule below. When neither is set the client uses IMDS's link-local
//!   address, which is not checked.
//!
//! The credential endpoints are checked whether or not the configured
//! credentials use that service (which provider answers is decided at the
//! first request), so an `http://` setting meant for another tool, such as
//! `AWS_ENDPOINT_URL` pointing at a local emulator on another host, fails
//! `open` until it is removed or the override is set. The one setting the
//! store does not check is the container credentials URI
//! (`AWS_CONTAINER_CREDENTIALS_FULL_URI`), which keeps the SDK's own rule
//! (`http://` only to loopback, 169.254.170.2, 169.254.170.23 or
//! `fd00:ec2::23`, or to a name that resolves to one of those; `https://`
//! to any host); see Proxies below for what `open` does with an `http://`
//! one. A refusal names the client, the setting the endpoint came from and
//! the problem, never the value.
//!
//! Syntax. The endpoint must be an absolute `http://` or `https://` URI
//! with a host, an optional port (0-65535) and an optional path. `open`
//! refuses user information (`user@`, `user:password@`; the SDK's
//! credential chain supplies the credentials), a query component, a
//! fragment, an IPv6 zone id (`[fe80::1%25eth0]`), and any value that
//! `http::Uri` or `url::Url` (the two parsers the SDK's endpoint rules use)
//! refuses, such as a stray space or `256.0.0.1`.
//!
//! Clear text. An `http://` endpoint sends requests, signed but not
//! encrypted, in clear text. `open` accepts an `http://` endpoint only when
//! its host is one of the loopback forms `127.x.y.z` (`127.0.0.0/8` in
//! dotted-quad form), an IPv6 address equal to `::1` (`[::1]`), or
//! `localhost` (any case), and, for the S3 endpoint, only with
//! `force_path_style = true`: without it the SDK chooses the addressing by
//! bucket name and sends to `<bucket>.<host>`, a name the system resolver
//! may look up in DNS. It does so for a virtual-hostable bucket name on
//! `localhost` and for an access point ARN on any custom endpoint; on an IP
//! host it addresses every plain bucket name path-style (aws-sdk-s3 1.122.0,
//! `config/endpoint/internals.rs`). The rule is keyed on the scheme rather
//! than on the host's or the bucket's form because that choice is the
//! SDK's and may change. The IMDS endpoint may also name `169.254.169.254`
//! or `[fd00:ec2::254]`, the two link-local addresses IMDS answers at. Other
//! spellings of a loopback address (`127.1`, `localhost.`,
//! `[::ffff:127.0.0.1]`) are refused. `allow_insecure_endpoint = true`
//! accepts any `http://` endpoint, S3, credential and IMDS ones alike, and
//! logs a warning. Without the override, every request the S3 client sends
//! is also checked before it is sent, and an `http://` request whose host
//! is not one of those forms is refused. The S3 client sends no S3 Express
//! `CreateSession` (the one request the SDK sends from a client without
//! the store's check): `open` refuses the two bucket names the SDK routes
//! to S3 Express, a directory bucket (`--x-s3`) and a directory-bucket
//! access point (`--xa-s3`), and disables S3 Express session
//! authentication, so even a client that reached such a name would not
//! send it. The credential providers' requests are not checked when they
//! are sent; the check at `open` is the only one they get, and it reads
//! the environment and the shared config files as they are at `open`: the
//! SDK's IMDS client reads its endpoint setting from the environment again
//! at every request, so a process that changed `AWS_EC2_METADATA_SERVICE_ENDPOINT`
//! after `open` would send where the check never looked (nothing in this
//! workspace changes its environment).
//!
//! Proxies. An `http://` S3 endpoint is always reached directly: the S3
//! client ignores `HTTP_PROXY`, `http_proxy`, `ALL_PROXY` and `all_proxy`
//! for it, so a clear-text S3 request goes to the endpoint's host and never
//! to a proxy. An `https://` endpoint (and the AWS endpoint when none is
//! set) uses the proxy settings the SDK reads from the environment
//! (`HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`); the connector speaks only HTTP
//! `CONNECT`, so with `ALL_PROXY=socks5://...` every `https://` request
//! fails. The credential chain's clients (STS, SSO, SSO OIDC, the container
//! credentials client and the IMDS client) use the SDK's default client,
//! which sends an `http://` request through `HTTP_PROXY` or `ALL_PROXY`
//! when set, unless any endpoint of the chain is `http://`: an STS, SSO,
//! SSO OIDC or general setting, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, or
//! the IMDS endpoint setting. Then the whole chain uses the same proxy-less
//! client as an `http://` S3 endpoint, so a credential request to a local
//! emulator or sidecar (a web identity token, an SSO access token or
//! refresh, a container authorization token, an IMDS session token) goes
//! to its host and never to a proxy; the chain's `https://` requests
//! then bypass `HTTPS_PROXY` as well (the S3 client's do not). Without such
//! a setting, two `http://` requests of the chain do go through
//! `HTTP_PROXY` or `ALL_PROXY`, and `NO_PROXY` must cover their hosts:
//! IMDS at its link-local address (169.254.169.254, or `fd00:ec2::254` with
//! `AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE=IPv6` or the profile's
//! `ec2_metadata_service_endpoint_mode = IPv6`), used for credentials and
//! for the region when none is configured, and the container credentials
//! client at 169.254.170.2 when `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` is
//! set. Otherwise the IMDS session token, the container authorization
//! token and the returned credentials cross the proxy in clear text.
//!
//! Region. When `region` is absent and neither `AWS_REGION`, then
//! `AWS_DEFAULT_REGION`, nor the profile names one, the SDK's region chain
//! queries IMDS during `open` (the S3 module doc has the details); off EC2,
//! set `region` or `AWS_EC2_METADATA_DISABLED=true`.
//!
//! Each request has a 60 s attempt timeout, and for `UploadPart` that
//! includes sending the part, so `upload_part_size_bytes` should be at most
//! about the link's bytes per second times 60: the default 8 MiB needs about
//! 140 KB/s, 5 GiB about 90 MB/s. A part that cannot finish in time is
//! retried and then fails the put. The SDK's retry settings
//! (`AWS_MAX_ATTEMPTS`, `AWS_RETRY_MODE`, the profile's `max_attempts` and
//! `retry_mode`) reach the S3 client only; the credential providers' clients
//! keep the SDK's standard three attempts. Resolving credentials (the whole
//! provider chain, every `AssumeRole` hop and an SSO refresh included) is
//! bounded separately by the SDK's 5 s identity-cache load timeout: a slow
//! credential path fails the request with `identity resolver timed out
//! after 5s`, not after 60 s.
//!
//! Wiring into `crates/config` and the server startup path is deliberately
//! not done here; this crate only defines the shapes and the
//! [`crate::open`] constructor over them.

use std::path::PathBuf;

use serde::Deserialize;

/// Which store implementation `[backup.store]` selects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupStoreKind {
    /// Local directory store. The default: local development works with no
    /// configuration, and the backup manual states the failure-domain
    /// consequence.
    #[default]
    Filesystem,
    /// S3 bucket store.
    S3,
}

/// The `[backup.store]` table.
///
/// Composition note for the wiring task: `deny_unknown_fields` does not
/// survive `serde(flatten)`, so the server nests this struct under its own
/// key (`store`) in its `[backup]` model rather than flattening it; that
/// keeps one definition of these fields and unknown-field rejection on both.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupStoreConfig {
    /// Selected store kind; `filesystem` when absent.
    #[serde(default)]
    pub kind: BackupStoreKind,
    /// `[backup.store.filesystem]`, required when `kind = "filesystem"`.
    #[serde(default)]
    pub filesystem: Option<FilesystemStoreConfig>,
    /// `[backup.store.s3]`, required when `kind = "s3"`.
    #[serde(default)]
    pub s3: Option<S3StoreConfig>,
}

/// `[backup.store.filesystem]` settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemStoreConfig {
    /// Root directory for backup objects. Must exist at startup.
    pub path: PathBuf,
}

/// `[backup.store.s3]` settings. Credentials are absent by design: they come
/// from the standard AWS SDK chain. `Debug` prints the endpoint without its
/// user information, which `open` refuses but a log of the configuration
/// written before `open` would otherwise carry.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3StoreConfig {
    /// Destination bucket.
    pub bucket: String,
    /// Key prefix inside the bucket. Optional; empty means the bucket root.
    #[serde(default)]
    pub prefix: String,
    /// Region override. When absent the SDK chain resolves the region,
    /// querying IMDS at `open` when the environment and the profile name
    /// none (module doc, Region).
    #[serde(default)]
    pub region: Option<String>,
    /// Endpoint override for S3-compatible stores.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Accept an `http://` endpoint that breaks the clear-text rule in the
    /// module doc (a host that is not one of the loopback forms, or an S3
    /// endpoint without `force_path_style`), for the S3 client, the
    /// credential providers' clients and the IMDS client alike. Off by
    /// default: such an endpoint can carry requests, credentials and
    /// objects in clear text across the network.
    #[serde(default)]
    pub allow_insecure_endpoint: bool,
    /// Use path-style addressing; required by most S3-compatible stores, and
    /// by an `http://` endpoint unless `allow_insecure_endpoint` is set.
    #[serde(default)]
    pub force_path_style: bool,
    /// Multipart part size in bytes. Defaults to 8 MiB; the enforced range
    /// is 5 MiB to 5 GiB, the smallest and largest part S3 accepts. Each part
    /// must upload within the 60 s attempt timeout (module doc), so keep it
    /// at most about the link's bytes per second times 60.
    #[serde(default = "default_upload_part_size_bytes")]
    pub upload_part_size_bytes: u64,
    /// Maximum concurrently in-flight part uploads per put. Defaults to 4.
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
}

impl std::fmt::Debug for S3StoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3StoreConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field(
                "endpoint",
                &self.endpoint.as_deref().map(redact_user_information),
            )
            .field("allow_insecure_endpoint", &self.allow_insecure_endpoint)
            .field("force_path_style", &self.force_path_style)
            .field("upload_part_size_bytes", &self.upload_part_size_bytes)
            .field("max_concurrent_uploads", &self.max_concurrent_uploads)
            .finish()
    }
}

/// `endpoint` with anything between the scheme's `//` and the last `@`
/// before the path replaced by `<redacted>@`. A value without `//` is
/// redacted up to its last `@` the same way (a scheme-less
/// `user:password@host`).
fn redact_user_information(endpoint: &str) -> String {
    let authority_start = endpoint.find("//").map_or(0, |at| at + 2);
    let rest = &endpoint[authority_start..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => format!("{}<redacted>{}", &endpoint[..authority_start], &rest[at..]),
        None => endpoint.to_owned(),
    }
}

fn default_upload_part_size_bytes() -> u64 {
    8 * 1024 * 1024
}

fn default_max_concurrent_uploads() -> usize {
    4
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Arc<dyn BackupStore>` has no `Debug`, so `unwrap_err` cannot be used.
    async fn open_err(config: &BackupStoreConfig) -> crate::StoreError {
        match crate::open(config).await {
            Err(e) => e,
            Ok(_) => panic!("expected open to fail"),
        }
    }

    #[test]
    fn parses_the_documented_filesystem_shape() {
        let config: BackupStoreConfig = toml::from_str(
            r#"
            kind = "filesystem"

            [filesystem]
            path = "/var/lib/extenddb/backups"
            "#,
        )
        .unwrap();
        assert_eq!(config.kind, BackupStoreKind::Filesystem);
        assert_eq!(
            config.filesystem.unwrap().path,
            PathBuf::from("/var/lib/extenddb/backups")
        );
        assert!(config.s3.is_none());
    }

    #[test]
    fn parses_the_documented_s3_shape_with_defaults() {
        let config: BackupStoreConfig = toml::from_str(
            r#"
            kind = "s3"

            [s3]
            bucket = "my-extenddb-backups"
            prefix = "extenddb/"
            region = "us-east-1"
            endpoint = "http://127.0.0.1:9000"
            force_path_style = true
            "#,
        )
        .unwrap();
        assert_eq!(config.kind, BackupStoreKind::S3);
        let s3 = config.s3.unwrap();
        assert_eq!(s3.bucket, "my-extenddb-backups");
        assert_eq!(s3.prefix, "extenddb/");
        assert_eq!(s3.region.as_deref(), Some("us-east-1"));
        assert_eq!(s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert!(s3.force_path_style);
        assert!(
            !s3.allow_insecure_endpoint,
            "the override is off by default"
        );
        assert_eq!(s3.upload_part_size_bytes, 8 * 1024 * 1024);
        assert_eq!(s3.max_concurrent_uploads, 4);
    }

    #[test]
    fn store_kind_defaults_to_filesystem() {
        let config: BackupStoreConfig = toml::from_str(
            r#"
            [filesystem]
            path = "/backups"
            "#,
        )
        .unwrap();
        assert_eq!(config.kind, BackupStoreKind::Filesystem);
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_level() {
        assert!(toml::from_str::<BackupStoreConfig>("stire = \"s3\"").is_err());
        assert!(
            toml::from_str::<BackupStoreConfig>(
                "kind = \"filesystem\"\n[filesystem]\npath = \"/b\"\npth = \"/b\"\n"
            )
            .is_err()
        );
        assert!(
            toml::from_str::<BackupStoreConfig>(
                "kind = \"s3\"\n[s3]\nbucket = \"b\"\nbukcet = \"b\"\n"
            )
            .is_err()
        );
    }

    #[test]
    fn unknown_store_kind_is_rejected() {
        assert!(toml::from_str::<BackupStoreConfig>("kind = \"gcs\"").is_err());
    }

    #[tokio::test]
    async fn open_requires_the_matching_section() {
        let missing_fs = BackupStoreConfig {
            kind: BackupStoreKind::Filesystem,
            filesystem: None,
            s3: None,
        };
        let err = open_err(&missing_fs).await;
        assert!(
            err.to_string().contains("[backup.store.filesystem]"),
            "{err}"
        );

        let missing_s3 = BackupStoreConfig {
            kind: BackupStoreKind::S3,
            filesystem: None,
            s3: None,
        };
        let err = open_err(&missing_s3).await;
        assert!(err.to_string().contains("[backup.store.s3]"), "{err}");
    }

    #[tokio::test]
    async fn open_enforces_the_part_size_floor() {
        let config = BackupStoreConfig {
            kind: BackupStoreKind::S3,
            filesystem: None,
            s3: Some(S3StoreConfig {
                bucket: "b".to_owned(),
                prefix: String::new(),
                region: Some("us-east-1".to_owned()),
                endpoint: None,
                allow_insecure_endpoint: false,
                force_path_style: false,
                upload_part_size_bytes: 4 * 1024 * 1024,
                max_concurrent_uploads: 4,
            }),
        };
        let err = open_err(&config).await;
        assert!(err.to_string().contains("minimum is 5242880"), "{err}");
    }

    #[tokio::test]
    async fn open_rejects_zero_concurrency() {
        let config = BackupStoreConfig {
            kind: BackupStoreKind::S3,
            filesystem: None,
            s3: Some(S3StoreConfig {
                bucket: "b".to_owned(),
                prefix: String::new(),
                region: Some("us-east-1".to_owned()),
                endpoint: None,
                allow_insecure_endpoint: false,
                force_path_style: false,
                upload_part_size_bytes: 8 * 1024 * 1024,
                max_concurrent_uploads: 0,
            }),
        };
        let err = open_err(&config).await;
        assert!(err.to_string().contains("max_concurrent_uploads"), "{err}");
    }

    #[tokio::test]
    async fn open_rejects_a_missing_filesystem_root() {
        let config = BackupStoreConfig {
            kind: BackupStoreKind::Filesystem,
            filesystem: Some(FilesystemStoreConfig {
                path: PathBuf::from("/nonexistent/extenddb-backup-root"),
            }),
            s3: None,
        };
        let err = open_err(&config).await;
        assert!(err.to_string().contains("not usable"), "{err}");
    }

    fn s3_config(bucket: &str, endpoint: Option<&str>) -> S3StoreConfig {
        S3StoreConfig {
            bucket: bucket.to_owned(),
            prefix: String::new(),
            region: Some("us-east-1".to_owned()),
            endpoint: endpoint.map(str::to_owned),
            allow_insecure_endpoint: false,
            force_path_style: true,
            upload_part_size_bytes: 8 * 1024 * 1024,
            max_concurrent_uploads: 4,
        }
    }

    fn s3_store_config(s3: S3StoreConfig) -> BackupStoreConfig {
        BackupStoreConfig {
            kind: BackupStoreKind::S3,
            filesystem: None,
            s3: Some(s3),
        }
    }

    #[tokio::test]
    async fn open_rejects_an_empty_bucket() {
        for bucket in ["", "  "] {
            let err = open_err(&s3_store_config(s3_config(bucket, None))).await;
            assert!(err.to_string().contains("bucket"), "{bucket:?}: {err}");
        }
        crate::open(&s3_store_config(s3_config("b", None)))
            .await
            .expect("a named bucket opens");
    }

    #[tokio::test]
    async fn open_enforces_the_part_size_ceiling() {
        let five_gib = 5 * 1024 * 1024 * 1024;
        let mut config = s3_config("b", None);
        config.upload_part_size_bytes = five_gib + 1;
        let err = open_err(&s3_store_config(config.clone())).await;
        assert!(err.to_string().contains("maximum is 5368709120"), "{err}");
        config.upload_part_size_bytes = five_gib;
        crate::open(&s3_store_config(config))
            .await
            .expect("exactly 5 GiB opens");
    }

    #[tokio::test]
    async fn open_refuses_a_plain_http_endpoint_that_is_not_loopback() {
        for endpoint in [
            "http://s3.example.com",
            "http://s3.example.com:9000/",
            "HTTP://10.0.0.5:9000",
            "http://128.0.0.1:9000",
            "http://[2001:db8::1]:9000",
            "http://localhost.example.com:9000",
            // Only a host that parses as an address is an address.
            "http://127.0.0.1.evil.example",
        ] {
            let err = open_err(&s3_store_config(s3_config("b", Some(endpoint)))).await;
            assert!(
                err.to_string().contains("allow_insecure_endpoint"),
                "{endpoint}: {err}"
            );
            let mut allowed = s3_config("b", Some(endpoint));
            allowed.allow_insecure_endpoint = true;
            crate::open(&s3_store_config(allowed))
                .await
                .unwrap_or_else(|e| panic!("{endpoint} with the override: {e}"));
        }
        for endpoint in [
            "http://127.0.0.1:9000",
            "http://127.255.0.9",
            "http://localhost:9000/",
            "http://LOCALHOST",
            "http://[::1]:9000",
            "https://s3.example.com",
            "https://10.0.0.5:9000",
        ] {
            crate::open(&s3_store_config(s3_config("b", Some(endpoint))))
                .await
                .unwrap_or_else(|e| panic!("{endpoint} must open: {e}"));
        }
        // An `http://` endpoint needs path-style addressing, whatever its
        // host; `https://` does not.
        for (endpoint, accepted) in [
            ("http://localhost:9000", false),
            ("http://LOCALHOST", false),
            ("http://127.0.0.1:9000", false),
            ("http://[::1]:9000", false),
            ("https://127.0.0.1:9000", true),
        ] {
            let mut config = s3_config("b", Some(endpoint));
            config.force_path_style = false;
            let result = crate::open(&s3_store_config(config)).await;
            match (result, accepted) {
                (Ok(_), true) => {}
                (Err(e), false) => assert!(
                    e.to_string().contains("force_path_style = true"),
                    "{endpoint}: {e}"
                ),
                (Ok(_), false) => panic!("{endpoint} without path style must be refused"),
                (Err(e), true) => panic!("{endpoint} without path style must open: {e}"),
            }
        }
    }

    /// An endpoint that breaks the syntax rule in the module doc is refused
    /// at `open`, not at the first request, and the refusal names the
    /// source and the problem class without repeating the value (an
    /// endpoint may carry a password). With or without the clear-text
    /// override. The same shapes through the SDK's own endpoint sources are
    /// in the conformance suite's endpoint cells.
    #[tokio::test]
    async fn open_refuses_a_malformed_endpoint_without_echoing_it() {
        for (endpoint, needle) in [
            ("", "is empty"),
            ("127.0.0.1:9200", "has no scheme"),
            ("ftp://127.0.0.1", "a scheme other than"),
            ("https://u:S3cretPW@h:1 ", "not a valid URI"),
            (
                "https://u:S3cretPW@127.0.0.1:19219/?a=b",
                "user information",
            ),
            (
                "http://u:S3cretPW@127.0.0.1:19219/?X-Amz-Signature=abc",
                "user information",
            ),
            ("https://u:S3cretPW@[::1%25lo]:19219", "user information"),
            ("https://S3cretPW@127.0.0.1:19219", "user information"),
            ("https://127.0.0.1:19219/?a=S3cretPW", "query component"),
            ("https://127.0.0.1:19219/#S3cretPW", "fragment"),
            ("https://[::1%25S3cretPW]:19219", "IPv6 zone id"),
            ("https://256.0.0.1:19219/S3cretPW", "not a valid URL"),
        ] {
            for allow_insecure in [false, true] {
                let mut config = s3_config("b", Some(endpoint));
                config.allow_insecure_endpoint = allow_insecure;
                let err = open_err(&s3_store_config(config)).await;
                let text = err.to_string();
                assert!(
                    text.contains(needle) && text.contains("(from [backup.store.s3] endpoint)"),
                    "{endpoint:?}: {text}"
                );
                assert!(
                    !text.contains("S3cretPW") && (endpoint.is_empty() || !text.contains(endpoint)),
                    "{endpoint:?}: the message repeats the endpoint: {text}"
                );
            }
        }
    }

    /// `Debug` of the configuration leaves out the endpoint's user
    /// information and keeps the rest of the endpoint.
    #[test]
    fn debug_redacts_the_endpoints_user_information() {
        for (endpoint, kept) in [
            (
                "https://u:S3cretPW@127.0.0.1:19219/p",
                "https://<redacted>@127.0.0.1:19219/p",
            ),
            ("http://S3cretPW@h:1", "http://<redacted>@h:1"),
            ("https://a@b:S3cretPW@h/x@y", "https://<redacted>@h/x@y"),
            (
                "u:S3cretPW@minio.example:9000",
                "<redacted>@minio.example:9000",
            ),
            ("https://h:1/?q=@", "https://h:1/?q=@"),
        ] {
            let text = format!("{:?}", s3_config("b", Some(endpoint)));
            assert!(!text.contains("S3cretPW"), "{endpoint}: {text}");
            assert!(text.contains(kept), "{endpoint}: {text}");
            let text = format!("{:?}", s3_store_config(s3_config("b", Some(endpoint))));
            assert!(!text.contains("S3cretPW"), "{endpoint}: {text}");
        }
        let text = format!("{:?}", s3_config("b", None));
        assert!(
            text.contains("endpoint: None") && text.contains("bucket: \"b\""),
            "{text}"
        );
    }

    #[test]
    fn allow_insecure_endpoint_parses() {
        let config: BackupStoreConfig = toml::from_str(
            "kind = \"s3\"\n[s3]\nbucket = \"b\"\nendpoint = \"http://10.0.0.5:9000\"\n\
             allow_insecure_endpoint = true\n",
        )
        .unwrap();
        assert!(config.s3.unwrap().allow_insecure_endpoint);
    }
}
