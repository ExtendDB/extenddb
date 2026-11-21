// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! S3 bulk store configuration (`[bulk.s3]` section) and its startup
//! checks, which are the shared S3 client checks (`s3_client`) under the
//! section's name.

use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::s3_client::{S3_CREDENTIAL_KEYS, S3ClientSection};

/// Settings for the S3 bulk store (`[bulk.s3]` section). Every key is
/// optional. Credentials never come from this section: the store reads the
/// standard AWS credential chain, and the credential keys below are
/// accepted by the parser only so [`BulkS3Config::validate`] can refuse
/// them with a message that names the chain instead of the generic
/// unknown-key error.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BulkS3Config {
    /// The signing region and the first region a bucket is tried in.
    /// Default: `[server] region`.
    pub region: Option<String>,
    /// An S3-compatible endpoint, an absolute `http` or `https` URL.
    /// Default: AWS S3 for the region.
    pub endpoint_url: Option<String>,
    /// Accept a plain `http` `endpoint_url` whose host is not a loopback
    /// form (`127.x.y.z`, `::1` or `localhost`), which otherwise refuses
    /// startup; the server then logs a warning. Such an endpoint carries the
    /// S3 requests and responses, input objects and reports included, across
    /// the network unencrypted. Default: false.
    #[serde(default)]
    pub allow_insecure_endpoint: bool,
    /// Address buckets in the path rather than the host name. Most
    /// S3-compatible services need it. Default: false.
    #[serde(default)]
    pub force_path_style: bool,
    #[serde(default)]
    access_key_id: Option<IgnoredAny>,
    #[serde(default)]
    secret_access_key: Option<IgnoredAny>,
    #[serde(default)]
    session_token: Option<IgnoredAny>,
    #[serde(default)]
    aws_access_key_id: Option<IgnoredAny>,
    #[serde(default)]
    aws_secret_access_key: Option<IgnoredAny>,
    #[serde(default)]
    aws_session_token: Option<IgnoredAny>,
}

impl BulkS3Config {
    /// Check the section against the startup rules
    /// ([`S3ClientSection::validate`]). Returns the notices the server logs
    /// at warn, or the startup refusal.
    ///
    /// # Errors
    ///
    /// A credential key, a region that is not one host label, or an
    /// `endpoint_url` that is not an absolute `http` or `https` URL with a
    /// host, that carries user information, a query or a fragment, or that
    /// is plain `http` to a host that is not loopback while
    /// `allow_insecure_endpoint` is false.
    pub fn validate(&self) -> Result<Vec<String>, String> {
        self.section().validate()
    }

    /// The store as the startup log line and the dev-mode banner name it
    /// ([`S3ClientSection::describe`]). `default_region` is `[server]
    /// region`. The console settings page shows `endpoint_url` as
    /// configured, after `validate` has refused user information, a query
    /// and a fragment.
    #[must_use]
    pub fn describe(&self, default_region: &str) -> String {
        self.section().describe(default_region)
    }

    /// The section as the shared S3 client checks read it.
    fn section(&self) -> S3ClientSection<'_> {
        let set = [
            self.access_key_id.is_some(),
            self.secret_access_key.is_some(),
            self.session_token.is_some(),
            self.aws_access_key_id.is_some(),
            self.aws_secret_access_key.is_some(),
            self.aws_session_token.is_some(),
        ];
        S3ClientSection {
            name: "[bulk.s3]",
            region: self.region.as_deref(),
            endpoint_url: self.endpoint_url.as_deref(),
            allow_insecure_endpoint: self.allow_insecure_endpoint,
            force_path_style: self.force_path_style,
            credential_key: S3_CREDENTIAL_KEYS
                .into_iter()
                .zip(set)
                .find_map(|(key, set)| set.then_some(key)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Narrow view for the `[bulk.s3]` tests, as in the `[bulk]` tests:
    /// the section deserializes independently of storage.
    #[derive(Debug, serde::Deserialize)]
    struct BulkSection {
        bulk: Option<crate::BulkConfig>,
    }

    fn parse_bulk(toml_text: &str) -> Result<BulkSection, config::ConfigError> {
        config::Config::builder()
            .add_source(config::File::from_str(toml_text, config::FileFormat::Toml))
            .build()?
            .try_deserialize()
    }

    fn s3_section(toml_text: &str) -> BulkS3Config {
        parse_bulk(toml_text)
            .unwrap()
            .bulk
            .unwrap()
            .s3
            .expect("[bulk.s3] present")
    }

    #[test]
    fn bulk_s3_section_parses_every_key() {
        let s3 = s3_section(
            "[bulk]\nstore = \"s3\"\n[bulk.s3]\nregion = \"us-west-2\"\n\
             endpoint_url = \"https://objects.example.internal:9000\"\n\
             allow_insecure_endpoint = true\nforce_path_style = true\n",
        );
        assert_eq!(s3.region.as_deref(), Some("us-west-2"));
        assert_eq!(
            s3.endpoint_url.as_deref(),
            Some("https://objects.example.internal:9000")
        );
        assert!(s3.allow_insecure_endpoint);
        assert!(s3.force_path_style);
        assert_eq!(s3.validate(), Ok(Vec::new()));
        assert_eq!(
            s3.describe("us-east-1"),
            "s3 (region us-west-2, endpoint https://objects.example.internal:9000, path-style)"
        );
    }

    #[test]
    fn bulk_s3_without_its_section_takes_every_default() {
        let bulk = parse_bulk("[bulk]\nstore = \"s3\"\n")
            .unwrap()
            .bulk
            .unwrap();
        assert!(bulk.s3.is_none());
        let defaults = BulkS3Config::default();
        assert_eq!(defaults.validate(), Ok(Vec::new()));
        assert!(!defaults.allow_insecure_endpoint);
        assert!(!defaults.force_path_style);
        assert_eq!(
            defaults.describe("eu-west-1"),
            "s3 (region eu-west-1, endpoint AWS S3, virtual-hosted-style)"
        );
        let empty = s3_section("[bulk]\nstore = \"s3\"\n[bulk.s3]\n");
        assert_eq!(empty.validate(), Ok(Vec::new()));
        assert!(!empty.allow_insecure_endpoint);
    }

    #[test]
    fn bulk_s3_credential_keys_are_refused_naming_the_credential_chain() {
        for key in [
            "access_key_id",
            "secret_access_key",
            "session_token",
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
        ] {
            let s3 = s3_section(&format!(
                "[bulk]\nstore = \"s3\"\n[bulk.s3]\n{key} = \"value-not-echoed\"\n"
            ));
            let refusal = s3.validate().expect_err(key);
            assert!(
                refusal.starts_with(&format!("[bulk.s3] {key} is not accepted")),
                "{refusal}"
            );
            assert!(
                refusal.contains("standard AWS credential chain"),
                "{refusal}"
            );
            assert!(!refusal.contains("value-not-echoed"), "{refusal}");
        }
    }

    #[test]
    fn bulk_s3_region_must_be_non_empty_without_whitespace() {
        let empty = s3_section("[bulk]\n[bulk.s3]\nregion = \"\"\n");
        assert_eq!(
            empty.validate(),
            Err("[bulk.s3] region must not be empty".to_owned())
        );
        for region in [" us-east-1", "us-east-1 ", "us east 1", "us-east-1\t"] {
            let s3 = BulkS3Config {
                region: Some(region.to_owned()),
                ..BulkS3Config::default()
            };
            assert_eq!(
                s3.validate(),
                Err("[bulk.s3] region must not contain whitespace".to_owned()),
                "{region:?}"
            );
        }
    }

    #[test]
    fn bulk_s3_region_must_be_one_host_label() {
        for region in ["us-east-1", "eu-west-3", "local1", "a"] {
            let s3 = BulkS3Config {
                region: Some(region.to_owned()),
                ..BulkS3Config::default()
            };
            assert_eq!(s3.validate(), Ok(Vec::new()), "{region}");
        }
        for region in [
            "us-east-1/../x",
            "US-EAST-1",
            "-us",
            "us-",
            "us_east_1",
            "us.east",
        ] {
            let s3 = BulkS3Config {
                region: Some(region.to_owned()),
                ..BulkS3Config::default()
            };
            let refusal = s3.validate().expect_err(region);
            assert_eq!(
                refusal,
                "[bulk.s3] region must be lowercase letters, digits and hyphens, beginning and \
                 ending with a letter or a digit",
                "{region}"
            );
            assert!(!refusal.contains(region));
        }
    }

    #[test]
    fn describe_names_the_endpoint_by_scheme_host_and_port() {
        for (url, named) in [
            (
                "https://objects.example:9000/base",
                "https://objects.example:9000",
            ),
            ("HTTP://Objects.Example/x/y", "http://objects.example"),
            ("http://[::1]:7070", "http://[::1]:7070"),
            ("http://AKID:SECRET@127.0.0.1:17070", "not a valid URL"),
            ("http://127.0.0.1:17070/?token=SECRET", "not a valid URL"),
        ] {
            let s3 = BulkS3Config {
                endpoint_url: Some(url.to_owned()),
                ..BulkS3Config::default()
            };
            let described = s3.describe("us-east-1");
            assert_eq!(
                described,
                format!("s3 (region us-east-1, endpoint {named}, virtual-hosted-style)")
            );
            assert!(!described.contains("SECRET") && !described.contains("AKID"));
        }
    }

    fn endpoint(url: &str) -> Result<Vec<String>, String> {
        BulkS3Config {
            endpoint_url: Some(url.to_owned()),
            ..BulkS3Config::default()
        }
        .validate()
    }

    #[test]
    fn bulk_s3_endpoint_url_accepts_absolute_http_and_https_urls() {
        for url in [
            "https://objects.example.internal:9000",
            "https://objects.example.internal",
            "HTTPS://Objects.Example.Internal/base/path",
            "http://127.0.0.1:7070",
            "http://localhost:9000/",
            "http://[::1]:7070",
            "https://[2001:db8::1]:443",
            "https://s3_compat.internal:65535",
        ] {
            assert_eq!(endpoint(url), Ok(Vec::new()), "{url}");
        }
    }

    #[test]
    fn bulk_s3_endpoint_url_refusals() {
        let shape = "[bulk.s3] endpoint_url must be an absolute http or https URL with a host";
        for url in [
            "objects.example.internal:9000",
            "/relative/path",
            "ftp://objects.example.internal",
            "s3://bucket",
            "https://",
            "https:///path",
            "https://:9000",
            "https://objects:0",
            "https://objects:65536",
            "https://objects:port",
            "https://objects:",
            "https://obj ects",
            "https://objects/\npath",
            "https://[::1",
            "https://[]:9000",
            "https://ob!ects",
        ] {
            assert_eq!(endpoint(url), Err(shape.to_owned()), "{url:?}");
        }
        let userinfo = endpoint("https://AKIDEXAMPLE:secretvalue@objects:9000").unwrap_err();
        assert_eq!(
            userinfo,
            "[bulk.s3] endpoint_url must not carry user information"
        );
        assert!(!userinfo.contains("secretvalue"));
        assert_eq!(
            endpoint("https://user@objects"),
            Err("[bulk.s3] endpoint_url must not carry user information".to_owned())
        );
        for url in [
            "https://objects:9000/?x-id=1",
            "https://objects:9000#part",
            "https://objects?",
        ] {
            assert_eq!(
                endpoint(url),
                Err("[bulk.s3] endpoint_url must not carry a query or a fragment".to_owned()),
                "{url}"
            );
        }
    }

    fn insecure_endpoint(url: &str) -> Result<Vec<String>, String> {
        BulkS3Config {
            endpoint_url: Some(url.to_owned()),
            allow_insecure_endpoint: true,
            ..BulkS3Config::default()
        }
        .validate()
    }

    /// The backup store's rule: a plain `http` endpoint must name a loopback
    /// form unless `allow_insecure_endpoint = true`, which accepts it with
    /// the notice. The refusal does not repeat the URL.
    #[test]
    fn bulk_s3_plain_http_to_a_host_that_is_not_loopback_is_refused() {
        let refusal = "[bulk.s3] endpoint_url uses http:// to a host that is not a recognised \
                       loopback form (use 127.x.y.z, ::1 or localhost); S3 requests and \
                       responses would cross the network unencrypted. Use https://, or set \
                       allow_insecure_endpoint = true to accept it";
        let notice = "[bulk.s3] endpoint_url uses plain http to a host that is not loopback; S3 \
                      requests and responses cross the network unencrypted";
        for url in [
            "http://objects.example.internal:9000",
            "http://s3.example.com",
            "http://s3.example.com:9000/",
            "HTTP://10.0.0.5:9000",
            "http://128.0.0.1:9000",
            "http://[2001:db8::1]:9000",
            "http://localhost.example.com:9000",
            // Only a host that parses as an address is an address.
            "http://127.0.0.1.evil.example",
            "http://127.example.internal",
            "http://127.0.0.256",
            // Other spellings of a loopback address.
            "http://127.1",
            "http://127.0.0.01:9000",
            "http://localhost.:9000",
            "http://[::ffff:127.0.0.1]:9000",
        ] {
            assert_eq!(endpoint(url), Err(refusal.to_owned()), "{url}");
            assert_eq!(insecure_endpoint(url), Ok(vec![notice.to_owned()]), "{url}");
        }
        for url in [
            "http://127.0.0.1:9000",
            "http://127.255.0.9",
            "http://127.10.20.30",
            "http://localhost:9000/",
            "http://LOCALHOST",
            "http://[::1]:9000",
            "http://[0:0:0:0:0:0:0:1]:9000",
            "https://s3.example.com",
            "https://10.0.0.5:9000",
        ] {
            assert_eq!(endpoint(url), Ok(Vec::new()), "{url}");
            assert_eq!(insecure_endpoint(url), Ok(Vec::new()), "{url}");
        }
    }
}
