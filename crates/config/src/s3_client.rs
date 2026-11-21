// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The keys and startup checks every S3 client section shares, such as
//! `[bulk.s3]`: `region`, `endpoint_url`, `allow_insecure_endpoint` and
//! `force_path_style`, the credential keys the section refuses, and the
//! checks, with the section's name in every message. A section declares its
//! own keys, so its unknown-key check stays its own, and hands what it read
//! to [`S3ClientSection`].

/// The credential keys an S3 client section refuses, in the order the
/// check names them: the store reads its credentials from the standard
/// AWS credential chain, never from the configuration file.
pub const S3_CREDENTIAL_KEYS: [&str; 6] = [
    "access_key_id",
    "secret_access_key",
    "session_token",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
];

/// One S3 client section as it was read.
#[derive(Debug, Clone, Copy)]
pub struct S3ClientSection<'a> {
    /// The section as its messages name it, brackets included: `[bulk.s3]`.
    pub name: &'a str,
    /// The signing region and the first region a bucket is tried in.
    pub region: Option<&'a str>,
    /// An S3-compatible endpoint, an absolute `http` or `https` URL.
    pub endpoint_url: Option<&'a str>,
    /// Accept a plain `http` endpoint whose host is not a loopback form,
    /// with a notice, where the check otherwise refuses it.
    pub allow_insecure_endpoint: bool,
    /// Address buckets in the path rather than the host name.
    pub force_path_style: bool,
    /// The first of [`S3_CREDENTIAL_KEYS`] the section sets.
    pub credential_key: Option<&'a str>,
}

impl S3ClientSection<'_> {
    /// Check the section against the startup rules. Returns the notices
    /// the server logs at warn (a plain `http` endpoint on a host that is
    /// not loopback, accepted with `allow_insecure_endpoint`), or the
    /// startup refusal.
    ///
    /// # Errors
    ///
    /// A credential key, a region that is empty, holds whitespace or is not
    /// a host label of lowercase letters, digits and hyphens (the form the S3
    /// endpoint rules accept), or an `endpoint_url` that is not an absolute
    /// `http` or `https` URL with a host, that carries user information, a
    /// query or a fragment, or that is plain `http` to a host that is not a
    /// loopback form while `allow_insecure_endpoint` is false. The messages
    /// never repeat the value: the URL may hold a secret.
    pub fn validate(&self) -> Result<Vec<String>, String> {
        let name = self.name;
        if let Some(key) = self.credential_key {
            return Err(format!(
                "{name} {key} is not accepted: the S3 store reads its credentials from the \
                 standard AWS credential chain (environment variables, the shared config and \
                 credentials files, web identity, container and instance credentials), never \
                 from the configuration file"
            ));
        }
        if let Some(region) = self.region {
            if region.is_empty() {
                return Err(format!("{name} region must not be empty"));
            }
            if region.chars().any(char::is_whitespace) {
                return Err(format!("{name} region must not contain whitespace"));
            }
            if !is_region_label(region) {
                return Err(format!(
                    "{name} region must be lowercase letters, digits and hyphens, beginning \
                     and ending with a letter or a digit"
                ));
            }
        }
        let mut notices = Vec::new();
        if let Some(url) = self.endpoint_url {
            let endpoint = parse_endpoint(name, url)?;
            if !endpoint.tls && !endpoint.loopback {
                if !self.allow_insecure_endpoint {
                    return Err(format!(
                        "{name} endpoint_url uses http:// to a host that is not a recognised \
                         loopback form (use 127.x.y.z, ::1 or localhost); S3 requests and \
                         responses would cross the network unencrypted. Use https://, or set \
                         allow_insecure_endpoint = true to accept it"
                    ));
                }
                notices.push(format!(
                    "{name} endpoint_url uses plain http to a host that is not loopback; S3 \
                     requests and responses cross the network unencrypted"
                ));
            }
        }
        Ok(notices)
    }

    /// The store as a startup log line and the dev-mode banner name it, for
    /// example `s3 (region us-east-1, endpoint https://objects:9000,
    /// path-style)`. `default_region` is the region the store takes when
    /// the section names none. The endpoint is named by its scheme, host
    /// and port only, so a path, user information, a query or a fragment
    /// never reaches that log line or the banner, whether or not the
    /// section was checked first.
    #[must_use]
    pub fn describe(&self, default_region: &str) -> String {
        let region = self.region.unwrap_or(default_region);
        let endpoint = match self.endpoint_url {
            None => "AWS S3".to_owned(),
            Some(url) => parse_endpoint(self.name, url)
                .map_or_else(|_| "not a valid URL".to_owned(), |endpoint| endpoint.origin),
        };
        let style = if self.force_path_style {
            "path-style"
        } else {
            "virtual-hosted-style"
        };
        format!("s3 (region {region}, endpoint {endpoint}, {style})")
    }
}

/// Whether `region` is one host label: lowercase letters, digits and
/// hyphens, beginning and ending with a letter or a digit.
fn is_region_label(region: &str) -> bool {
    let allowed = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    region.chars().all(|c| allowed(c) || c == '-')
        && region.starts_with(allowed)
        && region.ends_with(allowed)
}

/// What the startup check reads from an `endpoint_url`.
struct Endpoint {
    tls: bool,
    /// The host is a loopback form ([`is_loopback_host`]).
    loopback: bool,
    /// The scheme, host and port, lowercased, as `describe` names them.
    origin: String,
}

/// Parse the `endpoint_url` of the section `name`: `http` or `https` (any
/// case), `://`, an authority with a host and an optional port, and an
/// optional path. User information, a query and a fragment are refused, as
/// is whitespace.
fn parse_endpoint(name: &str, url: &str) -> Result<Endpoint, String> {
    let shape = || format!("{name} endpoint_url must be an absolute http or https URL with a host");
    if url.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(shape());
    }
    let (scheme, rest) = url.split_once("://").ok_or_else(shape)?;
    let tls = if scheme.eq_ignore_ascii_case("https") {
        true
    } else if scheme.eq_ignore_ascii_case("http") {
        false
    } else {
        return Err(shape());
    };
    if rest.contains(['?', '#']) {
        return Err(format!(
            "{name} endpoint_url must not carry a query or a fragment"
        ));
    }
    let authority = rest.split('/').next().unwrap_or_default();
    if authority.contains('@') {
        return Err(format!(
            "{name} endpoint_url must not carry user information"
        ));
    }
    let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        let (literal, after) = bracketed.split_once(']').ok_or_else(shape)?;
        if literal.is_empty()
            || !literal
                .chars()
                .all(|c| c.is_ascii_hexdigit() || c == ':' || c == '.')
        {
            return Err(shape());
        }
        let port = match after {
            "" => None,
            other => Some(other.strip_prefix(':').ok_or_else(shape)?),
        };
        (format!("[{literal}]"), port)
    } else {
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        };
        if host.is_empty()
            || !host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
        {
            return Err(shape());
        }
        (host.to_ascii_lowercase(), port)
    };
    if let Some(port) = port
        && (port.is_empty()
            || !port.chars().all(|c| c.is_ascii_digit())
            || port.parse::<u16>().map_or(true, |p| p == 0))
    {
        return Err(shape());
    }
    let loopback = is_loopback_host(
        host.strip_prefix('[')
            .and_then(|literal| literal.strip_suffix(']'))
            .unwrap_or(&host),
    );
    let scheme = if tls { "https" } else { "http" };
    let origin = match port {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    };
    Ok(Endpoint {
        tls,
        loopback,
        origin,
    })
}

/// `127.0.0.0/8`, `::1` and `localhost`, in the forms the backup store's
/// endpoint check recognises (`crates/backup-store`, the `config` module
/// doc): `127.x.y.z` in dotted-quad form, an IPv6 address equal to `::1`
/// (`host` holds it without its brackets), and `localhost` in any case.
/// Other spellings of a loopback address (`127.1`, `127.0.0.01`,
/// `::ffff:127.0.0.1`, a trailing dot) are not recognised.
fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.is_loopback())
        || host
            .parse::<std::net::Ipv6Addr>()
            .is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A section of another name than `[bulk.s3]`.
    fn backup(endpoint_url: Option<&'static str>) -> S3ClientSection<'static> {
        S3ClientSection {
            name: "[backup.s3]",
            region: None,
            endpoint_url,
            allow_insecure_endpoint: false,
            force_path_style: false,
            credential_key: None,
        }
    }

    #[test]
    fn every_message_names_the_section_it_checks() {
        let refusal = S3ClientSection {
            credential_key: Some("aws_session_token"),
            ..backup(None)
        }
        .validate()
        .unwrap_err();
        assert!(
            refusal.starts_with("[backup.s3] aws_session_token is not accepted: the S3 store"),
            "{refusal}"
        );
        for (region, refusal) in [
            ("", "[backup.s3] region must not be empty"),
            ("us east", "[backup.s3] region must not contain whitespace"),
            (
                "US-EAST-1",
                "[backup.s3] region must be lowercase letters, digits and hyphens, beginning \
                 and ending with a letter or a digit",
            ),
        ] {
            let section = S3ClientSection {
                region: Some(region),
                ..backup(None)
            };
            assert_eq!(section.validate(), Err(refusal.to_owned()), "{region:?}");
        }
        for (url, refusal) in [
            (
                "ftp://objects",
                "[backup.s3] endpoint_url must be an absolute http or https URL with a host",
            ),
            (
                "https://user:secret@objects",
                "[backup.s3] endpoint_url must not carry user information",
            ),
            (
                "https://objects/?x=1",
                "[backup.s3] endpoint_url must not carry a query or a fragment",
            ),
        ] {
            assert_eq!(
                backup(Some(url)).validate(),
                Err(refusal.to_owned()),
                "{url}"
            );
        }
        assert_eq!(
            backup(Some("http://objects.example.internal")).validate(),
            Err(
                "[backup.s3] endpoint_url uses http:// to a host that is not a recognised \
                 loopback form (use 127.x.y.z, ::1 or localhost); S3 requests and responses \
                 would cross the network unencrypted. Use https://, or set \
                 allow_insecure_endpoint = true to accept it"
                    .to_owned()
            )
        );
        assert_eq!(
            S3ClientSection {
                allow_insecure_endpoint: true,
                ..backup(Some("http://objects.example.internal"))
            }
            .validate(),
            Ok(vec![
                "[backup.s3] endpoint_url uses plain http to a host that is not loopback; S3 \
                 requests and responses cross the network unencrypted"
                    .to_owned()
            ])
        );
        assert_eq!(
            backup(Some("HTTPS://Objects:9000/base")).describe("eu-west-1"),
            "s3 (region eu-west-1, endpoint https://objects:9000, virtual-hosted-style)"
        );
    }

    /// The backup store's loopback forms, and the other spellings of a
    /// loopback address it does not recognise.
    #[test]
    fn loopback_hosts() {
        for host in [
            "127.0.0.1",
            "127.1.2.3",
            "localhost",
            "LocalHost",
            "::1",
            "0:0:0:0:0:0:0:1",
        ] {
            assert!(is_loopback_host(host), "{host}");
        }
        for host in [
            "128.0.0.1",
            "10.0.0.1",
            "::2",
            "example.com",
            "localhost.example",
            "",
            "127.1",
            "127.0.0.01",
            "localhost.",
            "::ffff:127.0.0.1",
        ] {
            assert!(!is_loopback_host(host), "{host}");
        }
    }
}
