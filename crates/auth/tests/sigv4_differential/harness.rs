// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Signing with `aws-sigv4`, the header map and body `authenticate` receives,
//! and the authentication call the HTTP layer makes.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings, sign as sdk_sign,
};
use aws_sigv4::sign::v4;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use extenddb_auth::sigv4::{canonical, parse};
use extenddb_auth::{
    AuthIdentity, AuthProvider, BuiltinAuthProvider, CredentialStore, StoredCredential,
};
use extenddb_core::error::DynamoDbError;

use crate::capture;

/// The region the server `authenticate` runs is deployed as. The server
/// refuses a request whose credential scope names another region, as the
/// service refuses a scope for a region other than the endpoint's.
pub const SERVER_REGION: &str = "us-east-1";

/// The account both keys belong to.
const ACCOUNT_ID: &str = "123456789012";
const USER_NAME: &str = "differential-user";
const ROLE_NAME: &str = "differential-role";
const SESSION_NAME: &str = "differential-session";

/// The session key's token: base64 text with `/`, `+` and `=`, as STS issues.
pub const SESSION_TOKEN: &str = "IQoJb3JpZ2luX2VjEXAMPLE//////////wEaCXVzLWVhc3QtMSJHMEUCIQD\
    EXAMPLEtoken+AiB/EXAMPLEsession8Qkfq4hYtHFNfc0EXAMPLEnZsnS+OTdtI2tcNmJbGXgoQTBYWHMmHUvjHU\
    S2EXAMPLE0hn5WGuR83kN3EXAMPLEvL0zHKAI1aWIGGHMhCKEXAMPLE+tokenEXAMPLE/value==";
/// A token the store does not hold.
pub const OTHER_TOKEN: &str = "IQoJb3JpZ2luX2VjEXAMPLEotherTOKEN+value/EXAMPLE==";

/// The two keys the store holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Key {
    /// A long-term `AKIA` key of an IAM user.
    LongTerm,
    /// A temporary `ASIA` key of a role session, used with its token.
    Session,
}

impl Key {
    pub const ALL: [Self; 2] = [Self::LongTerm, Self::Session];

    pub fn id(self) -> &'static str {
        match self {
            Self::LongTerm => "AKIAIOSFODNN7EXAMPLE",
            Self::Session => "ASIAIOSFODNN7EXAMPLE",
        }
    }

    pub fn secret(self) -> &'static str {
        match self {
            Self::LongTerm => "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            Self::Session => "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY",
        }
    }

    pub fn token(self) -> Option<&'static str> {
        match self {
            Self::LongTerm => None,
            Self::Session => Some(SESSION_TOKEN),
        }
    }

    /// The other key's secret, which the store does not hold for this key.
    pub fn other_secret(self) -> &'static str {
        match self {
            Self::LongTerm => Self::Session.secret(),
            Self::Session => Self::LongTerm.secret(),
        }
    }
}

/// An in-memory credential store holding the two keys.
pub struct MemoryStore;

#[async_trait::async_trait]
impl CredentialStore for MemoryStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        let Some(key) = Key::ALL.into_iter().find(|key| key.id() == access_key_id) else {
            return Ok(None);
        };
        let session = key == Key::Session;
        Ok(Some(StoredCredential {
            secret_key: key.secret().to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            principal_name: if session { ROLE_NAME } else { USER_NAME }.to_owned(),
            session_name: session.then(|| SESSION_NAME.to_owned()),
            is_session: session,
            session_token: key.token().map(str::to_owned),
            is_active: true,
            expires_at: session.then(|| time::OffsetDateTime::now_utc() + time::Duration::hours(1)),
        }))
    }
}

/// Where a client sends: the URI it signs and the Host value its HTTP stack
/// sends for that URI.
#[derive(Clone, Copy, Debug)]
pub struct Endpoint {
    pub uri: &'static str,
    pub host: &'static str,
}

/// A request as a client builds it before signing.
#[derive(Clone, Debug)]
pub struct Shape {
    pub key: Key,
    pub endpoint: Endpoint,
    pub region: String,
    /// The service the client signs for.
    pub service: &'static str,
    /// Seconds from the real clock to the time the client signs with.
    pub clock_offset: i64,
    /// Headers the client passes to the signer, names as the client writes
    /// them. The signer adds `host` when the list has none, `x-amz-date`, and
    /// the session token for a session key.
    pub signed: Vec<(String, String)>,
    /// Headers the client adds after signing.
    pub unsigned: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Whether the signer adds a signed `x-amz-content-sha256` header.
    pub content_sha256: bool,
    /// Whether the signer leaves out the headers it skips by default
    /// (`user-agent` and `x-amzn-trace-id` among them). Otherwise it signs
    /// every header in `signed`.
    pub sdk_exclusions: bool,
    /// The secret the client signs with.
    pub signing_secret: String,
    /// The session token the client signs with and sends.
    pub signing_token: Option<String>,
}

impl Shape {
    /// A DynamoDB request: `x-amz-target`, the JSON 1.0 content type and the
    /// content length go to the signer.
    pub fn new(key: Key, endpoint: Endpoint, region: &str, target: &str, body: Vec<u8>) -> Self {
        let mut shape = Self {
            key,
            endpoint,
            region: region.to_owned(),
            service: "dynamodb",
            clock_offset: 0,
            signed: vec![
                ("x-amz-target".to_owned(), target.to_owned()),
                (
                    "content-type".to_owned(),
                    "application/x-amz-json-1.0".to_owned(),
                ),
            ],
            unsigned: Vec::new(),
            body: Vec::new(),
            content_sha256: false,
            sdk_exclusions: false,
            signing_secret: key.secret().to_owned(),
            signing_token: key.token().map(str::to_owned),
        };
        shape.set_body(body);
        shape
    }

    /// Replace the body and the `content-length` value that goes with it.
    pub fn set_body(&mut self, body: Vec<u8>) {
        let length = body.len().to_string();
        match self
            .signed
            .iter_mut()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        {
            Some((_, value)) => *value = length,
            None => self.signed.push(("content-length".to_owned(), length)),
        }
        self.body = body;
    }

    pub fn with_signed(mut self, name: &str, value: &str) -> Self {
        self.signed.push((name.to_owned(), value.to_owned()));
        self
    }

    pub fn with_unsigned(mut self, name: &str, value: &str) -> Self {
        self.unsigned.push((name.to_owned(), value.to_owned()));
        self
    }
}

/// A request `aws-sigv4` signed, as `authenticate` receives it.
#[derive(Clone, Debug)]
pub struct Signed {
    pub key: Key,
    pub wire: Wire,
    /// The time the client signed with.
    pub time: SystemTime,
    /// The canonical request `aws-sigv4` signed.
    pub canonical_request: String,
}

/// Sign `shape` with `aws-sigv4` and build the header lines and body
/// `authenticate` receives: the Host line the client's HTTP stack adds when
/// the client set none, the client's headers, the headers the signer
/// returned, then the headers added after signing.
pub fn sign(shape: &Shape) -> Signed {
    let time = shift(SystemTime::now(), shape.clock_offset);
    let identity = Credentials::new(
        shape.key.id(),
        shape.signing_secret.clone(),
        shape.signing_token.clone(),
        None,
        "sigv4-differential",
    )
    .into();
    let mut settings = SigningSettings::default();
    if shape.content_sha256 {
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    }
    if !shape.sdk_exclusions {
        settings.excluded_headers = None;
    }
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(&shape.region)
        .name(shape.service)
        .time(time)
        .settings(settings)
        .build()
        .expect("the signing parameters are complete")
        .into();
    let request = SignableRequest::new(
        "POST",
        shape.endpoint.uri,
        shape
            .signed
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
        SignableBody::Bytes(&shape.body),
    )
    .expect("aws-sigv4 takes the request");
    let (output, canonical_request) = capture::canonical_request(|| sdk_sign(request, &params));
    let output = output.expect("aws-sigv4 signs the request");

    let mut headers = Vec::new();
    if !shape
        .signed
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("host"))
    {
        headers.push(("host".to_owned(), shape.endpoint.host.to_owned()));
    }
    headers.extend(shape.signed.iter().cloned());
    headers.extend(
        output
            .output()
            .headers()
            .map(|(name, value)| (name.to_owned(), value.to_owned())),
    );
    headers.extend(shape.unsigned.iter().cloned());
    Signed {
        key: shape.key,
        wire: Wire {
            headers,
            body: shape.body.clone(),
        },
        time,
        canonical_request: canonical_request.expect("aws-sigv4 records its canonical request"),
    }
}

/// `time` moved by `seconds`, either way.
pub fn shift(time: SystemTime, seconds: i64) -> SystemTime {
    let delta = Duration::from_secs(seconds.unsigned_abs());
    if seconds < 0 {
        time - delta
    } else {
        time + delta
    }
}

/// `time` as `X-Amz-Date` carries it: `YYYYMMDDTHHMMSSZ` in UTC.
pub fn amz_date(time: SystemTime) -> String {
    let time = time::OffsetDateTime::from(time);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second(),
    )
}

/// The header map and body `authenticate` receives. The headers are kept as
/// lines, and `header_map` builds the map from them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Wire {
    /// Header lines in the order sent, names as the client wrote them.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Wire {
    /// The header map `authenticate` receives: names lowercased, spaces and
    /// tabs trimmed from both ends of each value as httparse trims them, and
    /// a repeated name kept as several values in the order sent.
    pub fn header_map(&self) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in &self.headers {
            map.append(
                HeaderName::from_bytes(name.as_bytes()).expect("a valid header name"),
                HeaderValue::from_str(value.trim_matches([' ', '\t']))
                    .expect("a valid header value"),
            );
        }
        map
    }

    /// The first value of `name`.
    pub fn value(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(line, _)| line.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Replace the first value of `name`.
    pub fn replace(&mut self, name: &str, value: String) {
        let line = self
            .headers
            .iter_mut()
            .find(|(line, _)| line.eq_ignore_ascii_case(name))
            .unwrap_or_else(|| panic!("{name} is sent"));
        line.1 = value;
    }

    /// Remove every line of `name`.
    pub fn remove(&mut self, name: &str) {
        self.headers
            .retain(|(line, _)| !line.eq_ignore_ascii_case(name));
    }

    pub fn authorization(&self) -> Authorization {
        Authorization::parse(
            self.value("authorization")
                .expect("an authorization header"),
        )
    }

    pub fn set_authorization(&mut self, authorization: &Authorization) {
        self.replace("authorization", authorization.render());
    }

    /// Lowercased names of the header lines the signature does not cover,
    /// `authorization` aside.
    pub fn unsigned_names(&self) -> Vec<String> {
        let signed = self.authorization().signed_headers;
        let mut names: Vec<String> = self
            .headers
            .iter()
            .map(|(name, _)| name.to_ascii_lowercase())
            .filter(|name| name != "authorization" && !signed.contains(name))
            .collect();
        names.sort();
        names.dedup();
        names
    }
}

/// The parts of an `Authorization` value as `aws-sigv4` writes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Authorization {
    pub access_key_id: String,
    pub date: String,
    pub region: String,
    pub service: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

impl Authorization {
    pub fn parse(value: &str) -> Self {
        let rest = value
            .strip_prefix("AWS4-HMAC-SHA256 Credential=")
            .unwrap_or_else(|| panic!("a SigV4 authorization value: {value}"));
        let (credential, rest) = rest
            .split_once(", SignedHeaders=")
            .unwrap_or_else(|| panic!("SignedHeaders in {value}"));
        let (signed_headers, signature) = rest
            .split_once(", Signature=")
            .unwrap_or_else(|| panic!("Signature in {value}"));
        let scope: Vec<&str> = credential.split('/').collect();
        assert!(
            scope.len() == 5 && scope[4] == "aws4_request",
            "a credential scope: {credential}"
        );
        let parts = Self {
            access_key_id: scope[0].to_owned(),
            date: scope[1].to_owned(),
            region: scope[2].to_owned(),
            service: scope[3].to_owned(),
            signed_headers: signed_headers.split(';').map(str::to_owned).collect(),
            signature: signature.to_owned(),
        };
        assert_eq!(parts.render(), value, "the parts render the value back");
        parts
    }

    pub fn render(&self) -> String {
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/{}/{}/aws4_request, SignedHeaders={}, Signature={}",
            self.access_key_id,
            self.date,
            self.region,
            self.service,
            self.signed_headers.join(";"),
            self.signature,
        )
    }
}

/// The authentication call the HTTP layer makes for a DynamoDB request, on a
/// server deployed as `SERVER_REGION`.
pub fn authenticate(wire: &Wire) -> Result<AuthIdentity, DynamoDbError> {
    let provider = BuiltinAuthProvider::new(MemoryStore, SERVER_REGION);
    futures::executor::block_on(provider.authenticate(&wire.header_map(), &wire.body))
}

/// The canonical request the server builds for `wire`.
pub fn server_canonical_request(wire: &Wire) -> String {
    let value = wire
        .value("authorization")
        .expect("an authorization header");
    let parsed = parse::parse_authorization(value).expect("the server parses the header");
    canonical::canonical_request(
        "POST",
        "/",
        "",
        &wire.header_map(),
        &parsed.signed_headers,
        &wire.body,
    )
}

/// How the server refuses a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// `InvalidSignatureException`: the signature, the credential scope or a
    /// signed part does not match.
    Signature,
    /// `UnrecognizedClientException`: the session token is missing, is not
    /// the key's or is sent with a long-term key, or `X-Amz-Date` is outside
    /// the skew window (#805).
    Client,
    /// `IncompleteSignature`: a part the check needs is missing.
    Incomplete,
}

impl Refusal {
    fn of(err: &DynamoDbError) -> Option<Self> {
        match err {
            DynamoDbError::InvalidSignatureException(_) => Some(Self::Signature),
            DynamoDbError::UnrecognizedClientException(_) => Some(Self::Client),
            DynamoDbError::IncompleteSignature(_) => Some(Self::Incomplete),
            _ => None,
        }
    }
}

// The message each check refuses with, or its start. Checks that return the
// same error are told apart by their messages; `Message` says how a test
// compares each one.

/// The signature differs from the one the server computes.
pub const SIGNATURE_MISMATCH: &str =
    "The request signature we calculated does not match the signature you provided.";
/// The credential scope names a region other than `SERVER_REGION`. The
/// server checks the scope's region and service together, before it looks
/// the key up or compares the signature, and reports each complaint as a
/// sentence ending with a space, region first; the message does not name
/// the region.
pub const WRONG_REGION: &str = "Credential should be scoped to a valid region. ";
/// The credential scope names a service other than `dynamodb`.
pub const WRONG_SERVICE: &str = "Credential should be scoped to correct service: 'dynamodb'.";
/// The credential scope's date differs from the date in `X-Amz-Date`.
pub const SCOPE_DATE_MISMATCH: &str =
    "Date in Credential scope does not match YYYYMMDD from ISO-8601 version of date from HTTP";
/// The session key's token is missing. The server answers the same message
/// for a key the store does not hold or holds inactive.
pub const INVALID_TOKEN: &str = "The security token included in the request is invalid.";
/// A token is sent that is not the key's: another token with the session
/// key, or any token with the long-term key. The server reports it after the
/// signature is compared, with the same words as `INVALID_TOKEN` and no
/// trailing period, as the service does. This text is the start of
/// `INVALID_TOKEN`, so a test that expects either message compares the whole
/// message, with `Message::Exact`.
pub const WRONG_TOKEN: &str = "The security token included in the request is invalid";
/// `X-Amz-Date` is missing.
pub const MISSING_DATE: &str = "Authorization header requires existence of 'X-Amz-Date' header";

/// How a test compares the message the server refused with against the one
/// it expects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message {
    /// The whole message. Used where the expected text is also the start of
    /// another answer, so a prefix comparison would accept both: `WRONG_TOKEN`
    /// starts `INVALID_TOKEN`, and `WRONG_REGION` starts the answer for a
    /// scope wrong in both its region and its service.
    Exact(&'static str),
    /// The start of the message, which names the check that refused the
    /// request. The rest of the message carries details the test does not
    /// pin.
    Prefix(&'static str),
}

impl Message {
    pub fn matches(self, message: &str) -> bool {
        match self {
            Self::Exact(expected) => message == expected,
            Self::Prefix(expected) => message.starts_with(expected),
        }
    }
}

fn is_principal_of(identity: &AuthIdentity, key: Key) -> bool {
    match (identity, key) {
        (
            AuthIdentity::User {
                account_id,
                user_name,
            },
            Key::LongTerm,
        ) => account_id == ACCOUNT_ID && user_name == USER_NAME,
        (
            AuthIdentity::RoleSession {
                account_id,
                role_name,
                session_name,
            },
            Key::Session,
        ) => account_id == ACCOUNT_ID && role_name == ROLE_NAME && session_name == SESSION_NAME,
        _ => false,
    }
}

/// Check that `signed` verifies as its key's principal and that the server's
/// canonical request for it is the one `aws-sigv4` signed.
pub fn check_accepted(signed: &Signed) -> Result<(), String> {
    check_accepted_as(signed, &signed.wire)
}

/// Check that `wire`, sent in place of `signed`'s request, verifies as its
/// key's principal and canonicalizes to the request `aws-sigv4` signed.
pub fn check_accepted_as(signed: &Signed, wire: &Wire) -> Result<(), String> {
    match authenticate(wire) {
        Ok(identity) if is_principal_of(&identity, signed.key) => {}
        Ok(identity) => {
            return Err(format!(
                "verified as {identity:?}, not as the {:?} key",
                signed.key
            ));
        }
        Err(err) => {
            return Err(format!(
                "refused with {err:?}\n{}",
                differences(&signed.canonical_request, wire)
            ));
        }
    }
    let server = server_canonical_request(wire);
    if server == signed.canonical_request {
        Ok(())
    } else {
        Err(format!(
            "canonical requests differ\n{}",
            differences(&signed.canonical_request, wire)
        ))
    }
}

/// Check that the server refuses `wire` with the error `expected` names and a
/// message `message` matches, which names the check that refused it.
pub fn check_refused(wire: &Wire, expected: Refusal, message: Message) -> Result<(), String> {
    match authenticate(wire) {
        Ok(identity) => Err(format!(
            "verified as {identity:?}, expected {expected:?} with {message:?}\nheaders: {:?}",
            wire.headers
        )),
        Err(err) if Refusal::of(&err) == Some(expected) && message.matches(err.message()) => Ok(()),
        Err(err) => Err(format!(
            "refused with {err:?}, expected {expected:?} with {message:?}"
        )),
    }
}

/// Check that the server refuses `wire` with `InvalidSignatureException` or
/// `UnrecognizedClientException`, with any message but those of the scope
/// date check and the signature comparison, so neither of those checks
/// refused it.
pub fn check_refused_by_the_time_check(wire: &Wire) -> Result<(), String> {
    let err = match authenticate(wire) {
        Ok(identity) => {
            return Err(format!(
                "verified as {identity:?}, expected a refusal\nheaders: {:?}",
                wire.headers
            ));
        }
        Err(err) => err,
    };
    let message = err.message();
    let later_check =
        message.starts_with(SIGNATURE_MISMATCH) || message.starts_with(SCOPE_DATE_MISMATCH);
    match Refusal::of(&err) {
        Some(Refusal::Signature | Refusal::Client) if !later_check => Ok(()),
        _ => Err(format!(
            "refused with {err:?}, expected InvalidSignatureException or \
             UnrecognizedClientException from the time check, before the \
             scope date check and the signature comparison run"
        )),
    }
}

/// The lines where the canonical request `aws-sigv4` signed and the one the
/// server builds for `wire` differ.
pub fn differences(sdk: &str, wire: &Wire) -> String {
    let server = server_canonical_request(wire);
    let sdk_lines: Vec<&str> = sdk.split('\n').collect();
    let server_lines: Vec<&str> = server.split('\n').collect();
    let mut out = String::new();
    for index in 0..sdk_lines.len().max(server_lines.len()) {
        let left = sdk_lines.get(index).copied();
        let right = server_lines.get(index).copied();
        if left != right {
            out.push_str(&format!(
                "line {index}: aws-sigv4 {left:?}, server {right:?}\n"
            ));
        }
    }
    out
}

/// Requests checked per category. Each test prints its counts when it ends;
/// `--nocapture` shows them.
#[derive(Debug, Default)]
pub struct Tally(BTreeMap<&'static str, usize>);

impl Tally {
    pub fn add(&mut self, category: &'static str) {
        *self.0.entry(category).or_default() += 1;
    }

    pub fn report(&self, test: &str) {
        for (category, count) in &self.0 {
            eprintln!("{test}: {category}: {count}");
        }
    }
}
