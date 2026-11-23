// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Fixed request parts: endpoints, regions, operations, the headers DynamoDB
//! clients send, and bodies.

use crate::harness::{Endpoint, Key, SERVER_REGION, Shape};

/// Endpoints a client is configured with. None names the scheme's default
/// port, so the Host value the client's HTTP stack sends is the authority,
/// which is also what the signer signs.
pub const ENDPOINTS: &[Endpoint] = &[
    Endpoint {
        uri: "https://localhost:18443/",
        host: "localhost:18443",
    },
    Endpoint {
        uri: "https://127.0.0.1:18443/",
        host: "127.0.0.1:18443",
    },
    Endpoint {
        uri: "https://[::1]:18443/",
        host: "[::1]:18443",
    },
    Endpoint {
        uri: "http://extenddb.internal:8000/",
        host: "extenddb.internal:8000",
    },
    Endpoint {
        uri: "https://extenddb.example.com/",
        host: "extenddb.example.com",
    },
];

/// Regions a client signs for. The server checks the credential scope's
/// region against the region it is deployed as, `SERVER_REGION`, before it
/// looks the key up or compares the signature: a request signed for that
/// region verifies, and a request signed for any other region is refused
/// with `WRONG_REGION`. The list holds the AWS commercial, GovCloud, China
/// and European Sovereign Cloud region codes, and names a local deployment
/// uses.
pub const REGIONS: &[&str] = &[
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "af-south-1",
    "ap-east-1",
    "ap-east-2",
    "ap-south-1",
    "ap-south-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-southeast-5",
    "ap-southeast-6",
    "ap-southeast-7",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ca-central-1",
    "ca-west-1",
    "eu-central-1",
    "eu-central-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-south-1",
    "eu-south-2",
    "eu-north-1",
    "il-central-1",
    "me-south-1",
    "me-central-1",
    "mx-central-1",
    "sa-east-1",
    "us-gov-east-1",
    "us-gov-west-1",
    "cn-north-1",
    "cn-northwest-1",
    "eusc-de-east-1",
    "local",
    "localhost",
    "LOCAL",
];

/// `X-Amz-Target` values of operations the server serves.
pub const TARGETS: &[&str] = &[
    "DynamoDB_20120810.ListTables",
    "DynamoDB_20120810.PutItem",
    "DynamoDB_20120810.GetItem",
    "DynamoDB_20120810.Query",
    "DynamoDB_20120810.BatchWriteItem",
    "DynamoDB_20120810.TransactWriteItems",
    "DynamoDBStreams_20120810.GetRecords",
];

/// Headers SDKs send besides the ones DynamoDB needs, with values of the
/// form the AWS SDK for Rust sends.
pub const SDK_HEADERS: &[(&str, &str)] = &[
    (
        "user-agent",
        "aws-sdk-rust/1.3.8 ua/2.1 api/dynamodb/1.95.0 os/linux lang/rust/1.88.0",
    ),
    (
        "x-amz-user-agent",
        "aws-sdk-rust/1.3.8 ua/2.1 api/dynamodb/1.95.0 os/linux lang/rust/1.88.0 m/E,b,D",
    ),
    (
        "amz-sdk-invocation-id",
        "3f9d1c7a-58e2-4b7e-9c61-0a2b4d6e8f10",
    ),
    ("amz-sdk-request", "attempt=1; max=3"),
];

/// Headers clients and proxies send without signing them.
pub const UNSIGNED_HEADERS: &[(&str, &str)] = &[
    ("accept-encoding", "gzip, deflate"),
    (
        "x-amzn-trace-id",
        "Root=1-6727a6f0-1a2b3c4d5e6f708192a3b4c5;Sampled=1",
    ),
    ("accept", "*/*"),
    ("x-forwarded-for", "203.0.113.7"),
];

/// A PutItem body as SDKs write it.
pub const PUT_ITEM: &[u8] = br#"{"TableName":"Music","Item":{"Artist":{"S":"No One You Know"},"SongTitle":{"S":"Call Me Today"}}}"#;

/// Text with characters of two, three and four bytes in UTF-8.
pub const MULTIBYTE: &[&str] = &[
    "Bjørk",
    "日本語のテキスト",
    "Ünïcödé ✓",
    "🦀🦀🦀",
    "\u{10FFFF}",
    "€ £ ¥",
    "混合 mixed テキスト",
];

/// The number of JSON tokens `spaced_item` writes.
pub const ITEM_TOKENS: usize = 17;

/// A PutItem object with one string attribute holding `value`, with
/// `gaps[i]` written before token `i` and `gaps[ITEM_TOKENS]` after the last.
pub fn spaced_item(gaps: &[&str], table: &str, value: &str) -> Vec<u8> {
    let table = serde_json::to_string(table).expect("a string encodes");
    let value = serde_json::to_string(value).expect("a string encodes");
    let tokens: [&str; ITEM_TOKENS] = [
        "{",
        "\"TableName\"",
        ":",
        &table,
        ",",
        "\"Item\"",
        ":",
        "{",
        "\"pk\"",
        ":",
        "{",
        "\"S\"",
        ":",
        &value,
        "}",
        "}",
        "}",
    ];
    let mut body = String::new();
    for (gap, token) in gaps.iter().zip(tokens) {
        body.push_str(gap);
        body.push_str(token);
    }
    body.push_str(gaps.get(ITEM_TOKENS).copied().unwrap_or(""));
    body.into_bytes()
}

/// A PutItem object of exactly `size` bytes whose string value repeats
/// `fill`, topped up with ASCII.
pub fn sized_item(size: usize, fill: &str) -> Vec<u8> {
    let head = br#"{"TableName":"Music","Item":{"pk":{"S":""#;
    let tail = br#""}}}"#;
    let room = size - head.len() - tail.len();
    let mut value = fill.repeat(room / fill.len());
    while value.len() < room {
        value.push('a');
    }
    let mut body = head.to_vec();
    body.extend_from_slice(value.as_bytes());
    body.extend_from_slice(tail);
    assert_eq!(body.len(), size);
    body
}

/// The bodies every client shape is checked with: empty, a PutItem as SDKs
/// write it, JSON with odd spacing, and multi-byte UTF-8.
pub fn small_bodies() -> Vec<(&'static str, Vec<u8>)> {
    let gaps = ["\n", " ", "\t", "  ", "\r\n", " ", "\t\t", "", " \n "];
    let odd: Vec<&str> = gaps.iter().copied().cycle().take(ITEM_TOKENS + 1).collect();
    vec![
        ("empty body", Vec::new()),
        ("PutItem body", PUT_ITEM.to_vec()),
        ("JSON with odd spacing", spaced_item(&odd, "Music", "x")),
        (
            "multi-byte UTF-8",
            spaced_item(&[""; ITEM_TOKENS + 1], "Music", &MULTIBYTE.join(" ")),
        ),
    ]
}

/// Large bodies: 64 KiB, the 400 KiB item limit, and 1 MiB of multi-byte
/// text.
pub fn large_bodies() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("64 KiB body", sized_item(64 * 1024, "a")),
        ("400 KiB body", sized_item(400 * 1024, "item ")),
        ("1 MiB multi-byte body", sized_item(1024 * 1024, "日本語🦀")),
    ]
}

/// The server's request body limit, 16 MiB.
pub const BODY_LIMIT: usize = 16 * 1024 * 1024;

/// A DynamoDB PutItem to the server's default endpoint, signed for the
/// server's region.
pub fn base(key: Key, body: Vec<u8>) -> Shape {
    Shape::new(
        key,
        ENDPOINTS[0],
        SERVER_REGION,
        "DynamoDB_20120810.PutItem",
        body,
    )
}

/// The shapes DynamoDB clients send, for `key` with `body`.
pub fn client_shapes(key: Key, body: &[u8]) -> Vec<(&'static str, Shape)> {
    let mut sdk = base(key, body.to_vec());
    for (name, value) in SDK_HEADERS {
        sdk = sdk.with_signed(name, value);
    }
    sdk.sdk_exclusions = true;

    let mut every_header_signed = sdk.clone();
    every_header_signed.sdk_exclusions = false;

    let mut unsigned = base(key, body.to_vec());
    for (name, value) in SDK_HEADERS.iter().chain(UNSIGNED_HEADERS) {
        unsigned = unsigned.with_unsigned(name, value);
    }
    let unsigned = unsigned.with_unsigned("accept-encoding", "identity");

    vec![
        // Every header goes to the signer, which leaves user-agent out of the
        // signature by default.
        ("AWS SDK for Rust headers", sdk),
        // host, x-amz-date, x-amz-target, content-type and content-length.
        ("DynamoDB headers only", base(key, body.to_vec())),
        ("every SDK header signed", every_header_signed),
        // accept-encoding is sent twice.
        ("SDK and proxy headers unsigned", unsigned),
    ]
}

/// `name` spelled in one of four styles: as given, Title-Case, upper case,
/// and alternating case.
pub fn spell(name: &str, style: u8) -> String {
    match style % 4 {
        0 => name.to_owned(),
        1 => name
            .split('-')
            .map(|part| {
                let mut chars = part.chars();
                chars.next().map_or_else(String::new, |first| {
                    first.to_ascii_uppercase().to_string() + chars.as_str()
                })
            })
            .collect::<Vec<_>>()
            .join("-"),
        2 => name.to_ascii_uppercase(),
        _ => name
            .chars()
            .enumerate()
            .map(|(index, c)| {
                if index % 2 == 0 {
                    c.to_ascii_lowercase()
                } else {
                    c.to_ascii_uppercase()
                }
            })
            .collect(),
    }
}
