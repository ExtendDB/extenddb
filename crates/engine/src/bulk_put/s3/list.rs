// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The input bucket at Start: the bucket check and the enumeration, both
//! ListObjectsV2. The check lists one key under the request's prefix, so
//! S3 answers the owner, existence and `s3:ListBucket` faults with its own
//! code and message; a HEAD request would carry neither, since its error
//! has no body. The enumeration lists pages of 1,000 keys with
//! `encoding-type=url`, which keeps keys holding characters XML cannot
//! carry intact, and decodes them.

use aws_sdk_s3::types::EncodingType;

use super::client::Phase;
use super::{Location, S3Clients, source_error, unquoted};
use crate::bulk_put::source::{SourceError, SourceFile};

/// Keys asked for per ListObjectsV2 page.
const LIST_PAGE_KEYS: i32 = 1_000;

/// The Start-time check of the input bucket: one ListObjectsV2 of at most
/// one key under `prefix`, with the expected owner when one is set.
pub(super) async fn check_bucket(
    clients: &S3Clients,
    location: &Location,
    prefix: &str,
) -> Result<(), SourceError> {
    clients
        .call_in(Phase::Start, &location.bucket, |client| {
            client
                .list_objects_v2()
                .bucket(&location.bucket)
                .prefix(prefix)
                .max_keys(1)
                .encoding_type(EncodingType::Url)
                .set_expected_bucket_owner(location.owner.clone())
                .send()
        })
        .await
        .map(|_| ())
        .map_err(|error| source_error(&error, location, prefix, false, "bucket check"))
}

/// Every object under `prefix` (byte-wise, as given: a doubled or leading
/// `/` included), zero-byte keys ending in `/` among them, in UTF-8 byte
/// order whatever order the service listed them in. Collecting more than
/// `max_files` stops the listing with `max_files + 1` entries, so the
/// caller can tell the bound was passed. A fault on any page is the
/// service's answer for that page. Each page is one call under the Start
/// deadline, so a long listing is not cut short while its pages answer.
pub(super) async fn list(
    clients: &S3Clients,
    location: &Location,
    prefix: &str,
    max_files: u64,
) -> Result<Vec<SourceFile>, SourceError> {
    let cap = usize::try_from(max_files.saturating_add(1)).unwrap_or(usize::MAX);
    let mut files: Vec<SourceFile> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = clients
            .call_in(Phase::Start, &location.bucket, |client| {
                client
                    .list_objects_v2()
                    .bucket(&location.bucket)
                    .prefix(prefix)
                    .max_keys(LIST_PAGE_KEYS)
                    .encoding_type(EncodingType::Url)
                    .set_continuation_token(token.clone())
                    .set_expected_bucket_owner(location.owner.clone())
                    .send()
            })
            .await
            .map_err(|error| source_error(&error, location, prefix, false, "listing"))?;
        let encoded = page.encoding_type() == Some(&EncodingType::Url);
        for object in page.contents() {
            let Some(key) = object.key() else { continue };
            let path = if encoded {
                decode_key(key).ok_or_else(|| {
                    tracing::error!(
                        account_id = %location.account,
                        bucket = %location.bucket,
                        "S3 listing returned a key that does not decode"
                    );
                    SourceError::Io(std::io::Error::other("S3 listing key does not decode"))
                })?
            } else {
                key.to_owned()
            };
            // A service that ignored the prefix must not widen the input.
            if !path.as_bytes().starts_with(prefix.as_bytes()) {
                continue;
            }
            files.push(SourceFile {
                path,
                size: object
                    .size()
                    .and_then(|size| u64::try_from(size).ok())
                    .unwrap_or(0),
                etag: object.e_tag().map(unquoted),
            });
            if files.len() >= cap {
                break;
            }
        }
        if files.len() >= cap {
            break;
        }
        match (page.is_truncated(), page.next_continuation_token()) {
            (Some(true), Some(next)) if !next.is_empty() => token = Some(next.to_owned()),
            _ => break,
        }
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

/// Decode one key S3 listed with `encoding-type=url`: `%XX` escapes and
/// `+` for a space, as S3 encodes them. `None` for a malformed escape or a
/// result that is not UTF-8.
pub(super) fn decode_key(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                let hex = encoded.get(i + 1..i + 3)?;
                if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return None;
                }
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}
