// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bucket check and the enumeration at Start: a missing bucket, an
//! `s3:ListBucket` denial, an owner mismatch, paging, key decoding, and
//! the failures that are the server's.

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::primitives::SdkBody;

use super::*;
use crate::bulk_put::s3::list::{check_bucket, decode_key, list};

/// One ListObjectsV2 page. `keys` are (key as sent, size, ETag without
/// quotes); `encoded` adds `<EncodingType>url</EncodingType>`.
fn list_page(keys: &[(&str, u64, &str)], next: Option<&str>, encoded: bool) -> HttpResponse {
    let mut body = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult \
         xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>loads</Name><MaxKeys>1000\
         </MaxKeys>",
    );
    if encoded {
        body.push_str("<EncodingType>url</EncodingType>");
    }
    match next {
        Some(token) => body.push_str(&format!(
            "<IsTruncated>true</IsTruncated><NextContinuationToken>{token}</NextContinuationToken>"
        )),
        None => body.push_str("<IsTruncated>false</IsTruncated>"),
    }
    for (key, size, etag) in keys {
        body.push_str(&format!(
            "<Contents><Key>{key}</Key><ETag>&quot;{etag}&quot;</ETag><Size>{size}</Size>\
             <StorageClass>STANDARD</StorageClass></Contents>"
        ));
    }
    body.push_str("</ListBucketResult>");
    with_headers(
        HttpResponse::new(200.try_into().unwrap(), SdkBody::from(body)),
        &[
            ("x-amz-request-id", REQUEST_ID),
            ("x-amz-id-2", EXTENDED_ID),
        ],
    )
}

#[tokio::test]
async fn the_bucket_check_answers_a_missing_bucket_with_the_amazon_s3_tail() {
    let (clients, replay) = clients(vec![s3_error(
        404,
        "NoSuchBucket",
        "The specified bucket does not exist",
    )]);
    let error = check_bucket(&clients, &location("loads", None), "in/")
        .await
        .unwrap_err();
    let fault = fault(error);
    assert_eq!(fault.code, "S3NoSuchBucket");
    assert_eq!(
        fault.message,
        format!(
            "The specified bucket does not exist{}",
            amazon_s3_tail(404, "NoSuchBucket")
        )
    );
    assert!(!fault.after_first_byte);
    let request = uri(&replay, 0);
    assert!(request.contains("list-type=2"), "{request}");
    assert!(request.contains("max-keys=1"), "{request}");
    assert!(request.contains("prefix=in%2F"), "{request}");
    assert!(request.contains("encoding-type=url"), "{request}");
    assert_eq!(header(&replay, 0, "x-amz-expected-bucket-owner"), None);
}

#[tokio::test]
async fn the_bucket_check_answers_a_list_bucket_denial_with_s3s_text() {
    let denial = "User: arn:aws:sts::111122223333:assumed-role/extenddb/server is not \
                  authorized to perform: s3:ListBucket on resource: \"arn:aws:s3:::loads\" \
                  with an explicit deny in a resource-based policy";
    let (clients, _replay) = clients(vec![s3_error(403, "AccessDenied", denial)]);
    let fault = fault(
        check_bucket(&clients, &location("loads", None), "in/")
            .await
            .unwrap_err(),
    );
    assert_eq!(fault.code, "S3AccessDenied");
    assert_eq!(
        fault.message,
        format!("{denial}{}", amazon_s3_tail(403, "AccessDenied"))
    );
    assert!(!fault.after_first_byte);
}

#[tokio::test]
async fn an_expected_owner_travels_as_the_header_and_s3_decides() {
    let (clients, replay) = clients(vec![s3_error(403, "AccessDenied", "Access Denied")]);
    let fault = fault(
        check_bucket(&clients, &location("loads", Some("444455556666")), "")
            .await
            .unwrap_err(),
    );
    assert_eq!(fault.code, "S3AccessDenied");
    assert_eq!(
        fault.message,
        format!("Access Denied{}", amazon_s3_tail(403, "AccessDenied"))
    );
    assert_eq!(
        header(&replay, 0, "x-amz-expected-bucket-owner").as_deref(),
        Some("444455556666")
    );
}

#[tokio::test]
async fn a_present_bucket_passes_the_check() {
    let (clients, _replay) = clients(vec![list_page(&[], None, true)]);
    check_bucket(&clients, &location("loads", None), "in/")
        .await
        .unwrap();
}

#[tokio::test]
async fn the_listing_decodes_filters_sorts_and_unquotes_across_pages() {
    let (clients, replay) = clients(vec![
        list_page(
            &[
                ("in%2Fz.json", 30, "etag-z"),
                ("in%2F%2Fdouble+slash.json", 20, "etag-d"),
                ("out%2Fnot-under-the-prefix.json", 5, "etag-o"),
            ],
            Some("token-2"),
            true,
        ),
        list_page(
            &[
                (
                    "in%2Fa%2Bplus.json",
                    10,
                    "0123456789abcdef0123456789abcdef-3",
                ),
                ("in%2Fdir%2F", 0, "d41d8cd98f00b204e9800998ecf8427e"),
            ],
            None,
            true,
        ),
    ]);
    let files = list(&clients, &location("loads", None), "in/", 100)
        .await
        .unwrap();
    let listed: Vec<(&str, u64, Option<&str>)> = files
        .iter()
        .map(|f| (f.path.as_str(), f.size, f.etag.as_deref()))
        .collect();
    assert_eq!(
        listed,
        vec![
            ("in//double slash.json", 20, Some("etag-d")),
            (
                "in/a+plus.json",
                10,
                Some("0123456789abcdef0123456789abcdef-3")
            ),
            ("in/dir/", 0, Some("d41d8cd98f00b204e9800998ecf8427e")),
            ("in/z.json", 30, Some("etag-z")),
        ]
    );
    assert!(uri(&replay, 0).contains("max-keys=1000"));
    assert!(!uri(&replay, 0).contains("continuation-token"));
    assert!(uri(&replay, 1).contains("continuation-token=token-2"));
}

#[tokio::test]
async fn a_listing_without_url_encoding_keeps_keys_as_given() {
    let (clients, _replay) = clients(vec![list_page(&[("in/a%2Fb+c", 1, "e")], None, false)]);
    let files = list(&clients, &location("loads", None), "in/", 10)
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "in/a%2Fb+c");
}

#[tokio::test]
async fn the_listing_stops_one_object_past_the_cap() {
    let keys: Vec<(String, u64, &str)> = (0..5).map(|i| (format!("k{i}"), 1, "e")).collect();
    let page: Vec<(&str, u64, &str)> = keys.iter().map(|(k, s, e)| (k.as_str(), *s, *e)).collect();
    let (clients, replay) = clients(vec![list_page(&page, Some("more"), true)]);
    let files = list(&clients, &location("loads", None), "", 2)
        .await
        .unwrap();
    assert_eq!(files.len(), 3);
    assert_eq!(replay.actual_requests().count(), 1, "no page past the cap");
}

#[tokio::test]
async fn a_fault_on_a_later_page_is_rendered_for_that_page() {
    let (clients, _replay) = clients(vec![
        list_page(&[("in%2Fa", 1, "e")], Some("token-2"), true),
        s3_error(404, "NoSuchBucket", "The specified bucket does not exist"),
    ]);
    let fault = fault(
        list(&clients, &location("loads", None), "in/", 10)
            .await
            .unwrap_err(),
    );
    assert_eq!(fault.code, "S3NoSuchBucket");
    assert_eq!(
        fault.message,
        format!(
            "The specified bucket does not exist{}",
            amazon_s3_tail(404, "NoSuchBucket")
        )
    );
}

#[tokio::test]
async fn server_errors_and_unknown_codes_on_the_listing_are_internal() {
    for response in [
        s3_error(500, "InternalError", "We encountered an internal error."),
        s3_error(503, "SlowDown", "Please reduce your request rate."),
        s3_error(400, "RequestTimeout", "Your socket connection timed out."),
        s3_error(400, "SomeFutureCode", "Something new."),
    ] {
        let (clients, _replay) = clients(vec![response]);
        let error = list(&clients, &location("loads", None), "", 10)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, SourceError::Io(io) if io.kind() == std::io::ErrorKind::Other),
            "{error:?}"
        );
    }
}

#[test]
fn keys_decode_escapes_and_plus_and_refuse_malformed_escapes() {
    assert_eq!(decode_key("a%2Fb+c%2B").as_deref(), Some("a/b c+"));
    assert_eq!(decode_key("%E2%82%AC").as_deref(), Some("\u{20ac}"));
    assert_eq!(decode_key("plain").as_deref(), Some("plain"));
    for malformed in ["%", "%2", "%G1", "%+1", "%ff", "a%2"] {
        assert_eq!(decode_key(malformed), None, "{malformed}");
    }
}
