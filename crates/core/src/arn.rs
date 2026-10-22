// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Strict parsing of `DynamoDB` resource ARNs.
//!
//! One parser serves request validation, authorization resource derivation, and
//! the engine resolvers, so they never disagree about the resource an ARN names.
//! When authorization reads a different table, stream, or backup from a field
//! than the engine resolves from the same field, a Deny or a narrow Allow
//! written against the real resource is bypassed. Every entry point that turns
//! an ARN into a resource identity routes through [`parse`] (or [`fields`] for
//! the raw five-field split), so a respelling that one layer accepts and another
//! rejects cannot exist.
//!
//! [`parse`] requires the literal `arn:` prefix and five colon-separated fields.
//! The partition, service, region, and account are captured but not constrained
//! to any value: a caller that pins them to its deployment does so itself, and a
//! caller that ignores them (authorization canonicalizes every supplied ARN to
//! its own deployment) reads only the table and sub-resource.

/// A table sub-resource: a secondary index, a stream, or a backup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubResource {
    Index(String),
    Stream(String),
    Backup(String),
}

/// The five colon-separated fields of an ARN, with the resource kept whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArnFields<'a> {
    pub partition: &'a str,
    pub service: &'a str,
    pub region: &'a str,
    pub account: &'a str,
    /// Everything after the fifth `:`; keeps any `:` and `/` it contains.
    pub resource: &'a str,
}

/// A parsed `DynamoDB` table resource ARN: its ARN fields, the table name, and
/// an optional sub-resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableResource {
    pub partition: String,
    pub service: String,
    pub region: String,
    pub account: String,
    pub table: String,
    pub sub: Option<SubResource>,
}

/// Split a string into the five ARN fields after the `arn:` prefix.
///
/// The resource field keeps every `:` and `/` it contains, so a stream ARN's
/// label (`.../stream/2026-10-20T16:17:40.188`) survives intact. Returns `None`
/// unless the string begins with `arn:` and has five colon-separated fields.
#[must_use]
pub fn fields(arn: &str) -> Option<ArnFields<'_>> {
    let rest = arn.strip_prefix("arn:")?;
    let mut it = rest.splitn(5, ':');
    Some(ArnFields {
        partition: it.next()?,
        service: it.next()?,
        region: it.next()?,
        account: it.next()?,
        resource: it.next()?,
    })
}

/// Parse an ARN that names a `DynamoDB` table resource.
///
/// Returns `Some` only for
/// `arn:<partition>:<service>:<region>:<account>:table/<name>[/<kind>/<id...>]`
/// with a non-empty table name and, when a sub-resource is present, a known
/// kind (`index`, `stream`, or `backup`). Returns `None` for a missing `arn:`
/// prefix, fewer than five fields, a resource that is not `table/...`, an empty
/// table name, or an unknown sub-resource kind.
///
/// A trailing slash directly after the table (`table/T/`) is the bare table. An
/// empty sub-resource id (`table/T/stream/`) is kept as an empty id, and a
/// sub-resource id may itself contain `/` (`table/T/stream/a/b` has id `a/b`),
/// so the set of streams this accepts is exactly the set the engine stream
/// resolver accepts.
#[must_use]
pub fn parse(arn: &str) -> Option<TableResource> {
    let f = fields(arn)?;
    let tail = f.resource.strip_prefix("table/")?;
    // `tail` is `<table>[/<kind>/<id...>]`. Table names contain no `/`.
    let mut segs = tail.split('/');
    let table = segs.next()?;
    if table.is_empty() {
        return None;
    }
    let sub = match segs.next() {
        None | Some("") => None,
        Some(kind) => {
            let id = segs.collect::<Vec<&str>>().join("/");
            match kind {
                "index" => Some(SubResource::Index(id)),
                "stream" => Some(SubResource::Stream(id)),
                "backup" => Some(SubResource::Backup(id)),
                // An unrecognized sub-resource kind is not a nameable resource.
                _ => return None,
            }
        }
    };
    Some(TableResource {
        partition: f.partition.to_owned(),
        service: f.service.to_owned(),
        region: f.region.to_owned(),
        account: f.account.to_owned(),
        table: table.to_owned(),
        sub,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_requires_arn_prefix_and_five_fields() {
        assert_eq!(fields("banana"), None);
        // A leading field other than `arn` has no `arn:` prefix.
        assert_eq!(fields("xrn:aws:dynamodb:us-east-1:1:table/T/stream/x"), None);
        // An empty leading field (`:aws:...`) has no `arn:` prefix either.
        assert_eq!(fields(":aws:dynamodb:us-east-1:1:table/T"), None);
        assert_eq!(fields("arn:aws:dynamodb:us-east-1"), None);
        let f = fields("arn:aws:dynamodb:us-east-1:1:table/T").unwrap();
        assert_eq!(f.partition, "aws");
        assert_eq!(f.service, "dynamodb");
        assert_eq!(f.region, "us-east-1");
        assert_eq!(f.account, "1");
        assert_eq!(f.resource, "table/T");
    }

    #[test]
    fn parses_table_index_stream_backup() {
        assert_eq!(
            parse("arn:aws:dynamodb:us-east-1:1:table/T").unwrap().sub,
            None
        );
        assert_eq!(
            parse("arn:aws:dynamodb:us-east-1:1:table/T/index/gsi1")
                .unwrap()
                .sub,
            Some(SubResource::Index("gsi1".into()))
        );
        assert_eq!(
            parse("arn:aws:dynamodb:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188")
                .unwrap()
                .sub,
            Some(SubResource::Stream("2026-10-20T16:17:40.188".into()))
        );
        assert_eq!(
            parse("arn:aws:dynamodb:us-east-1:1:table/T/backup/01-ab")
                .unwrap()
                .sub,
            Some(SubResource::Backup("01-ab".into()))
        );
    }

    #[test]
    fn captures_all_fields() {
        let r = parse("arn:aws-cn:dynamodb:eu-west-1:111122223333:table/T/index/g").unwrap();
        assert_eq!(r.partition, "aws-cn");
        assert_eq!(r.service, "dynamodb");
        assert_eq!(r.region, "eu-west-1");
        assert_eq!(r.account, "111122223333");
        assert_eq!(r.table, "T");
        assert_eq!(r.sub, Some(SubResource::Index("g".into())));
    }

    #[test]
    fn trailing_slash_is_bare_table_and_empty_id_is_empty() {
        assert_eq!(parse("arn:aws:dynamodb:us-east-1:1:table/T/").unwrap().sub, None);
        assert_eq!(
            parse("arn:aws:dynamodb:us-east-1:1:table/T/stream/")
                .unwrap()
                .sub,
            Some(SubResource::Stream(String::new()))
        );
    }

    #[test]
    fn sub_resource_id_keeps_inner_slashes() {
        assert_eq!(
            parse("arn:aws:dynamodb:us-east-1:1:table/T/stream/a/b")
                .unwrap()
                .sub,
            Some(SubResource::Stream("a/b".into()))
        );
    }

    #[test]
    fn rejects_non_table_empty_table_and_unknown_kind() {
        assert_eq!(parse("banana"), None);
        assert_eq!(parse("arn:aws:dynamodb:us-east-1:1:banana/x"), None);
        assert_eq!(parse("arn:aws:dynamodb:us-east-1:1:table/"), None);
        assert_eq!(parse("arn:aws:dynamodb:us-east-1:1:table//index/g"), None);
        assert_eq!(parse("arn:aws:dynamodb:us-east-1:1:table/T/weird/x"), None);
        // No `arn:` prefix: the A2 bypass input is rejected here, so every
        // caller built on this parser rejects it.
        assert_eq!(parse("xrn:aws:dynamodb:us-east-1:1:table/T/stream/x"), None);
    }
}
