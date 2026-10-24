// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Resolution of `aws:RequestTag/<key>` against the tags a request carries.

use super::RequestContext;

impl RequestContext {
    /// Resolve `aws:RequestTag/<sub_key>` against the tags the request carries.
    ///
    /// The tag name in the condition key is matched under Unicode case folding,
    /// as the principal-tag and resource-tag keys are on this path. The engine
    /// stores every spelling the request names as its own tag, so a request
    /// carrying `Env=prod` and `env=dev` stores both. When two or more stored
    /// spellings fold to the policy's name, the key has one value only if they
    /// all carry the same value; otherwise it is ambiguous, and the caller
    /// fails the statement closed rather than pick one spelling and let the
    /// other value through. IAM refuses a request context carrying two case
    /// variants of one tag name, so the service's own answer for a request of
    /// this shape is not measured.
    pub(super) fn request_tag_values(&self, sub_key: &str) -> RequestTagLookup<'_> {
        let Some(tags) = self.request_tags.as_ref() else {
            return RequestTagLookup::Absent;
        };
        let want = crate::policy::casefold::case_fold(sub_key);
        let mut found: Option<&str> = None;
        for (k, v) in tags {
            if crate::policy::casefold::case_fold(k) != want {
                continue;
            }
            match found {
                None => found = Some(v.as_str()),
                Some(seen) if seen == v.as_str() => {}
                Some(_) => return RequestTagLookup::Ambiguous,
            }
        }
        found.map_or(RequestTagLookup::Absent, RequestTagLookup::Value)
    }
}

/// The outcome of resolving `aws:RequestTag/<key>` on a `DynamoDB` request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestTagLookup<'a> {
    /// The request names no tag that folds to the key.
    Absent,
    /// Every tag that folds to the key carries this value.
    Value(&'a str),
    /// Two or more tags fold to the key with differing values.
    Ambiguous,
}
