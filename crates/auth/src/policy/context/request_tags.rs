// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Resolution of `aws:RequestTag/<key>` against the tags a request carries.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use super::RequestContext;

/// The tags a request carries, indexed by case-folded name.
///
/// The index is built once, when the context is built, so resolving
/// `aws:RequestTag/<key>` folds the policy's key name and probes the index. The
/// cost does not grow with the number of tags in the request or with the number
/// of statements that read the key.
#[derive(Debug, PartialEq, Eq)]
pub struct RequestTagIndex {
    by_folded_name: HashMap<String, FoldedTag>,
}

#[derive(Debug, PartialEq, Eq)]
enum FoldedTag {
    /// Every tag whose name folds to this one carries this value.
    Value(String),
    /// Two or more tags whose names fold to this one carry differing values.
    Ambiguous,
}

impl RequestTagIndex {
    /// Index the request's tags by the Unicode case fold of each tag name. An
    /// operation that carries no tag set, or an empty one, has no index.
    pub(super) fn from_tags(tags: Option<HashMap<String, String>>) -> Option<Self> {
        tags.filter(|t| !t.is_empty()).map(Self::new)
    }

    fn new(tags: HashMap<String, String>) -> Self {
        let mut by_folded_name: HashMap<String, FoldedTag> = HashMap::with_capacity(tags.len());
        for (name, value) in tags {
            match by_folded_name.entry(crate::policy::casefold::case_fold(&name)) {
                Entry::Vacant(slot) => {
                    slot.insert(FoldedTag::Value(value));
                }
                Entry::Occupied(mut slot) => {
                    if matches!(slot.get(), FoldedTag::Value(seen) if *seen != value) {
                        slot.insert(FoldedTag::Ambiguous);
                    }
                }
            }
        }
        Self { by_folded_name }
    }

    fn lookup(&self, sub_key: &str) -> RequestTagLookup<'_> {
        match self
            .by_folded_name
            .get(&crate::policy::casefold::case_fold(sub_key))
        {
            None => RequestTagLookup::Absent,
            Some(FoldedTag::Value(value)) => RequestTagLookup::Value(value),
            Some(FoldedTag::Ambiguous) => RequestTagLookup::Ambiguous,
        }
    }
}

impl RequestContext {
    /// Resolve `aws:RequestTag/<sub_key>` against the tags the request carries.
    ///
    /// The tag name in the condition key is matched under Unicode case folding,
    /// as the principal-tag and resource-tag keys are on this path. The service
    /// matches names that differ in the case of one character (`Étag` and
    /// `étag`) and does not fold the sharp s to `ss`, which this lookup does, so
    /// `Straße` and `STRASSE` are one name here and two on the service (measured
    /// 2026-10-27 with `iam simulate-custom-policy`). The engine
    /// stores every spelling the request names as its own tag, so a request
    /// carrying `Env=prod` and `env=dev` stores both. When two or more stored
    /// spellings fold to the policy's name, the key has one value only if they
    /// all carry the same value; otherwise it is ambiguous, and the caller
    /// fails the statement closed rather than pick one spelling and let the
    /// other value through. IAM refuses a request context carrying two case
    /// variants of one tag name, so the service's own answer for a request of
    /// this shape is not measured.
    pub(super) fn request_tag_values(&self, sub_key: &str) -> RequestTagLookup<'_> {
        self.request_tags
            .as_ref()
            .map_or(RequestTagLookup::Absent, |index| index.lookup(sub_key))
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
