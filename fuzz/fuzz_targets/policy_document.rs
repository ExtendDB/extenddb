// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Mirrors the parse paths for identity policies and permissions boundaries. On
// a write, crates/server/src/management/iam_policy.rs and
// permissions_boundary.rs call PolicyDocument::from_json_for_write; on the
// request path, crates/server/src/authz_cache.rs calls PolicyDocument::from_json
// on a stored identity policy, permissions boundary or session policy.
// from_json caps the document at 6,144 bytes itself. Both are driven here with
// the same arbitrary JSON text. A trust policy is checked on write by
// validate_trust_policy in crates/server/src/management/assume_role.rs, which
// this target does not call. AssumeRole evaluates its conditions with
// parse_conditions, which from_json also calls.

#![no_main]

use libfuzzer_sys::fuzz_target;

use extenddb_auth::policy::document::PolicyDocument;

fuzz_target!(|data: &[u8]| {
    if let Ok(json) = std::str::from_utf8(data) {
        let _ = PolicyDocument::from_json(json);
        let _ = PolicyDocument::from_json_for_write(json);
    }
});
