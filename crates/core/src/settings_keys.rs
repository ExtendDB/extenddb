// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Canonical names for runtime settings keys.
//!
//! These strings live in one place because they are read by both storage backends,
//! written by the management API, seeded by both schemas, and documented. Scattering
//! the literal is what let one of them drift out of step with its own meaning.

/// Propagation delay applied to asynchronous secondary-index maintenance, in
/// milliseconds. `0` means maintenance is applied synchronously in the write's own
/// transaction.
///
/// Governs GSIs and vector indexes alike, which is why it is not named for either.
/// Real DynamoDB exposes no such knob: this exists so a test can choose between
/// asserting eventual-consistency behaviour and asserting steady state without
/// waiting.
pub const INDEX_PROPAGATION_DELAY_MS: &str = "index_propagation_delay_ms";

/// The pre-rename name of [`INDEX_PROPAGATION_DELAY_MS`], still honoured.
///
/// Two reasons this cannot simply be deleted. A catalog created before the rename
/// holds the operator's value under the old name, and the server refuses to start on
/// a catalog-version mismatch rather than migrating, so there is no upgrade step in
/// which the row could be rewritten. Silently reading past that row would reset a
/// deliberately configured delay to the default, and a delay of 0 means synchronous,
/// so the silent change would be from strict to eventually consistent.
///
/// Reads therefore prefer the canonical key and fall back to this one; writes to this
/// name are redirected to the canonical key so a deployment converges on one row
/// rather than accumulating two that disagree.
pub const LEGACY_GSI_PROPAGATION_DELAY_MS: &str = "gsi_propagation_delay_ms";

/// Milliseconds to pause between batches of a vector index backfill.
///
/// Zero, and meant to stay zero outside tests. It exists because the correctness
/// property that matters during a backfill is an ordering one: a write that lands
/// while the index is building must end up in the index with its NEW value, not be
/// overwritten by the backfill's older snapshot of the same item. Proving that needs
/// a write to land mid-backfill, and a backfill over a test-sized table finishes far
/// too quickly for a test to hit that window reliably.
///
/// Without this the test would be a race against the backfill and would pass whether
/// or not the ordering is correct, which is worse than having no test.
pub const VECTOR_BACKFILL_BATCH_DELAY_MS: &str = "vector_backfill_batch_delay_ms";

/// Minimum milliseconds an UpdateTable-created vector index stays in `CREATING`
/// before its `ACTIVE` flip.
///
/// The service's online-index machinery never completes an added index
/// instantly (~17 minutes observed for a 25-item table, eu-west-2, 2026-08-11),
/// so the CREATING-with-`Backfilling` walk is always observable there. A local
/// backfill over a small table finishes in milliseconds, which would collapse
/// the documented lifecycle — `CREATING`/`Backfilling: false`, then `true`,
/// then `ACTIVE` with the member absent — into a single unobservable instant.
/// Holding the flip for a short floor keeps the walk observable to a client
/// polling DescribeTable, the same reason `control_plane_delay_seconds` exists
/// for table status. The default is 1000; zero disables the hold.
pub const VECTOR_INDEX_MIN_CREATING_MS: &str = "vector_index_min_creating_ms";

/// Milliseconds to hold a new vector index in the resource-allocation phase before
/// its backfill starts.
///
/// Zero, and meant to stay zero outside tests, exactly like
/// [`VECTOR_BACKFILL_BATCH_DELAY_MS`] above and for the same kind of reason. The
/// service refuses a delete of an index that is still allocating resources, with a
/// byte-exact `ResourceInUseException`, and accepts the same delete once the
/// backfill is running. Both halves are measured behaviour, so both deserve a wire
/// test.
///
/// Without this there is no deterministic way to observe the first half from a
/// client: the allocation phase exists only between the catalog row's insert and
/// the flip to `Backfilling: true`, both inside one `UpdateTable` call, so a test
/// could only race it. A race that asserts a whole measured string is worse than
/// no test, because it fails for reasons unrelated to the rule.
///
/// Unset, nothing waits and no branch is taken.
pub const VECTOR_ALLOCATION_PHASE_DELAY_MS: &str = "vector_allocation_phase_delay_ms";

/// Resolve a caller-supplied settings key to its canonical name.
///
/// Accepting the old name keeps `extenddb settings set gsi_propagation_delay_ms 0`
/// working for anyone with it in a script or runbook, while ensuring the value lands
/// where the read path looks first.
#[must_use]
pub fn canonical_key(key: &str) -> &str {
    if key == LEGACY_GSI_PROPAGATION_DELAY_MS {
        INDEX_PROPAGATION_DELAY_MS
    } else {
        key
    }
}

#[cfg(test)]
mod tests {
    use super::{INDEX_PROPAGATION_DELAY_MS, LEGACY_GSI_PROPAGATION_DELAY_MS, canonical_key};

    #[test]
    fn the_legacy_name_resolves_to_the_canonical_one() {
        assert_eq!(
            canonical_key(LEGACY_GSI_PROPAGATION_DELAY_MS),
            INDEX_PROPAGATION_DELAY_MS
        );
    }

    #[test]
    fn an_unrelated_key_is_returned_unchanged() {
        assert_eq!(canonical_key("throttling_enabled"), "throttling_enabled");
        assert_eq!(
            canonical_key(INDEX_PROPAGATION_DELAY_MS),
            INDEX_PROPAGATION_DELAY_MS
        );
    }
}
