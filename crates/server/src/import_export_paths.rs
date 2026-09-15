// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Resolution of the effective import/export path lists and their diagnostics.
//!
//! Two config surfaces feed one pair of path lists: the `[import]` / `[export]`
//! sections, and the deprecated `import_export_root` key that supplies a single
//! root for both. Which surface won determines what a diagnostic should tell the
//! operator, so resolution records the source of each list rather than only its
//! contents: advice to use separate directories is actionable for two
//! independently configured roots and misleading for one deprecated key that
//! makes them identical by construction.
//!
//! Kept out of `serve` and free of I/O so every branch is unit-testable.

use std::path::PathBuf;
use std::sync::Arc;

/// Where an effective path list came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathSource {
    /// The `[import]` or `[export]` section supplied it.
    Section,
    /// The deprecated `import_export_root` key supplied it.
    LegacyRoot,
    /// Neither: the operation is disabled. Bookkeeping only; no diagnostic
    /// branches on it, since a list with no source produces no notice.
    Unset,
}

/// The effective raw path lists, with the origin of each recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectivePaths {
    pub(crate) import: Vec<String>,
    pub(crate) export: Vec<String>,
    pub(crate) import_source: PathSource,
    pub(crate) export_source: PathSource,
    /// The deprecated key as configured, retained so diagnostics read from the
    /// same value the resolution used. Passing it back in would let a caller
    /// silence a notice about a key that is in force.
    legacy_root: Option<String>,
}

impl EffectivePaths {
    /// Resolve the effective lists. A populated section always wins; the
    /// deprecated key fills only a list a section left empty.
    pub(crate) fn resolve(
        import_section: &[String],
        export_section: &[String],
        legacy_root: Option<&str>,
    ) -> Self {
        let mut out = Self {
            import: import_section.to_vec(),
            export: export_section.to_vec(),
            legacy_root: legacy_root.map(str::to_owned),
            import_source: if import_section.is_empty() {
                PathSource::Unset
            } else {
                PathSource::Section
            },
            export_source: if export_section.is_empty() {
                PathSource::Unset
            } else {
                PathSource::Section
            },
        };

        if let Some(legacy) = legacy_root {
            if out.import.is_empty() {
                out.import.push(legacy.to_owned());
                out.import_source = PathSource::LegacyRoot;
            }
            if out.export.is_empty() {
                out.export.push(legacy.to_owned());
                out.export_source = PathSource::LegacyRoot;
            }
        }

        out
    }

    /// Whether any overlap between the two lists is inherent rather than chosen.
    /// True only when the deprecated key supplied both, in which case the roots
    /// are the same path by construction and the remedy is migration.
    pub(crate) fn overlap_is_inherent(&self) -> bool {
        self.import_source == PathSource::LegacyRoot && self.export_source == PathSource::LegacyRoot
    }

    /// Diagnostics describing what the deprecated key actually did.
    ///
    /// Every case in which the key is set produces exactly one notice, including
    /// the case where it silently supplies just one of the two lists. Reporting
    /// only the fully-ignored case would leave an operator believing a key that
    /// is in force is inert.
    pub(crate) fn legacy_notices(&self) -> Vec<String> {
        let Some(ref legacy) = self.legacy_root else {
            return Vec::new();
        };

        let notice = match (self.import_source, self.export_source) {
            (PathSource::LegacyRoot, PathSource::LegacyRoot) => format!(
                "Deprecated import_export_root supplies both the import and export root ({legacy}), \
                 so they are the same directory. Files are namespaced per account, so this is not a \
                 cross-account exposure, but migrate to separate [import] and [export] paths to \
                 keep the two apart."
            ),
            (PathSource::LegacyRoot, _) => format!(
                "Deprecated import_export_root supplies the import root ({legacy}) because [import] \
                 has no paths. Migrate to [import] paths; the key will be removed."
            ),
            (_, PathSource::LegacyRoot) => format!(
                "Deprecated import_export_root supplies the export root ({legacy}) because [export] \
                 has no paths. Migrate to [export] paths; the key will be removed."
            ),
            _ => format!(
                "Deprecated import_export_root ({legacy}) is unused because both [import] and \
                 [export] have paths. Remove it."
            ),
        };
        vec![notice]
    }
}

/// Diagnostics for roots that resolve to the same directory or nest.
///
/// With per-account namespacing an overlap is no longer a cross-account
/// exposure, but it still means a tenant's own exports are re-importable and the
/// two operations share a disk budget, so it is worth surfacing.
///
/// When `overlap_is_inherent` the identical-root case is left to the deprecation
/// notice, which names the remedy that applies. Nesting is always reported: it
/// cannot arise from a single legacy root.
///
/// The sources are decided on the raw config strings while this runs on the
/// canonicalized roots, which is sound rather than a mismatch: inherent means
/// both lists hold the identical legacy string, so canonicalization maps them to
/// the same path and the suppressed case is always the one that matched.
pub(crate) fn overlap_notices(
    import: &[Arc<PathBuf>],
    export: &[Arc<PathBuf>],
    overlap_is_inherent: bool,
) -> Vec<String> {
    let mut notices = Vec::new();
    for i in import {
        for e in export {
            if i == e {
                if !overlap_is_inherent {
                    notices.push(format!(
                        "Import and export are configured with the same root ({}); files remain \
                         namespaced per account, but consider separate directories.",
                        i.display()
                    ));
                }
            } else if i.starts_with(e.as_path()) || e.starts_with(i.as_path()) {
                notices.push(format!(
                    "Import root ({}) and export root ({}) are nested; files remain namespaced per \
                     account, but consider separate directories.",
                    i.display(),
                    e.display()
                ));
            }
        }
    }
    notices
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    fn roots(items: &[&str]) -> Vec<Arc<PathBuf>> {
        items.iter().map(|s| Arc::new(PathBuf::from(s))).collect()
    }

    #[test]
    fn sections_only() {
        let p = EffectivePaths::resolve(&v(&["/in"]), &v(&["/out"]), None);
        assert_eq!(p.import, v(&["/in"]));
        assert_eq!(p.export, v(&["/out"]));
        assert_eq!(p.import_source, PathSource::Section);
        assert_eq!(p.export_source, PathSource::Section);
        assert!(p.legacy_notices().is_empty());
        assert!(!p.overlap_is_inherent());
    }

    #[test]
    fn nothing_configured_leaves_both_unset() {
        let p = EffectivePaths::resolve(&[], &[], None);
        assert!(p.import.is_empty() && p.export.is_empty());
        assert_eq!(p.import_source, PathSource::Unset);
        assert_eq!(p.export_source, PathSource::Unset);
        assert!(
            !p.overlap_is_inherent(),
            "disabled is not an inherent overlap"
        );
    }

    /// One section on, one off, no legacy key: the Section/Unset split is the
    /// only thing distinguishing "disabled" from "configured" downstream.
    #[test]
    fn one_section_only_without_a_legacy_key() {
        let export_only = EffectivePaths::resolve(&[], &v(&["/out"]), None);
        assert!(export_only.import.is_empty());
        assert_eq!(export_only.import_source, PathSource::Unset);
        assert_eq!(export_only.export_source, PathSource::Section);
        assert!(export_only.legacy_notices().is_empty());

        let import_only = EffectivePaths::resolve(&v(&["/in"]), &[], None);
        assert!(import_only.export.is_empty());
        assert_eq!(import_only.import_source, PathSource::Section);
        assert_eq!(import_only.export_source, PathSource::Unset);
        assert!(import_only.legacy_notices().is_empty());
    }

    /// Second guard on the reviewed defect, with a different root, so the
    /// suppression is not pinned to one literal.
    #[test]
    fn identical_roots_from_legacy_key_are_suppressed_for_any_root() {
        let p = EffectivePaths::resolve(&[], &[], Some("/srv/extenddb/data"));
        assert!(p.overlap_is_inherent());
        let n = overlap_notices(
            &roots(&["/srv/extenddb/data"]),
            &roots(&["/srv/extenddb/data"]),
            p.overlap_is_inherent(),
        );
        assert!(n.is_empty(), "{n:?}");
    }

    #[test]
    fn legacy_root_fills_both_when_no_sections() {
        let p = EffectivePaths::resolve(&[], &[], Some("/legacy"));
        assert_eq!(p.import, v(&["/legacy"]));
        assert_eq!(p.export, v(&["/legacy"]));
        assert_eq!(p.import_source, PathSource::LegacyRoot);
        assert_eq!(p.export_source, PathSource::LegacyRoot);
        assert!(p.overlap_is_inherent());

        let n = p.legacy_notices();
        assert_eq!(n.len(), 1);
        assert!(n[0].contains("both the import and export root"), "{}", n[0]);
        assert!(n[0].contains("migrate"), "must name the remedy: {}", n[0]);
    }

    /// The case that previously produced no diagnostic at all: the key is in
    /// force for one operation while the operator is told nothing.
    #[test]
    fn legacy_root_filling_only_export_is_reported() {
        let p = EffectivePaths::resolve(&v(&["/in"]), &[], Some("/legacy"));
        assert_eq!(p.import, v(&["/in"]));
        assert_eq!(p.export, v(&["/legacy"]));
        assert_eq!(p.import_source, PathSource::Section);
        assert_eq!(p.export_source, PathSource::LegacyRoot);
        assert!(
            !p.overlap_is_inherent(),
            "only one list came from the legacy key, so an overlap here is chosen"
        );

        let n = p.legacy_notices();
        assert_eq!(n.len(), 1, "must not be silent");
        // "export root" alone is not discriminating: the both-legacy message
        // contains it too ("both the import and export root").
        assert!(
            n[0].contains("because [export] has no paths"),
            "must identify the branch, not just mention the export root: {}",
            n[0]
        );
        assert!(!n[0].contains("both"), "wrong branch: {}", n[0]);
        assert!(
            !n[0].contains("unused"),
            "the key is in force, not unused: {}",
            n[0]
        );
    }

    #[test]
    fn legacy_root_filling_only_import_is_reported() {
        let p = EffectivePaths::resolve(&[], &v(&["/out"]), Some("/legacy"));
        assert_eq!(p.import, v(&["/legacy"]));
        assert_eq!(p.import_source, PathSource::LegacyRoot);
        assert_eq!(p.export_source, PathSource::Section);
        let n = p.legacy_notices();
        assert_eq!(n.len(), 1);
        assert!(
            n[0].contains("because [import] has no paths"),
            "must identify the branch: {}",
            n[0]
        );
        assert!(!n[0].contains("both"), "wrong branch: {}", n[0]);
        assert!(!n[0].contains("unused"), "{}", n[0]);
    }

    #[test]
    fn legacy_root_is_unused_when_both_sections_populated() {
        let p = EffectivePaths::resolve(&v(&["/in"]), &v(&["/out"]), Some("/legacy"));
        assert_eq!(p.import, v(&["/in"]));
        assert_eq!(p.export, v(&["/out"]));
        assert_eq!(p.import_source, PathSource::Section);
        assert_eq!(p.export_source, PathSource::Section);
        let n = p.legacy_notices();
        assert_eq!(n.len(), 1);
        assert!(n[0].contains("unused"), "{}", n[0]);
    }

    /// A notice about a key that is in force must not be silenceable from the
    /// call site: the legacy value is captured at resolve time, not re-supplied.
    #[test]
    fn legacy_notice_reads_the_value_resolution_used() {
        let applied = EffectivePaths::resolve(&[], &[], Some("/legacy"));
        assert_eq!(applied.legacy_notices().len(), 1);

        let absent = EffectivePaths::resolve(&[], &[], None);
        assert!(absent.legacy_notices().is_empty());
        assert_ne!(
            applied.legacy_notices(),
            absent.legacy_notices(),
            "the notice must follow the resolved config, not a caller argument"
        );
    }

    /// The reviewed defect: advising separate directories is not actionable when
    /// the deprecated key is what made the roots identical.
    #[test]
    fn identical_roots_from_legacy_key_emit_no_overlap_advice() {
        let p = EffectivePaths::resolve(&[], &[], Some("/legacy"));
        let n = overlap_notices(
            &roots(&["/legacy"]),
            &roots(&["/legacy"]),
            p.overlap_is_inherent(),
        );
        assert!(n.is_empty(), "deprecation notice owns this case: {n:?}");
    }

    /// Converse: two independently configured roots that happen to be identical
    /// must still be reported, because changing one is the remedy.
    #[test]
    fn identical_roots_from_sections_are_reported() {
        let p = EffectivePaths::resolve(&v(&["/same"]), &v(&["/same"]), None);
        let n = overlap_notices(
            &roots(&["/same"]),
            &roots(&["/same"]),
            p.overlap_is_inherent(),
        );
        assert_eq!(n.len(), 1);
        assert!(n[0].contains("same root"), "{}", n[0]);
    }

    #[test]
    fn nested_roots_are_reported_even_when_a_legacy_key_is_present() {
        let p = EffectivePaths::resolve(&v(&["/data/in"]), &[], Some("/data"));
        let n = overlap_notices(
            &roots(&["/data/in"]),
            &roots(&["/data"]),
            p.overlap_is_inherent(),
        );
        assert_eq!(n.len(), 1, "nesting is never inherent");
        assert!(n[0].contains("nested"), "{}", n[0]);
    }

    #[test]
    fn disjoint_roots_are_silent() {
        let n = overlap_notices(&roots(&["/in"]), &roots(&["/out"]), false);
        assert!(n.is_empty(), "{n:?}");
    }

    /// A shared prefix that is not a path boundary must not count as nesting.
    #[test]
    fn sibling_roots_with_a_shared_string_prefix_are_silent() {
        let n = overlap_notices(&roots(&["/data-in"]), &roots(&["/data"]), false);
        assert!(n.is_empty(), "component-wise containment only: {n:?}");
    }

    #[test]
    fn every_pair_is_checked_across_multiple_roots() {
        let n = overlap_notices(
            &roots(&["/a", "/shared"]),
            &roots(&["/b", "/shared"]),
            false,
        );
        assert_eq!(n.len(), 1, "{n:?}");
        assert!(n[0].contains("/shared"), "{}", n[0]);
    }
}
