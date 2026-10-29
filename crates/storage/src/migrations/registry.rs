// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The migrations this binary carries, numbered once for every backend.
//!
//! The list below is the only place an id is added: it generates the enum,
//! [`MigrationId::ALL`], the names and the descriptions, and
//! [`MigrationId::LATEST`] is the last entry of `ALL`. Every backend's table
//! then fails to compile until it has an arm for the new id.

macro_rules! migration_ids {
    ($(
        $(#[$meta:meta])*
        $variant:ident = $number:literal, $name:literal, $description:literal;
    )+) => {
        /// A migration's number, the same on every backend. The discriminant
        /// is the id written to every ledger and never changes once released.
        #[repr(u32)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum MigrationId {
            $( $(#[$meta])* $variant = $number, )+
        }

        impl MigrationId {
            /// Every id this binary carries, in order.
            pub const ALL: [Self; [$($number),+].len()] = [$(Self::$variant),+];

            /// The name recorded beside the number, and used in file names.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self {
                    $( Self::$variant => $name, )+
                }
            }

            /// One line for `migrate --list`.
            #[must_use]
            pub const fn description(self) -> &'static str {
                match self {
                    $( Self::$variant => $description, )+
                }
            }
        }
    };
}

migration_ids! {
    /// Creates the v1.0 schema on an empty store and brings every released
    /// catalog to it.
    Baseline = 1, "baseline",
        "create the v1.0 catalog schema, or bring a released catalog to it";
    /// Rewrites stored policies from `dynamodb:ResourceTag/<key>` to
    /// `aws:ResourceTag/<key>`.
    ResourceTagConditionKeys = 2, "resource_tag_condition_keys",
        "rewrite dynamodb:ResourceTag condition keys in stored policies to aws:ResourceTag";
    /// Restores sort key attribute definitions that the pre-fix `UpdateTable`
    /// deleted (#259).
    SortKeyDefinitions = 3, "sort_key_definitions",
        "restore sort key attribute definitions an UpdateTable before the #259 fix deleted";
    /// Gives every index table its base key index.
    IndexTableBaseKey = 4, "index_table_base_key",
        "give every secondary index table its base key index";
}

/// Highest id that has shipped in a release. Bodies at or below it are frozen:
/// each backend pins their checksums, kinds and convergent flags, and
/// `forget` refuses them. The v1.0 release PR raises it to 4.
pub const RELEASED_THROUGH: u32 = 0;

impl MigrationId {
    /// The highest id this binary carries.
    pub const LATEST: Self = Self::ALL[Self::ALL.len() - 1];

    /// The number recorded in the ledgers.
    #[must_use]
    pub const fn number(self) -> u32 {
        self as u32
    }

    /// The id with this number, if this binary carries it.
    #[must_use]
    pub fn from_number(number: u32) -> Option<Self> {
        Self::ALL.into_iter().find(|id| id.number() == number)
    }
}

impl std::fmt::Display for MigrationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.number(), self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::{MigrationId, RELEASED_THROUGH};

    #[test]
    fn ids_are_numbered_from_one_without_gaps() {
        for (i, id) in MigrationId::ALL.iter().enumerate() {
            assert_eq!(id.number() as usize, i + 1, "{id:?}");
            assert_eq!(MigrationId::from_number(id.number()), Some(*id));
        }
        assert_eq!(MigrationId::ALL.last(), Some(&MigrationId::LATEST));
        assert_eq!(MigrationId::from_number(0), None);
        assert_eq!(
            MigrationId::from_number(MigrationId::LATEST.number() + 1),
            None
        );
    }

    #[test]
    fn names_are_unique_snake_case() {
        let mut names: Vec<&str> = MigrationId::ALL.iter().map(|id| id.name()).collect();
        for name in &names {
            assert!(
                name.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "{name}"
            );
        }
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), MigrationId::ALL.len());
    }

    #[test]
    fn the_released_bound_is_within_the_list() {
        assert!(RELEASED_THROUGH == 0 || MigrationId::from_number(RELEASED_THROUGH).is_some());
    }
}
