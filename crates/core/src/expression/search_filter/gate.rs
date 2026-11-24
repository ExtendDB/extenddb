// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which accounts get the extended grammar.
//!
//! The service serves the grammar to every account. The setting names the
//! accounts ExtendDB serves it to: `all` (the default), `off`, or a
//! comma-separated list of 12-digit account IDs, in which case a request
//! gets the grammar when the caller or the table owner is listed.

use std::collections::BTreeSet;

/// The parsed value of the setting.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum EnabledAccounts {
    /// No account.
    Off,
    /// Every account. The default, and the value when the setting is absent
    /// or empty.
    #[default]
    All,
    /// The listed accounts.
    Listed(BTreeSet<String>),
}

impl EnabledAccounts {
    /// Parse a setting value.
    ///
    /// # Errors
    ///
    /// Returns the reason the value is refused, for the settings validator.
    pub fn parse(value: &str) -> Result<Self, &'static str> {
        match value.trim() {
            "off" => return Ok(Self::Off),
            "all" | "" => return Ok(Self::All),
            _ => {}
        }
        let mut accounts = BTreeSet::new();
        for entry in value.split(',') {
            let account = entry.trim();
            if account.len() != 12 || !account.bytes().all(|b| b.is_ascii_digit()) {
                return Err(
                    "must be 'off', 'all', or a comma-separated list of 12-digit account IDs",
                );
            }
            accounts.insert(account.to_owned());
        }
        Ok(Self::Listed(accounts))
    }

    /// Whether a request by `caller` on a table owned by `owner` gets the
    /// grammar. `owner` is `None` when the caller owns the table.
    #[must_use]
    pub fn enabled_for(&self, caller: &str, owner: Option<&str>) -> bool {
        match self {
            Self::Off => false,
            Self::All => true,
            Self::Listed(accounts) => std::iter::once(caller)
                .chain(owner)
                .any(|account| accounts.contains(account)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EnabledAccounts;

    #[test]
    fn every_account_is_the_default() {
        assert_eq!(EnabledAccounts::default(), EnabledAccounts::All);
        for empty in ["", " ", "\t"] {
            assert_eq!(
                EnabledAccounts::parse(empty),
                Ok(EnabledAccounts::All),
                "{empty:?}"
            );
        }
    }

    #[test]
    fn off_and_all_parse() {
        assert_eq!(EnabledAccounts::parse("off"), Ok(EnabledAccounts::Off));
        assert_eq!(EnabledAccounts::parse("all"), Ok(EnabledAccounts::All));
        assert!(!EnabledAccounts::Off.enabled_for("123456789012", None));
        assert!(EnabledAccounts::All.enabled_for("123456789012", None));
    }

    #[test]
    fn a_list_enables_the_caller_or_the_owner() {
        let listed = EnabledAccounts::parse("111111111111, 222222222222").unwrap();
        assert!(listed.enabled_for("111111111111", None));
        assert!(listed.enabled_for("333333333333", Some("222222222222")));
        assert!(!listed.enabled_for("333333333333", None));
        assert!(!listed.enabled_for("333333333333", Some("444444444444")));
    }

    #[test]
    fn a_malformed_value_is_refused() {
        for bad in [
            "on",
            "true",
            "12345",
            "111111111111,",
            "11111111111a",
            "ALL",
        ] {
            assert!(EnabledAccounts::parse(bad).is_err(), "{bad:?}");
        }
    }
}
