// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Which accounts get the extended grammar.
//!
//! The service serves the grammar to every account. The setting names the
//! accounts ExtendDB serves it to: `all` (the default), `off`, or a
//! comma-separated list of 12-digit account IDs, in which case a request
//! gets the grammar when the caller is listed. A request on a table of
//! another account is refused before the grammar is chosen, so the caller
//! owns the table.

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
                return Err("must be empty, 'off', 'all', or a comma-separated list of \
                            12-digit account IDs");
            }
            accounts.insert(account.to_owned());
        }
        Ok(Self::Listed(accounts))
    }

    /// Whether a request by `caller` gets the grammar.
    #[must_use]
    pub fn enabled_for(&self, caller: &str) -> bool {
        match self {
            Self::Off => false,
            Self::All => true,
            Self::Listed(accounts) => accounts.contains(caller),
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
        assert!(!EnabledAccounts::Off.enabled_for("123456789012"));
        assert!(EnabledAccounts::All.enabled_for("123456789012"));
    }

    #[test]
    fn a_list_enables_the_accounts_it_names() {
        let listed = EnabledAccounts::parse("111111111111, 222222222222").unwrap();
        assert!(listed.enabled_for("111111111111"));
        assert!(listed.enabled_for("222222222222"));
        assert!(!listed.enabled_for("333333333333"));
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
