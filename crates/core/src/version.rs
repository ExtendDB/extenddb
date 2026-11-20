// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog version type with strict parsing and display.
//!
//! The `CatalogVersion` struct is the single source of truth for catalog
//! schema versions. The tuple `(major, minor, patch)` is authoritative;
//! the string representation is derived via `Display` (REQ-CAT-006, D-9).

use std::fmt;
use std::str::FromStr;

/// A semver-style catalog version: `major.minor.patch`.
///
/// Construct from a tuple via `CatalogVersion::new` or parse from a string
/// via `FromStr`. The string form is derived from the tuple — never
/// hand-maintained (D-9).
///
/// # Examples
///
/// ```
/// use extenddb_core::version::CatalogVersion;
///
/// let v = CatalogVersion::new(1, 2, 0);
/// assert_eq!(v.to_string(), "1.2.0");
///
/// let parsed: CatalogVersion = "1.2.0".parse().unwrap();
/// assert_eq!(parsed, v);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CatalogVersion {
    major: u32,
    minor: u32,
    patch: u32,
}

impl CatalogVersion {
    /// Create a new catalog version from components.
    #[must_use]
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl fmt::Display for CatalogVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Error returned when a catalog version string cannot be parsed.
///
/// Contains the raw input and a description of what went wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCatalogVersionError {
    input: String,
    reason: String,
}

impl fmt::Display for ParseCatalogVersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid catalog version '{}': {} (expected format: MAJOR.MINOR.PATCH)",
            self.input, self.reason
        )
    }
}

impl std::error::Error for ParseCatalogVersionError {}

impl FromStr for CatalogVersion {
    type Err = ParseCatalogVersionError;

    /// Parse a catalog version string with strict validation (D-10).
    ///
    /// Accepts exactly `MAJOR.MINOR.PATCH` where each component is a
    /// decimal `u32` with no leading zeros (except `"0"` itself),
    /// no whitespace, and no trailing characters.
    ///
    /// # Errors
    ///
    /// Returns `ParseCatalogVersionError` if the input is malformed.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = |reason: String| ParseCatalogVersionError {
            input: s.to_owned(),
            reason,
        };

        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return Err(err(format!(
                "expected 3 dot-separated components, found {}",
                parts.len()
            )));
        }

        let mut components = [0u32; 3];
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                return Err(err(format!("component {} is empty", i + 1)));
            }
            if part.len() > 1 && part.starts_with('0') {
                return Err(err(format!(
                    "component {} has leading zero: '{part}'",
                    i + 1
                )));
            }
            components[i] = part
                .parse::<u32>()
                .map_err(|_| err(format!("component {} is not a valid u32: '{part}'", i + 1)))?;
        }

        Ok(Self {
            major: components[0],
            minor: components[1],
            patch: components[2],
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn orders_by_component() {
        use super::CatalogVersion;
        assert!(CatalogVersion::new(0, 0, 3) < CatalogVersion::new(0, 0, 4));
        assert!(CatalogVersion::new(0, 0, 10) > CatalogVersion::new(0, 0, 9));
        assert!(CatalogVersion::new(0, 1, 0) > CatalogVersion::new(0, 0, 99));
        assert!(CatalogVersion::new(1, 0, 0) > CatalogVersion::new(0, 99, 99));
    }

    use super::*;

    #[test]
    fn display_roundtrip() {
        let v = CatalogVersion::new(1, 2, 3);
        assert_eq!(v.to_string(), "1.2.3");
        assert_eq!("1.2.3".parse::<CatalogVersion>().unwrap(), v);
    }

    #[test]
    fn parse_zero_components() {
        assert_eq!(
            "0.0.0".parse::<CatalogVersion>().unwrap(),
            CatalogVersion::new(0, 0, 0),
        );
    }

    #[test]
    fn reject_leading_zeros() {
        assert!("01.0.0".parse::<CatalogVersion>().is_err());
        assert!("0.01.0".parse::<CatalogVersion>().is_err());
        assert!("0.0.01".parse::<CatalogVersion>().is_err());
    }

    #[test]
    fn reject_wrong_component_count() {
        assert!("1.0".parse::<CatalogVersion>().is_err());
        assert!("1.0.0.0".parse::<CatalogVersion>().is_err());
        assert!("1".parse::<CatalogVersion>().is_err());
    }

    #[test]
    fn reject_whitespace() {
        assert!(" 1.0.0".parse::<CatalogVersion>().is_err());
        assert!("1.0.0 ".parse::<CatalogVersion>().is_err());
        assert!("1. 0.0".parse::<CatalogVersion>().is_err());
    }

    #[test]
    fn reject_empty_and_garbage() {
        assert!("".parse::<CatalogVersion>().is_err());
        assert!("abc".parse::<CatalogVersion>().is_err());
        assert!("1.0.x".parse::<CatalogVersion>().is_err());
    }

    #[test]
    fn reject_negative() {
        assert!("-1.0.0".parse::<CatalogVersion>().is_err());
    }

    #[test]
    fn error_message_includes_input() {
        let err = "garbage".parse::<CatalogVersion>().unwrap_err();
        assert!(err.to_string().contains("garbage"));
        assert!(err.to_string().contains("MAJOR.MINOR.PATCH"));
    }
}
