// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Write-path validation for vector-valued and search-schema attributes.
//!
//! When a table has a vector index, an item written to that table must carry a
//! well-formed vector attribute (a list of 32-bit floats of the declared
//! dimension) when present, and any search-schema attribute it carries must
//! match the declared scalar type and stay within the size limits. Missing
//! attributes are allowed: the write simply is not indexed for that field.
//!
//! Pure synchronous Rust: no async, no I/O.

use crate::error::DynamoDbError;
use crate::types::{
    AttributeDefinition, AttributeValue, Item, ScalarAttributeType, SearchSchemaElementType,
    VectorIndexKeyInfo, attribute_value_size,
};

/// Maximum byte size of a search-schema partition-key attribute value.
pub const MAX_HASH_KEY_SIZE: usize = 2048;
/// Maximum byte size of a search-schema inline-filter attribute value.
pub const MAX_INLINE_FILTER_SIZE: usize = 10240;

fn invalid(msg: impl Into<String>) -> DynamoDbError {
    DynamoDbError::ValidationException(msg.into())
}

/// Validate the vector-relevant attributes of an item being written against the
/// table's vector indexes.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when a present vector attribute
/// is not a list of the declared dimension of 32-bit floats, or when a present
/// search-schema attribute has the wrong type or exceeds its size limit.
pub fn validate_vector_write(
    item: &Item,
    vector_indexes: &[VectorIndexKeyInfo],
    attribute_definitions: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    static EMPTY: std::sync::LazyLock<Item> = std::sync::LazyLock::new(Item::new);
    validate_vector_write_changed(item, &EMPTY, vector_indexes, attribute_definitions)
}

/// Validate only the vector-relevant attributes whose value CHANGED relative
/// to `before`.
///
/// This is the UpdateItem semantics, measured live 2026-08-14 (probe table
/// vixdelta-1786706774): the service does not re-reject a pre-existing
/// invalid value when an unrelated attribute is updated; it validates what
/// the write changes. Whole-image validation here would make items the
/// backfill deliberately skipped (poison rows) permanently un-updatable,
/// which the service does not do. The changed-only rule for the vector
/// attribute itself follows the same measured principle; the search-schema
/// half is the one the probe exercised directly.
///
/// Put paths validate the full image by passing an empty `before` (a put
/// writes every value), which is what [`validate_vector_write`] does.
///
/// # Errors
/// Returns [`DynamoDbError::ValidationException`] for a changed value that is
/// not a valid vector, exceeds declared dimensions or size limits, or does
/// not match the declared search-schema attribute type.
pub fn validate_vector_write_changed(
    item: &Item,
    before: &Item,
    vector_indexes: &[VectorIndexKeyInfo],
    attribute_definitions: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    for index in vector_indexes {
        if let Some(value) = item.get(&index.vector_attribute_name)
            && before.get(&index.vector_attribute_name) != Some(value)
        {
            validate_vector_attribute(value, index)?;
        }
        for element in &index.search_schema {
            if let Some(value) = item.get(&element.attribute_name)
                && before.get(&element.attribute_name) != Some(value)
            {
                validate_search_schema_attribute(
                    &element.attribute_name,
                    element.element_type,
                    value,
                    attribute_definitions,
                    &index.index_name,
                )?;
            }
        }
    }
    Ok(())
}

/// Extract the components of a vector attribute as `f32`s.
///
/// Lives beside [`validate_vector_write`] deliberately: the two must agree on what
/// a vector attribute is, and separating them invites a backend that stores
/// something the validator would have rejected. Every backend indexing a vector
/// should use this rather than reading the attribute itself.
///
/// Returns `None` when the value is not a list of numbers each parsing to a finite
/// `f32`, which is exactly the condition `validate_vector_write` rejects. A caller
/// that has already validated can treat `None` as an internal inconsistency rather
/// than as bad input.
///
/// The narrowing to `f32` is deliberate and lossy for a caller supplying more
/// precision: the wire type is arbitrary-precision decimal, embedding models emit
/// single precision, and the service's declared dimensionality is in `f32` terms.
#[must_use]
pub fn vector_components(value: &AttributeValue) -> Option<Vec<f32>> {
    let AttributeValue::L(elements) = value else {
        return None;
    };
    let mut out = Vec::with_capacity(elements.len());
    for element in elements {
        let AttributeValue::N(number) = element else {
            return None;
        };
        let parsed = number.parse::<f32>().ok()?;
        if !parsed.is_finite() {
            return None;
        }
        out.push(parsed);
    }
    Some(out)
}

/// Rebuild a vector attribute from the `f32` components a backend stored.
///
/// The inverse of [`vector_components`], and the only way a backend should return a
/// stored vector. An index holds 32-bit floats, which is the width the service
/// validates against: it rejects a component outside
/// `[-3.4028235E38, 3.4028235E38]`, exactly `f32::MAX`, and names the expected type
/// "32-bit floating point number". So a client that writes more precision than an
/// `f32` carries reads back the narrowed value rather than the decimal it sent, and
/// reconstructing from the stored bits is what reproduces that. Retaining the
/// client's original text would be a divergence dressed up as fidelity.
///
/// Living here rather than in a backend keeps every backend returning identical
/// text for identical stored bits.
#[must_use]
pub fn vector_attribute(components: &[f32]) -> AttributeValue {
    AttributeValue::L(
        components
            .iter()
            .map(|component| AttributeValue::N(format_component(*component)))
            .collect(),
    )
}

/// Canonical `N` text for one stored component.
///
/// Plain decimal, always, which is `f32`'s `Display`. Measured against the live
/// service on 2026-08-07: DynamoDB never returns an exponent, whatever it was sent.
/// `3.4028235E+38` reads back as `340282350000000000000000000000000000000` and
/// `1E-40` as `0.0000000000000000000000000000000000000001`, both of which are
/// exactly what `Display` produces for the same `f32`.
///
/// This corrects an earlier reading of the 38-digit limit as a limit on characters.
/// It bounds *significant* digits, and an `f32`'s shortest round-tripping form
/// carries at most 9 of them, so no stored component can approach it however long
/// the expansion runs. A 38-significant-digit value was accepted and returned
/// verbatim in the same probe.
///
/// The one departure from `Display` is negative zero, which the service also
/// normalises: `-0` reads back as `0`.
fn format_component(value: f32) -> String {
    if value == 0.0 {
        // Covers -0.0, which compares equal to 0.0.
        return "0".to_owned();
    }
    format!("{value}")
}

/// The L2 norm of a vector, precomputed at write time.
///
/// Stored alongside the vector so a cosine search costs one dot product per
/// candidate instead of also summing squares. Computed here rather than in a
/// backend so every backend stores the same value for the same vector.
#[must_use]
pub fn vector_norm(components: &[f32]) -> f32 {
    components.iter().map(|x| x * x).sum::<f32>().sqrt()
}

/// Validate a single vector-valued attribute against an index definition.
fn validate_vector_attribute(
    value: &AttributeValue,
    index: &VectorIndexKeyInfo,
) -> Result<(), DynamoDbError> {
    let attr = &index.vector_attribute_name;
    let index_name = &index.index_name;
    let dimensions = index.dimensions as usize;

    let AttributeValue::L(elements) = value else {
        // Measured 2026-08-07 against N, S and NS in the vector position, all three
        // of which produce this exact text. Note "32-bit floating point number
        // list" rather than any phrasing of "list of numbers", and no full stop
        // before IndexName, matching the size message below.
        return Err(invalid(format!(
            "One or more parameter values were invalid. Invalid type for parameter {attr}, \
             Expected: 32-bit floating point number list IndexName: {index_name}"
        )));
    };

    if elements.len() != dimensions {
        // Punctuation matches the service exactly, verified 2026-08-05:
        // "...were invalid. Invalid size for parameter emb, Expected: 4,
        //  Actual: 3 IndexName: vidx"
        // Note the full stop after "invalid", the comma after the parameter
        // name, and the absence of one after the actual count.
        return Err(invalid(format!(
            "One or more parameter values were invalid. Invalid size for parameter {attr}, \
             Expected: {dimensions}, Actual: {} IndexName: {index_name}",
            elements.len()
        )));
    }

    for (position, element) in elements.iter().enumerate() {
        match element {
            AttributeValue::N(number) => {
                // A component is in range when it parses to a finite 32-bit
                // float. Comparing decimal magnitude against f32::MAX would
                // wrongly reject the boundary value, whose shortest decimal
                // rounds just above the exact f32 maximum.
                let representable =
                    matches!(number.parse::<f32>(), Ok(parsed) if parsed.is_finite());
                if !representable {
                    let display = number
                        .parse::<f64>()
                        .map(format_scientific)
                        .unwrap_or_else(|_| number.clone());
                    return Err(invalid(format!(
                        "One or more parameter values were invalid. Invalid value for parameter \
                         {attr}[{position}], Value: {display} is outside valid range \
                         [-3.4028235E38, 3.4028235E38]. IndexName: {index_name}"
                    )));
                }
            }
            other => {
                return Err(invalid(format!(
                    "One or more parameter values were invalid. Invalid type for parameter \
                     {attr}[{position}], Expected: 32-bit floating point number, Actual: {}. \
                     IndexName: {index_name}",
                    attribute_type_token(other)
                )));
            }
        }
    }

    Ok(())
}

/// Validate a single search-schema attribute value (type then size).
fn validate_search_schema_attribute(
    name: &str,
    element_type: SearchSchemaElementType,
    value: &AttributeValue,
    attribute_definitions: &[AttributeDefinition],
    index_name: &str,
) -> Result<(), DynamoDbError> {
    // Measured live 2026-08-14 (probe table vixdelta-1786706774): a wrong-typed
    // value for a SearchSchema HASH or INLINE_FILTER attribute REJECTS the
    // write; sparse-skip applies to MISSING attributes only. Note the periods:
    // "invalid." not "invalid:", and the sentence-per-clause shape.
    if let Some(definition) = attribute_definitions
        .iter()
        .find(|definition| definition.attribute_name == name)
        && !value_matches_scalar_type(value, definition.attribute_type)
    {
        let expected = match definition.attribute_type {
            ScalarAttributeType::S => "S",
            ScalarAttributeType::N => "N",
            ScalarAttributeType::B => "B",
        };
        return Err(invalid(format!(
            "One or more parameter values were invalid. Attribute '{name}' type mismatch. \
             Expected: {expected}, Actual: {}. IndexName: {index_name}",
            attribute_type_token(value)
        )));
    }

    let size = attribute_value_size(value);
    match element_type {
        SearchSchemaElementType::Hash if size > MAX_HASH_KEY_SIZE => Err(invalid(format!(
            "One or more parameter values were invalid: Aggregate size for HASH key attributes \
             exceeds the maximum of {MAX_HASH_KEY_SIZE} bytes"
        ))),
        SearchSchemaElementType::InlineFilter if size > MAX_INLINE_FILTER_SIZE => {
            Err(invalid(format!(
                "One or more parameter values were invalid: Size limit exceeded for SearchSchema \
                 attribute '{name}': maximum {MAX_INLINE_FILTER_SIZE} bytes"
            )))
        }
        _ => Ok(()),
    }
}

/// Whether an attribute value matches a scalar key type (`S`, `N`, or `B`).
fn value_matches_scalar_type(value: &AttributeValue, scalar_type: ScalarAttributeType) -> bool {
    matches!(
        (value, scalar_type),
        (AttributeValue::S(_), ScalarAttributeType::S)
            | (AttributeValue::N(_), ScalarAttributeType::N)
            | (AttributeValue::B(_), ScalarAttributeType::B)
    )
}

/// DynamoDB type token for an attribute value (used in error messages).
fn attribute_type_token(value: &AttributeValue) -> &'static str {
    match value {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::M(_) => "M",
        AttributeValue::L(_) => "L",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
    }
}

/// Format a float in upper-case scientific notation with an explicit exponent
/// sign, e.g. `1.3E+40` or `-1.3E+40`.
fn format_scientific(value: f64) -> String {
    with_exponent_sign(format!("{value:E}"))
}

/// Rust omits the `+` on a positive exponent; the service includes it.
fn with_exponent_sign(formatted: String) -> String {
    if let Some(exponent_pos) = formatted.find('E') {
        let (mantissa, exponent) = formatted.split_at(exponent_pos);
        let digits = &exponent[1..];
        if digits.starts_with('-') || digits.starts_with('+') {
            formatted
        } else {
            format!("{mantissa}E+{digits}")
        }
    } else {
        formatted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SearchSchemaElement;

    fn index() -> VectorIndexKeyInfo {
        VectorIndexKeyInfo {
            index_name: "ProductIndex".to_owned(),
            dimensions: 5,
            vector_attribute_name: "ProductEmbedding".to_owned(),
            search_schema: vec![
                SearchSchemaElement {
                    attribute_name: "Country".to_owned(),
                    element_type: SearchSchemaElementType::Hash,
                },
                SearchSchemaElement {
                    attribute_name: "Category".to_owned(),
                    element_type: SearchSchemaElementType::InlineFilter,
                },
            ],
            projection: crate::types::Projection {
                projection_type: crate::types::ProjectionType::All,
                non_key_attributes: None,
            },
        }
    }

    fn defs() -> Vec<AttributeDefinition> {
        vec![
            AttributeDefinition {
                attribute_name: "Country".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "Category".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ]
    }

    fn num_vec(values: &[&str]) -> AttributeValue {
        AttributeValue::L(
            values
                .iter()
                .map(|v| AttributeValue::N((*v).to_owned()))
                .collect(),
        )
    }

    fn item_with_vector(value: AttributeValue) -> Item {
        let mut item = Item::new();
        item.insert("ProductId".to_owned(), AttributeValue::S("p1".to_owned()));
        item.insert("ProductEmbedding".to_owned(), value);
        item
    }

    fn err(item: &Item) -> String {
        match validate_vector_write(item, &[index()], &defs()).unwrap_err() {
            DynamoDbError::ValidationException(m) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn accepts_valid_vector() {
        let item = item_with_vector(num_vec(&["0.1", "0.2", "0.3", "0.4", "0.5"]));
        validate_vector_write(&item, &[index()], &defs()).unwrap();
    }

    #[test]
    fn missing_vector_and_schema_attributes_are_allowed() {
        let mut item = Item::new();
        item.insert("ProductId".to_owned(), AttributeValue::S("p1".to_owned()));
        validate_vector_write(&item, &[index()], &defs()).unwrap();
    }

    #[test]
    fn rejects_too_few_dimensions() {
        let message = err(&item_with_vector(num_vec(&["0.1", "0.2", "0.3"])));
        assert!(message.contains("Invalid size for parameter ProductEmbedding"));
        assert!(message.contains("Expected: 5, Actual: 3"));
        assert!(message.contains("IndexName: ProductIndex"));
    }

    /// Exact wording, not fragments.
    ///
    /// Verified against real DynamoDB on 2026-08-05: a `PutItem` carrying three
    /// values against a four-dimension index returned
    /// "One or more parameter values were invalid. Invalid size for parameter
    /// emb, Expected: 4, Actual: 3 IndexName: vidx".
    ///
    /// The punctuation is load-bearing: a full stop after "invalid", a comma
    /// after the parameter name, and no separator after the actual count. The
    /// fragment assertions above cannot catch a regression in any of those, so
    /// this asserts the whole string.
    #[test]
    fn dimension_mismatch_message_matches_the_service_exactly() {
        let message = err(&item_with_vector(num_vec(&["0.1", "0.2", "0.3"])));
        assert_eq!(
            message,
            "One or more parameter values were invalid. Invalid size for parameter \
             ProductEmbedding, Expected: 5, Actual: 3 IndexName: ProductIndex"
        );
    }

    #[test]
    fn rejects_empty_vector() {
        let message = err(&item_with_vector(AttributeValue::L(vec![])));
        assert!(message.contains("Expected: 5, Actual: 0"));
    }

    /// Asserted whole rather than by fragment. Every message below was measured
    /// against the live service on 2026-08-07, and the previous fragment assertions
    /// excluded the "One or more parameter values were invalid" prefix, which is
    /// precisely where three of the four had drifted: two used a colon where the
    /// service uses a full stop, and one omitted the prefix entirely.
    #[test]
    fn rejects_non_list_vector() {
        let message = err(&item_with_vector(AttributeValue::N("0.1".to_owned())));
        assert_eq!(
            message,
            "One or more parameter values were invalid. Invalid type for parameter \
             ProductEmbedding, Expected: 32-bit floating point number list IndexName: ProductIndex"
        );
    }

    #[test]
    fn rejects_string_element() {
        let value = AttributeValue::L(vec![
            AttributeValue::N("0.1".to_owned()),
            AttributeValue::N("0.2".to_owned()),
            AttributeValue::S("x".to_owned()),
            AttributeValue::N("0.4".to_owned()),
            AttributeValue::N("0.5".to_owned()),
        ]);
        let message = err(&item_with_vector(value));
        assert_eq!(
            message,
            "One or more parameter values were invalid. Invalid type for parameter \
             ProductEmbedding[2], Expected: 32-bit floating point number, Actual: S. \
             IndexName: ProductIndex"
        );
    }

    /// The offending value is echoed in scientific notation whatever form it was
    /// sent in: the service answered `Value: 4E+38` to a plain 39-digit integer.
    #[test]
    fn rejects_value_out_of_range() {
        let value = AttributeValue::L(vec![
            AttributeValue::N("0.1".to_owned()),
            AttributeValue::N("1.3E40".to_owned()),
            AttributeValue::N("0.3".to_owned()),
            AttributeValue::N("0.4".to_owned()),
            AttributeValue::N("0.5".to_owned()),
        ]);
        let message = err(&item_with_vector(value));
        assert_eq!(
            message,
            "One or more parameter values were invalid. Invalid value for parameter \
             ProductEmbedding[1], Value: 1.3E+40 is outside valid range \
             [-3.4028235E38, 3.4028235E38]. IndexName: ProductIndex"
        );
    }

    #[test]
    fn rejects_negative_value_out_of_range() {
        let value = AttributeValue::L(vec![
            AttributeValue::N("0.1".to_owned()),
            AttributeValue::N("-1.3E40".to_owned()),
            AttributeValue::N("0.3".to_owned()),
            AttributeValue::N("0.4".to_owned()),
            AttributeValue::N("0.5".to_owned()),
        ]);
        assert!(err(&item_with_vector(value)).contains("Value: -1.3E+40 is outside"));
    }

    #[test]
    fn rejects_partition_key_type_mismatch() {
        let mut item = item_with_vector(num_vec(&["0.1", "0.2", "0.3", "0.4", "0.5"]));
        item.insert("Country".to_owned(), AttributeValue::N("123".to_owned()));
        assert!(err(&item).contains("type mismatch"));
    }

    #[test]
    fn rejects_partition_key_too_large() {
        let mut item = item_with_vector(num_vec(&["0.1", "0.2", "0.3", "0.4", "0.5"]));
        item.insert(
            "Country".to_owned(),
            AttributeValue::S("A".repeat(MAX_HASH_KEY_SIZE + 1)),
        );
        assert!(err(&item).contains("Aggregate size for HASH key attributes"));
    }

    #[test]
    fn rejects_inline_filter_too_large() {
        let mut item = item_with_vector(num_vec(&["0.1", "0.2", "0.3", "0.4", "0.5"]));
        item.insert(
            "Category".to_owned(),
            AttributeValue::S("B".repeat(MAX_INLINE_FILTER_SIZE + 1)),
        );
        assert!(err(&item).contains("Size limit exceeded for SearchSchema"));
    }

    #[test]
    fn accepts_f32_max_boundary() {
        // The shortest decimal for f32::MAX rounds just above the exact value;
        // it must still be accepted as representable.
        let item = item_with_vector(num_vec(&["0.1", "0.2", "0.3", "0.4", "3.4028235E38"]));
        validate_vector_write(&item, &[index()], &defs()).unwrap();
    }

    #[test]
    fn format_scientific_adds_exponent_sign() {
        assert_eq!(format_scientific(1.3e40), "1.3E+40");
        assert_eq!(format_scientific(-1.3e40), "-1.3E+40");
    }

    /// The invariant that makes reconstruction safe: whatever text is produced for a
    /// stored component must parse back to the identical bits. If this fails, a
    /// search returns a vector that is not the one indexed.
    ///
    /// Negative zero is the one deliberate exception and is asserted separately.
    #[test]
    fn rebuilding_a_vector_round_trips_the_stored_bits() {
        let components = [
            0.0f32,
            1.0,
            -1.0,
            0.1,
            0.123_456_79,
            f32::MAX,
            f32::MIN,
            f32::MIN_POSITIVE,
            1e-40, // subnormal
            3.4e38,
            -1.5e-30,
        ];
        let rebuilt = vector_attribute(&components);
        let parsed = vector_components(&rebuilt).expect("rebuilt vector must revalidate");
        assert_eq!(parsed.len(), components.len());
        for (original, back) in components.iter().zip(parsed.iter()) {
            assert_eq!(
                original.to_bits(),
                back.to_bits(),
                "component {original} did not round-trip"
            );
        }
    }

    /// `N` carries one zero, so a stored `-0.0` returns as `0`. The sign of zero is
    /// unobservable through any of the three distance functions, so normalising it
    /// cannot change a score or an ordering.
    #[test]
    fn negative_zero_is_normalised() {
        assert_eq!(format_component(-0.0), "0");
        assert_eq!(format_component(0.0), "0");
        let parsed = vector_components(&vector_attribute(&[-0.0f32])).expect("valid");
        assert_eq!(parsed[0].to_bits(), 0.0f32.to_bits());
    }

    /// Ordinary embedding magnitudes must not be dressed up in exponents: the plain
    /// form is what a client wrote and what it expects to read.
    #[test]
    fn ordinary_magnitudes_stay_in_plain_decimal() {
        assert_eq!(format_component(0.5), "0.5");
        assert_eq!(format_component(-1.25), "-1.25");
        assert_eq!(format_component(1.0), "1");
        assert_eq!(format_component(0.000_123), "0.000123");
    }

    /// `Display` never uses an exponent, and neither does the service. These exact
    /// strings were read back from real DynamoDB on 2026-08-07 after sending
    /// `3.4028235E+38` and `1E-40`, so matching them is measured parity rather than
    /// a formatting preference.
    #[test]
    fn extreme_magnitudes_match_the_plain_form_the_service_returns() {
        assert_eq!(
            format_component(f32::MAX),
            "340282350000000000000000000000000000000"
        );
        assert_eq!(
            format_component(1e-40),
            "0.0000000000000000000000000000000000000001"
        );
        assert!(
            !format_component(f32::MAX).contains('E'),
            "an exponent is never returned by the service"
        );
    }

    /// A client writing more precision than an `f32` carries reads back the narrowed
    /// value, which is what the service does, rather than its own decimal string.
    #[test]
    fn excess_client_precision_is_narrowed_not_preserved() {
        let written =
            AttributeValue::L(vec![AttributeValue::N("0.12345678901234567890".to_owned())]);
        let stored = vector_components(&written).expect("valid vector");
        let returned = vector_attribute(&stored);
        assert_eq!(
            returned,
            AttributeValue::L(vec![AttributeValue::N("0.12345679".to_owned())])
        );
    }

    /// The extractor must accept exactly what the validator accepts. If these
    /// drift, a backend stores a vector the validator would have rejected, or
    /// refuses one it accepted.
    #[test]
    fn the_extractor_accepts_what_the_validator_accepts() {
        let good = AttributeValue::L(vec![
            AttributeValue::N("0.5".to_owned()),
            AttributeValue::N("-1".to_owned()),
            AttributeValue::N("0".to_owned()),
        ]);
        assert_eq!(vector_components(&good), Some(vec![0.5, -1.0, 0.0]));
    }

    #[test]
    fn the_extractor_rejects_a_non_list() {
        assert!(vector_components(&AttributeValue::S("nope".to_owned())).is_none());
    }

    #[test]
    fn the_extractor_rejects_a_non_numeric_element() {
        let bad = AttributeValue::L(vec![AttributeValue::S("1".to_owned())]);
        assert!(vector_components(&bad).is_none());
    }

    /// A component that overflows f32 is rejected rather than becoming infinity,
    /// which would poison every distance computed against it.
    #[test]
    fn the_extractor_rejects_a_component_that_is_not_finite_in_f32() {
        let bad = AttributeValue::L(vec![AttributeValue::N("1e40".to_owned())]);
        assert!(vector_components(&bad).is_none());
    }

    #[test]
    fn the_norm_is_the_euclidean_length() {
        assert!((vector_norm(&[3.0, 4.0]) - 5.0).abs() < 1e-6);
        assert!(vector_norm(&[0.0, 0.0]).abs() < 1e-6);
    }
}
