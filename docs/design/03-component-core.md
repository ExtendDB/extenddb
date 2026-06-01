# extenddb — Component Design: Core

**Version:** 1.0
**Date:** 2026-04-03
**Status:** Draft
**Crate:** `dynamodb-core`

## 1. Purpose

The `core` crate contains all DynamoDB-specific business logic that is independent of the storage backend, HTTP server, and async runtime. It is pure synchronous Rust — types, expression parsing/evaluation, validation, capacity calculation, and error types.

**Dependencies:** serde, serde_json, thiserror, time, uuid, base64, bigdecimal (no tokio, no sqlx, no axum, no async runtime)

> **Note:** Operation handlers (PutItem, GetItem, Query, etc.) live in the `engine` crate, not here. The `engine` crate is async and depends on both `core` and `storage`. This keeps `core` free of async runtime dependencies and testable without a database.

## 2. Module Structure

```
crates/core/src/
├── lib.rs
├── types/
│   ├── mod.rs
│   ├── attribute_value.rs    # AttributeValue enum and serialization
│   ├── key_schema.rs         # KeySchemaElement, KeyType
│   ├── table.rs              # TableMetadata, TableStatus, BillingMode
│   ├── index.rs              # GSI/LSI metadata, Projection types
│   ├── stream.rs             # StreamSpecification, StreamViewType
│   └── consumed_capacity.rs  # ConsumedCapacity, Capacity
├── expression/
│   ├── mod.rs
│   ├── tokenizer.rs          # Lexer: string → tokens
│   ├── parser.rs             # Condition/filter/key condition parser
│   ├── update_parser.rs      # SET/REMOVE/ADD/DELETE parser
│   ├── projection_parser.rs  # Projection expression parser
│   ├── ast.rs                # Expression AST types
│   ├── evaluator.rs          # Condition/filter evaluation against an item
│   ├── update_evaluator.rs   # Apply update expression to an item
│   ├── projection.rs         # Apply projection to an item
│   ├── resolver.rs           # #name and :value resolution
│   └── path.rs               # Nested document path parsing and traversal
├── validation/
│   ├── mod.rs
│   ├── table_name.rs         # 3-255 chars, [a-zA-Z0-9_.-]
│   ├── attribute.rs          # Attribute name length, key size
│   ├── item.rs               # Item size calculation and validation
│   ├── expression.rs         # Expression length, placeholder count
│   └── request.rs            # Per-operation input validation
├── capacity/
│   ├── mod.rs
│   └── calculator.rs         # Item size → RCU/WCU (pure math, no state)
├── error/
│   ├── mod.rs                # DynamoDbError enum
│   └── messages.rs           # Error message constants
└── limits/
    └── mod.rs                # LimitsConfig with all configurable limits
```

## 3. Type System

### 3.1 AttributeValue

The central type. Must serialize/deserialize to exactly match the DynamoDB JSON wire format.

```rust
/// DynamoDB attribute value — the fundamental data type.
///
/// Each variant maps to a DynamoDB type descriptor. Custom Serialize/Deserialize
/// impls produce the DynamoDB JSON wire format: `{"S": "hello"}`, `{"N": "42"}`,
/// `{"M": {"key": {"S": "val"}}}`.
///
/// Using an enum (rather than a flat struct with Option fields) gives us:
/// - Exhaustive match: the compiler catches missing cases in every consumer
/// - Impossible states are unrepresentable: exactly one type is set by construction
/// - Cleaner downstream code: no `if let Some(s) = &av.s` chains
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    S(String),
    N(String),                              // arbitrary precision, up to 38 digits
    B(Vec<u8>),                             // raw bytes; base64-encoded on wire
    SS(BTreeSet<String>),
    NS(BTreeSet<String>),
    BS(BTreeSet<Vec<u8>>),
    Bool(bool),
    Null,
    L(Vec<AttributeValue>),
    M(BTreeMap<String, AttributeValue>),
}
```

Custom serde implementation (~50 lines) produces the exact DynamoDB wire format:

```rust
impl Serialize for AttributeValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Self::S(v) => map.serialize_entry("S", v)?,
            Self::N(v) => map.serialize_entry("N", v)?,
            Self::B(v) => map.serialize_entry("B", &BASE64.encode(v))?,
            Self::SS(v) => map.serialize_entry("SS", v)?,
            Self::NS(v) => map.serialize_entry("NS", v)?,
            Self::BS(v) => {
                let encoded: Vec<String> = v.iter().map(|b| BASE64.encode(b)).collect();
                map.serialize_entry("BS", &encoded)?;
            }
            Self::Bool(v) => map.serialize_entry("BOOL", v)?,
            Self::Null => map.serialize_entry("NULL", &true)?,
            Self::L(v) => map.serialize_entry("L", v)?,
            Self::M(v) => map.serialize_entry("M", v)?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for AttributeValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Deserialize as a single-entry map, match on the key ("S", "N", etc.)
        // to construct the correct variant. Reject if zero or multiple keys are set
        // (REQ-TYPE-001: exactly one type descriptor per value).
    }
}
```

**Design notes:**
- `BTreeMap` for Map type and `BTreeSet` for Set types ensure deterministic serialization order (important for CRC32 checksums and test reproducibility).
- Number stored as `String` to preserve arbitrary precision (up to 38 digits). Numeric operations in expressions use `bigdecimal::BigDecimal` for comparison and arithmetic, which supports DynamoDB's full 38-digit precision requirement.
- Binary values stored as `Vec<u8>` internally; base64 encoding/decoding happens only at the serde boundary.
- The `Null` variant carries no data — DynamoDB's `{"NULL": true}` is the only valid form, enforced by the Deserialize impl. `{"NULL": false}` must be rejected with `SerializationException`.

### 3.2 Key Schema Types

```rust
pub struct KeySchemaElement {
    pub attribute_name: String,
    pub key_type: KeyType,  // HASH or RANGE
}

pub enum KeyType { Hash, Range }

pub struct AttributeDefinition {
    pub attribute_name: String,
    pub attribute_type: ScalarAttributeType,  // S, N, or B
}

pub enum ScalarAttributeType { S, N, B }
```

### 3.3 Table Metadata

```rust
pub struct TableMetadata {
    pub table_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub billing_mode: BillingMode,
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    pub global_secondary_indexes: Vec<GsiMetadata>,
    pub local_secondary_indexes: Vec<LsiMetadata>,
    pub stream_specification: Option<StreamSpecification>,
    pub table_status: TableStatus,
    pub creation_date_time: OffsetDateTime,
    pub table_size_bytes: i64,
    pub item_count: i64,
    pub table_arn: String,
    pub table_id: String,
    pub ttl_attribute: Option<String>,
    pub tags: Vec<Tag>,
}

pub enum BillingMode { Provisioned, PayPerRequest }
pub enum TableStatus { Creating, Active, Deleting, Updating }
```

## 4. Expression Engine

### 4.1 Architecture

```
Input string → Tokenizer → Token stream → Parser → AST → Evaluator → Result
```

The expression engine is split into distinct phases for clarity and testability.

### 4.2 Tokenizer

Converts expression strings into a stream of typed tokens.

```rust
pub enum Token {
    Identifier(String),       // attribute name
    Placeholder(String),      // :value
    NameRef(String),          // #name
    Number(String),           // numeric literal
    StringLiteral(String),    // 'string'
    // Operators
    Eq, Ne, Lt, Le, Gt, Ge,
    Plus, Minus,
    Comma, Dot, LBracket, RBracket, LParen, RParen,
    // Keywords (case-insensitive)
    And, Or, Not, Between, In,
    Set, Remove, Add, Delete,  // update expression keywords
}
```

### 4.3 AST

```rust
pub enum Expr {
    /// Attribute path: `address.city`, `tags[0].name`, `#n`
    Path(Vec<PathElement>),
    /// Literal or placeholder value
    Value(AttributeValue),
    /// Placeholder reference: `:val1`
    Placeholder(String),
    /// Binary comparison: `price > :min`
    Compare { left: Box<Expr>, op: CompareOp, right: Box<Expr> },
    /// Logical: `cond1 AND cond2`
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    /// BETWEEN: `price BETWEEN :lo AND :hi`
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr> },
    /// IN: `status IN (:a, :b, :c)`
    In { expr: Box<Expr>, values: Vec<Expr> },
    /// Function call: `begins_with(#name, :prefix)`
    Function { name: String, args: Vec<Expr> },
    /// Arithmetic: `price + :tax` (used in SET expressions)
    Arithmetic { left: Box<Expr>, op: ArithOp, right: Box<Expr> },
}

pub enum PathElement {
    Attribute(String),  // `address`, `#name`
    Index(usize),       // `[0]`
}

pub enum CompareOp { Eq, Ne, Lt, Le, Gt, Ge }
pub enum ArithOp { Add, Sub }
```

### 4.4 Supported Functions

| Function | Signature | Used In |
|----------|-----------|---------|
| `attribute_exists(path)` | path → bool | Condition, Filter |
| `attribute_not_exists(path)` | path → bool | Condition, Filter |
| `attribute_type(path, type)` | path × string → bool | Condition, Filter |
| `begins_with(path, substr)` | path × string → bool | Condition, Filter, KeyCondition |
| `contains(path, operand)` | path × value → bool | Condition, Filter |
| `size(path)` | path → number | Condition, Filter |
| `if_not_exists(path, value)` | path × value → value | Update (SET) |
| `list_append(list1, list2)` | list × list → list | Update (SET) |

### 4.5 Update Expression Actions

```rust
pub enum UpdateAction {
    Set { path: Vec<PathElement>, value: Expr },
    Remove { path: Vec<PathElement> },
    Add { path: Vec<PathElement>, value: Expr },
    Delete { path: Vec<PathElement>, value: Expr },
}
```

- **SET**: Assign a value to an attribute. Supports arithmetic (`SET price = price + :inc`) and functions (`SET attr = if_not_exists(attr, :default)`).
- **REMOVE**: Remove an attribute or list element.
- **ADD**: Add a number to a numeric attribute, or add elements to a set.
- **DELETE**: Remove elements from a set.

### 4.6 Expression Evaluation

The evaluator takes an AST and an `EvalContext` (the current item + attribute name/value maps) and returns a boolean (for conditions/filters) or a modified item (for updates).

> **Important:** `ConditionExpression` evaluation for write operations (PutItem, UpdateItem, DeleteItem) must happen inside the storage backend's transaction, not in the core handler after a round-trip. The core crate provides the `evaluate_condition` function, but the storage backend calls it within its transaction after `SELECT FOR UPDATE` to prevent TOCTOU races. `FilterExpression` evaluation (for Query/Scan) happens in the core handler after items are returned from storage — this is safe because filters are read-only.

```rust
pub struct EvalContext<'a> {
    pub item: &'a BTreeMap<String, AttributeValue>,
    pub names: &'a HashMap<String, String>,
    pub values: &'a HashMap<String, AttributeValue>,
}

/// Evaluate a condition/filter expression against an item.
pub fn evaluate_condition(expr: &Expr, ctx: &EvalContext) -> Result<bool, DynamoDbError>;

/// Apply an update expression to an item, returning the modified item.
pub fn apply_update(
    actions: &[UpdateAction],
    item: &mut BTreeMap<String, AttributeValue>,
    ctx: &EvalContext,
) -> Result<(), DynamoDbError>;

/// Apply a projection expression to an item, returning only selected attributes.
pub fn apply_projection(
    paths: &[Vec<PathElement>],
    item: &BTreeMap<String, AttributeValue>,
    ctx: &EvalContext,
) -> BTreeMap<String, AttributeValue>;
```

## 5. Operation Dispatch

Operation handlers (PutItem, GetItem, Query, etc.) live in the `engine` crate, not in `core`. This keeps `core` free of async runtime dependencies. See the engine crate documentation for the handler pattern and dispatch logic.

The `engine` crate depends on `core` (for types, expressions, validation) and `storage` (for the `StorageEngine` trait). Each handler follows this pattern:

1. Deserialize and validate input (using `core::validation`)
2. Parse expressions (using `core::expression`)
3. Call the storage engine (async)
4. Apply post-read processing (filter expressions, projections — using `core::expression`)
5. Calculate consumed capacity (using `core::capacity`)
6. Format the response

### 5.1 UpdateItem Transaction Flow

For `UpdateItem`, the storage backend must execute the following steps inside a single transaction to prevent TOCTOU races:

```
1. BEGIN transaction
2. SELECT FOR UPDATE existing item by primary key
3. If no existing item: create a new item containing only the key attributes (UpdateItem is an upsert)
4. If condition expression present:
   call core::expression::evaluate_condition(condition, item) → pass/fail
   If fail: ROLLBACK, return ConditionFailed { old_item }
5. Call core::expression::apply_update(actions, &mut item, ctx) → modified item
6. Validate modified item (size limits, key attributes unchanged)
7. INSERT/UPDATE the modified item
8. If GSIs exist: update backend-specific secondary-index state
9. If stream_capture provided: construct full StreamRecord (with old_image/new_image
   based on stream_view_type), INSERT stream record (within same transaction)
10. COMMIT
```

Both `evaluate_condition` and `apply_update` are sync functions from `core` — they operate on in-memory data and are called by the storage backend inside its transaction.

## 6. Validation

### 6.1 Table Name Validation

```rust
pub fn validate_table_name(name: &str, limits: &LimitsConfig) -> Result<(), DynamoDbError> {
    // Length: 3-255 (configurable)
    // Characters: [a-zA-Z0-9_.-]
    // Must not be empty
}
```

### 6.2 Item Size Calculation

Item size is calculated as the sum of all attribute (name size + value size) pairs. This must match DynamoDB's calculation exactly for accurate capacity unit computation.

**Attribute name size:** UTF-8 byte length.

**Attribute value size by type:**

| Type | Size Calculation |
|------|-----------------|
| `S` | UTF-8 byte length (empty string = 0 bytes, but minimum 1 byte for the attribute) |
| `N` | Number of significant digits (before + after decimal, excluding leading/trailing zeros) + 1 byte for sign + 1 byte for length. Minimum 2 bytes. |
| `B` | Raw byte length (not base64 length) |
| `SS` | Sum of UTF-8 byte lengths of all elements. No per-element overhead. |
| `NS` | Sum of numeric sizes (same formula as N) for all elements. |
| `BS` | Sum of raw byte lengths of all elements. |
| `BOOL` | 1 byte |
| `NULL` | 1 byte |
| `L` | Sum of (element value size + 1 byte overhead per element) for all elements. Empty list = 3 bytes overhead. |
| `M` | Sum of (key name UTF-8 byte length + value size + 3 bytes overhead per entry) for all entries. Empty map = 3 bytes overhead. |

**Total item size** = sum of (attribute name byte length + attribute value size) for all top-level attributes.

```rust
pub fn calculate_item_size(item: &BTreeMap<String, AttributeValue>) -> usize {
    item.iter()
        .map(|(name, value)| name.len() + calculate_attribute_size(value))
        .sum()
}

fn calculate_attribute_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => calculate_number_size(n),
        AttributeValue::B(b) => b.len(),
        AttributeValue::SS(set) => set.iter().map(|s| s.len()).sum(),
        AttributeValue::NS(set) => set.iter().map(|n| calculate_number_size(n)).sum(),
        AttributeValue::BS(set) => set.iter().map(|b| b.len()).sum(),
        AttributeValue::Bool(_) => 1,
        AttributeValue::Null => 1,
        AttributeValue::L(list) => {
            3 + list.iter().map(|v| calculate_attribute_size(v) + 1).sum::<usize>()
        }
        AttributeValue::M(map) => {
            3 + map.iter().map(|(k, v)| k.len() + calculate_attribute_size(v) + 3).sum::<usize>()
        }
    }
}
```

> **Note:** The exact overhead bytes per type must be validated against DynamoDB's actual behavior via integration tests. The values above are based on observed DynamoDB behavior but AWS does not publish the precise algorithm.

### 6.3 Key Validation

```rust
pub fn validate_key(
    item: &BTreeMap<String, AttributeValue>,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    // All key attributes present
    // Key attribute types match AttributeDefinitions
    // Partition key size ≤ 2048 bytes
    // Sort key size ≤ 1024 bytes
    // Key attributes are S, N, or B only
}
```

## 7. Capacity Calculation

### 7.1 Read Capacity Units

```
Strongly consistent read:  ceil(item_size / 4096) RCU
Eventually consistent read: ceil(item_size / 4096) * 0.5 RCU
Transactional read:         ceil(item_size / 4096) * 2 RCU
```

### 7.2 Write Capacity Units

```
Standard write:      ceil(item_size / 1024) WCU
Transactional write: ceil(item_size / 1024) * 2 WCU
```

### 7.3 Throughput Enforcement

Throughput enforcement is a runtime concern, not pure business logic. PostgreSQL can use server-side token buckets for single-frontend fidelity tests, while TiDB uses native Resource Control/resource groups for distributed enforcement. See the server component design for the backend-aware request path.

`core::capacity` only contains the pure math: `calculator.rs` (item size → RCU/WCU).

## 8. Error Types

```rust
/// All DynamoDB error types with HTTP status codes.
/// Uses a macro to define variants, status codes, type strings, and constructors.
#[derive(Debug, thiserror::Error)]
pub enum DynamoDbError {
    #[error("ValidationException: {0}")]
    ValidationException(String),           // 400
    #[error("ResourceNotFoundException: {0}")]
    ResourceNotFoundException(String),     // 400
    #[error("ConditionalCheckFailedException: {0}")]
    ConditionalCheckFailedException(String), // 400
    // ... all other variants from the error catalog in 01-requirements.md
}

impl DynamoDbError {
    /// HTTP status code for this error.
    pub fn status_code(&self) -> u16 { /* ... */ }

    /// Error type string for the `__type` field.
    pub fn error_type(&self) -> &str { /* ... */ }

    /// Serialize to DynamoDB error JSON format.
    pub fn to_json(&self) -> Value {
        json!({
            "__type": format!("com.amazonaws.dynamodb.v20120810#{}", self.error_type()),
            "message": self.to_string()
        })
    }
}
```

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
