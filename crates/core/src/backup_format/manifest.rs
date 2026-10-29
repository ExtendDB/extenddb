// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `extenddb-manifest.json` types: the `ExtendDB` extension to the
//! `DynamoDB` export layout, and the commit record of a backup.
//!
//! The manifest is written last; its presence marks a complete backup. It
//! carries everything a restore needs that the `DynamoDB` export manifests do
//! not: the full source table definition, the snapshot descriptor, the item
//! order guarantee, and a sha256 per data file.
//!
//! Integrity of the manifest itself is a sidecar object,
//! `extenddb-manifest.sha256`, in `sha256sum` format: the lowercase hex
//! sha256 of the exact bytes of `extenddb-manifest.json`, two spaces, the
//! file name, newline. The writer computes it from the bytes it is about to
//! store and writes the sidecar before the manifest; a reader hashes the
//! bytes it fetched and compares, and an operator can run
//! `sha256sum -c extenddb-manifest.sha256`. The reader accepts that one line,
//! with or without its final newline, and refuses any other shape, a bare
//! digest included, which `sha256sum -c` refuses too
//! ([`verify_manifest_checksum`]). Hashing stored bytes avoids
//! defining a canonical JSON form: an earlier revision hashed a re-serialized
//! struct with the checksum field blanked, which tied the digest to one JSON
//! library's float formatting and field order and would have failed for any
//! field an older reader did not model.
//!
//! Optional members of [`SourceTableSchema`] are always serialized, as `null`
//! when absent, for human readers and non-Rust tools; index lists are plain
//! arrays, empty when the table has none. The index lists, `vector_indexes`,
//! `stream_specification` and `time_to_live` are required members: a
//! document without one is refused (no v1 writer omits them, and a missing
//! `vector_indexes` would read as "the source had no vector support" and
//! drop vector indexes silently). This build's reader accepts a missing
//! scalar optional member (`provisioned_throughput`, `on_demand_throughput`,
//! `table_class`, `sse_description`) as `null` (`#[serde(default)]`), so
//! "the writer did not know this field" is signalled by `format_version`,
//! not by absence. `vector_indexes` is the one member whose `null` carries
//! meaning, see its docs. `stream_specification` and `time_to_live` record
//! what `DescribeTable` and `DescribeTimeToLive` reported for the source;
//! `DescribeBackup` reports them as `SourceTableFeatureDetails` and a
//! restore does not reproduce them (the restored table has no stream and no
//! TTL, as on the service). Nested members reuse the `DynamoDB` wire types, as on the wire. Most
//! of their optional members are omitted when absent; `OnDemandThroughput`
//! is not, and writes `null` for an absent `MaxReadRequestUnits` or
//! `MaxWriteRequestUnits` (`{"MaxReadRequestUnits":100,
//! "MaxWriteRequestUnits":null}`), which is its wire serialization and part
//! of format version 1. The checked-in golden manifests under
//! `testdata/extenddb-manifest/` pin the exact bytes, and two tests pin each
//! type's members and each enum's values, so a change to one of those
//! shared types fails a test here (the testdata README says which test
//! catches which change).
//!
//! Casing: top-level and `source_table` member names are `snake_case`;
//! `item_order` values are `snake_case`; members of the nested wire types
//! and their enum values (`PROVISIONED`, `HASH`) keep the `DynamoDB` API
//! spelling. Frozen with format version 1.
//!
//! Grammar facts the field docs do not state, which another implementation
//! must reproduce: the document is UTF-8 JSON without a byte order mark.
//! `format_version` is a JSON integer in `u32` range; `backup_size_bytes`,
//! `source_table.item_count` and `source_table.table_size_bytes` are `i64`
//! (not negative; a data-file total of 2^63 or more is refused as a size
//! mismatch); each data file's `item_count`, `bytes` and
//! `uncompressed_bytes` are `u64`; a vector index's `Dimensions` is `u32`; a
//! JSON `-1` or `1.5` in any of them is a parse error. The two timestamps
//! are JSON numbers read as `f64`, so a JSON integer (`1757500000`) is
//! accepted where a writer omits the fraction. The backup id in `backup_arn`
//! is the 14-digit form `<14 digits of epoch milliseconds>-<8 lowercase
//! hex>` ([`super::format_backup_id`]); the 13-digit and bare ids earlier
//! releases issued, which [`super::is_backup_id`] accepts for API requests,
//! are refused here because they carry no instant to check
//! `backup_creation_date_time` against. Within a `key_range` endpoint an
//! `S` or `B` value is non-empty, a `B` value is standard base64 with `=`
//! padding in canonical form (`AQ==`, not `AQ` or `AR==`), and an `N` value
//! is a `DynamoDB` number in the normalized form [`super::key_order`]
//! states; a data file's `key_range` is `null` or absent on an unordered
//! manifest and an object on a key-ordered one. Member order within an
//! object does not matter to the reader; the writer emits the order the
//! struct declares, which the golden fixtures pin.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::FormatError;
use super::data_file::{DataFileDecoder, hex_encode};
use super::error::truncated;
use super::key_order::{CheckedKey, HeldKey, KeyOrder};
use super::layout::{
    BackupArn, DATA_PREFIX, EXTENDDB_MANIFEST_FILE, MANIFEST_CHECKSUM_FILE,
    backup_creation_date_time, backup_id_epoch_millis, is_region, lower_hex,
};
use super::strict_item::item_without_duplicates_raw;
use crate::types::{
    AttributeDefinition, BillingMode, GsiInput, Item, KeySchemaElement, KeyType, LsiInput,
    OnDemandThroughput, ProvisionedThroughput, SseDescription, StreamSpecification,
    TimeToLiveDescription, VectorIndexSpecification,
};

/// The backup format version this build reads and writes.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

/// How the source backend took its consistent snapshot.
///
/// `kind` names the mechanism (for example `postgres_repeatable_read`) and
/// `marker` records the backend-specific position, kept for diagnostics and
/// for a later change-log archive to anchor against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotDescriptor {
    /// The snapshot mechanism, named by the backend that took it; the
    /// backends in this tree write one of the `KIND_*` constants on this
    /// type. It is a free string on the wire: the reader does not validate
    /// it, so a later backend can name a new mechanism without a format
    /// change, and a restore must not branch on it (every kind is a
    /// consistent snapshot; the data files mean the same whatever the
    /// mechanism). It is recorded for diagnostics.
    pub kind: String,
    /// Backend-specific snapshot position.
    pub marker: String,
}

impl SnapshotDescriptor {
    /// PostgreSQL: a `REPEATABLE READ READ ONLY` transaction; the marker is
    /// `pg_current_snapshot()` text.
    pub const KIND_POSTGRES_REPEATABLE_READ: &'static str = "postgres_repeatable_read";
    /// SQLite: a WAL read transaction; the marker is empty (a WAL snapshot has
    /// no addressable position).
    pub const KIND_SQLITE_WAL_READ_TRANSACTION: &'static str = "sqlite_wal_read_transaction";
    /// MongoDB: a session with snapshot read concern; the marker is the
    /// cluster time `<seconds>:<increment>`.
    pub const KIND_MONGODB_SNAPSHOT_READ_CONCERN: &'static str = "mongodb_snapshot_read_concern";
}

/// Order of items within and across a backup's data files.
///
/// For `KeyOrdered`, the manifest parser proves only that the files' ranges
/// ascend without overlap. [`ExtenddbManifest::reader`] also runs a
/// [`KeyRangeCheck`] over each file's items as it decodes them: that is what
/// ties a file's items to its range and so rules out a duplicate key across
/// files. The writer runs a [`KeyRangeTracker`] over the items it encodes,
/// so it does not publish a file the check refuses.
///
/// A restore may exploit `KeyOrdered` (SQLite inserts into a fresh table 3 to
/// 4 times faster when rows arrive in primary-key order; measured with the
/// bundled SQLite on a 1M-item fixture, ratio only) but must produce the same
/// table either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemOrder {
    /// Items ascend in the order [`super::key_order::KeyOrder`] defines (hash
    /// key by its text encoding, bytewise; range key `S`/`B` bytewise, `N`
    /// numeric), strictly within each file and from one file to the next;
    /// every [`DataFileEntry::key_range`] is set and the ranges do not
    /// overlap. Only hash-only and single-range-key tables can be ordered.
    KeyOrdered,
    /// No ordering guarantee.
    Unordered,
}

/// The source table definition captured at backup time.
///
/// Field types reuse the existing wire types, so the nested JSON matches what
/// the `DynamoDB` API itself carries (`PascalCase` members inside each element).
/// Timestamps are `f64` epoch seconds, the same representation
/// [`crate::types::BackupDetails`] uses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceTableSchema {
    /// Name of the source table.
    pub table_name: String,
    /// Unique id of the source table.
    pub table_id: String,
    /// ARN of the source table.
    pub table_arn: String,
    /// When the source table was created, as epoch seconds.
    pub table_creation_date_time: f64,
    /// Base table key schema.
    pub key_schema: Vec<KeySchemaElement>,
    /// Attribute definitions for all key attributes.
    pub attribute_definitions: Vec<AttributeDefinition>,
    /// Billing mode at backup time.
    pub billing_mode: BillingMode,
    /// Provisioned throughput; `null` when the table is on demand.
    #[serde(default)]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    /// On-demand throughput limits; `null` when none are configured.
    #[serde(default)]
    pub on_demand_throughput: Option<OnDemandThroughput>,
    /// Table class; `null` when the source reported none.
    #[serde(default)]
    pub table_class: Option<String>,
    /// Server-side encryption as `DescribeTable` reports it; `null` when the
    /// source reported none. A restore maps it to an `SSESpecification`.
    #[serde(default)]
    pub sse_description: Option<SseDescription>,
    /// Global secondary indexes defined on the source table; empty when none.
    /// Required (module docs).
    pub global_secondary_indexes: Vec<GsiInput>,
    /// Local secondary indexes defined on the source table; empty when none.
    /// Required (module docs).
    pub local_secondary_indexes: Vec<LsiInput>,
    /// Vector indexes, as creation specifications. Three states: `null`, the
    /// source backend has no vector support (a target with vector support
    /// restores the table without vector indexes); `[]`, the source supports
    /// vector search and the table had no vector indexes; a non-empty list,
    /// which a target without vector support refuses to restore rather than
    /// dropping them. Required: a missing member is refused, not read as
    /// `null` (module docs).
    #[serde(deserialize_with = "Option::deserialize")]
    pub vector_indexes: Option<Vec<VectorIndexSpecification>>,
    /// The source table's stream specification as `DescribeTable` reported
    /// it at backup time; `null` when the table had none. `DescribeBackup`
    /// reports it as `SourceTableFeatureDetails.StreamDescription`; a
    /// restore does not create a stream. Required: a missing member is
    /// refused (module docs).
    #[serde(deserialize_with = "Option::deserialize")]
    pub stream_specification: Option<StreamSpecification>,
    /// The source table's TTL description as `DescribeTimeToLive` reported
    /// it at backup time; `null` when the source reported none.
    /// `DescribeBackup` reports it as
    /// `SourceTableFeatureDetails.TimeToLiveDescription`; a restore does not
    /// enable TTL. Required: a missing member is refused (module docs).
    #[serde(deserialize_with = "Option::deserialize")]
    pub time_to_live: Option<TimeToLiveDescription>,
    /// Item count at backup time; not negative.
    pub item_count: i64,
    /// Table size in bytes at backup time; not negative.
    pub table_size_bytes: i64,
}

/// First and last base-table key in a data file, as `DynamoDB` JSON items
/// holding exactly the key attributes (enforced by
/// [`ExtenddbManifest::validate_data_files`]). Required when the backup is
/// [`ItemOrder::KeyOrdered`] and refused when it is unordered. The parser
/// refuses overlapping ranges before any file is downloaded; that alone does
/// not rule out a duplicate primary key across files, since nothing in the
/// manifest ties a file's items to its range (two files with ranges `A..F`
/// and `G..M` could each hold `A..M`). [`ExtenddbManifest::reader`] runs a
/// [`KeyRangeCheck`] over each file's items: strict ascent within every
/// file, first and last item equal to the range's endpoints, and
/// non-overlapping ranges together mean no key appears twice in the backup.
/// The writer records the range with a [`KeyRangeTracker`].
///
/// An `N` endpoint must be written in normalized form (`5`, not `5.0` or
/// `5E0`): the parser reads the string as written, before the
/// `AttributeValue` deserializer normalizes it, and refuses any other
/// spelling, so the bytes the sidecar vouches for have one reading.
///
/// An endpoint that names one attribute twice is refused at parse time: a
/// map type would keep the last value silently, and a reader in another
/// language might keep the first and compute a different range from the
/// bytes the sidecar vouches for. The data file reader refuses the same
/// shape in an item line with the same deserializer. An endpoint value is
/// one JSON object with exactly one member, its type descriptor: a value
/// that repeats its descriptor (`{"S":"0","S":"A"}`) or names two is refused
/// at parse time for the same reason, as a data file line of that shape is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyRange {
    /// Key of the first item in the file.
    #[serde(deserialize_with = "key_item_as_written")]
    pub first: Item,
    /// Key of the last item in the file.
    #[serde(deserialize_with = "key_item_as_written")]
    pub last: Item,
}

/// The writer's side of a key-ordered data file: it takes every item the
/// writer encodes, in file order, and gives the file's [`KeyRange`] at the
/// end.
///
/// [`Self::push`] refuses an item whose key [`KeyOrder::endpoint_key`]
/// refuses (an `N` key `abc`, for one) and an item that is not strictly
/// after the one before it, so a snapshot stream that is out of order, or
/// repeats a key, fails the backup instead of producing a file every
/// restore refuses. [`KeyRangeCheck`] is this tracker plus the comparison
/// with the recorded range, so the writer and the reader apply one ascent
/// rule. The tracker keeps two keys, the first and the latest; the latest
/// is overwritten in place and the item is never copied.
///
/// One tracker serves a whole backup: [`Self::next_file`] closes the file
/// so far, returning its range, and keeps the latest key, so the next
/// file's first item is checked strictly after the previous file's last.
/// A writer that calls it between files is refused at that item, with
/// nothing of the new file encoded, where a writer with one tracker per
/// file is refused only at [`super::BackupDocuments::build`]
/// ([`ExtenddbManifest::validate_data_files`] compares the ranges), after
/// every file is stored. [`Self::finish`] closes the last file.
#[derive(Debug, Clone)]
pub struct KeyRangeTracker {
    order: KeyOrder,
    /// The current file's first key; `None` until it has an item.
    first: Option<HeldKey>,
    /// The latest key taken, of this file or, until this file has an item,
    /// of the previous one.
    latest: Option<HeldKey>,
    /// Items taken in the current file.
    items: u64,
}

impl KeyRangeTracker {
    /// A tracker for the files of one backup under the table's key order.
    #[must_use]
    pub fn new(order: &KeyOrder) -> Self {
        Self {
            order: order.clone(),
            first: None,
            latest: None,
            items: 0,
        }
    }

    /// Take the next item of the file.
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidKey`] when the item's key fails
    /// [`KeyOrder::endpoint_key`] (a key attribute missing or of the wrong
    /// type, an empty `S` or `B`, an `N` that is not a valid number in
    /// normalized form), and [`FormatError::KeyOrderViolation`] when the item
    /// is not strictly after the one before it; for the first item of a file
    /// after [`Self::next_file`], the one before it is the previous file's
    /// last item and the message names that boundary and its key. Nothing
    /// is recorded for a refused item.
    pub fn push(&mut self, item: &Item) -> Result<(), FormatError> {
        self.push_key(item).map(|_| ())
    }

    /// [`Self::push`], returning the key now held as the latest.
    fn push_key(&mut self, item: &Item) -> Result<&HeldKey, FormatError> {
        let key = self.order.checked_key(item)?;
        self.push_checked(key)
    }

    /// [`Self::push_key`] for a key already checked under this order.
    fn push_checked(&mut self, key: CheckedKey<'_>) -> Result<&HeldKey, FormatError> {
        let n = self.items + 1;
        let latest = match self.latest.take() {
            None => HeldKey::new(key),
            Some(mut latest) => {
                if latest.as_checked().cmp(&key) != Ordering::Less {
                    let message = if self.items == 0 {
                        let shown =
                            serde_json::to_string(&latest.to_item(&self.order)).unwrap_or_default();
                        format!(
                            "item 1 is not strictly after the previous file's last item, key {}",
                            truncated(&shown)
                        )
                    } else {
                        format!("item {n} is not strictly after item {}", n - 1)
                    };
                    self.latest = Some(latest);
                    return Err(FormatError::KeyOrderViolation(message));
                }
                latest.assign(key);
                latest
            }
        };
        if self.items == 0 {
            self.first = Some(latest.clone());
        }
        self.items = n;
        Ok(self.latest.insert(latest))
    }

    /// Number of items taken in the current file.
    #[must_use]
    pub fn item_count(&self) -> u64 {
        self.items
    }

    /// Close the current file and start the next: the closed file's range,
    /// its first and latest keys as key-only items, or `None` when it had no
    /// items (nothing changes then). The latest key is kept, so the next
    /// file's first item must be strictly after it; [`Self::item_count`]
    /// starts again at 0.
    #[must_use]
    pub fn next_file(&mut self) -> Option<KeyRange> {
        let latest = self.latest.as_ref()?;
        let first = self.first.take()?;
        self.items = 0;
        Some(KeyRange {
            first: first.to_item(&self.order),
            last: latest.to_item(&self.order),
        })
    }

    /// The last file's range, its first and latest keys as key-only items,
    /// or `None` when no item was taken since the last [`Self::next_file`]
    /// (the format has no empty data file).
    #[must_use]
    pub fn finish(self) -> Option<KeyRange> {
        Some(KeyRange {
            first: self.first?.to_item(&self.order),
            last: self.latest?.to_item(&self.order),
        })
    }
}

/// Checks one data file's items, in file order, against the file's
/// [`KeyRange`] in a key-ordered backup. [`ExtenddbManifest::reader`] runs
/// it for every item it decodes and [`Self::finish`] at the end of the file;
/// it is public for callers that decode otherwise.
///
/// It is a [`KeyRangeTracker`] (so it refuses an item whose key
/// [`KeyOrder::endpoint_key`] refuses, wherever it falls in the file, and
/// an item that is not strictly after the one before it) plus the recorded
/// range: it refuses a first item whose key is not `range.first`, an item
/// after `range.last` (as soon as it is seen), and at the end a last item
/// whose key is not `range.last` or a file with no items. Together with the
/// manifest's non-overlapping ranges
/// ([`ExtenddbManifest::validate_data_files`]) this means the backup's keys
/// ascend strictly across all files, so no key appears twice; the manifest
/// alone cannot prove that. Keys compare in [`KeyOrder::compare`]'s order,
/// so "equal" means equal on the key attributes; other attributes are not
/// looked at. The recorded endpoints are checked and parsed once, in
/// [`Self::new`]; a range [`KeyOrder::validate_key_item`] refuses makes
/// every call return that error.
#[derive(Debug, Clone)]
pub struct KeyRangeCheck {
    tracker: KeyRangeTracker,
    /// The recorded `first` and `last`, or the reason they are not keys.
    range: Result<(HeldKey, HeldKey), String>,
}

impl KeyRangeCheck {
    /// A check for one file with recorded range `range`, under the table's
    /// key order.
    #[must_use]
    pub fn new(order: &KeyOrder, range: &KeyRange) -> Self {
        let endpoint = |which: &str, key: &Item| {
            order
                .validate_key_item(key)
                .and_then(|()| order.checked_key(key).map(HeldKey::new))
                .map_err(|e| {
                    let reason = match e {
                        FormatError::InvalidKey(reason) => reason,
                        other => other.to_string(),
                    };
                    let shown = serde_json::to_string(key).unwrap_or_default();
                    format!(
                        "key_range {which} {} is not a key of this order: {reason}",
                        truncated(&shown)
                    )
                })
        };
        let range = endpoint("first", &range.first)
            .and_then(|first| Ok((first, endpoint("last", &range.last)?)));
        Self {
            tracker: KeyRangeTracker::new(order),
            range,
        }
    }

    fn range(
        range: &Result<(HeldKey, HeldKey), String>,
    ) -> Result<&(HeldKey, HeldKey), FormatError> {
        range
            .as_ref()
            .map_err(|reason| FormatError::InvalidKey(reason.clone()))
    }

    /// Check the next item of the file.
    ///
    /// # Errors
    ///
    /// [`FormatError::KeyOrderViolation`] when the item breaks the file's
    /// order or range (type docs), [`FormatError::InvalidKey`] when its key
    /// fails [`KeyOrder::endpoint_key`]: a key attribute missing or of the
    /// wrong type, an empty `S` or `B`, or an `N` value that is not a valid
    /// number in normalized form (a key-ordered file cannot hold a key its
    /// writer could not record as an endpoint). As for
    /// [`KeyRangeTracker::push`], nothing is recorded for a refused item:
    /// the range comparisons run before the tracker takes the key.
    pub fn check(&mut self, item: &Item) -> Result<(), FormatError> {
        let (first, last) = Self::range(&self.range)?;
        let key = self.tracker.order.checked_key(item)?;
        let n = self.tracker.items + 1;
        if n == 1 && key.cmp(&first.as_checked()) != Ordering::Equal {
            return Err(FormatError::KeyOrderViolation(
                "the first item's key is not the file's key_range first".to_owned(),
            ));
        }
        if key.cmp(&last.as_checked()) == Ordering::Greater {
            return Err(FormatError::KeyOrderViolation(format!(
                "item {n} is after the file's key_range last"
            )));
        }
        self.tracker.push_checked(key).map(|_| ())
    }

    /// Check the end of the file: it had items and the last one is
    /// `range.last`.
    ///
    /// # Errors
    ///
    /// [`FormatError::KeyOrderViolation`] when the file had no items or its
    /// last item is not the range's `last`, and the [`Self::new`] error for
    /// a range that is not two keys.
    pub fn finish(&self) -> Result<(), FormatError> {
        let (_, last) = Self::range(&self.range)?;
        let Some(latest) = &self.tracker.latest else {
            return Err(FormatError::KeyOrderViolation(
                "the file has a key_range and no items".to_owned(),
            ));
        };
        if latest.as_checked().cmp(&last.as_checked()) != Ordering::Equal {
            return Err(FormatError::KeyOrderViolation(format!(
                "the last item (item {}) is not the file's key_range last",
                self.tracker.items
            )));
        }
        Ok(())
    }
}

/// One data file read for a restore, from [`ExtenddbManifest::reader`]: a
/// [`DataFileDecoder`] whose items, in a key-ordered backup, have passed the
/// file's [`KeyRangeCheck`], and whose end of stream runs the check's
/// [`KeyRangeCheck::finish`] after the decoder's own end-of-stream checks.
///
/// As with the decoder, an item is unverified until the stream ends (or the
/// source was verified first, [`ExtenddbManifest::reader_prehashed`]); any
/// error ends the stream, every later [`Self::next_item`] returns
/// [`FormatError::Decode`] naming the earlier failure, and the [`Iterator`]
/// impl yields the first error once and then `None`.
pub struct DataFileReader<R: std::io::Read> {
    decoder: DataFileDecoder<R>,
    check: Option<KeyRangeCheck>,
    /// The end of stream was reached and checked.
    ended: bool,
    /// An error was returned; the iterator yields nothing more.
    errored: bool,
    /// Set by the first error of the range check, replayed by every later
    /// call (the decoder replays its own).
    check_failed: Option<&'static str>,
}

impl<R: std::io::Read> DataFileReader<R> {
    fn new(decoder: DataFileDecoder<R>, check: Option<KeyRangeCheck>) -> Self {
        Self {
            decoder,
            check,
            ended: false,
            errored: false,
            check_failed: None,
        }
    }

    /// The next item, or `Ok(None)` once the decoder's end-of-stream checks
    /// and the key range check's [`KeyRangeCheck::finish`] have passed.
    ///
    /// # Errors
    ///
    /// The errors of [`DataFileDecoder::next_item`] and of
    /// [`KeyRangeCheck::check`] and [`KeyRangeCheck::finish`].
    pub fn next_item(&mut self) -> Result<Option<Item>, FormatError> {
        if let Some(reason) = self.check_failed {
            return Err(FormatError::Decode(format!(
                "data file reader failed earlier: {reason}"
            )));
        }
        if self.ended {
            return Ok(None);
        }
        let checked = match self.decoder.next_item() {
            Err(e) => {
                self.errored = true;
                return Err(e);
            }
            Ok(Some(item)) => match &mut self.check {
                Some(check) => check.check(&item).map(|()| Some(item)),
                None => Ok(Some(item)),
            },
            Ok(None) => {
                self.ended = true;
                match &self.check {
                    Some(check) => check.finish().map(|()| None),
                    None => Ok(None),
                }
            }
        };
        if let Err(e) = &checked {
            self.errored = true;
            self.check_failed = Some(match e {
                FormatError::KeyOrderViolation(_) => "key order violation",
                _ => "invalid key",
            });
        }
        checked
    }
}

impl<R: std::io::Read> Iterator for DataFileReader<R> {
    type Item = Result<Item, FormatError>;

    /// Fused, as [`DataFileDecoder`]'s: the first error once, then `None`.
    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }
        self.next_item().transpose()
    }
}

impl<R: std::io::Read> std::iter::FusedIterator for DataFileReader<R> {}

/// `deserialize_with` target for a [`KeyRange`] endpoint: an item read as
/// [`item_without_duplicates`] reads it (a repeated top-level name refused),
/// each value read from the text as its one type descriptor and payload (an
/// empty value, or one with a second descriptor, `{"S":"0","S":"A"}`
/// included, refused before a value is built), with each `N` value checked
/// as written. A valid number that is not in normalized form is refused; the
/// `AttributeValue` deserializer would rewrite it, so the parsed range and
/// the stored bytes would disagree. An invalid number is left for
/// [`KeyOrder::validate_key_item`] to refuse.
fn key_item_as_written<'de, D>(deserializer: D) -> Result<Item, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use crate::validation::number::validate_and_normalize_number;
    use serde::de::Error as _;
    let item = item_without_duplicates_raw(deserializer)?;
    let mut out = Item::new();
    for (name, (descriptor, payload)) in item {
        if descriptor == "N"
            && let Some(n) = payload.as_str()
            && let Ok(normalized) = validate_and_normalize_number(n)
            && normalized != n
        {
            return Err(D::Error::custom(format!(
                "key_range attribute {:?} N value {:?} is not in normalized form ({:?})",
                truncated(&name),
                truncated(n),
                truncated(&normalized)
            )));
        }
        let raw = serde_json::Value::Object(std::iter::once((descriptor, payload)).collect());
        let value = serde_json::from_value(raw).map_err(D::Error::custom)?;
        out.insert(name, value);
    }
    Ok(out)
}

/// One data file recorded in the manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFileEntry {
    /// Store key of the file, relative to the backup prefix
    /// (for example `data/000001.json.gz`).
    pub key: String,
    /// Number of items in the file.
    pub item_count: u64,
    /// Lowercase hex sha256 of the file's compressed bytes.
    pub sha256: String,
    /// Base64 MD5 of the file's compressed bytes; the same value the
    /// `manifest-files.json` line for this file carries as `md5Checksum`.
    pub md5: String,
    /// Size of the file's compressed bytes. A decoder enforces it as it
    /// reads, so a file longer than recorded is refused once the reader has
    /// consumed one byte more than this.
    pub bytes: u64,
    /// Size of the file's decompressed stream: every line with its newline,
    /// except that the reader contract (`data_file` module docs) lets the
    /// last line's be absent. A decoder enforces it incrementally, so a corrupt or hostile file is rejected
    /// before it is fully expanded.
    pub uncompressed_bytes: u64,
    /// Key range covered by the file. Required for a key-ordered backup and
    /// must be `null` for an unordered one; both enforced by
    /// [`ExtenddbManifest::validate_data_files`].
    #[serde(default)]
    pub key_range: Option<KeyRange>,
}

impl DataFileEntry {
    /// The entry for a data file the encoder produced: its digests, sizes
    /// and count copied from `checksums`, so the manifest cannot record a
    /// value the file does not have, and `key_range` as the writer observed
    /// it ([`KeyRangeTracker::finish`]; `None` on an unordered backup).
    /// [`super::BackupDocuments::build`] derives the `manifest-files.json`
    /// line from the result, so the two manifests carry one `md5` and one
    /// `item_count` by construction.
    #[must_use]
    pub fn from_checksums(
        key: String,
        checksums: &super::data_file::DataFileChecksums,
        key_range: Option<KeyRange>,
    ) -> Self {
        Self {
            key,
            item_count: checksums.item_count,
            sha256: checksums.sha256_hex.clone(),
            md5: checksums.md5_base64.clone(),
            bytes: checksums.compressed_bytes,
            uncompressed_bytes: checksums.uncompressed_bytes,
            key_range,
        }
    }

    /// Validate that `key` names a data file within the backup's `data/`
    /// prefix and nowhere else.
    ///
    /// The key must be [`DATA_PREFIX`] followed by exactly one path segment
    /// that matches the data-file naming pattern (`<digits>.json.gz`). This
    /// rejects `..`, absolute paths, and any extra directory level, so a
    /// malicious manifest cannot point the restore path at a store key outside
    /// the backup. Validation lives in the format layer, the natural home for
    /// the layout rule, independent of any store-level sandboxing.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidDataFileKey`] when the key is not a
    /// single data-file entry under the prefix.
    pub fn validate_key(&self) -> Result<(), FormatError> {
        let reject = |reason: &'static str| FormatError::InvalidDataFileKey {
            key: truncated(&self.key),
            reason,
        };

        let name = self
            .key
            .strip_prefix(DATA_PREFIX)
            .ok_or_else(|| reject("must start with the data/ prefix"))?;

        if name.is_empty() {
            return Err(reject("names no file under data/"));
        }
        // Exactly one segment: no further path separators, no traversal, no
        // absolute component.
        if name.contains('/') {
            return Err(reject("must hold exactly one path segment under data/"));
        }
        if name.contains('\\') {
            return Err(reject("must not contain a backslash"));
        }
        if name == "." || name == ".." || name.contains("..") {
            return Err(reject("must not contain a path-traversal component"));
        }
        // The data-file naming pattern: one or more digits then .json.gz.
        let digits = name
            .strip_suffix(".json.gz")
            .ok_or_else(|| reject("must end with .json.gz"))?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(reject("file stem must be all digits"));
        }
        Ok(())
    }

    /// The expectations a decoder enforces for this file: every digest,
    /// size and count the entry records (`sha256`, `md5`, `bytes`,
    /// `item_count`, `uncompressed_bytes`).
    #[must_use]
    pub fn expectations(&self) -> super::data_file::DataFileExpectations {
        super::data_file::DataFileExpectations {
            sha256_hex: Some(self.sha256.clone()),
            md5_base64: Some(self.md5.clone()),
            compressed_bytes: Some(self.bytes),
            item_count: Some(self.item_count),
            uncompressed_bytes: Some(self.uncompressed_bytes),
        }
    }

    /// Streaming decoder over this entry's data file read from `compressed`,
    /// with the entry's digests and counts as expectations. Validates the key
    /// first. The digests are checked when the stream ends, so a caller that
    /// must know them earlier hashes a local copy with
    /// [`crate::backup_format::CompressedHasher`] first and decodes it with
    /// [`Self::decoder_prehashed`]. A restore reads through
    /// [`ExtenddbManifest::reader`], which wraps this decoder and adds the
    /// key range check.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidDataFileKey`] when the key is not a
    /// single data-file entry under the prefix.
    pub fn decoder<R: std::io::Read>(
        &self,
        compressed: R,
    ) -> Result<super::data_file::DataFileDecoder<R>, FormatError> {
        self.validate_key()?;
        Ok(super::data_file::DataFileDecoder::new(
            compressed,
            self.expectations(),
        ))
    }

    /// [`Self::decoder`] for a source whose `sha256`, `md5` and size the
    /// caller has just verified against this entry (a
    /// [`crate::backup_format::CompressedHasher`] pass and
    /// [`crate::backup_format::DataFileExpectations::verify_digest`]): the
    /// bytes are not hashed a second time, and the compressed size, the
    /// uncompressed size, the item count and every line are still checked.
    /// Safe when `compressed` yields exactly the bytes that were verified:
    /// a local spool file this process wrote and hashed, or a file read in
    /// place from a store root that only the service's user can open; not a
    /// second download, which is a second set of bytes. A source that
    /// differs is not refused by its digest; what remains is the compressed
    /// size check, which refuses a file replaced by one of another length
    /// and does not detect a replacement of the same length. The second hash
    /// would cost about 0.43 µs per 318-byte item (sha256 and md5 at about
    /// 150 MB/s of compressed bytes on a Xeon Platinum 8124M, which has no
    /// SHA extensions), about 9% of decoding.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidDataFileKey`] when the key is not a
    /// single data-file entry under the prefix.
    pub fn decoder_prehashed<R: std::io::Read>(
        &self,
        compressed: R,
    ) -> Result<super::data_file::DataFileDecoder<R>, FormatError> {
        self.validate_key()?;
        Ok(super::data_file::DataFileDecoder::prehashed(
            compressed,
            self.expectations(),
        ))
    }

    /// Decode this entry's in-memory data file into a `Vec`, validating the
    /// key first and verifying the digests before the first item. For tests
    /// and files known to be small; see [`Self::decoder`] for the streaming
    /// form.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidDataFileKey`] when the key is not a
    /// single data-file entry under the prefix, and any error
    /// [`crate::backup_format::decode_data_file`] can produce.
    pub fn decode(&self, compressed: &[u8]) -> Result<Vec<Item>, FormatError> {
        self.validate_key()?;
        super::data_file::decode_data_file(compressed, self.expectations())
    }
}

/// The `extenddb-manifest.json` document.
///
/// Serde leaves unknown fields tolerated (no `deny_unknown_fields`) at every
/// level, so a reader does not fail on a member it does not know; the hard
/// gate is [`Self::check_supported`], which refuses a `format_version` this
/// build does not understand.
///
/// Evolution rule: any change to the bytes a writer produces for the golden
/// fixtures' inputs (`testdata/extenddb-manifest/`) is a format version
/// bump, a new member included, whether or not an older reader could have
/// ignored it. A new member that is omitted when absent does not change
/// those bytes, nor does a new or renamed enum value no fixture uses; the
/// member list of every type and the value list of every enum in the
/// document (`AttributeValue`'s type descriptors included) are pinned by
/// tests (`nested_types_have_the_pinned_members`,
/// `enum_values_are_pinned`), so it fails there instead. A member is added by bumping [`CURRENT_FORMAT_VERSION`],
/// giving the member `#[serde(default)]` so the new reader still reads v1,
/// and writing `v{N}*.json` fixtures beside the v1 ones; every version's
/// fixtures are permanent parse tests. Readers accept
/// `1..=CURRENT_FORMAT_VERSION`, so a bump excludes older readers from
/// newer backups whatever the backup contains, and there is no
/// minimum-reader-version escape; that is the chosen trade (one rule, one
/// fixture set per version) over a per-member negotiation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtenddbManifest {
    /// Format version; see [`CURRENT_FORMAT_VERSION`].
    pub format_version: u32,
    /// ARN of the backup, in the shape
    /// `arn:<partition>:dynamodb:<region>:<account>:table/<table>/backup/<id>`.
    pub backup_arn: String,
    /// Name given to the backup at creation: 3 to 255 characters of
    /// `[a-zA-Z0-9_.-]`, the service's `BackupName` rule, enforced on load
    /// because the value is echoed to API clients.
    pub backup_name: String,
    /// The instant the snapshot pinned, as epoch seconds with millisecond
    /// precision, the representation [`crate::types::BackupDetails`] uses.
    /// It equals the instant encoded in `backup_arn`'s backup id
    /// ([`super::backup_creation_date_time`]); the parser refuses a manifest
    /// where the two differ, so the ARN, the manifest and the API carry one
    /// time.
    pub backup_creation_date_time: f64,
    /// Total size of the backup's data files, in compressed bytes.
    pub backup_size_bytes: i64,
    /// The storage backend that produced the backup.
    pub source_backend: String,
    /// The `ExtendDB` version that produced the backup.
    pub source_extenddb_version: String,
    /// How the consistent snapshot was taken.
    pub snapshot: SnapshotDescriptor,
    /// Order of items in the data files.
    pub item_order: ItemOrder,
    /// The source table definition.
    pub source_table: SourceTableSchema,
    /// The data files that make up the backup, in the order a restore applies
    /// them. This order is authoritative; a store listing is never used to
    /// order files (zero-padding in [`super::data_file_key`] runs out at a
    /// million files).
    pub data_files: Vec<DataFileEntry>,
}

/// The content of the `extenddb-manifest.sha256` sidecar for a stored
/// manifest: `<lowercase hex sha256>  extenddb-manifest.json\n`, the
/// `sha256sum` output format.
#[must_use]
pub(super) fn manifest_checksum(manifest_bytes: &[u8]) -> String {
    format!(
        "{}  {EXTENDDB_MANIFEST_FILE}\n",
        hex_encode(&Sha256::digest(manifest_bytes))
    )
}

/// Compare fetched manifest bytes against the sidecar's content, both as
/// the store returned them.
///
/// The sidecar is exactly what the writer stores
/// ([`super::BackupDocuments::sidecar`]), the `sha256sum`
/// output line `<64 lowercase hex digits>  extenddb-manifest.json\n`, with or
/// without the final `\n`. Anything else is refused rather than read
/// leniently: bytes that are not UTF-8, a bare digest (which `sha256sum -c`
/// refuses too, so the sidecar would not check from a shell), another file
/// name (the sidecar of a different object), a second line, upper-case hex
/// (the manifest records digests in lower case, and one spelling keeps the
/// sidecar comparable as bytes), `\r\n`, or `sha256sum`'s binary-mode `*`
/// marker.
///
/// # Errors
///
/// Returns [`FormatError::InvalidManifest`] when the sidecar is not in that
/// shape (an empty one and a non-UTF-8 one included) and
/// [`FormatError::ChecksumMismatch`] when the digests differ.
pub fn verify_manifest_checksum(manifest_bytes: &[u8], sidecar: &[u8]) -> Result<(), FormatError> {
    let Ok(sidecar) = std::str::from_utf8(sidecar) else {
        return Err(FormatError::InvalidManifest(format!(
            "{MANIFEST_CHECKSUM_FILE} is not UTF-8 ({} bytes)",
            sidecar.len()
        )));
    };
    let line = sidecar.strip_suffix('\n').unwrap_or(sidecar);
    let suffix = format!("  {EXTENDDB_MANIFEST_FILE}");
    let digest = line.strip_suffix(suffix.as_str()).unwrap_or("");
    if !is_sha256_hex(digest) {
        return Err(FormatError::InvalidManifest(format!(
            "{MANIFEST_CHECKSUM_FILE} is not one line of 64 lowercase hex digits, \
             two spaces and {EXTENDDB_MANIFEST_FILE}: {:?}",
            truncated(sidecar)
        )));
    }
    let expected = digest.to_owned();
    let computed = hex_encode(&Sha256::digest(manifest_bytes));
    if expected == computed {
        Ok(())
    } else {
        Err(FormatError::ChecksumMismatch {
            subject: "manifest",
            expected,
            computed,
        })
    }
}

/// The members a reader needs before it can decide whether to parse the
/// rest: the version gate runs on this so a document from a later version
/// fails with [`FormatError::UnsupportedFormatVersion`] rather than with a
/// parse error about a member this build does not know.
#[derive(Deserialize)]
struct VersionOnly {
    format_version: u32,
}

impl ExtenddbManifest {
    /// Serialize to the bytes stored as `extenddb-manifest.json`, after
    /// [`Self::validate`], so a writer cannot produce bytes
    /// [`Self::from_json_bytes`] refuses (an un-normalized `N` endpoint built
    /// in code, a 13-digit backup id, a `backup_size_bytes` off by one).
    /// [`super::BackupDocuments::build`] calls it and hashes exactly these
    /// bytes for the sidecar.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::validate`], and [`FormatError::Json`] when
    /// serialization fails.
    pub(super) fn to_json_bytes(&self) -> Result<Vec<u8>, FormatError> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    /// Every check a parsed or built manifest must pass, in the order
    /// [`Self::from_json_bytes`] runs them: [`Self::check_supported`],
    /// [`Self::validate_source_table`], [`Self::validate_identity`],
    /// [`Self::validate_data_files`]. [`super::BackupDocuments::build`] runs
    /// it too, so the reader and the writer enforce one rule set and a
    /// backup job does not repeat the sequence.
    ///
    /// # Errors
    ///
    /// The first failing check's error.
    pub fn validate(&self) -> Result<(), FormatError> {
        self.check_supported()?;
        self.validate_source_table()?;
        self.validate_identity()?;
        self.validate_data_files()
    }

    /// Parse stored manifest bytes: the version gate first, then the full
    /// document, then [`Self::validate`]. Callers verify the sidecar
    /// checksum against the same bytes first; see
    /// [`verify_manifest_checksum`].
    ///
    /// The gate: the document must be JSON and `format_version` a JSON
    /// integer in `u32` range (otherwise the error is [`FormatError::Json`],
    /// whatever the version); then the version is checked before any other
    /// member is interpreted, so a document from a later version is reported
    /// as unsupported rather than as a parse error about a member this build
    /// does not know. The document is parsed twice for that, once for the
    /// version and once in full; the version parse is under 10% of the
    /// whole. The cost grows with the data files: with short keys (a 15-byte
    /// `S` hash key and a 6-character `N` range key, about 325 bytes of
    /// document per file) parsing takes about 6 to 12 µs and retains about
    /// 1.6 KB per data file: 0.6 s on an idle host and 1.0 to 1.2 s on a
    /// loaded one, and 155 MiB, at 10^5 files (release build, one shared
    /// 16-core Xeon Platinum 8124M). Longer keys cost more: the two
    /// endpoints are most of each entry.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::UnsupportedFormatVersion`] when the version is
    /// not one this build reads ([`check_format_version`]),
    /// [`FormatError::Json`] when the bytes do not parse (its message bounds
    /// serde's text, which quotes the input), and the errors of
    /// [`Self::validate`].
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        // The gate reads only `format_version`, so a document from a later
        // version fails with that reason instead of a deserialization error
        // about a member this build does not know.
        let version: VersionOnly = serde_json::from_slice(bytes)?;
        check_format_version(version.format_version)?;
        let manifest: Self = serde_json::from_slice(bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// The parsed backup ARN.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidArn`] when `backup_arn` is malformed.
    pub fn backup_arn(&self) -> Result<BackupArn, FormatError> {
        BackupArn::parse(&self.backup_arn)
    }

    /// Check that the manifest names one table: `backup_arn` parses, its
    /// table name equals `source_table.table_name`, and
    /// `source_table.table_arn` is a table ARN for the same account and
    /// table. `backup_arn` is the authoritative identity; the source-table
    /// fields are checked against it so a restore never has two answers to
    /// "which table is this". The table ARN's region is not compared: it is
    /// recorded when the table is created and the backup ARN carries the
    /// region configured when the backup is taken.
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidArn`] when `backup_arn` is malformed,
    /// [`FormatError::InvalidManifest`] when the source-table fields disagree
    /// with it.
    pub fn validate_identity(&self) -> Result<(), FormatError> {
        let arn = self.backup_arn()?;
        // A machine-written field has one spelling; whitespace the parser
        // would tolerate on a request is a defect here.
        if self.backup_arn != arn.to_arn_string() {
            return Err(FormatError::InvalidManifest(format!(
                "backup_arn {:?} is not in canonical form",
                truncated(&self.backup_arn)
            )));
        }
        let Some(id_millis) = backup_id_epoch_millis(arn.backup_id()) else {
            return Err(FormatError::InvalidManifest(format!(
                "backup id {:?} does not carry the backup's creation instant",
                arn.backup_id()
            )));
        };
        // Exact equality, as an f64: the writer derives the field from the
        // id with `backup_creation_date_time`, so any other value is a
        // different time, however close. An ms/1000 value has a short
        // decimal form, which `serde_json` writes and reads back to the same
        // bits. That does not hold for every f64 with the default parser
        // (no `float_roundtrip`): a microsecond timestamp divided by 1e9, as
        // `table_creation_date_time` can be, may come back one ulp off,
        // which nothing refuses.
        #[allow(clippy::cast_precision_loss)]
        let expected = id_millis as f64 / 1000.0;
        debug_assert_eq!(
            Some(expected.to_bits()),
            backup_creation_date_time(arn.backup_id()).map(f64::to_bits)
        );
        if self.backup_creation_date_time.to_bits() != expected.to_bits() {
            return Err(FormatError::InvalidManifest(format!(
                "backup_creation_date_time {:?} does not match the instant in backup_arn \
                 ({id_millis} ms)",
                self.backup_creation_date_time
            )));
        }
        if !is_valid_backup_name(&self.backup_name) {
            return Err(FormatError::InvalidManifest(format!(
                "backup_name {:?} is not 3 to 255 characters of [a-zA-Z0-9_.-]",
                truncated(&self.backup_name)
            )));
        }
        if arn.table_name() != self.source_table.table_name {
            return Err(FormatError::InvalidManifest(format!(
                "backup_arn names table {:?} but source_table.table_name is {:?}",
                truncated(arn.table_name()),
                truncated(&self.source_table.table_name)
            )));
        }
        // The table ARN is read with `crate::arn::parse`, the parser
        // authorization uses (`extenddb_storage::util::is_backup_resource_arn`
        // still splits ARNs on its own). The region is checked for the shape
        // `check_backup_arn` requires, not compared (recorded at table
        // creation, while the backup ARN carries the region configured at
        // backup time).
        let table_arn = &self.source_table.table_arn;
        let matches = crate::arn::parse(table_arn).is_some_and(|t| {
            t.sub.is_none()
                && t.service == "dynamodb"
                && t.partition == arn.partition()
                && is_region(&t.region)
                && t.account == arn.account_id()
                && t.table == arn.table_name()
        });
        if !matches {
            return Err(FormatError::InvalidManifest(format!(
                "source_table.table_arn {:?} is not a table ARN in partition {} for account {} \
                 and table {:?} named by backup_arn",
                truncated(table_arn),
                truncated(arn.partition()),
                truncated(arn.account_id()),
                truncated(arn.table_name())
            )));
        }
        Ok(())
    }

    /// The key order of the source table, when it has one.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::UnorderableKeySchema`] for a multi-part key.
    pub fn key_order(&self) -> Result<KeyOrder, FormatError> {
        KeyOrder::resolve(
            &self.source_table.key_schema,
            &self.source_table.attribute_definitions,
        )
    }

    /// The restore's reader of one of this manifest's data files, read from
    /// `compressed`: [`DataFileEntry::decoder`]'s checks, and when the
    /// manifest is key-ordered every item passed through a
    /// [`KeyRangeCheck`] of `entry.key_range` and the check's
    /// [`KeyRangeCheck::finish`] run at the end of the stream, after the
    /// decoder's own end-of-stream checks. The caller does not construct the
    /// check, so it cannot omit it. `entry` is one of `self.data_files`.
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidDataFileKey`] for a bad key,
    /// [`FormatError::InvalidDataFileEntry`] when a key-ordered manifest's
    /// entry has no `key_range` or an unordered one's has one, and
    /// [`FormatError::UnorderableKeySchema`] when a key-ordered manifest's
    /// schema has no order.
    pub fn reader<R: std::io::Read>(
        &self,
        entry: &DataFileEntry,
        compressed: R,
    ) -> Result<DataFileReader<R>, FormatError> {
        let check = self.range_check(entry)?;
        Ok(DataFileReader::new(entry.decoder(compressed)?, check))
    }

    /// [`Self::reader`] over [`DataFileEntry::decoder_prehashed`], for a
    /// source whose digests the caller has just verified: a spooled download
    /// this process hashed, or a file read in place from a store root only
    /// the service's user can open. The digests are not compared again, so a
    /// source that changed after the hash is refused only by its compressed
    /// size (another length), not when the replacement has the same length
    /// (see there).
    ///
    /// # Errors
    ///
    /// Those of [`Self::reader`].
    pub fn reader_prehashed<R: std::io::Read>(
        &self,
        entry: &DataFileEntry,
        compressed: R,
    ) -> Result<DataFileReader<R>, FormatError> {
        let check = self.range_check(entry)?;
        Ok(DataFileReader::new(
            entry.decoder_prehashed(compressed)?,
            check,
        ))
    }

    fn range_check(&self, entry: &DataFileEntry) -> Result<Option<KeyRangeCheck>, FormatError> {
        match (self.item_order, &entry.key_range) {
            (ItemOrder::KeyOrdered, Some(range)) => {
                Ok(Some(KeyRangeCheck::new(&self.key_order()?, range)))
            }
            (ItemOrder::KeyOrdered, None) => Err(FormatError::InvalidDataFileEntry {
                key: truncated(&entry.key),
                reason: "key_ordered manifest entry has no key_range",
            }),
            (ItemOrder::Unordered, None) => Ok(None),
            (ItemOrder::Unordered, Some(_)) => Err(FormatError::InvalidDataFileEntry {
                key: truncated(&entry.key),
                reason: "unordered manifest entry has a key_range",
            }),
        }
    }

    /// Refuse a manifest whose format version this build does not read
    /// ([`check_format_version`]).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::UnsupportedFormatVersion`] when `format_version`
    /// is 0 or later than [`CURRENT_FORMAT_VERSION`].
    pub fn check_supported(&self) -> Result<(), FormatError> {
        check_format_version(self.format_version)
    }

    /// Check what the manifest alone can prove about the source table:
    /// `item_count` and `table_size_bytes` are not negative,
    /// `table_creation_date_time` is finite (JSON cannot carry NaN or an
    /// infinity; `backup_creation_date_time`, the other `f64`, is checked
    /// against the backup id by [`Self::validate_identity`]), and the key
    /// schema has the shape every table `CreateTable` accepts, on every
    /// manifest whatever its `item_order`: at least one `HASH` element,
    /// every `HASH` element before every `RANGE` element, each key attribute
    /// named once, and each key attribute with exactly one definition in
    /// `attribute_definitions`. The number of `HASH` and `RANGE` elements is
    /// not bounded here: `CreateTable` accepts a multi-part key when the
    /// operator enables the preview (`allow_multipart_table_keys`), such a
    /// table is backed up `unordered` ([`KeyOrder::resolve`] refuses it an
    /// order), and the restore's `CreateTable` validation decides whether
    /// the target accepts the shape. Key endpoint lengths (`DynamoDB`'s
    /// 2048-byte hash key and 1024-byte range key limits) are left to the
    /// restore's item validation, which checks every item against the
    /// target's limits.
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidManifest`] naming the negative count, the
    /// non-finite timestamp or the key schema rule.
    pub fn validate_source_table(&self) -> Result<(), FormatError> {
        for (name, value) in [
            ("item_count", self.source_table.item_count),
            ("table_size_bytes", self.source_table.table_size_bytes),
        ] {
            if value < 0 {
                return Err(FormatError::InvalidManifest(format!(
                    "source_table.{name} is negative ({value})"
                )));
            }
        }
        // JSON has no NaN or infinity: serde_json writes them as `null`,
        // which the reader refuses as a type error.
        if !self.source_table.table_creation_date_time.is_finite() {
            return Err(FormatError::InvalidManifest(format!(
                "source_table.table_creation_date_time is not a finite number ({})",
                self.source_table.table_creation_date_time
            )));
        }
        let schema = &self.source_table.key_schema;
        let first_range = schema
            .iter()
            .position(|e| e.key_type == KeyType::Range)
            .unwrap_or(schema.len());
        if first_range == 0 {
            return Err(FormatError::InvalidManifest(
                "source_table.key_schema has no HASH element before its RANGE elements".to_owned(),
            ));
        }
        if schema[first_range..]
            .iter()
            .any(|e| e.key_type == KeyType::Hash)
        {
            return Err(FormatError::InvalidManifest(
                "source_table.key_schema lists a HASH element after a RANGE element".to_owned(),
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for element in schema {
            let name = element.attribute_name.as_str();
            if !names.insert(name) {
                return Err(FormatError::InvalidManifest(format!(
                    "source_table.key_schema names attribute {:?} more than once",
                    truncated(name)
                )));
            }
            let definitions = self
                .source_table
                .attribute_definitions
                .iter()
                .filter(|d| d.attribute_name == name)
                .count();
            if definitions != 1 {
                return Err(FormatError::InvalidManifest(format!(
                    "source_table key attribute {:?} has {definitions} attribute definitions, \
                     not one",
                    truncated(name)
                )));
            }
        }
        Ok(())
    }

    /// Validate the data-file list: every key passes the layout rule and
    /// appears once; every `sha256` is 64 lowercase hex digits and every `md5`
    /// the 24-character base64 of 16 bytes, so a mismatch is reported here and not as a
    /// decode-time checksum failure; a file with items has non-zero sizes;
    /// `backup_size_bytes` equals the sum of the files' sizes (an empty table
    /// is a manifest with no data files and `backup_size_bytes` 0);
    /// for a key-ordered manifest every file carries a `key_range` whose
    /// `first` and `last` are key-only items of the schema's shape with
    /// `first <= last`, and the ranges ascend without overlap from one file
    /// to the next; for an unordered manifest no file carries a `key_range`
    /// (there is no order for it to describe, and a reader must not be
    /// handed ranges it cannot check).
    ///
    /// Callers run this when a manifest is parsed, and the decode path runs
    /// the same per-entry [`DataFileEntry::validate_key`] again before it
    /// resolves a key, so a key that slips past one check is caught at the
    /// other. Within-file order, and the match between a file's items and
    /// its `key_range`, are checked while the restore streams the file
    /// ([`KeyRangeCheck`], which [`Self::reader`] runs for a key-ordered
    /// backup); this check covers what the manifest alone can prove.
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidDataFileKey`] for a bad or duplicate key;
    /// [`FormatError::InvalidDataFileEntry`] for a malformed digest, a zero
    /// size or item count, a missing range on a key-ordered manifest, or a
    /// range on an unordered one; [`FormatError::InvalidManifest`] for a size mismatch or a range
    /// violation; [`FormatError::InvalidKey`] for a malformed range endpoint;
    /// [`FormatError::UnorderableKeySchema`] for a key-ordered manifest whose
    /// schema has no order.
    pub fn validate_data_files(&self) -> Result<(), FormatError> {
        let mut seen = std::collections::BTreeSet::new();
        let mut total: u64 = 0;
        for entry in &self.data_files {
            entry.validate_key()?;
            if !seen.insert(entry.key.as_str()) {
                return Err(FormatError::InvalidDataFileKey {
                    key: truncated(&entry.key),
                    reason: "listed more than once",
                });
            }
            if self.item_order == ItemOrder::Unordered && entry.key_range.is_some() {
                return Err(FormatError::InvalidDataFileEntry {
                    key: truncated(&entry.key),
                    reason: "unordered manifest entry has a key_range",
                });
            }
            if !is_sha256_hex(&entry.sha256) {
                return Err(FormatError::InvalidDataFileEntry {
                    key: truncated(&entry.key),
                    reason: "sha256 is not 64 lowercase hex digits",
                });
            }
            if !is_md5_base64(&entry.md5) {
                return Err(FormatError::InvalidDataFileEntry {
                    key: truncated(&entry.key),
                    reason: "md5 is not the base64 of 16 bytes",
                });
            }
            // An empty table is a manifest with no files; the format has no
            // empty data file (it would carry a key_range with no items and
            // is not a gzip stream at 0 bytes).
            if entry.item_count == 0 {
                return Err(FormatError::InvalidDataFileEntry {
                    key: truncated(&entry.key),
                    reason: "a data file has no items",
                });
            }
            if entry.bytes == 0 || entry.uncompressed_bytes == 0 {
                return Err(FormatError::InvalidDataFileEntry {
                    key: truncated(&entry.key),
                    reason: "a file with items has zero bytes",
                });
            }
            total = total.checked_add(entry.bytes).ok_or_else(|| {
                FormatError::InvalidManifest("data file sizes overflow".to_owned())
            })?;
        }
        if i64::try_from(total).ok() != Some(self.backup_size_bytes) {
            return Err(FormatError::InvalidManifest(format!(
                "backup_size_bytes is {} but the data files total {total}",
                self.backup_size_bytes
            )));
        }
        if self.item_order == ItemOrder::KeyOrdered {
            let order = self.key_order()?;
            let mut previous_last: Option<&Item> = None;
            for entry in &self.data_files {
                let range =
                    entry
                        .key_range
                        .as_ref()
                        .ok_or_else(|| FormatError::InvalidDataFileEntry {
                            key: truncated(&entry.key),
                            reason: "key_ordered manifest entry has no key_range",
                        })?;
                order.validate_key_item(&range.first)?;
                order.validate_key_item(&range.last)?;
                if order.compare(&range.first, &range.last)? == std::cmp::Ordering::Greater {
                    return Err(FormatError::InvalidManifest(format!(
                        "{:?}: key_range first is after last",
                        truncated(&entry.key)
                    )));
                }
                if let Some(prev) = previous_last
                    && order.compare(prev, &range.first)? != std::cmp::Ordering::Less
                {
                    return Err(FormatError::InvalidManifest(format!(
                        "{:?}: key_range overlaps or does not follow the previous file",
                        truncated(&entry.key)
                    )));
                }
                previous_last = Some(&range.last);
            }
        }
        Ok(())
    }
}

/// Whether this build reads manifests of format version `found`: every
/// version from 1 to [`CURRENT_FORMAT_VERSION`]. A build keeps reading the
/// versions before its own, so a backup taken by an earlier release stays
/// restorable; 0 was never written.
///
/// # Errors
///
/// [`FormatError::UnsupportedFormatVersion`] for 0 or a version later than
/// [`CURRENT_FORMAT_VERSION`].
pub fn check_format_version(found: u32) -> Result<(), FormatError> {
    if (1..=CURRENT_FORMAT_VERSION).contains(&found) {
        Ok(())
    } else {
        Err(FormatError::UnsupportedFormatVersion {
            found,
            supported: CURRENT_FORMAT_VERSION,
        })
    }
}

/// The service's `BackupName` rule: 3 to 255 characters of `[a-zA-Z0-9_.-]`,
/// the table-name alphabet of `validate_table_name_chars`. That is not the
/// only copy of the alphabet: the engine's `validate_name_member`
/// (`BackupName` on `CreateBackup`) and `validate_index_name` spell it again,
/// and the engine counts the bounds in characters where this counts bytes
/// (the same for this ASCII alphabet).
fn is_valid_backup_name(name: &str) -> bool {
    (3..=255).contains(&name.len()) && crate::validation::validate_table_name_chars(name).is_ok()
}

fn is_sha256_hex(s: &str) -> bool {
    lower_hex(s, 64)
}

/// The base64 of an MD5 digest: 24 characters that decode, with the standard
/// alphabet and canonical padding, to exactly 16 bytes (`manifest-files.json`'s
/// `md5Checksum` encoding). A string of the right length and alphabet is not
/// enough: 24 `=` characters are both.
fn is_md5_base64(s: &str) -> bool {
    use base64::Engine;
    s.len() == 24
        && base64::engine::general_purpose::STANDARD
            .decode(s)
            .is_ok_and(|bytes| bytes.len() == 16)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeyType, Projection, ProjectionType, ScalarAttributeType};

    #[allow(clippy::too_many_lines)]
    fn sample_manifest() -> ExtenddbManifest {
        ExtenddbManifest {
            format_version: CURRENT_FORMAT_VERSION,
            backup_arn:
                "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01757500000123-73d8d5bc"
                    .to_owned(),
            backup_name: "music-nightly".to_owned(),
            backup_creation_date_time: 1_757_500_000.123,
            backup_size_bytes: 61_728,
            source_backend: "postgres".to_owned(),
            source_extenddb_version: "0.1.11".to_owned(),
            snapshot: SnapshotDescriptor {
                kind: SnapshotDescriptor::KIND_POSTGRES_REPEATABLE_READ.to_owned(),
                marker: "1234:1234:".to_owned(),
            },
            source_table: SourceTableSchema {
                table_name: "Music".to_owned(),
                table_id: "12345a12-abcd-123a-ab12-1234abc12345".to_owned(),
                table_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/Music".to_owned(),
                table_creation_date_time: 1_757_400_000.0,
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "Artist".to_owned(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "SongTitle".to_owned(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "Artist".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "SongTitle".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "Genre".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                billing_mode: BillingMode::Provisioned,
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 5,
                    write_capacity_units: 5,
                }),
                on_demand_throughput: None,
                table_class: Some("STANDARD".to_owned()),
                sse_description: Some(SseDescription {
                    status: "ENABLED".to_owned(),
                    sse_type: Some(crate::types::SseType::KMS),
                    kms_master_key_arn: Some(
                        "arn:aws:kms:us-east-1:123456789012:key/abcd".to_owned(),
                    ),
                }),
                global_secondary_indexes: vec![GsiInput {
                    index_name: "GenreIndex".to_owned(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "Genre".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: Some(ProvisionedThroughput {
                        read_capacity_units: 1,
                        write_capacity_units: 1,
                    }),
                }],
                local_secondary_indexes: vec![LsiInput {
                    index_name: "TitleIndex".to_owned(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "Artist".to_owned(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "SongTitle".to_owned(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::KeysOnly,
                        non_key_attributes: None,
                    },
                }],
                vector_indexes: Some(vec![VectorIndexSpecification {
                    index_name: "embeddings".to_owned(),
                    dimensions: 1024,
                    distance_function: crate::types::DistanceFunction::Cosine,
                    vector_attribute: crate::types::VectorAttribute {
                        attribute_name: "vec".to_owned(),
                    },
                    search_schema: None,
                    projection: None,
                }]),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(crate::types::StreamViewType::NewAndOldImages),
                }),
                time_to_live: Some(TimeToLiveDescription {
                    time_to_live_status: crate::types::TimeToLiveStatus::Enabled,
                    attribute_name: Some("expires_at".to_owned()),
                }),
                item_count: 100_000,
                table_size_bytes: 123_456,
            },
            item_order: ItemOrder::Unordered,
            data_files: vec![DataFileEntry {
                key: "data/000001.json.gz".to_owned(),
                item_count: 50_000,
                sha256: "0".repeat(64),
                md5: "1B2M2Y8AsgTpgAmY7PhCfg==".to_owned(),
                bytes: 61_728,
                uncompressed_bytes: 300_000,
                key_range: None,
            }],
        }
    }

    /// The bytes of `manifest` without [`ExtenddbManifest::to_json_bytes`]'s
    /// validation, for tests that hand the parser a manifest the writer
    /// refuses.
    fn raw_bytes(manifest: &ExtenddbManifest) -> Vec<u8> {
        serde_json::to_vec(manifest).unwrap()
    }

    /// The writer runs every check the reader runs: a manifest the parser
    /// would refuse cannot be serialized. An `N` endpoint built in code in
    /// a spelling other than its normalized one is the case a backup job
    /// would otherwise ship.
    #[test]
    fn to_json_bytes_refuses_what_from_json_bytes_refuses() {
        let mut manifest = sample_manifest();
        manifest.item_order = ItemOrder::KeyOrdered;
        manifest.source_table.attribute_definitions[1].attribute_type = ScalarAttributeType::N;
        let n = |v: &str| -> Item {
            [
                (
                    "Artist".to_owned(),
                    crate::types::AttributeValue::S("a".into()),
                ),
                (
                    "SongTitle".to_owned(),
                    crate::types::AttributeValue::N(v.into()),
                ),
            ]
            .into_iter()
            .collect()
        };
        manifest.data_files[0].key_range = Some(KeyRange {
            first: n("1"),
            last: n("2"),
        });
        manifest
            .to_json_bytes()
            .expect("normalized endpoints serialize");
        manifest.data_files[0].key_range = Some(KeyRange {
            first: n("1"),
            last: n("2.0"),
        });
        let err = manifest.to_json_bytes().unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidKey(m) if m.contains("normalized form")),
            "{err}"
        );
        // The other validation steps are run too, in the reader's order.
        let mut off = sample_manifest();
        off.backup_size_bytes += 1;
        assert!(matches!(
            off.to_json_bytes(),
            Err(FormatError::InvalidManifest(_))
        ));
        let mut legacy = sample_manifest();
        legacy.backup_arn =
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1757500000123-73d8d5bc"
                .to_owned();
        assert!(matches!(
            legacy.to_json_bytes(),
            Err(FormatError::InvalidManifest(_))
        ));
        let mut negative = sample_manifest();
        negative.source_table.item_count = -1;
        assert!(matches!(
            negative.to_json_bytes(),
            Err(FormatError::InvalidManifest(_))
        ));
        let mut later = sample_manifest();
        later.format_version = CURRENT_FORMAT_VERSION + 1;
        assert!(matches!(
            later.to_json_bytes(),
            Err(FormatError::UnsupportedFormatVersion { .. })
        ));
        // A non-finite timestamp would be written as `null`, which the
        // reader refuses as a type error; both `f64` members are refused.
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut table_time = sample_manifest();
            table_time.source_table.table_creation_date_time = bad;
            let err = table_time.to_json_bytes().unwrap_err();
            assert!(
                matches!(&err, FormatError::InvalidManifest(m)
                    if m.contains("table_creation_date_time") && m.contains("finite")),
                "{bad}: {err}"
            );
            let mut backup_time = sample_manifest();
            backup_time.backup_creation_date_time = bad;
            let err = backup_time.to_json_bytes().unwrap_err();
            assert!(
                matches!(&err, FormatError::InvalidManifest(m)
                    if m.contains("backup_creation_date_time")),
                "{bad}: {err}"
            );
        }
        // `validate` alone reports the same.
        assert!(matches!(
            later.validate(),
            Err(FormatError::UnsupportedFormatVersion { .. })
        ));
        sample_manifest().validate().unwrap();
    }

    /// The key schema's shape is checked on every manifest, an unordered one
    /// included: a multi-part key (which `CreateTable` accepts under the
    /// preview flag and which is backed up `unordered`) passes, while a
    /// schema no table can have is refused.
    #[test]
    fn key_schema_shape_is_checked_on_every_manifest() {
        let good = sample_manifest();
        assert_eq!(good.item_order, ItemOrder::Unordered);
        good.validate_source_table().unwrap();

        let element = |name: &str, key_type| KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        };
        let definition = |name: &str| AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: ScalarAttributeType::S,
        };
        // Two HASH and two RANGE parts: unorderable, but a valid table.
        let mut multi = good.clone();
        multi.source_table.key_schema = vec![
            element("a", KeyType::Hash),
            element("b", KeyType::Hash),
            element("c", KeyType::Range),
            element("d", KeyType::Range),
        ];
        multi.source_table.attribute_definitions =
            ["a", "b", "c", "d"].iter().map(|n| definition(n)).collect();
        multi.validate_source_table().unwrap();
        ExtenddbManifest::from_json_bytes(&multi.to_json_bytes().unwrap()).unwrap();
        assert!(matches!(
            multi.key_order(),
            Err(FormatError::UnorderableKeySchema(_))
        ));

        let cases: [(&str, Vec<KeySchemaElement>, Vec<AttributeDefinition>); 7] = [
            ("no HASH element", vec![], vec![]),
            (
                "no HASH element",
                vec![element("a", KeyType::Range)],
                vec![definition("a")],
            ),
            (
                "HASH element after a RANGE",
                vec![
                    element("a", KeyType::Hash),
                    element("b", KeyType::Range),
                    element("c", KeyType::Hash),
                ],
                vec![definition("a"), definition("b"), definition("c")],
            ),
            (
                "more than once",
                vec![element("a", KeyType::Hash), element("a", KeyType::Range)],
                vec![definition("a")],
            ),
            (
                "0 attribute definitions",
                vec![element("a", KeyType::Hash), element("b", KeyType::Range)],
                vec![definition("a")],
            ),
            (
                "0 attribute definitions",
                vec![element("a", KeyType::Hash)],
                vec![],
            ),
            (
                "2 attribute definitions",
                vec![element("a", KeyType::Hash)],
                vec![
                    definition("a"),
                    AttributeDefinition {
                        attribute_name: "a".to_owned(),
                        attribute_type: ScalarAttributeType::N,
                    },
                ],
            ),
        ];
        for (reason, key_schema, attribute_definitions) in cases {
            let mut m = good.clone();
            m.source_table.key_schema = key_schema;
            m.source_table.attribute_definitions = attribute_definitions;
            let err = m.validate_source_table().unwrap_err();
            assert!(
                matches!(&err, FormatError::InvalidManifest(text) if text.contains(reason)),
                "{reason}: {err}"
            );
            // Through the parser, on an unordered manifest, where the
            // key-range rules would not have looked at the schema.
            let err = ExtenddbManifest::from_json_bytes(&raw_bytes(&m)).unwrap_err();
            assert!(
                matches!(&err, FormatError::InvalidManifest(text) if text.contains(reason)),
                "{reason}: {err}"
            );
        }
        // A long attribute name is bounded in the message.
        let mut long = good;
        long.source_table.key_schema = vec![element(&"k".repeat(5000), KeyType::Hash)];
        let err = long.validate_source_table().unwrap_err();
        assert!(err.to_string().len() < 200, "{}", err.to_string().len());
    }

    /// `DataFileEntry::from_checksums` copies every digest, size and count
    /// from the encoder's result, so the manifest's entry and the file agree
    /// by construction.
    #[test]
    fn data_file_entry_from_checksums_matches_the_encoder() {
        let items = vec![key("a", "a"), key("b", "b")];
        let (compressed, checksums) =
            crate::backup_format::encode_data_file(items.clone()).unwrap();
        let entry = DataFileEntry::from_checksums(
            crate::backup_format::data_file_key(1),
            &checksums,
            Some(KeyRange {
                first: key("a", "a"),
                last: key("b", "b"),
            }),
        );
        assert_eq!(entry.key, "data/000001.json.gz");
        assert_eq!(entry.item_count, checksums.item_count);
        assert_eq!(entry.item_count, 2);
        assert_eq!(entry.sha256, checksums.sha256_hex);
        assert_eq!(entry.md5, checksums.md5_base64);
        assert_eq!(entry.bytes, checksums.compressed_bytes);
        assert_eq!(entry.bytes, compressed.len() as u64);
        assert_eq!(entry.uncompressed_bytes, checksums.uncompressed_bytes);
        assert!(entry.key_range.is_some());
        // The entry's expectations verify the file it was built from.
        assert_eq!(entry.decode(&compressed).unwrap(), items);
        // And the entry passes the manifest's rules in a key-ordered manifest.
        let mut manifest = sample_manifest();
        manifest.item_order = ItemOrder::KeyOrdered;
        manifest.backup_size_bytes = i64::try_from(entry.bytes).unwrap();
        manifest.data_files = vec![entry];
        manifest.to_json_bytes().unwrap();
    }

    #[test]
    fn manifest_serde_round_trip() {
        let manifest = sample_manifest();
        let bytes = manifest.to_json_bytes().unwrap();
        let parsed = ExtenddbManifest::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn snapshot_descriptor_serde_round_trip() {
        let snapshot = SnapshotDescriptor {
            kind: SnapshotDescriptor::KIND_SQLITE_WAL_READ_TRANSACTION.to_owned(),
            marker: String::new(),
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        let parsed: SnapshotDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, snapshot);
    }

    #[test]
    fn source_table_schema_serde_round_trip() {
        let schema = sample_manifest().source_table;
        let json = serde_json::to_string(&schema).unwrap();
        let parsed: SourceTableSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, schema);
    }

    #[test]
    fn data_file_entry_serde_round_trip() {
        let entry = DataFileEntry {
            key: "data/000042.json.gz".to_owned(),
            item_count: 7,
            sha256: "ab".repeat(32),
            md5: "cd".repeat(11),
            bytes: 1234,
            uncompressed_bytes: 5678,
            key_range: Some(KeyRange {
                first: [(
                    "pk".to_owned(),
                    crate::types::AttributeValue::S("a".to_owned()),
                )]
                .into_iter()
                .collect(),
                last: [(
                    "pk".to_owned(),
                    crate::types::AttributeValue::S("z".to_owned()),
                )]
                .into_iter()
                .collect(),
            }),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: DataFileEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, entry);
    }

    /// Every optional member is written as an explicit `null` and every list
    /// as `[]`: the member is present, not omitted (indexing a
    /// `serde_json::Value` gives `null` for a missing key too, so presence
    /// is asserted with `contains_key`).
    #[test]
    fn absent_optional_members_serialize_as_null() {
        let mut manifest = sample_manifest();
        let t = &mut manifest.source_table;
        t.provisioned_throughput = None;
        t.on_demand_throughput = None;
        t.table_class = None;
        t.sse_description = None;
        t.vector_indexes = None;
        t.stream_specification = None;
        t.time_to_live = None;
        t.global_secondary_indexes.clear();
        t.local_secondary_indexes.clear();
        manifest.data_files[0].key_range = None;
        let value = serde_json::to_value(&manifest).unwrap();
        let table = value["source_table"].as_object().unwrap();
        for member in [
            "provisioned_throughput",
            "on_demand_throughput",
            "table_class",
            "sse_description",
            "vector_indexes",
            "stream_specification",
            "time_to_live",
        ] {
            assert!(table.contains_key(member), "{member} omitted");
            assert!(table[member].is_null(), "{member}: {}", table[member]);
        }
        for member in ["global_secondary_indexes", "local_secondary_indexes"] {
            assert!(table.contains_key(member), "{member} omitted");
            assert_eq!(table[member], serde_json::json!([]), "{member}");
        }
        let file = value["data_files"][0].as_object().unwrap();
        assert!(file.contains_key("key_range"), "key_range omitted");
        assert!(file["key_range"].is_null());
        // An empty table: the file list is present and empty.
        manifest.data_files.clear();
        let value = serde_json::to_value(&manifest).unwrap();
        assert!(value.as_object().unwrap().contains_key("data_files"));
        assert_eq!(value["data_files"], serde_json::json!([]));
    }

    #[test]
    fn vector_indexes_distinguish_unsupported_from_none() {
        let mut manifest = sample_manifest();
        manifest.source_table.vector_indexes = Some(vec![]);
        let bytes = manifest.to_json_bytes().unwrap();
        let parsed = ExtenddbManifest::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.source_table.vector_indexes, Some(vec![]));
        manifest.source_table.vector_indexes = None;
        let bytes = manifest.to_json_bytes().unwrap();
        let parsed = ExtenddbManifest::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.source_table.vector_indexes, None);
    }

    fn key(artist: &str, title: &str) -> Item {
        [
            (
                "Artist".to_owned(),
                crate::types::AttributeValue::S(artist.to_owned()),
            ),
            (
                "SongTitle".to_owned(),
                crate::types::AttributeValue::S(title.to_owned()),
            ),
        ]
        .into_iter()
        .collect()
    }

    fn second_file(first: Item, last: Item) -> DataFileEntry {
        DataFileEntry {
            key: "data/000002.json.gz".to_owned(),
            item_count: 1,
            sha256: "1".repeat(64),
            md5: "AAAAAAAAAAAAAAAAAAAAAA==".to_owned(),
            bytes: 100,
            uncompressed_bytes: 200,
            key_range: Some(KeyRange { first, last }),
        }
    }

    #[test]
    fn key_ordered_manifest_requires_valid_non_overlapping_key_ranges() {
        let mut manifest = sample_manifest();
        manifest.item_order = ItemOrder::KeyOrdered;
        let err = manifest.validate_data_files().unwrap_err();
        assert!(
            matches!(
                err,
                FormatError::InvalidDataFileEntry {
                    reason: "key_ordered manifest entry has no key_range",
                    ..
                }
            ),
            "{err}"
        );

        // Endpoints must be key-only items of the schema's shape.
        manifest.data_files[0].key_range = Some(KeyRange {
            first: Item::new(),
            last: Item::new(),
        });
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidKey(_))
        ));
        let mut with_extra = key("a", "a");
        with_extra.insert(
            "Genre".to_owned(),
            crate::types::AttributeValue::S("x".into()),
        );
        manifest.data_files[0].key_range = Some(KeyRange {
            first: with_extra,
            last: key("a", "b"),
        });
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidKey(_))
        ));

        // last is checked as a key item too.
        manifest.data_files[0].key_range = Some(KeyRange {
            first: key("a", "a"),
            last: Item::new(),
        });
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidKey(_))
        ));

        // first must not be after last.
        manifest.data_files[0].key_range = Some(KeyRange {
            first: key("b", "a"),
            last: key("a", "a"),
        });
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidManifest(_))
        ));

        // Valid single file.
        manifest.data_files[0].key_range = Some(KeyRange {
            first: key("a", "a"),
            last: key("m", "z"),
        });
        manifest.validate_data_files().unwrap();
        assert_eq!(
            serde_json::to_value(&manifest).unwrap()["item_order"],
            serde_json::json!("key_ordered")
        );

        // A second file must start strictly after the first ends.
        manifest
            .data_files
            .push(second_file(key("m", "z"), key("z", "z")));
        manifest.backup_size_bytes += 100;
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidManifest(_))
        ));
        manifest.data_files[1].key_range = Some(KeyRange {
            first: key("n", "a"),
            last: key("z", "z"),
        });
        manifest.validate_data_files().unwrap();
        // Files out of order are rejected too.
        manifest.data_files.swap(0, 1);
        manifest.data_files[0].key = "data/000001.json.gz".to_owned();
        manifest.data_files[1].key = "data/000002.json.gz".to_owned();
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidManifest(_))
        ));
    }

    fn ordered_files(ranges: &[(Item, Item)]) -> ExtenddbManifest {
        let mut manifest = sample_manifest();
        manifest.item_order = ItemOrder::KeyOrdered;
        manifest.data_files = ranges
            .iter()
            .enumerate()
            .map(|(i, (first, last))| {
                let mut f = second_file(first.clone(), last.clone());
                f.key = crate::backup_format::data_file_key(u32::try_from(i + 1).unwrap());
                f
            })
            .collect();
        manifest.backup_size_bytes = 100 * i64::try_from(ranges.len()).unwrap();
        manifest
    }

    /// Each file is compared with the one before it, not only with the
    /// first: [A..F], [G..M], [H..Z] overlaps between the second and third
    /// file only.
    #[test]
    fn overlap_between_later_files_is_refused() {
        let good = ordered_files(&[
            (key("A", "a"), key("F", "z")),
            (key("G", "a"), key("M", "z")),
            (key("N", "a"), key("Z", "z")),
        ]);
        good.validate_data_files().unwrap();
        let overlap = ordered_files(&[
            (key("A", "a"), key("F", "z")),
            (key("G", "a"), key("M", "z")),
            (key("H", "a"), key("Z", "z")),
        ]);
        let err = overlap.validate_data_files().unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidManifest(m) if m.contains("data/000003.json.gz")),
            "{err}"
        );
        // Touching endpoints overlap too: one key in two files.
        let touching = ordered_files(&[
            (key("A", "a"), key("F", "z")),
            (key("G", "a"), key("M", "z")),
            (key("M", "z"), key("Z", "z")),
        ]);
        assert!(touching.validate_data_files().is_err());
    }

    /// `last` is checked as a key item on its own: a valid key plus a
    /// non-key attribute compares fine (compare ignores extra attributes),
    /// so only the shape check refuses it.
    #[test]
    fn last_endpoint_with_a_non_key_attribute_is_refused() {
        let mut last = key("M", "z");
        last.insert(
            "Genre".to_owned(),
            crate::types::AttributeValue::S("x".into()),
        );
        let manifest = ordered_files(&[(key("A", "a"), last)]);
        let err = manifest.validate_data_files().unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidKey(m) if m.contains("attributes")),
            "{err}"
        );
        // And an `N`-typed endpoint that is not normalized, in `last` only.
        let mut manifest = sample_manifest();
        manifest.item_order = ItemOrder::KeyOrdered;
        manifest.source_table.attribute_definitions[1].attribute_type = ScalarAttributeType::N;
        let n = |v: &str| -> Item {
            [
                (
                    "Artist".to_owned(),
                    crate::types::AttributeValue::S("a".into()),
                ),
                (
                    "SongTitle".to_owned(),
                    crate::types::AttributeValue::N(v.into()),
                ),
            ]
            .into_iter()
            .collect()
        };
        manifest.data_files[0].key_range = Some(KeyRange {
            first: n("1"),
            last: n("2"),
        });
        manifest.validate_data_files().unwrap();
        manifest.data_files[0].key_range = Some(KeyRange {
            first: n("1"),
            last: n("2.0"),
        });
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidKey(_))
        ));
    }

    /// File sizes whose sum does not fit a u64 are refused as an overflow,
    /// even when `backup_size_bytes` equals the wrapped sum.
    #[test]
    fn data_file_sizes_that_overflow_are_refused() {
        let mut manifest = sample_manifest();
        let mut second = manifest.data_files[0].clone();
        second.key = "data/000002.json.gz".to_owned();
        manifest.data_files[0].bytes = u64::MAX;
        second.bytes = 101;
        manifest.data_files.push(second);
        // u64::MAX + 101 wraps to 100.
        manifest.backup_size_bytes = 100;
        let err = manifest.validate_data_files().unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidManifest(m) if m.contains("overflow")),
            "{err}"
        );
        // A sum that fits u64 but not i64 is a size mismatch.
        manifest.data_files[0].bytes = u64::MAX - 101;
        manifest.backup_size_bytes = i64::MAX;
        let err = manifest.validate_data_files().unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidManifest(m) if m.contains("total")),
            "{err}"
        );
    }

    #[test]
    fn duplicate_data_file_keys_are_rejected() {
        let mut manifest = sample_manifest();
        let dup = manifest.data_files[0].clone();
        manifest.data_files.push(dup);
        manifest.backup_size_bytes *= 2;
        let err = manifest.validate_data_files().unwrap_err();
        assert!(matches!(
            err,
            FormatError::InvalidDataFileKey {
                reason: "listed more than once",
                ..
            }
        ));
    }

    #[test]
    fn backup_size_must_equal_the_sum_of_data_files() {
        let mut manifest = sample_manifest();
        manifest.backup_size_bytes += 1;
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidManifest(_))
        ));
    }

    #[test]
    fn version_gate_runs_before_the_full_parse() {
        // A later version with a shape this build cannot parse must still be
        // reported as an unsupported version, not as a JSON error.
        let bytes = br#"{"format_version": 7, "something_new": {"x": 1}}"#;
        let err = ExtenddbManifest::from_json_bytes(bytes).unwrap_err();
        assert!(
            matches!(err, FormatError::UnsupportedFormatVersion { found: 7, .. }),
            "{err}"
        );
    }

    #[test]
    fn malformed_backup_arn_is_rejected_on_load() {
        let mut manifest = sample_manifest();
        manifest.backup_arn =
            "arn:aws:dynamodb:us-east-1:evil:1234:table/Music/backup/1".to_owned();
        let err = ExtenddbManifest::from_json_bytes(&raw_bytes(&manifest)).unwrap_err();
        assert!(matches!(err, FormatError::InvalidArn(_)), "{err}");
    }

    /// `backup_arn` is the identity; the source-table name and ARN must agree
    /// with it.
    #[test]
    fn source_table_must_match_backup_arn() {
        let good = sample_manifest();
        good.validate_identity().unwrap();

        let mut other_name = good.clone();
        other_name.source_table.table_name = "Other".to_owned();
        let err = ExtenddbManifest::from_json_bytes(&raw_bytes(&other_name)).unwrap_err();
        assert!(matches!(err, FormatError::InvalidManifest(_)), "{err}");
        assert!(err.to_string().contains("source_table.table_name"), "{err}");

        for bad_arn in [
            "arn:aws:dynamodb:us-east-1:123456789012:table/Other",
            "arn:aws:dynamodb:us-east-1:999999999999:table/Music",
            "arn:aws:s3:us-east-1:123456789012:table/Music",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/index/x",
            "Music",
        ] {
            let mut m = good.clone();
            m.source_table.table_arn = bad_arn.to_owned();
            let err = m.validate_identity().unwrap_err();
            assert!(
                matches!(err, FormatError::InvalidManifest(_)),
                "{bad_arn}: {err}"
            );
            assert!(err.to_string().contains("table_arn"), "{err}");
        }
        // The table ARN's region is recorded at table creation and is not
        // compared.
        let mut other_region = good;
        other_region.source_table.table_arn =
            "arn:aws:dynamodb:eu-west-1:123456789012:table/Music".to_owned();
        other_region.validate_identity().unwrap();
    }

    /// The identity rules over hostile input: non-canonical ARN spelling,
    /// an id that disagrees with the creation time, a name the API would
    /// echo, and a table ARN in another partition.
    #[test]
    fn identity_rejects_non_canonical_and_disagreeing_fields() {
        let good = sample_manifest();
        good.validate_identity().unwrap();

        let mut padded = good.clone();
        padded.backup_arn = format!(" {}\n", good.backup_arn);
        let err = padded.validate_identity().unwrap_err();
        assert!(err.to_string().contains("canonical"), "{err}");

        let mut off = good.clone();
        off.backup_creation_date_time += 0.001;
        let err = off.validate_identity().unwrap_err();
        assert!(
            err.to_string().contains("does not match the instant"),
            "{err}"
        );
        // Equality is exact: half a millisecond off is a different time.
        let mut near = good.clone();
        near.backup_creation_date_time += 0.0004;
        let err = near.validate_identity().unwrap_err();
        assert!(
            err.to_string().contains("does not match the instant"),
            "{err}"
        );
        let mut negative = good.clone();
        negative.backup_creation_date_time = -5.0;
        assert!(negative.validate_identity().is_err());

        let mut legacy = good.clone();
        legacy.backup_arn =
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/1757500000123-73d8d5bc"
                .to_owned();
        let err = legacy.validate_identity().unwrap_err();
        assert!(err.to_string().contains("creation instant"), "{err}");

        for bad_name in ["ab", "a b", "a/b", &"x".repeat(256), "\u{0}\u{7}<script>"] {
            let mut m = good.clone();
            m.backup_name = bad_name.to_owned();
            let err = m.validate_identity().unwrap_err();
            assert!(
                err.to_string().contains("backup_name"),
                "{bad_name:?}: {err}"
            );
        }

        for bad_arn in [
            "arn:aws-cn:dynamodb:us-east-1:123456789012:table/Music",
            "arn::dynamodb::123456789012:table/Music",
            "arn:aws:dynamodb::123456789012:table/Music",
            "arn:aws:dynamodb:us east 1:123456789012:table/Music",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/index/x",
            "arn:aws:dynamodb:us-east-1:123456789012:table/music",
        ] {
            let mut m = good.clone();
            m.source_table.table_arn = bad_arn.to_owned();
            assert!(m.validate_identity().is_err(), "{bad_arn}");
        }
    }

    /// Digest shapes and sizes are validated on the manifest, not discovered
    /// at decode time.
    type Mutate = fn(&mut DataFileEntry);

    #[test]
    fn data_file_entries_need_well_formed_digests_and_sizes() {
        const MD5: &str = "md5 is not the base64 of 16 bytes";
        let good = sample_manifest();
        good.validate_data_files().unwrap();
        let cases: [(&str, Mutate); 13] = [
            ("sha256 is not 64 lowercase hex digits", |e| {
                e.sha256 = "ABC".to_owned();
            }),
            ("sha256 is not 64 lowercase hex digits", |e| {
                e.sha256 = "A".repeat(64);
            }),
            (MD5, |e| e.md5 = String::new()),
            (MD5, |e| {
                e.md5 = "1B2M2Y8AsgTpgAmY7PhCfg".to_owned();
            }),
            // 24 characters of the alphabet, 18 bytes.
            (MD5, |e| {
                e.md5 = "1B2M2Y8AsgTpgAmY7PhCfgAA".to_owned();
            }),
            // URL-safe alphabet.
            (MD5, |e| {
                e.md5 = "1B2M2Y8AsgTpgAmY7PhC_g==".to_owned();
            }),
            // 24 padding characters: the right length and alphabet, no bytes.
            (MD5, |e| e.md5 = "=".repeat(24)),
            // Padding in the middle.
            (MD5, |e| {
                e.md5 = "1B2M=Y8AsgTpgAmY7PhCfg==".to_owned();
            }),
            // Non-zero trailing bits in the last character: not the
            // canonical encoding of any 16 bytes.
            (MD5, |e| {
                e.md5 = "1B2M2Y8AsgTpgAmY7PhCfh==".to_owned();
            }),
            // 17 bytes would need 24 characters with one `=`.
            (MD5, |e| {
                e.md5 = "1B2M2Y8AsgTpgAmY7PhCfgA=".to_owned();
            }),
            // Whitespace inside.
            (MD5, |e| {
                e.md5 = "1B2M2Y8AsgTpgAmY7PhC g==".to_owned();
            }),
            ("a file with items has zero bytes", |e| e.bytes = 0),
            ("a file with items has zero bytes", |e| {
                e.uncompressed_bytes = 0;
            }),
        ];
        // The canonical encoding of 16 bytes is accepted, whatever they are.
        for md5 in [
            "AAAAAAAAAAAAAAAAAAAAAA==",
            "/////////////////////w==",
            "1B2M2Y8AsgTpgAmY7PhCfg==",
        ] {
            let mut m = good.clone();
            m.data_files[0].md5 = md5.to_owned();
            m.validate_data_files()
                .unwrap_or_else(|e| panic!("{md5}: {e}"));
        }
        for (reason, mutate) in cases {
            let mut m = good.clone();
            let delta = m.data_files[0].bytes;
            mutate(&mut m.data_files[0]);
            m.backup_size_bytes -= i64::try_from(delta - m.data_files[0].bytes).unwrap();
            let err = m.validate_data_files().unwrap_err();
            assert!(
                matches!(err, FormatError::InvalidDataFileEntry { reason: r, .. } if r == reason),
                "{reason}: {err}"
            );
        }
        // An empty table is a manifest with no files and size 0; a data
        // file with no items is refused.
        let mut empty = good.clone();
        empty.data_files.clear();
        empty.backup_size_bytes = 0;
        empty.validate_data_files().unwrap();
        let mut zero = good;
        zero.data_files[0].item_count = 0;
        let err = zero.validate_data_files().unwrap_err();
        assert!(
            matches!(err, FormatError::InvalidDataFileEntry { reason, .. } if reason == "a data file has no items"),
            "{err}"
        );
    }

    /// Untrusted field lengths for the bounded-message tests: just over the
    /// bound, inside the range a raised bound would let through whole, and
    /// far over it.
    const LONG: [usize; 5] = [81, 300, 1000, 5000, 2_000_000];

    fn assert_bounded(what: &str, err: &FormatError, max: usize) {
        let text = err.to_string();
        assert!(
            text.len() < max,
            "{what}: {} bytes: {}",
            text.len(),
            &text[..text.len().min(300)]
        );
    }

    /// Error messages carry at most a bounded slice of an untrusted field.
    #[test]
    fn untrusted_fields_are_bounded_in_errors() {
        let good = sample_manifest();
        for n in LONG {
            let mut padded = good.clone();
            padded.backup_arn = format!("{}{}", " ".repeat(n), good.backup_arn);
            assert_bounded(
                "padded backup_arn",
                &padded.validate_identity().unwrap_err(),
                200,
            );
            let mut long_table = good.clone();
            long_table.source_table.table_name = "x".repeat(n);
            // Two fields, each bounded.
            assert_bounded(
                "table_name",
                &long_table.validate_identity().unwrap_err(),
                300,
            );
            let mut long_arn = good.clone();
            long_arn.source_table.table_arn = format!("arn:{}", "x".repeat(n));
            assert_bounded("table_arn", &long_arn.validate_identity().unwrap_err(), 300);
            let mut long_name = good.clone();
            long_name.backup_name = "\u{7f}".repeat(n);
            assert_bounded(
                "backup_name",
                &long_name.validate_identity().unwrap_err(),
                200,
            );
        }
        // A key that fails the prefix rule.
        let mut long_key = good;
        long_key.data_files[0].key = "k".repeat(2_000_000);
        assert_bounded("key", &long_key.validate_data_files().unwrap_err(), 200);
    }

    /// A data key of any length that passes `validate_key` reaches the later
    /// rules, and each of their messages is bounded.
    #[test]
    fn long_valid_data_keys_are_bounded_in_later_errors() {
        for n in LONG {
            let data_key = format!("data/{}.json.gz", "1".repeat(n));
            let mut m = sample_manifest();
            m.data_files[0].key.clone_from(&data_key);
            m.data_files[0].validate_key().unwrap();
            m.validate_data_files().unwrap();
            // A digest rule.
            let mut bad_digest = m.clone();
            bad_digest.data_files[0].sha256 = "x".to_owned();
            assert_bounded(
                "sha256",
                &bad_digest.validate_data_files().unwrap_err(),
                200,
            );
            // A duplicate.
            let mut dup = m.clone();
            dup.data_files.push(dup.data_files[0].clone());
            dup.backup_size_bytes *= 2;
            assert_bounded("duplicate", &dup.validate_data_files().unwrap_err(), 200);
            // The key-range rules.
            let mut ordered = m.clone();
            ordered.item_order = ItemOrder::KeyOrdered;
            ordered.data_files[0].key_range = Some(KeyRange {
                first: key("b", "a"),
                last: key("a", "a"),
            });
            let err = ordered.validate_data_files().unwrap_err();
            assert!(err.to_string().contains("first is after last"), "{err}");
            assert_bounded("first after last", &err, 200);
            ordered.data_files[0].key_range = Some(KeyRange {
                first: key("a", "a"),
                last: key("c", "a"),
            });
            let mut second = second_file(key("b", "a"), key("d", "a"));
            second.key = format!("data/{}.json.gz", "2".repeat(n));
            ordered.data_files.push(second);
            ordered.backup_size_bytes += 100;
            let err = ordered.validate_data_files().unwrap_err();
            assert!(err.to_string().contains("overlaps"), "{err}");
            assert_bounded("overlap", &err, 200);
            // Through the parser too.
            let err = ExtenddbManifest::from_json_bytes(&raw_bytes(&ordered)).unwrap_err();
            assert_bounded("parsed overlap", &err, 200);
        }
    }

    /// serde's messages quote the input (an unknown variant, a string where
    /// a number belongs); the manifest parser's errors are bounded anyway,
    /// in the version gate and in the full parse.
    #[test]
    fn serde_messages_from_the_manifest_are_bounded() {
        let bytes = sample_manifest().to_json_bytes().unwrap();
        for n in LONG {
            let mut doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            doc["item_order"] = serde_json::json!("v".repeat(n));
            let err =
                ExtenddbManifest::from_json_bytes(&serde_json::to_vec(&doc).unwrap()).unwrap_err();
            assert!(matches!(err, FormatError::Json(_)), "{err}");
            assert!(err.to_string().contains("unknown variant"), "{err}");
            assert_bounded("item_order", &err, 200);

            let mut doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            doc["format_version"] = serde_json::json!("9".repeat(n));
            let err =
                ExtenddbManifest::from_json_bytes(&serde_json::to_vec(&doc).unwrap()).unwrap_err();
            assert!(matches!(err, FormatError::Json(_)), "{err}");
            assert_bounded("format_version", &err, 200);

            let mut doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            doc["source_table"]["billing_mode"] = serde_json::json!("\u{1}".repeat(n));
            let err =
                ExtenddbManifest::from_json_bytes(&serde_json::to_vec(&doc).unwrap()).unwrap_err();
            assert_bounded("billing_mode", &err, 200);
            assert!(
                !err.to_string().contains('\u{1}'),
                "control characters are escaped"
            );

            // A duplicate key-range attribute with a long name.
            let name = "a".repeat(n);
            let json =
                format!(r#"{{"first":{{"{name}":{{"S":"Z"}},"{name}":{{"S":"A"}}}},"last":{{}}}}"#);
            let err: FormatError = serde_json::from_str::<KeyRange>(&json).unwrap_err().into();
            assert_bounded("key_range duplicate", &err, 200);
        }
        // Short messages keep serde's text and position whole.
        let err = ExtenddbManifest::from_json_bytes(b"{\"format_version\": \"x\"}").unwrap_err();
        assert_eq!(
            err.to_string(),
            "JSON error: invalid type: string \"x\", expected u32 at line 1 column 22"
        );
    }

    /// The backup ARN's own messages are bounded: a 900-byte table name is
    /// a legal ARN (under the 1024-byte limit) that the store-prefix rule
    /// refuses.
    #[test]
    fn backup_arn_errors_are_bounded() {
        let good = sample_manifest();
        for n in [256, 300, 900] {
            let mut m = good.clone();
            let table = "x".repeat(n);
            m.backup_arn = format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{table}/backup/01757500000123-73d8d5bc"
            );
            m.source_table.table_name.clone_from(&table);
            assert!(m.backup_arn.len() <= super::super::MAX_BACKUP_ARN_LEN);
            let err = ExtenddbManifest::from_json_bytes(&raw_bytes(&m)).unwrap_err();
            assert!(matches!(err, FormatError::InvalidArn(_)), "{err}");
            assert!(err.to_string().contains("longer than 255"), "{err}");
            assert_bounded("backup_arn table", &err, 200);
        }
        for arn in [
            format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{}/backup/x",
                "x".repeat(900)
            ),
            format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{}/backup/x",
                "x".repeat(2000)
            ),
            format!("arn:{}", "\u{7f}".repeat(900)),
        ] {
            let err = BackupArn::parse(&arn).unwrap_err();
            assert_bounded("parse", &err, 200);
        }
        let err = BackupArn::new("aws", "us-east-1", "1", &"x".repeat(900), &"y".repeat(900))
            .unwrap_err();
        assert_bounded("new", &err, 300);
    }

    /// A key-range endpoint that names an attribute twice is refused at
    /// parse time, rather than keeping one of the two values silently.
    #[test]
    fn key_range_with_a_duplicate_attribute_is_refused() {
        let json =
            r#"{"first":{"Artist":{"S":"Z"},"Artist":{"S":"A"}},"last":{"Artist":{"S":"Z"}}}"#;
        let err = serde_json::from_str::<KeyRange>(json).unwrap_err();
        assert!(err.to_string().contains("more than once"), "{err}");
    }

    /// An endpoint attribute value that repeats its type descriptor is
    /// refused before the value is built, as a data file line of that shape
    /// is, rather than read last-wins (`{"S":"0","S":"A"}` as `A`, or
    /// `{"N":"5.0","N":"5"}` past the normalized-form check). Two different
    /// descriptors and an empty value are refused the same way.
    #[test]
    fn key_range_endpoint_repeating_its_type_descriptor_is_refused() {
        let manifest = ordered_files(&[(key("A", "a"), key("B", "a"))]);
        let bytes = String::from_utf8(raw_bytes(&manifest)).unwrap();
        assert!(ExtenddbManifest::from_json_bytes(bytes.as_bytes()).is_ok());
        let first = r#""first":{"Artist":{"S":"A"}"#;
        assert_eq!(bytes.matches(first).count(), 1, "{bytes}");
        let numbers = String::from_utf8(number_key_manifest("1", "5")).unwrap();
        let last = r#"{"Artist":{"N":"5"}"#;
        assert_eq!(numbers.matches(last).count(), 1, "{numbers}");
        for doc in [
            bytes.replace(first, r#""first":{"Artist":{"S":"0","S":"A"}"#),
            bytes.replace(first, r#""first":{"Artist":{"S":"A","S":"A"}"#),
            bytes.replace(first, r#""first":{"Artist":{"S":"A","N":"1"}"#),
            numbers.replace(last, r#"{"Artist":{"N":"5.0","N":"5"}"#),
        ] {
            let err = ExtenddbManifest::from_json_bytes(doc.as_bytes()).unwrap_err();
            assert!(
                matches!(err, FormatError::Json(_))
                    && err.to_string().contains("more than one type descriptor")
                    && err.to_string().contains("Artist"),
                "{doc}: {err}"
            );
        }
        let err = ExtenddbManifest::from_json_bytes(
            bytes.replace(first, r#""first":{"Artist":{}"#).as_bytes(),
        )
        .unwrap_err();
        assert!(
            matches!(err, FormatError::Json(_)) && err.to_string().contains("no type descriptor"),
            "{err}"
        );
    }

    /// Version 0 was never written and is refused like a later version; a
    /// document missing one of the index members, `stream_specification` or
    /// `time_to_live` is refused, not read as empty or `null`; a missing
    /// scalar optional member still reads as `null`.
    #[test]
    fn version_zero_and_missing_members() {
        let bytes = sample_manifest().to_json_bytes().unwrap();
        let mut doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        doc["format_version"] = serde_json::json!(0);
        assert!(matches!(
            ExtenddbManifest::from_json_bytes(&serde_json::to_vec(&doc).unwrap()),
            Err(FormatError::UnsupportedFormatVersion { found: 0, .. })
        ));
        for member in [
            "global_secondary_indexes",
            "local_secondary_indexes",
            "vector_indexes",
            "stream_specification",
            "time_to_live",
        ] {
            let mut doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            doc["source_table"].as_object_mut().unwrap().remove(member);
            let err =
                ExtenddbManifest::from_json_bytes(&serde_json::to_vec(&doc).unwrap()).unwrap_err();
            assert!(
                matches!(err, FormatError::Json(_))
                    && err
                        .to_string()
                        .contains(&format!("missing field `{member}`")),
                "{member}: {err}"
            );
        }
        // `vector_indexes: null` is a value (no vector support), not absence;
        // the explicit `null` of the two feature members parses the same way.
        let mut doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        doc["source_table"]["vector_indexes"] = serde_json::Value::Null;
        doc["source_table"]["stream_specification"] = serde_json::Value::Null;
        doc["source_table"]["time_to_live"] = serde_json::Value::Null;
        let parsed = ExtenddbManifest::from_json_bytes(&serde_json::to_vec(&doc).unwrap())
            .expect("an explicit null parses");
        assert!(parsed.source_table.vector_indexes.is_none());
        assert!(parsed.source_table.stream_specification.is_none());
        assert!(parsed.source_table.time_to_live.is_none());
        let mut doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        doc["source_table"]
            .as_object_mut()
            .unwrap()
            .remove("table_class");
        let parsed = ExtenddbManifest::from_json_bytes(&serde_json::to_vec(&doc).unwrap())
            .expect("a missing scalar optional member reads as null");
        assert!(parsed.source_table.table_class.is_none());
    }

    #[test]
    fn unordered_manifest_must_not_carry_key_ranges() {
        let mut manifest = sample_manifest();
        assert_eq!(manifest.item_order, ItemOrder::Unordered);
        manifest.data_files[0].key_range = Some(KeyRange {
            first: Item::new(),
            last: Item::new(),
        });
        let err = manifest.validate_data_files().unwrap_err();
        assert!(
            matches!(
                err,
                FormatError::InvalidDataFileEntry {
                    reason: "unordered manifest entry has a key_range",
                    ..
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn unknown_fields_are_tolerated() {
        let manifest = sample_manifest();
        let mut value = serde_json::to_value(&manifest).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("added_in_a_later_version".to_owned(), serde_json::json!(42));
        let parsed: ExtenddbManifest = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn check_supported_accepts_current_version() {
        let manifest = sample_manifest();
        manifest.check_supported().unwrap();
    }

    #[test]
    fn check_supported_rejects_version_two() {
        let mut manifest = sample_manifest();
        manifest.format_version = 2;
        let err = manifest.check_supported().unwrap_err();
        match err {
            FormatError::UnsupportedFormatVersion { found, supported } => {
                assert_eq!(found, 2);
                assert_eq!(supported, CURRENT_FORMAT_VERSION);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn sidecar_checksum_round_trip() {
        let bytes = sample_manifest().to_json_bytes().unwrap();
        let sidecar = manifest_checksum(&bytes);
        // sha256sum format: 64 hex, two spaces, the file name, newline.
        assert!(
            sidecar.ends_with("  extenddb-manifest.json\n"),
            "{sidecar:?}"
        );
        assert_eq!(sidecar.len(), 64 + 2 + EXTENDDB_MANIFEST_FILE.len() + 1);
        verify_manifest_checksum(&bytes, sidecar.as_bytes()).unwrap();
        // The same line without its newline is accepted.
        verify_manifest_checksum(&bytes, sidecar.trim_end_matches('\n').as_bytes()).unwrap();
    }

    /// The sidecar has one shape; every other spelling of the same digest is
    /// refused as malformed, not read leniently.
    #[test]
    fn sidecar_in_any_other_shape_is_refused() {
        let bytes = sample_manifest().to_json_bytes().unwrap();
        let sidecar = manifest_checksum(&bytes);
        let digest = &sidecar[..64];
        for bad in [
            String::new(),
            "\n".to_owned(),
            // A bare digest, which `sha256sum -c` refuses too.
            digest.to_owned(),
            format!("{digest}\n"),
            format!("  {EXTENDDB_MANIFEST_FILE}\n"),
            digest.to_ascii_uppercase(),
            format!(
                "{}  {EXTENDDB_MANIFEST_FILE}\n",
                digest.to_ascii_uppercase()
            ),
            format!("{digest}  other.json\n"),
            format!("{digest}  {EXTENDDB_MANIFEST_FILE}.bak\n"),
            format!("{digest}  ./{EXTENDDB_MANIFEST_FILE}\n"),
            format!("{digest} {EXTENDDB_MANIFEST_FILE}\n"),
            format!("{digest} *{EXTENDDB_MANIFEST_FILE}\n"),
            format!("{digest}  {EXTENDDB_MANIFEST_FILE}\r\n"),
            format!("{digest}\r\n"),
            format!("{sidecar}\n"),
            format!("{sidecar}{sidecar}"),
            format!("{sidecar}{digest}  other.json\n"),
            format!(" {sidecar}"),
            format!("{digest} \n"),
            format!("{digest}\t{EXTENDDB_MANIFEST_FILE}\n"),
            digest[..63].to_owned(),
            format!("{digest}0"),
            format!("{}g", &digest[..63]),
        ] {
            let err = verify_manifest_checksum(&bytes, bad.as_bytes()).unwrap_err();
            assert!(
                matches!(err, FormatError::InvalidManifest(_)),
                "{bad:?}: {err}"
            );
            assert!(err.to_string().len() < 300, "{bad:?}: {err}");
        }
        // Bytes that are not UTF-8 are refused with their own reason, and
        // not shown: the correct line with one byte of the digest replaced,
        // a 0xff appended, the file name with a Latin-1 byte, and 4 KiB of
        // 0xff.
        let mut digest_byte = sidecar.clone().into_bytes();
        digest_byte[10] = 0xff;
        let mut appended = sidecar.clone().into_bytes();
        appended.push(0xff);
        let mut latin1 = sidecar.clone().into_bytes();
        latin1[66] = 0xe9;
        assert_eq!(sidecar.as_bytes()[66], b'e');
        for bad in [digest_byte, appended, latin1, vec![0xff; 4096]] {
            let err = verify_manifest_checksum(&bytes, &bad).unwrap_err();
            assert!(
                matches!(&err, FormatError::InvalidManifest(m)
                    if m == &format!("{MANIFEST_CHECKSUM_FILE} is not UTF-8 ({} bytes)", bad.len())),
                "{bad:?}: {err}"
            );
        }
        // A well-formed sidecar for other bytes is a mismatch, not malformed.
        let other = manifest_checksum(b"{}");
        assert!(matches!(
            verify_manifest_checksum(&bytes, other.as_bytes()),
            Err(FormatError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn sidecar_checksum_detects_tampered_bytes() {
        let bytes = sample_manifest().to_json_bytes().unwrap();
        let sidecar = manifest_checksum(&bytes);
        let mut tampered = bytes.clone();
        let pos = tampered.windows(5).position(|w| w == b"50000").unwrap();
        tampered[pos] = b'7';
        let err = verify_manifest_checksum(&tampered, sidecar.as_bytes()).unwrap_err();
        assert!(matches!(
            err,
            FormatError::ChecksumMismatch {
                subject: "manifest",
                ..
            }
        ));
    }

    #[test]
    fn sidecar_checksum_is_over_stored_bytes_not_a_canonical_form() {
        // Re-serializing with different whitespace changes the digest: the
        // sidecar binds the stored object, nothing else.
        let manifest = sample_manifest();
        let compact = manifest.to_json_bytes().unwrap();
        let pretty = serde_json::to_vec_pretty(&manifest).unwrap();
        assert_ne!(manifest_checksum(&compact), manifest_checksum(&pretty));
        assert!(verify_manifest_checksum(&compact, manifest_checksum(&pretty).as_bytes()).is_err());
        verify_manifest_checksum(&pretty, manifest_checksum(&pretty).as_bytes()).unwrap();
    }

    #[test]
    fn from_json_bytes_runs_version_gate_and_key_check() {
        let mut manifest = sample_manifest();
        manifest.format_version = 2;
        let err = ExtenddbManifest::from_json_bytes(&raw_bytes(&manifest)).unwrap_err();
        assert!(matches!(err, FormatError::UnsupportedFormatVersion { .. }));
        let mut manifest = sample_manifest();
        manifest.data_files[0].key = "../escape.json.gz".to_owned();
        let err = ExtenddbManifest::from_json_bytes(&raw_bytes(&manifest)).unwrap_err();
        assert!(matches!(err, FormatError::InvalidDataFileKey { .. }));
    }

    #[test]
    fn valid_data_file_keys_pass() {
        for key in [
            "data/000001.json.gz",
            "data/42.json.gz",
            "data/1000000.json.gz",
        ] {
            let entry = DataFileEntry {
                key: key.to_owned(),
                item_count: 1,
                sha256: "0".repeat(64),
                md5: String::new(),
                bytes: 1,
                uncompressed_bytes: 1,
                key_range: None,
            };
            entry
                .validate_key()
                .unwrap_or_else(|e| panic!("{key:?} rejected: {e}"));
        }
    }

    #[test]
    fn data_file_key_rejects_bad_shapes() {
        for key in [
            "../x",                     // traversal above the prefix
            "/etc/passwd",              // absolute path, no prefix
            "other/f.json.gz",          // wrong prefix
            "data/../secret.json.gz",   // traversal inside the prefix
            "data/sub/000001.json.gz",  // extra directory level
            "data/",                    // no file
            "data/000001.json",         // wrong suffix
            "data/000001.txt.gz",       // wrong suffix
            "data/abc.json.gz",         // non-digit stem
            "data/.json.gz",            // empty stem
            "data\\000001.json.gz",     // backslash separator
            "data/000001.json.gz.evil", // trailing suffix
        ] {
            let entry = DataFileEntry {
                key: key.to_owned(),
                item_count: 1,
                sha256: "0".repeat(64),
                md5: String::new(),
                bytes: 1,
                uncompressed_bytes: 1,
                key_range: None,
            };
            assert!(
                matches!(
                    entry.validate_key(),
                    Err(FormatError::InvalidDataFileKey { .. })
                ),
                "expected rejection for {key:?}"
            );
        }
    }

    #[test]
    fn validate_data_files_reports_first_bad_key() {
        let mut manifest = sample_manifest();
        manifest.data_files[0].key = "../escape.json.gz".to_owned();
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidDataFileKey { .. })
        ));
    }

    #[test]
    fn entry_decode_rejects_traversal_key_before_reading_bytes() {
        let entry = DataFileEntry {
            key: "../escape.json.gz".to_owned(),
            item_count: 0,
            sha256: "0".repeat(64),
            md5: String::new(),
            bytes: 0,
            uncompressed_bytes: 0,
            key_range: None,
        };
        // Bytes are irrelevant: the key check fires first.
        let err = entry.decode(b"not even gzip").unwrap_err();
        assert!(matches!(err, FormatError::InvalidDataFileKey { .. }));
    }

    /// The Music schema's key order and a range `[first..last]` for the
    /// key-range check tests.
    fn music_check(first: (&str, &str), last: (&str, &str)) -> KeyRangeCheck {
        let order = sample_manifest().key_order().unwrap();
        KeyRangeCheck::new(
            &order,
            &KeyRange {
                first: key(first.0, first.1),
                last: key(last.0, last.1),
            },
        )
    }

    /// Feed `items` and finish; the error, if any, with the 0-based index of
    /// the item that raised it (`items.len()` for `finish`).
    fn run_check(mut check: KeyRangeCheck, items: &[Item]) -> Result<(), (usize, FormatError)> {
        for (i, item) in items.iter().enumerate() {
            check.check(item).map_err(|e| (i, e))?;
        }
        check.finish().map_err(|e| (items.len(), e))
    }

    fn songs(keys: &[(&str, &str)]) -> Vec<Item> {
        keys.iter()
            .map(|(a, t)| {
                let mut item = key(a, t);
                // A non-key attribute is not looked at.
                item.insert(
                    "Genre".to_owned(),
                    crate::types::AttributeValue::S("rock".into()),
                );
                item
            })
            .collect()
    }

    #[test]
    fn key_range_check_accepts_a_conforming_file() {
        run_check(
            music_check(("A", "a"), ("C", "z")),
            &songs(&[("A", "a"), ("A", "b"), ("B", "a"), ("C", "z")]),
        )
        .unwrap();
        // One item that is both endpoints.
        run_check(music_check(("A", "a"), ("A", "a")), &songs(&[("A", "a")])).unwrap();
    }

    #[test]
    fn key_range_check_refuses_each_violation() {
        let violation = |r: Result<(), (usize, FormatError)>| match r {
            Err((i, FormatError::KeyOrderViolation(m))) => (i, m),
            other => panic!("expected a key order violation, got {other:?}"),
        };
        // The first item is not range.first (after it, and before it).
        for first in [("A", "b"), ("0", "a")] {
            let (at, m) = violation(run_check(
                music_check(("A", "a"), ("C", "z")),
                &songs(&[first, ("C", "z")]),
            ));
            assert_eq!(at, 0, "{m}");
            assert!(m.contains("first"), "{m}");
        }
        // Not strictly after the previous item: equal (the same key as item
        // 2), and descending.
        for second in [("B", "a"), ("A", "0")] {
            let (at, m) = violation(run_check(
                music_check(("A", "0"), ("C", "z")),
                &songs(&[("A", "0"), ("B", "a"), second, ("C", "z")]),
            ));
            assert_eq!(at, 2, "{m}");
            assert!(m.contains("not strictly after item 2"), "{m}");
        }
        // Past range.last: refused at that item, not at the end.
        let (at, m) = violation(run_check(
            music_check(("A", "a"), ("C", "z")),
            &songs(&[("A", "a"), ("C", "z"), ("D", "a"), ("E", "a")]),
        ));
        assert_eq!(at, 2, "{m}");
        assert!(m.contains("item 3 is after"), "{m}");
        // The file ends before range.last.
        let (at, m) = violation(run_check(
            music_check(("A", "a"), ("C", "z")),
            &songs(&[("A", "a"), ("B", "a")]),
        ));
        assert_eq!(at, 2, "{m}");
        assert!(m.contains("last item (item 2)"), "{m}");
        // A file with a range and no items.
        let (at, m) = violation(run_check(music_check(("A", "a"), ("C", "z")), &[]));
        assert_eq!(at, 0, "{m}");
        assert!(m.contains("no items"), "{m}");
        // An item without the key is an invalid key, not a pass.
        let mut check = music_check(("A", "a"), ("C", "z"));
        assert!(matches!(
            check.check(&Item::new()),
            Err(FormatError::InvalidKey(_))
        ));
        // A refused item is not recorded: after an item past `last` is
        // refused, the end of the file is judged on the items before it,
        // and a first item that is not `first` leaves the file empty.
        let mut check = music_check(("A", "a"), ("C", "z"));
        for item in songs(&[("A", "a"), ("C", "z")]) {
            check.check(&item).unwrap();
        }
        assert!(check.check(&songs(&[("D", "a")])[0]).is_err());
        check.finish().unwrap();
        let mut check = music_check(("A", "a"), ("C", "z"));
        assert!(check.check(&songs(&[("A", "b")])[0]).is_err());
        let err = check.finish().unwrap_err();
        assert!(err.to_string().contains("no items"), "{err}");
    }

    /// Two files whose ranges do not overlap, each holding every key of
    /// both: the manifest is valid, so only the per-file check can refuse
    /// it, and it does for both files.
    #[test]
    fn non_overlapping_ranges_with_the_same_items_are_refused() {
        let manifest = ordered_files(&[
            (key("A", "a"), key("F", "a")),
            (key("G", "a"), key("M", "a")),
        ]);
        manifest.validate_data_files().unwrap();
        let all: Vec<Item> = [
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        ]
        .iter()
        .map(|a| key(a, "a"))
        .collect();
        let order = manifest.key_order().unwrap();
        for entry in &manifest.data_files {
            let check = KeyRangeCheck::new(&order, entry.key_range.as_ref().unwrap());
            let err = run_check(check, &all).unwrap_err();
            assert!(
                matches!(err.1, FormatError::KeyOrderViolation(_)),
                "{}: {err:?}",
                entry.key
            );
        }
    }

    /// The Music manifest with `N` key types and one data file whose
    /// endpoints are written as given, as JSON bytes.
    fn number_key_manifest(first: &str, last: &str) -> Vec<u8> {
        let mut manifest = ordered_files(&[(key("A", "a"), key("B", "a"))]);
        for d in &mut manifest.source_table.attribute_definitions {
            d.attribute_type = ScalarAttributeType::N;
        }
        let mut doc = serde_json::to_value(&manifest).unwrap();
        let range = &mut doc["data_files"][0]["key_range"];
        range["first"] = serde_json::json!({"Artist": {"N": first}, "SongTitle": {"N": "1"}});
        range["last"] = serde_json::json!({"Artist": {"N": last}, "SongTitle": {"N": "1"}});
        serde_json::to_vec(&doc).unwrap()
    }

    /// An `N` endpoint is read as written: a valid number in any spelling
    /// but its normalized one is refused, so the sidecar-verified bytes have
    /// one reading (`"6E0"` and `"6"` would otherwise both read as 6).
    #[test]
    fn un_normalized_number_endpoints_are_refused() {
        let parsed = ExtenddbManifest::from_json_bytes(&number_key_manifest("1", "5"))
            .expect("normalized endpoints parse");
        assert_eq!(
            parsed.data_files[0].key_range.as_ref().unwrap().last["Artist"],
            crate::types::AttributeValue::N("5".into())
        );
        for (first, last) in [("1", "5.0"), ("1", "6E0"), ("01", "5"), ("1.50", "5")] {
            let err =
                ExtenddbManifest::from_json_bytes(&number_key_manifest(first, last)).unwrap_err();
            assert!(
                matches!(err, FormatError::Json(_)) && err.to_string().contains("normalized form"),
                "{first}..{last}: {err}"
            );
        }
        // An invalid number is left to the key check, which names it.
        let err =
            ExtenddbManifest::from_json_bytes(&number_key_manifest("1", "1E999")).unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidKey(m) if m.contains("not a valid number")),
            "{err}"
        );
    }

    /// A stored `N` key that `validate_key_item` refuses (`BatchWriteItem`
    /// stores `abc`, `1E126` and `1E-131` as written) cannot be in a
    /// key-ordered backup: a manifest with it as an endpoint is refused by
    /// the writer, and the per-file check refuses an item with it wherever
    /// it falls, each naming the key.
    #[test]
    fn invalid_stored_number_keys_are_refused_in_a_key_ordered_backup() {
        let n = |artist: &str| -> Item {
            [
                (
                    "Artist".to_owned(),
                    crate::types::AttributeValue::N(artist.to_owned()),
                ),
                (
                    "SongTitle".to_owned(),
                    crate::types::AttributeValue::N("1".to_owned()),
                ),
            ]
            .into_iter()
            .collect()
        };
        let numbered = |first: &str, last: &str| {
            let mut manifest = ordered_files(&[(n(first), n(last))]);
            for d in &mut manifest.source_table.attribute_definitions {
                d.attribute_type = ScalarAttributeType::N;
            }
            manifest
        };
        numbered("1", "2").to_json_bytes().unwrap();
        for bad in ["abc", "1E126", "1E-131"] {
            for (first, last) in [(bad, "2"), ("1", bad)] {
                let err = numbered(first, last).to_json_bytes().unwrap_err();
                assert!(
                    matches!(&err, FormatError::InvalidKey(m)
                        if m.contains("\"Artist\"") && m.contains(&format!("{bad:?}"))),
                    "{first}..{last}: {err}"
                );
            }
        }
        let order = numbered("1", "2").key_order().unwrap();
        let range = KeyRange {
            first: n("1"),
            last: n("2"),
        };
        // As text, `1E126` and `1E-131` sort between `1` and `2`, so only
        // the key check refuses them.
        for bad in ["1E126", "1E-131"] {
            let err = run_check(
                KeyRangeCheck::new(&order, &range),
                &[n("1"), n(bad), n("2")],
            )
            .unwrap_err();
            assert_eq!(err.0, 1, "{bad}: {err:?}");
            assert!(
                matches!(&err.1, FormatError::InvalidKey(m)
                    if m.contains(&format!(r#"{{"Artist":{{"N":"{bad}"}}"#))
                        && m.contains("not a valid number")),
                "{bad}: {err:?}"
            );
        }
        // A range built in code whose endpoints are the invalid key itself.
        let abc = KeyRange {
            first: n("abc"),
            last: n("abc"),
        };
        let err = run_check(KeyRangeCheck::new(&order, &abc), &[n("abc")]).unwrap_err();
        assert!(
            matches!(&err, (0, FormatError::InvalidKey(m)) if m.contains(r#""N":"abc""#)),
            "{err:?}"
        );
    }

    #[test]
    fn negative_source_counts_are_refused() {
        for (field, value) in [("item_count", -1), ("table_size_bytes", i64::MIN)] {
            let mut doc = serde_json::to_value(sample_manifest()).unwrap();
            doc["source_table"][field] = serde_json::json!(value);
            let err =
                ExtenddbManifest::from_json_bytes(&serde_json::to_vec(&doc).unwrap()).unwrap_err();
            assert!(
                matches!(&err, FormatError::InvalidManifest(m) if m.contains(field)),
                "{field}: {err}"
            );
        }
        let mut manifest = sample_manifest();
        manifest.source_table.item_count = 0;
        manifest.source_table.table_size_bytes = 0;
        manifest.validate_source_table().unwrap();
    }

    #[test]
    fn empty_string_and_binary_endpoints_are_refused() {
        let order = sample_manifest().key_order().unwrap();
        let err = order.validate_key_item(&key("", "a")).unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidKey(m) if m.contains("empty")),
            "{err}"
        );
        let err = order.validate_key_item(&key("a", "")).unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidKey(m) if m.contains("empty")),
            "{err}"
        );
        let binary = KeyOrder::resolve(
            &[KeySchemaElement {
                attribute_name: "b".to_owned(),
                key_type: KeyType::Hash,
            }],
            &[AttributeDefinition {
                attribute_name: "b".to_owned(),
                attribute_type: ScalarAttributeType::B,
            }],
        )
        .unwrap();
        let item = |bytes: Vec<u8>| -> Item {
            [("b".to_owned(), crate::types::AttributeValue::B(bytes))]
                .into_iter()
                .collect()
        };
        assert!(binary.validate_key_item(&item(vec![])).is_err());
        binary.validate_key_item(&item(vec![0])).unwrap();
        // Parsed endpoints go through the same check.
        let mut manifest = ordered_files(&[(key("", "a"), key("B", "a"))]);
        manifest.backup_size_bytes = 100;
        assert!(matches!(
            ExtenddbManifest::from_json_bytes(&raw_bytes(&manifest)),
            Err(FormatError::InvalidKey(_))
        ));
    }

    /// Every version from 1 to the current one is read; 0 and later ones
    /// are refused with a message that names the accepted range, not only
    /// its upper end.
    #[test]
    fn format_versions_from_one_to_current_are_read() {
        for v in 1..=CURRENT_FORMAT_VERSION {
            check_format_version(v).unwrap();
        }
        for v in [0, CURRENT_FORMAT_VERSION + 1, u32::MAX] {
            let err = check_format_version(v).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!(
                    "unsupported backup format version {v}; this build reads format \
                     versions 1 to {CURRENT_FORMAT_VERSION}"
                )
            );
        }
    }

    /// A key-ordered manifest of `files`, each the encoded file of its
    /// items with the range its first and last items' keys give (or
    /// `range` for every file, when set), under the Music schema with `ty`
    /// keys. Returns the manifest and each file's bytes.
    fn encoded_manifest(
        files: &[Vec<Item>],
        ty: ScalarAttributeType,
        range: Option<&KeyRange>,
    ) -> (ExtenddbManifest, Vec<Vec<u8>>) {
        let mut manifest = sample_manifest();
        manifest.item_order = ItemOrder::KeyOrdered;
        for d in &mut manifest.source_table.attribute_definitions {
            d.attribute_type = ty;
        }
        let order = manifest.key_order().unwrap();
        let mut bytes = Vec::new();
        manifest.data_files = files
            .iter()
            .enumerate()
            .map(|(i, items)| {
                let (file, checksums) =
                    crate::backup_format::encode_data_file(items.iter().cloned()).unwrap();
                bytes.push(file);
                let range = range.cloned().unwrap_or_else(|| KeyRange {
                    first: order.endpoint_key(&items[0]).unwrap(),
                    last: order.endpoint_key(items.last().unwrap()).unwrap(),
                });
                DataFileEntry::from_checksums(
                    crate::backup_format::data_file_key(u32::try_from(i + 1).unwrap()),
                    &checksums,
                    Some(range),
                )
            })
            .collect();
        manifest.backup_size_bytes = manifest
            .data_files
            .iter()
            .map(|f| i64::try_from(f.bytes).unwrap())
            .sum();
        (manifest, bytes)
    }

    fn read_all(
        manifest: &ExtenddbManifest,
        entry: &DataFileEntry,
        bytes: &[u8],
    ) -> Result<Vec<Item>, FormatError> {
        manifest.reader(entry, bytes)?.collect()
    }

    /// A number key whose exponent is at the end of `i32` and whose value
    /// overflows is refused with the overflow reason, as a `key_range`
    /// endpoint and as a key in a data file line, not with a panic or the
    /// underflow reason.
    #[test]
    fn exponent_at_the_end_of_i32_is_refused_as_an_overflow() {
        let overflow = "Number overflow. Attempting to store a number with magnitude larger \
                        than supported range";
        let err = ExtenddbManifest::from_json_bytes(&number_key_manifest("1", "10E2147483647"))
            .unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidKey(m) if m.contains(overflow)),
            "{err}"
        );
        let n = |artist: &str| -> Item {
            [
                (
                    "Artist".to_owned(),
                    crate::types::AttributeValue::N(artist.to_owned()),
                ),
                (
                    "SongTitle".to_owned(),
                    crate::types::AttributeValue::N("1".to_owned()),
                ),
            ]
            .into_iter()
            .collect()
        };
        let range = KeyRange {
            first: n("1"),
            last: n("2"),
        };
        let (manifest, bytes) = encoded_manifest(
            &[vec![n("1"), n("10E2147483647"), n("2")]],
            ScalarAttributeType::N,
            Some(&range),
        );
        let err = read_all(&manifest, &manifest.data_files[0], &bytes[0]).unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidKey(m)
                if m.contains(overflow) && m.contains(r#""N":"10E2147483647""#)),
            "{err}"
        );
    }

    /// The writer's tracker refuses a key that does not ascend strictly,
    /// a repeated one included, and records nothing for it; it gives the
    /// file's range at the end.
    #[test]
    fn key_range_tracker_refuses_out_of_order_and_repeated_keys() {
        let order = sample_manifest().key_order().unwrap();
        let violation = |r: Result<(), FormatError>| match r {
            Err(FormatError::KeyOrderViolation(m)) => m,
            other => panic!("expected a key order violation, got {other:?}"),
        };
        for (items, at) in [
            // The same key twice.
            (songs(&[("A", "a"), ("A", "b"), ("A", "b")]), 3),
            // Descending range key, then descending hash key.
            (songs(&[("A", "b"), ("A", "a")]), 2),
            (songs(&[("B", "a"), ("A", "z")]), 2),
        ] {
            let mut tracker = KeyRangeTracker::new(&order);
            for item in &items[..at - 1] {
                tracker.push(item).unwrap();
            }
            let m = violation(tracker.push(&items[at - 1]));
            assert_eq!(
                m,
                format!("item {at} is not strictly after item {}", at - 1),
                "{items:?}"
            );
            // The refused item is not recorded: the range ends at the item
            // before it.
            assert_eq!(tracker.item_count(), u64::try_from(at - 1).unwrap());
            let range = tracker.finish().unwrap();
            assert_eq!(range.last, order.endpoint_key(&items[at - 2]).unwrap());
        }
        // A conforming stream gives its first and last keys, key attributes
        // only; one item is both; none is no range.
        let items = songs(&[("A", "a"), ("A", "b"), ("B", "a"), ("C", "z")]);
        let mut tracker = KeyRangeTracker::new(&order);
        for item in &items {
            tracker.push(item).unwrap();
        }
        assert_eq!(
            tracker.finish().unwrap(),
            KeyRange {
                first: key("A", "a"),
                last: key("C", "z"),
            }
        );
        let mut tracker = KeyRangeTracker::new(&order);
        tracker.push(&items[1]).unwrap();
        let range = tracker.finish().unwrap();
        assert_eq!((range.first, range.last), (key("A", "b"), key("A", "b")));
        assert!(KeyRangeTracker::new(&order).finish().is_none());
        // A key `endpoint_key` refuses is refused with its error, and not
        // recorded.
        let mut tracker = KeyRangeTracker::new(&order);
        tracker.push(&items[0]).unwrap();
        let mut empty = key("A", "b");
        empty.insert(
            "SongTitle".to_owned(),
            crate::types::AttributeValue::S(String::new()),
        );
        let err = tracker.push(&empty).unwrap_err();
        assert_eq!(
            err.to_string(),
            order.endpoint_key(&empty).unwrap_err().to_string()
        );
        assert_eq!(tracker.item_count(), 1);
    }

    /// The tracker and the check compare in `KeyOrder::compare`'s order for
    /// every key type, including keys whose held storage is reused with a
    /// different length (`9`, `10`, `100.5`) and binary hash keys, which
    /// sort by their base64 text.
    #[test]
    fn key_range_tracker_agrees_with_compare_for_every_key_type() {
        use crate::types::AttributeValue::{B, N, S};
        let schema = |hash: ScalarAttributeType, range: ScalarAttributeType| {
            let mut manifest = sample_manifest();
            manifest.source_table.attribute_definitions[0].attribute_type = hash;
            manifest.source_table.attribute_definitions[1].attribute_type = range;
            manifest.key_order().unwrap()
        };
        let k = |h: crate::types::AttributeValue, r: crate::types::AttributeValue| -> Item {
            [("Artist".to_owned(), h), ("SongTitle".to_owned(), r)]
                .into_iter()
                .collect()
        };
        let cases: Vec<(KeyOrder, Vec<Item>)> = vec![
            (
                schema(ScalarAttributeType::S, ScalarAttributeType::N),
                vec![
                    k(S("a".into()), N("-1.5".into())),
                    k(S("a".into()), N("9".into())),
                    k(S("a".into()), N("10".into())),
                    k(S("a".into()), N("100.5".into())),
                    k(S("b".into()), N("1".into())),
                    k(S("é".into()), N("0".into())),
                ],
            ),
            (
                schema(ScalarAttributeType::B, ScalarAttributeType::B),
                vec![
                    k(B(vec![0xff]), B(vec![0x00])),
                    k(B(vec![0xff]), B(vec![0x00, 0x00])),
                    k(B(vec![0x00]), B(vec![0x01])),
                    k(B(vec![0x00, 0x01]), B(vec![0x01])),
                ],
            ),
            (
                schema(ScalarAttributeType::N, ScalarAttributeType::S),
                vec![
                    k(N("10".into()), S("b".into())),
                    k(N("10".into()), S("ba".into())),
                    k(N("9".into()), S("a".into())),
                ],
            ),
        ];
        for (order, items) in cases {
            for pair in items.windows(2) {
                assert_eq!(
                    order.compare(&pair[0], &pair[1]).unwrap(),
                    std::cmp::Ordering::Less,
                    "{pair:?}"
                );
            }
            let mut tracker = KeyRangeTracker::new(&order);
            for item in &items {
                tracker.push(item).unwrap();
            }
            let range = tracker.finish().unwrap();
            assert_eq!(range.first, items[0]);
            assert_eq!(&range.last, items.last().unwrap());
            run_check(KeyRangeCheck::new(&order, &range), &items).unwrap();
            // Every adjacent pair swapped is refused at the second item.
            for i in 1..items.len() {
                let mut swapped = items.clone();
                swapped.swap(i - 1, i);
                let mut tracker = KeyRangeTracker::new(&order);
                let refused = swapped.iter().position(|item| tracker.push(item).is_err());
                assert_eq!(refused, Some(i), "{swapped:?}");
            }
        }
    }

    /// `reader` applies the key range check without the caller building
    /// one: a forged key-ordered file holding a key after its range's
    /// `last`, or ending before it, is refused through `reader`, and a
    /// conforming file reads whole. The errors end the stream.
    #[test]
    fn reader_runs_the_key_range_check() {
        let items = songs(&[("A", "a"), ("A", "b"), ("B", "a")]);
        let (manifest, bytes) =
            encoded_manifest(std::slice::from_ref(&items), ScalarAttributeType::S, None);
        let entry = &manifest.data_files[0];
        assert_eq!(read_all(&manifest, entry, &bytes[0]).unwrap(), items);
        // The same bytes under a range that ends before the file does: the
        // third item is after `last`.
        let mut forged = entry.clone();
        forged.key_range.as_mut().unwrap().last = key("A", "b");
        let mut reader = manifest.reader(&forged, &bytes[0][..]).unwrap();
        assert_eq!(reader.next_item().unwrap().as_ref(), Some(&items[0]));
        assert_eq!(reader.next_item().unwrap().as_ref(), Some(&items[1]));
        let err = reader.next_item().unwrap_err();
        assert!(
            matches!(&err, FormatError::KeyOrderViolation(m) if m.contains("after")),
            "{err}"
        );
        // Sticky for `next_item`, fused for the iterator.
        assert!(matches!(reader.next_item(), Err(FormatError::Decode(_))));
        assert!(reader.next().is_none());
        // A range whose `last` is after the file's last item: refused at the
        // end of the stream.
        let mut forged = entry.clone();
        forged.key_range.as_mut().unwrap().last = key("C", "a");
        let err = read_all(&manifest, &forged, &bytes[0]).unwrap_err();
        assert!(
            matches!(&err, FormatError::KeyOrderViolation(m) if m.contains("last item")),
            "{err}"
        );
        // Two files with non-overlapping ranges, each holding the other's
        // key: every read refuses its file.
        let (manifest, bytes) = encoded_manifest(
            &[
                songs(&[("A", "a"), ("C", "a")]),
                songs(&[("D", "a"), ("E", "a")]),
            ],
            ScalarAttributeType::S,
            None,
        );
        let mut duplicate = manifest.clone();
        duplicate.data_files[0].key_range.as_mut().unwrap().last = key("B", "a");
        duplicate.validate_data_files().unwrap();
        assert!(read_all(&duplicate, &duplicate.data_files[0], &bytes[0]).is_err());
        assert!(read_all(&manifest, &manifest.data_files[1], &bytes[1]).is_ok());
        // The entry and the manifest must agree on order.
        let mut unranged = entry.clone();
        unranged.key_range = None;
        assert!(matches!(
            manifest.reader(&unranged, &bytes[0][..]),
            Err(FormatError::InvalidDataFileEntry { .. })
        ));
        let mut unordered = manifest.clone();
        unordered.item_order = ItemOrder::Unordered;
        assert!(matches!(
            unordered.reader(&manifest.data_files[0], &bytes[0][..]),
            Err(FormatError::InvalidDataFileEntry { .. })
        ));
        // An unordered manifest reads without a check.
        let mut unordered_entry = manifest.data_files[0].clone();
        unordered_entry.key_range = None;
        assert_eq!(
            read_all(&unordered, &unordered_entry, &bytes[0]).unwrap(),
            songs(&[("A", "a"), ("C", "a")])
        );
    }

    /// `reader` refuses a file whose first item is not its range's `first`
    /// and a file with no items under a range, and `reader_prehashed`
    /// applies the same check while leaving the digests uncompared.
    #[test]
    fn reader_checks_both_endpoints_and_reader_prehashed_skips_the_digest() {
        let (manifest, bytes) = encoded_manifest(
            &[
                songs(&[("A", "a"), ("C", "a")]),
                songs(&[("D", "a"), ("E", "a")]),
            ],
            ScalarAttributeType::S,
            None,
        );
        let mut unordered = manifest.clone();
        unordered.item_order = ItemOrder::Unordered;
        // The prehashed reader applies the same check.
        let mut forged = manifest.data_files[0].clone();
        forged.key_range.as_mut().unwrap().last = key("B", "a");
        let mut reader = manifest.reader_prehashed(&forged, &bytes[0][..]).unwrap();
        reader.next_item().unwrap();
        assert!(matches!(
            reader.next_item(),
            Err(FormatError::KeyOrderViolation(_))
        ));
        // The prehashed reader does not compare the digests: an entry with
        // wrong digests reads whole through it, and `reader` refuses the
        // same entry at the end of the stream.
        let mut wrong_digests = manifest.data_files[0].clone();
        wrong_digests.sha256 = "0".repeat(64);
        wrong_digests.md5 = "AAAAAAAAAAAAAAAAAAAAAA==".to_owned();
        let items: Vec<Item> = manifest
            .reader_prehashed(&wrong_digests, &bytes[0][..])
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(items, songs(&[("A", "a"), ("C", "a")]));
        assert!(matches!(
            read_all(&manifest, &wrong_digests, &bytes[0]),
            Err(FormatError::ChecksumMismatch { .. })
        ));
        // A range whose `first` is not the file's first item: refused at
        // item 1, before anything is returned.
        let mut forged = manifest.data_files[0].clone();
        forged.key_range.as_mut().unwrap().first = key("A", "b");
        let mut reader = manifest.reader(&forged, &bytes[0][..]).unwrap();
        let err = reader.next_item().unwrap_err();
        assert!(
            matches!(&err, FormatError::KeyOrderViolation(m) if m.contains("first item")),
            "{err}"
        );
        assert!(reader.next().is_none());
        // A file with no items under a key range: the decoder's checks pass
        // (the entry records zero items and zero bytes) and the range check
        // refuses the end of the stream.
        let (empty_bytes, checksums) =
            crate::backup_format::encode_data_file(std::iter::empty()).unwrap();
        assert_eq!(checksums.item_count, 0);
        let empty = DataFileEntry::from_checksums(
            crate::backup_format::data_file_key(3),
            &checksums,
            manifest.data_files[0].key_range.clone(),
        );
        let mut reader = manifest.reader(&empty, &empty_bytes[..]).unwrap();
        let err = reader.next_item().unwrap_err();
        assert!(
            matches!(&err, FormatError::KeyOrderViolation(m) if m.contains("no items")),
            "{err}"
        );
        let mut without_range = empty.clone();
        without_range.key_range = None;
        assert_eq!(
            read_all(&unordered, &without_range, &empty_bytes).unwrap(),
            Vec::<Item>::new()
        );
    }

    /// One tracker over a whole backup: `next_file` closes a file with its
    /// range and keeps the last key, so a stream whose only violation is at
    /// a file boundary (the next file starts with the previous file's last
    /// key, or with a key before it) is refused at item 1 of the next file,
    /// with nothing of that file encoded. A conforming boundary gives both
    /// files their ranges, the counts start again per file, and a file with
    /// no items closes as `None`.
    #[test]
    fn key_range_tracker_checks_ascent_across_files() {
        let order = sample_manifest().key_order().unwrap();
        let file_1 = songs(&[("A", "a"), ("A", "b"), ("B", "a")]);
        let write_file_1 = || {
            let mut tracker = KeyRangeTracker::new(&order);
            for item in &file_1 {
                tracker.push(item).unwrap();
            }
            tracker
        };
        // Conforming: the second file's first key is after the first's last.
        let mut tracker = write_file_1();
        assert_eq!(tracker.item_count(), 3);
        assert_eq!(
            tracker.next_file().unwrap(),
            KeyRange {
                first: key("A", "a"),
                last: key("B", "a"),
            }
        );
        assert_eq!(tracker.item_count(), 0);
        let file_2 = songs(&[("B", "b"), ("C", "a")]);
        for item in &file_2 {
            tracker.push(item).unwrap();
        }
        assert_eq!(tracker.item_count(), 2);
        assert_eq!(
            tracker.finish().unwrap(),
            KeyRange {
                first: key("B", "b"),
                last: key("C", "a"),
            }
        );
        // The boundary violations: the last key of file 1 again, and a key
        // before it; each refused at item 1 of file 2, naming the boundary
        // and its key, with nothing recorded and nothing encoded.
        for first_of_file_2 in [("B", "a"), ("A", "z")] {
            let mut tracker = write_file_1();
            tracker.next_file().unwrap();
            let mut encoder = crate::backup_format::DataFileEncoder::new(Vec::new());
            let items = songs(&[first_of_file_2, ("C", "a")]);
            let mut refused = None;
            for (i, item) in items.iter().enumerate() {
                if let Err(e) = tracker.push(item) {
                    refused = Some((i, e));
                    break;
                }
                encoder.write_item(item).unwrap();
            }
            let (at, err) = refused.expect("the boundary is refused");
            assert_eq!(at, 0, "{first_of_file_2:?}");
            assert_eq!(
                err.to_string(),
                format!(
                    "data file items break key order: item 1 is not strictly after the \
                     previous file's last item, key {}",
                    serde_json::to_string(&key("B", "a")).unwrap()
                ),
                "{first_of_file_2:?}"
            );
            assert_eq!(encoder.uncompressed_bytes(), 0);
            assert_eq!(tracker.item_count(), 0);
            assert!(tracker.finish().is_none());
        }
        // Within a file the message names the items, as before.
        let mut tracker = write_file_1();
        let err = tracker.push(&file_1[2]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "data file items break key order: item 4 is not strictly after item 3"
        );
        // Closing a file with no items changes nothing.
        let mut tracker = KeyRangeTracker::new(&order);
        assert!(tracker.next_file().is_none());
        tracker.push(&file_1[0]).unwrap();
        assert!(tracker.next_file().is_some());
        assert!(tracker.next_file().is_none());
        assert_eq!(tracker.item_count(), 0);
        assert!(tracker.push(&file_1[0]).is_err());
        tracker.push(&file_1[1]).unwrap();
        assert_eq!(
            tracker.finish().unwrap(),
            KeyRange {
                first: key("A", "b"),
                last: key("A", "b"),
            }
        );
    }
}
