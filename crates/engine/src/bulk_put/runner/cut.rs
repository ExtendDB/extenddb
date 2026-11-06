// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The abandon cut: how many bytes of an input object the progress counter
//! holds after an abandon part way through it, by the read geometry the
//! hosted service was measured to use.
//!
//! A plain object is read in 8,000-byte reads (measured on the service).
//! A compressed object is read through its decoder
//! and the counter holds the compressed bytes the decoder's reads pulled
//! (measured on the service). A gzip object is read as its 10-byte header then
//! 65,536-byte reads (faults leave the counter at 65,546, 131,082 and the
//! whole 30,758-byte object). A zstd object is read in 131,075-byte reads,
//! the decoder's input size for one block of the largest size plus its
//! header (a fault in the first read of a larger object leaves the counter
//! at 131,075; faults inside the second and the first read of objects
//! those reads cover count the whole object). When bytes of the object lie beyond the read that
//! held the fault, they are never counted and the operation ends `FAILED`
//! with the items-failed-validation pair; when the fault's read reached
//! the end of the object, its bytes count in full and the operation ends
//! `COMPLETED_WITH_ERRORS`.

use super::InputCodec;

/// Bytes of one read of a plain input object (measured on the service:
/// `ProcessedInputBytes` moves in whole multiples of
/// this figure, and an abandon leaves the counter at the end of the read
/// that held the fault).
pub(super) const READ_CHUNK_BYTES: u64 = 8_000;

/// Bytes of one read of a gzip object after its header read (measured on
/// the service: the counter stops at 10 plus whole reads of this size).
pub(super) const GZIP_READ_BYTES: u64 = 65_536;

/// Bytes of the header read of a gzip object.
const GZIP_HEAD_BYTES: u64 = 10;

/// Bytes of one read of a zstd object: the largest block, 128 KiB, plus
/// its 3-byte header, the input size a zstd decoder asks for (measured on
/// the service: the counter stops at exactly this figure, and whole objects
/// of 198,831 and 99,334 bytes count in full with faults inside the second
/// and the first read).
pub(super) const ZSTD_READ_BYTES: u64 = 131_075;

/// The read geometry of one input object: the size of the first read,
/// which is the codec's header read, and the size of every read after it.
/// A plain object has no header read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ReadGeometry {
    pub(super) head: u64,
    pub(super) chunk: u64,
}

impl ReadGeometry {
    /// The geometry the hosted service reads an object of `codec` with.
    pub(super) fn of(codec: InputCodec) -> Self {
        match codec {
            InputCodec::None => Self {
                head: 0,
                chunk: READ_CHUNK_BYTES,
            },
            InputCodec::Gzip => Self {
                head: GZIP_HEAD_BYTES,
                chunk: GZIP_READ_BYTES,
            },
            InputCodec::Zstd => Self {
                head: 0,
                chunk: ZSTD_READ_BYTES,
            },
        }
    }

    /// The index of the read holding the byte at `offset`: 0 is the header
    /// read, 1 the first read after it.
    fn read_index(self, offset: u64) -> u64 {
        if offset < self.head {
            0
        } else {
            1 + (offset - self.head) / self.chunk
        }
    }

    /// The exclusive end offset of read `index`.
    fn read_end(self, index: u64) -> u64 {
        self.head.saturating_add(index.saturating_mul(self.chunk))
    }

    /// The processed bytes of an object abandoned at `fault_offset`, the
    /// offset within the stored object just past the last byte the fault is
    /// attributed to, when `size` is the object's enumerated size.
    ///
    /// `Some(bytes)` means the object is cut: the fault's read left bytes of
    /// the object behind it, those bytes are never counted, and the
    /// operation ends with the items-failed-validation pair. `None` means
    /// the fault's read reached the end of the object, so its bytes count
    /// in full and the terminal status follows the ordinary rule.
    pub(super) fn cut_processed_bytes(self, fault_offset: u64, size: u64) -> Option<u64> {
        if size == 0 {
            return None;
        }
        // `fault_offset` is exclusive, so the read that held the fault is
        // the one holding the byte before it: a line ending exactly on a
        // read boundary belongs to the read before the boundary.
        let fault_read = self.read_index(fault_offset.saturating_sub(1));
        let last_read = self.read_index(size - 1);
        (fault_read < last_read).then(|| self.read_end(fault_read))
    }
}

/// The plain-object cut (measured on the service): a fault on line 1 of
/// 23,840 bytes gives 8,000 processed, FAILED; a fault at about byte
/// 1,100 of 9,990 gives 8,000 processed, FAILED; a fault on line 1 of
/// 1,980 bytes gives 1,980 processed, COMPLETED_WITH_ERRORS; a fault on
/// the last line of 23,840 bytes gives 23,840 processed,
/// COMPLETED_WITH_ERRORS. Kept for the plain-object tests; the runner
/// calls the geometry directly.
#[cfg(test)]
pub(super) fn cut_processed_bytes(fault_offset: u64, size: u64) -> Option<u64> {
    ReadGeometry::of(InputCodec::None).cut_processed_bytes(fault_offset, size)
}
