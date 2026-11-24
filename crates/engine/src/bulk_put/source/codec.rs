// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Byte-stream adapters shared by the reader: the counting, hashing
//! reader and the multi-frame zstd decoder.

use std::io::{BufRead, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Counts raw file bytes as they are pulled, from beneath the gzip decoder
/// when one is stacked on top, so the figure is compressed bytes and
/// comparable to the enumerated size. Buffered readahead can run past the
/// lines handed out so far; the enumerated size stays the ceiling the
/// runner reports against.
pub(super) struct CountingReader<R> {
    pub(super) inner: R,
    pub(super) count: Arc<AtomicU64>,
    /// Running MD5 of the raw bytes read, the file's entity tag once the
    /// stream is consumed whole. Shared with the line reader, which reports
    /// it at end of file. A poisoned lock (a panic mid-update) means no tag
    /// is reported and the manifest hashes the file instead.
    pub(super) hasher: Arc<Mutex<md5::Md5>>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count
            .fetch_add(u64::try_from(n).unwrap_or(u64::MAX), Ordering::Relaxed);
        if let Ok(mut hasher) = self.hasher.lock() {
            md5::Digest::update(&mut *hasher, &buf[..n]);
        }
        Ok(n)
    }
}

/// Reads every frame of a concatenated zstd stream (RFC 8878 allows frames
/// back to back, and the reference tool writes them that way for appended
/// inputs), the way `MultiGzDecoder` reads every gzip member. The one-frame
/// `StreamingDecoder` answers end of file at the first frame's end; this
/// adapter starts the next frame while the source still has bytes. A
/// decoder failure in the first frame is reported as `InvalidData` so
/// `read_capped_line` classifies it as a failed decompression; a failure
/// in a later frame ends the file instead, with the remaining bytes read
/// and discarded so the byte count and the entity tag still cover the whole
/// object (measured on the service: a complete frame followed by a
/// truncated one is read as the first frame's rows alone,
/// with no failed row and every byte counted, where a truncated first
/// frame is one failed row). An I/O failure of the source underneath (any
/// kind other than the decoder's own end-of-input) keeps its kind and is
/// reported as the file's failure.
pub(super) struct ZstdFrames<R: BufRead> {
    source: R,
    decoder: ruzstd::decoding::FrameDecoder,
    /// Frames read to their end so far.
    completed_frames: u64,
}

impl<R: BufRead> ZstdFrames<R> {
    pub(super) fn new(mut source: R) -> std::io::Result<Self> {
        let mut decoder = ruzstd::decoding::FrameDecoder::new();
        decoder.init(&mut source).map_err(Self::classify)?;
        Ok(Self {
            source,
            decoder,
            completed_frames: 0,
        })
    }

    /// Map a decoder error: a source I/O error inside it keeps its kind
    /// (a read of the jailed file failed), anything else is corrupt data.
    fn classify(error: ruzstd::decoding::errors::FrameDecoderError) -> std::io::Error {
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        while let Some(current) = cause {
            if let Some(io) = current.downcast_ref::<std::io::Error>()
                && io.kind() != std::io::ErrorKind::UnexpectedEof
            {
                // A fault the store rendered, and a store failure that
                // outlasted its retries, travel on unchanged.
                if let Some(fault) = super::StoreFault::of_io(io) {
                    return std::io::Error::other(fault);
                }
                if let Some(unavailable) = io
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<super::StoreUnavailable>())
                {
                    return std::io::Error::other(unavailable.clone());
                }
                return std::io::Error::new(io.kind(), error.to_string());
            }
            cause = current.source();
        }
        std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
    }

    /// A decoder failure past the first frame ends the file (see the type
    /// doc); the bytes left in the source are consumed so the counters
    /// and the tag describe the whole object. A source I/O error is still
    /// the file's failure.
    fn after_decoder_error(&mut self, error: std::io::Error) -> std::io::Result<usize> {
        if self.completed_frames == 0 || error.kind() != std::io::ErrorKind::InvalidData {
            return Err(error);
        }
        std::io::copy(&mut self.source, &mut std::io::sink())?;
        Ok(0)
    }
}

impl<R: BufRead> Read for ZstdFrames<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use ruzstd::decoding::BlockDecodingStrategy;
        loop {
            if self.decoder.is_finished() && self.decoder.can_collect() == 0 {
                // The frame is drained: end of file if the source is spent,
                // otherwise the next frame begins here.
                self.completed_frames += 1;
                if self.source.fill_buf()?.is_empty() {
                    return Ok(0);
                }
                if let Err(error) = self.decoder.reset(&mut self.source).map_err(Self::classify) {
                    return self.after_decoder_error(error);
                }
                continue;
            }
            while self.decoder.can_collect() == 0 && !self.decoder.is_finished() {
                if let Err(error) = self
                    .decoder
                    .decode_blocks(&mut self.source, BlockDecodingStrategy::UptoBlocks(1))
                    .map_err(Self::classify)
                {
                    return self.after_decoder_error(error);
                }
            }
            let n = self.decoder.read(buf)?;
            if n > 0 || buf.is_empty() {
                return Ok(n);
            }
        }
    }
}
