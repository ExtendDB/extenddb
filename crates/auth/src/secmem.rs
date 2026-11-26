// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The locked key store: 32-byte key slots in memory that stays out of swap
//! and out of core dumps (RFC-0000, section 6.2).
//!
//! The store is set up once per process with [`init`] or [`init_unlocked`].
//! It holds the signing keys of the credential cache, the data encryption
//! key, and the temporary buffers of the cache. A [`KeySlot`] is one slot;
//! it is wiped and returned to the store when it drops. Before any init a
//! slot is a heap `Zeroizing` buffer, so tests and tools need no setup.
//!
//! On Linux the store tries `memfd_secret` first. Its pages leave the kernel
//! direct map, never reach swap and stay out of core dumps. When the kernel
//! has no `memfd_secret` or refuses it, the store is an anonymous private
//! mapping under `mlock`, `MADV_DONTDUMP` and `MADV_WIPEONFORK`. macOS locks
//! with `mlock`, Windows with `VirtualLock`. Other targets cannot lock.

use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use zeroize::{Zeroize, Zeroizing};

/// The size of one slot: an AES-256 key or an HMAC-SHA256 signing key.
const SLOT_BYTES: usize = 32;

static STORE: OnceLock<Arc<Store>> = OnceLock::new();

/// How the store's pages are protected.
///
/// `Display` gives the word for the startup log line: `memfd_secret`,
/// `locked pages` or `none`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protection {
    /// Linux `memfd_secret` pages: outside the kernel direct map, never
    /// swapped, absent from core dumps.
    MemfdSecret,
    /// An anonymous private mapping under `mlock`; on Linux also
    /// `MADV_DONTDUMP` and `MADV_WIPEONFORK`. macOS: `mlock`. Windows:
    /// `VirtualLock`.
    LockedPages,
    /// Ordinary heap memory: before `init`, after `init_unlocked`, and on
    /// targets with no lock call.
    Unlocked,
}

impl fmt::Display for Protection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::MemfdSecret => "memfd_secret",
            Self::LockedPages => "locked pages",
            Self::Unlocked => "none",
        })
    }
}

/// The store could not lock its memory.
///
/// `Display` names the bytes needed, the call that failed with its errno
/// text, and the two fixes. It never names a key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockError {
    /// The locked memory the store needs, `required_bytes(slots)`.
    pub bytes: usize,
    /// The call that failed: `memfd_secret`, `ftruncate`, `mmap`, `close`,
    /// `mlock`, `madvise`, `VirtualAlloc`, `VirtualLock`, or `unsupported`
    /// on a target with no lock call.
    pub call: &'static str,
    /// The errno of that call, 0 for `unsupported`.
    pub errno: i32,
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.call == "unsupported" {
            return write!(
                f,
                "this platform has no memory lock call, so the key store cannot lock {} bytes",
                self.bytes
            );
        }
        write!(
            f,
            "cannot lock {} bytes of memory for the key store ({}: {}); raise the memlock \
             limit to at least {} bytes (LimitMEMLOCK= in the systemd unit, --ulimit memlock= \
             in Docker) or lower [auth.cache] max_entries",
            self.bytes,
            self.call,
            std::io::Error::from_raw_os_error(self.errno),
            self.bytes,
        )
    }
}

impl std::error::Error for LockError {}

/// The store has no free slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreFull;

impl fmt::Display for StoreFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the locked key store has no free slot")
    }
}

impl std::error::Error for StoreFull {}

/// Set the process-wide store up with `slots` slots in locked memory.
///
/// `serve` and each encryption-key command call this once at startup, after
/// they decided from `key_file` and `require_locked_memory` that locking is
/// required. On failure nothing is set up, so the caller may call
/// [`init_unlocked`] next. A second call after a store exists changes
/// nothing and returns the protection in force.
///
/// # Errors
///
/// [`LockError`] when the platform cannot lock `required_bytes(slots)`.
pub fn init(slots: usize) -> Result<Protection, LockError> {
    if let Some(store) = STORE.get() {
        return Ok(store.protection);
    }
    let store = Store::locked(slots)?;
    Ok(STORE.get_or_init(|| Arc::new(store)).protection)
}

/// Set the store up with `slots` slots in unlocked heap memory: the
/// warn-and-start path. Returns [`Protection::Unlocked`], or the protection
/// in force when a store exists.
pub fn init_unlocked(slots: usize) -> Protection {
    STORE
        .get_or_init(|| Arc::new(Store::unlocked(slots)))
        .protection
}

/// The protection in force. [`Protection::Unlocked`] before any init.
#[must_use]
pub fn protection() -> Protection {
    STORE.get().map_or(Protection::Unlocked, |s| s.protection)
}

/// The locked memory `init(slots)` needs: `slots * 32` rounded up to whole
/// pages, the unit that the memlock limit counts in.
#[must_use]
pub fn required_bytes(slots: usize) -> usize {
    let page = platform::page_size();
    (slots * SLOT_BYTES).div_ceil(page) * page
}

/// One 32-byte key in a slot of the store.
///
/// The slot is wiped and returned to the store when the value drops. Before
/// any init a slot is a heap `Zeroizing` buffer. Not `Clone`: each slot has
/// one owner. `Debug` prints no bytes.
pub struct KeySlot(Inner);

enum Inner {
    // Boxed, so a move of the slot copies a pointer and leaves no unwiped
    // copy of the key behind.
    Heap(Box<Zeroizing<[u8; SLOT_BYTES]>>),
    Store { store: Arc<Store>, index: usize },
}

impl KeySlot {
    /// A zeroed slot.
    ///
    /// # Errors
    ///
    /// [`StoreFull`] when the store has no free slot. Never fails before
    /// init.
    pub fn new() -> Result<Self, StoreFull> {
        match STORE.get() {
            Some(store) => Self::in_store(store),
            None => Ok(Self(Inner::Heap(Box::new(Zeroizing::new([0; SLOT_BYTES]))))),
        }
    }

    /// A zeroed slot of `store`.
    fn in_store(store: &Arc<Store>) -> Result<Self, StoreFull> {
        store.take().map(|index| {
            Self(Inner::Store {
                store: Arc::clone(store),
                index,
            })
        })
    }

    /// A slot holding a copy of `bytes`.
    ///
    /// # Errors
    ///
    /// [`StoreFull`] when the store has no free slot.
    pub fn from_bytes(bytes: &[u8; SLOT_BYTES]) -> Result<Self, StoreFull> {
        let mut slot = Self::new()?;
        slot.expose_mut().copy_from_slice(bytes);
        Ok(slot)
    }

    /// The key bytes.
    #[must_use]
    pub fn expose(&self) -> &[u8; SLOT_BYTES] {
        match &self.0 {
            Inner::Heap(bytes) => bytes,
            // SAFETY: `index` was taken from the store's free list and is
            // released only in `drop`, so this slot's 32 bytes are reachable
            // through this value alone, and the `Arc` keeps the region
            // mapped for as long as the value lives.
            Inner::Store { store, index } => unsafe { &*store.slot_ptr(*index) },
        }
    }

    /// The key bytes, for writing a derived key straight into the slot.
    pub fn expose_mut(&mut self) -> &mut [u8; SLOT_BYTES] {
        match &mut self.0 {
            Inner::Heap(bytes) => bytes,
            // SAFETY: as in `expose`; `&mut self` makes the access exclusive.
            Inner::Store { store, index } => unsafe { &mut *store.slot_ptr(*index) },
        }
    }
}

impl Drop for KeySlot {
    fn drop(&mut self) {
        if let Inner::Store { store, index } = &self.0 {
            // SAFETY: as in `expose_mut`; the index is still ours until the
            // `release` call below.
            unsafe { &mut *store.slot_ptr(*index) }.zeroize();
            store.release(*index);
        }
    }
}

impl fmt::Debug for KeySlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let protection = match &self.0 {
            Inner::Heap(_) => Protection::Unlocked,
            Inner::Store { store, .. } => store.protection,
        };
        write!(f, "KeySlot({SLOT_BYTES} bytes, {protection})")
    }
}

/// A region of `len` bytes holding `slots` slots, and the free slot indices.
struct Store {
    region: platform::Region,
    slots: usize,
    protection: Protection,
    free: Mutex<Vec<usize>>,
}

// SAFETY: the region pointer is shared only through `slot_ptr`, whose
// callers hold distinct indices, and the free list is behind a mutex.
unsafe impl Send for Store {}
unsafe impl Sync for Store {}

impl Store {
    fn locked(slots: usize) -> Result<Self, LockError> {
        let bytes = required_bytes(slots);
        let (region, protection) = platform::lock(bytes.max(platform::page_size()))?;
        Ok(Self::new(region, slots, protection))
    }

    fn unlocked(slots: usize) -> Self {
        let bytes = (slots * SLOT_BYTES).max(SLOT_BYTES);
        Self::new(platform::Region::heap(bytes), slots, Protection::Unlocked)
    }

    fn new(region: platform::Region, slots: usize, protection: Protection) -> Self {
        Self {
            region,
            slots,
            protection,
            free: Mutex::new((0..slots).rev().collect()),
        }
    }

    fn take(&self) -> Result<usize, StoreFull> {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .ok_or(StoreFull)
    }

    fn release(&self, index: usize) {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(index);
    }

    fn slot_ptr(&self, index: usize) -> *mut [u8; SLOT_BYTES] {
        debug_assert!(index < self.slots);
        // SAFETY: `index < slots` and `slots * 32 <= len`, so the offset is
        // inside the region.
        unsafe { self.region.ptr().add(index * SLOT_BYTES) }.cast()
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // SAFETY: every `KeySlot` holds an `Arc<Store>`, so none is alive
        // here and the whole region is ours to wipe before it is released.
        unsafe { std::slice::from_raw_parts_mut(self.region.ptr(), self.region.len()) }.zeroize();
    }
}

mod platform;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{is_child, run_test_in_child};

    /// Before any init a slot is a heap buffer: it holds its bytes, prints
    /// none of them, and the protection reads `none`.
    #[test]
    fn a_slot_before_init_is_a_heap_buffer() {
        assert_eq!(protection(), Protection::Unlocked);
        let mut slot = KeySlot::from_bytes(&[7; SLOT_BYTES]).unwrap();
        assert_eq!(slot.expose(), &[7; SLOT_BYTES]);
        slot.expose_mut()[0] = 9;
        assert_eq!(slot.expose()[0], 9);
        assert_eq!(format!("{slot:?}"), "KeySlot(32 bytes, none)");
    }

    /// Slot allocation, exhaustion, free and wipe on a store of three slots,
    /// in each protection the host gives.
    #[test]
    fn slots_are_distinct_refused_when_full_and_wiped_when_freed() {
        let stores = [
            ("unlocked", Arc::new(Store::unlocked(3))),
            ("locked", Arc::new(Store::locked(3).unwrap())),
        ];
        for (name, store) in stores {
            let mut slots: Vec<KeySlot> = (0..3)
                .map(|i| {
                    let mut slot = KeySlot::in_store(&store).unwrap();
                    slot.expose_mut().fill(i + 1);
                    slot
                })
                .collect();
            assert!(
                matches!(KeySlot::in_store(&store), Err(StoreFull)),
                "{name}"
            );
            let indices: Vec<usize> = slots
                .iter()
                .map(|s| match &s.0 {
                    Inner::Store { index, .. } => *index,
                    Inner::Heap(_) => unreachable!("a store slot"),
                })
                .collect();
            let mut sorted = indices.clone();
            sorted.sort_unstable();
            assert_eq!(sorted, vec![0, 1, 2], "{name}");
            for (slot, fill) in slots.iter().zip(1u8..) {
                assert_eq!(slot.expose(), &[fill; SLOT_BYTES], "{name}");
            }

            // Freeing one slot wipes it and makes it the next one handed out.
            let freed = indices[1];
            drop(slots.remove(1));
            // SAFETY: the slot was freed, so no `KeySlot` reaches it, and the
            // store is alive.
            let bytes = unsafe { &*store.slot_ptr(freed) };
            assert_eq!(bytes, &[0; SLOT_BYTES], "{name}: wiped on drop");
            let again = KeySlot::in_store(&store).unwrap();
            assert!(
                matches!(&again.0, Inner::Store { index, .. } if *index == freed),
                "{name}"
            );
            assert_eq!(
                again.expose(),
                &[0; SLOT_BYTES],
                "{name}: handed out zeroed"
            );
            assert_eq!(
                format!("{again:?}"),
                format!("KeySlot(32 bytes, {})", store.protection),
                "{name}"
            );
        }
    }

    /// The store the host gives: `memfd_secret` where the `memfd_secret`
    /// call succeeds, locked pages otherwise, and locked pages when
    /// `memfd_secret` is switched off. One page fits the default memlock
    /// limit, so a one-page store in locked pages on a host where the call
    /// succeeds means the store lost `memfd_secret`.
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_locks_with_memfd_secret_or_falls_back() {
        let expected = if platform::secret_fd(platform::page_size()).is_ok() {
            Protection::MemfdSecret
        } else {
            Protection::LockedPages
        };
        let (region, protection) = platform::lock(platform::page_size()).unwrap();
        assert_eq!(protection, expected);
        assert_eq!(region.len(), platform::page_size());
        let (_, fallback) = platform::lock_with(platform::page_size(), false).unwrap();
        assert_eq!(fallback, Protection::LockedPages);
    }

    /// The `memfd_secret` descriptor is close-on-exec, so a process that
    /// another thread spawns before the store closes it does not inherit it.
    /// The flag check runs everywhere; the descriptor check needs a kernel
    /// that has `memfd_secret`.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_memfd_secret_descriptor_is_close_on_exec() {
        assert_eq!(platform::SECRET_FD_FLAGS, libc::O_CLOEXEC);
        match platform::secret_fd(platform::page_size()) {
            Ok(fd) => {
                // SAFETY: `F_GETFD` reads the flags of a descriptor this test holds.
                let flags = unsafe { libc::fcntl(fd.0, libc::F_GETFD) };
                assert_eq!(flags & libc::FD_CLOEXEC, libc::FD_CLOEXEC, "{flags}");
            }
            Err(e) => eprintln!("skipped the descriptor check: {e}"),
        }
    }

    /// `required_bytes` rounds the slots up to whole pages.
    #[test]
    fn required_bytes_rounds_up_to_pages() {
        let page = platform::page_size();
        let per_page = page / SLOT_BYTES;
        let cases = [
            (0, 0),
            (1, page),
            (per_page, page),
            (per_page + 1, 2 * page),
            (20_001, (20_001 * SLOT_BYTES).div_ceil(page) * page),
        ];
        for (slots, bytes) in cases {
            assert_eq!(required_bytes(slots), bytes, "{slots} slots");
        }
    }

    /// The refusal names the bytes, the call and both fixes, and no key.
    #[test]
    fn lock_error_names_the_limit_and_the_fixes() {
        let error = LockError {
            bytes: 655_360,
            call: "mlock",
            errno: 12,
        };
        let text = error.to_string();
        for part in [
            "655360 bytes",
            "mlock",
            "LimitMEMLOCK=",
            "--ulimit memlock=",
            "[auth.cache] max_entries",
        ] {
            assert!(text.contains(part), "{part:?} not in {text:?}");
        }
        let unsupported = LockError {
            bytes: 4096,
            call: "unsupported",
            errno: 0,
        }
        .to_string();
        assert!(unsupported.contains("no memory lock call"), "{unsupported}");
        assert!(unsupported.contains("4096"), "{unsupported}");
        assert_eq!(Protection::MemfdSecret.to_string(), "memfd_secret");
        assert_eq!(Protection::LockedPages.to_string(), "locked pages");
        assert_eq!(Protection::Unlocked.to_string(), "none");
    }

    const MEMLOCK_CHILD: &str = "EXTENDDB_TEST_MEMLOCK_CHILD";

    /// Under a memlock limit below the store, `init` refuses with the bytes
    /// it needs and sets nothing up; `init_unlocked` then starts the store
    /// unlocked, and the first call decides for the process. Runs in a child
    /// process, which owns the limit and the process-wide store.
    #[cfg(unix)]
    #[test]
    fn init_refuses_under_a_low_memlock_limit_then_starts_unlocked() {
        let output = run_test_in_child("secmem::tests::memlock_limit_child_process", MEMLOCK_CHILD);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(unix)]
    #[test]
    fn memlock_limit_child_process() {
        if !is_child(MEMLOCK_CHILD) {
            return;
        }
        // SAFETY: `geteuid` takes no arguments and cannot fail.
        if unsafe { libc::geteuid() } == 0 {
            // Root holds CAP_IPC_LOCK, which the limit does not bind.
            eprintln!("running as root: the memlock limit does not apply, skipping");
            return;
        }
        let limit = libc::rlimit {
            rlim_cur: 64 * 1024,
            rlim_max: 64 * 1024,
        };
        // SAFETY: a valid `rlimit`; lowering a limit needs no privilege.
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &raw const limit) },
            0
        );

        // 1 MiB of slots is above the limit.
        let slots = (1024 * 1024) / SLOT_BYTES;
        let error = init(slots).unwrap_err();
        assert_eq!(error.bytes, required_bytes(slots));
        assert!(
            matches!(error.call, "mlock" | "mmap"),
            "{}: {error}",
            error.call
        );
        assert_eq!(protection(), Protection::Unlocked, "nothing was set up");

        // 32 KiB of slots is within the limit.
        let small = Store::locked((32 * 1024) / SLOT_BYTES).unwrap();
        assert_ne!(small.protection, Protection::Unlocked);
        drop(small);

        assert_eq!(init_unlocked(4), Protection::Unlocked);
        assert_eq!(protection(), Protection::Unlocked);
        assert_eq!(init(4), Ok(Protection::Unlocked), "the first call decides");
        let slot = KeySlot::from_bytes(&[3; SLOT_BYTES]).unwrap();
        assert_eq!(slot.expose(), &[3; SLOT_BYTES]);
        assert_eq!(format!("{slot:?}"), "KeySlot(32 bytes, none)");
        let held: Vec<KeySlot> = (0..3).map(|_| KeySlot::new().unwrap()).collect();
        assert_eq!(KeySlot::new().map(|_| ()), Err(StoreFull));
        drop(held);
    }
}
