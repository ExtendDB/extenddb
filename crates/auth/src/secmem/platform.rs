// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The platform calls. `lock` returns a region of `len` bytes that the
//! platform locked, and the protection it gave. Every return value is
//! checked.

use std::ptr::NonNull;

use super::{LockError, Protection};

/// A region of memory and how to release it.
pub(super) struct Region {
    ptr: NonNull<u8>,
    len: usize,
    kind: Kind,
}

enum Kind {
    Heap(std::alloc::Layout),
    #[cfg(unix)]
    Mapped,
    #[cfg(windows)]
    Virtual,
}

impl Region {
    /// `len` zeroed bytes from the heap.
    pub(super) fn heap(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, super::SLOT_BYTES)
            .expect("a slot count that fits in memory");
        // SAFETY: `len >= 32`, so the layout has a non-zero size.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self {
            ptr,
            len,
            kind: Kind::Heap(layout),
        }
    }

    pub(super) fn ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        match self.kind {
            // SAFETY: `ptr` came from `alloc_zeroed` with this layout.
            Kind::Heap(layout) => unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout) },
            // SAFETY: `ptr` and `len` are the mapping `mmap` returned.
            #[cfg(unix)]
            Kind::Mapped => unsafe {
                libc::munmap(self.ptr.as_ptr().cast(), self.len);
            },
            // SAFETY: `ptr` is the base `VirtualAlloc` returned.
            #[cfg(windows)]
            Kind::Virtual => unsafe {
                win::VirtualFree(self.ptr.as_ptr().cast(), 0, win::MEM_RELEASE);
            },
        }
    }
}

fn last_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

#[cfg(unix)]
pub(super) fn page_size() -> usize {
    // SAFETY: `sysconf` reads a constant; a failure returns -1.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(page).unwrap_or(4096)
}

#[cfg(not(unix))]
pub(super) fn page_size() -> usize {
    4096
}

/// Linux: `memfd_secret`, then an anonymous mapping under `mlock` with
/// `MADV_DONTDUMP` and `MADV_WIPEONFORK`.
#[cfg(target_os = "linux")]
pub(super) fn lock(len: usize) -> Result<(Region, Protection), LockError> {
    lock_with(len, true)
}

/// `lock`, with the `memfd_secret` attempt switched off by tests that
/// exercise the fallback on a kernel that has it.
#[cfg(target_os = "linux")]
pub(super) fn lock_with(
    len: usize,
    try_memfd_secret: bool,
) -> Result<(Region, Protection), LockError> {
    // Some kernels do not check RLIMIT_MEMLOCK when `memfd_secret` is mapped.
    // The refusal then arrives as SIGBUS when the pages fault in. When the
    // limit is below the store, take the `mlock` path, which reports the
    // limit as an error (or succeeds for a process with CAP_IPC_LOCK).
    if try_memfd_secret && !exceeds_memlock_limit(len) {
        match memfd_secret(len) {
            Ok(region) => return Ok((region, Protection::MemfdSecret)),
            Err(e) if matches!(e.errno, libc::ENOSYS | libc::EPERM | libc::EAGAIN) => {}
            Err(e) => return Err(e),
        }
    }
    mlocked(len).map(|region| (region, Protection::LockedPages))
}

/// Whether `len` bytes are above the soft `RLIMIT_MEMLOCK`.
#[cfg(target_os = "linux")]
fn exceeds_memlock_limit(len: usize) -> bool {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid out-pointer for `getrlimit`.
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &raw mut limit) } != 0 {
        return false;
    }
    limit.rlim_cur != libc::RLIM_INFINITY && (len as u64) > limit.rlim_cur
}

/// macOS and the other Unix targets: an anonymous mapping under `mlock`.
#[cfg(all(unix, not(target_os = "linux")))]
pub(super) fn lock(len: usize) -> Result<(Region, Protection), LockError> {
    mlocked(len).map(|region| (region, Protection::LockedPages))
}

/// Windows: `VirtualAlloc` and `VirtualLock`.
#[cfg(windows)]
pub(super) fn lock(len: usize) -> Result<(Region, Protection), LockError> {
    let fail = |call: &'static str| LockError {
        bytes: len,
        call,
        errno: last_errno(),
    };
    // SAFETY: a fresh allocation; the arguments are the documented
    // constants and `len` is a whole number of pages.
    let ptr = unsafe {
        win::VirtualAlloc(
            std::ptr::null_mut(),
            len,
            win::MEM_COMMIT | win::MEM_RESERVE,
            win::PAGE_READWRITE,
        )
    };
    let Some(ptr) = NonNull::new(ptr.cast::<u8>()) else {
        return Err(fail("VirtualAlloc"));
    };
    let region = Region {
        ptr,
        len,
        kind: Kind::Virtual,
    };
    // SAFETY: `ptr` and `len` are the allocation above.
    if unsafe { win::VirtualLock(ptr.as_ptr().cast(), len) } == 0 {
        return Err(fail("VirtualLock"));
    }
    Ok((region, Protection::LockedPages))
}

#[cfg(not(any(unix, windows)))]
pub(super) fn lock(len: usize) -> Result<(Region, Protection), LockError> {
    Err(LockError {
        bytes: len,
        call: "unsupported",
        errno: 0,
    })
}

/// The flags of `memfd_secret`: `O_CLOEXEC`, its one flag, so that a process
/// that another thread spawns before the store closes the descriptor does
/// not inherit it across `exec`.
#[cfg(target_os = "linux")]
pub(super) const SECRET_FD_FLAGS: libc::c_int = libc::O_CLOEXEC;

#[cfg(target_os = "linux")]
fn memfd_secret(len: usize) -> Result<Region, LockError> {
    let fail = |call: &'static str| LockError {
        bytes: len,
        call,
        errno: last_errno(),
    };
    let fd = secret_fd(len)?;
    let Ok(size) = libc::off_t::try_from(len) else {
        return Err(LockError {
            bytes: len,
            call: "ftruncate",
            errno: libc::EINVAL,
        });
    };
    // SAFETY: `fd` is the descriptor opened above.
    if unsafe { libc::ftruncate(fd.0, size) } != 0 {
        return Err(fail("ftruncate"));
    }
    // SAFETY: a fresh shared mapping of the whole descriptor.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd.0,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(fail("mmap"));
    }
    let region = Region {
        ptr: NonNull::new(ptr.cast::<u8>()).expect("mmap returns a null page only as MAP_FAILED"),
        len,
        kind: Kind::Mapped,
    };
    fd.close().map_err(|_| fail("close"))?;
    // SAFETY: the mapping is `len` writable bytes. Writing it faults
    // every page in now, so a later key write cannot hit a page the
    // kernel refuses to allocate.
    unsafe { std::ptr::write_bytes(region.ptr(), 0, len) };
    Ok(region)
}

/// A `memfd_secret` descriptor for a store of `len` bytes, opened with
/// [`SECRET_FD_FLAGS`].
#[cfg(target_os = "linux")]
pub(super) fn secret_fd(len: usize) -> Result<SecretFd, LockError> {
    // SAFETY: `memfd_secret` takes its flags and no pointer; it returns a
    // file descriptor or -1 with errno set.
    let fd = unsafe { libc::syscall(libc::SYS_memfd_secret, SECRET_FD_FLAGS) };
    match libc::c_int::try_from(fd) {
        Ok(fd) if fd >= 0 => Ok(SecretFd(fd)),
        _ => Err(LockError {
            bytes: len,
            call: "memfd_secret",
            errno: last_errno(),
        }),
    }
}

/// A descriptor closed once, with the return value read.
#[cfg(target_os = "linux")]
pub(super) struct SecretFd(pub(super) libc::c_int);

#[cfg(target_os = "linux")]
impl SecretFd {
    fn close(self) -> Result<(), ()> {
        let fd = self.0;
        std::mem::forget(self);
        // SAFETY: `fd` is open and closed only here.
        if unsafe { libc::close(fd) } == 0 {
            Ok(())
        } else {
            Err(())
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for SecretFd {
    fn drop(&mut self) {
        // SAFETY: `fd` is open; this is the error path, so the result is
        // not needed.
        unsafe { libc::close(self.0) };
    }
}

#[cfg(unix)]
fn mlocked(len: usize) -> Result<Region, LockError> {
    let fail = |call: &'static str| LockError {
        bytes: len,
        call,
        errno: last_errno(),
    };
    // SAFETY: a fresh anonymous private mapping.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(fail("mmap"));
    }
    let region = Region {
        ptr: NonNull::new(ptr.cast::<u8>()).expect("mmap returns a null page only as MAP_FAILED"),
        len,
        kind: Kind::Mapped,
    };
    // SAFETY: `ptr` and `len` are the mapping above.
    if unsafe { libc::mlock(ptr, len) } != 0 {
        return Err(fail("mlock"));
    }
    #[cfg(target_os = "linux")]
    for advice in [libc::MADV_DONTDUMP, libc::MADV_WIPEONFORK] {
        // SAFETY: `ptr` and `len` are the mapping above.
        if unsafe { libc::madvise(ptr, len, advice) } != 0 {
            return Err(fail("madvise"));
        }
    }
    Ok(region)
}

/// The four kernel32 calls, declared here so that no crate is added.
#[cfg(windows)]
mod win {
    use std::ffi::c_void;

    pub const MEM_COMMIT: u32 = 0x1000;
    pub const MEM_RESERVE: u32 = 0x2000;
    pub const MEM_RELEASE: u32 = 0x8000;
    pub const PAGE_READWRITE: u32 = 0x04;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        pub fn VirtualAlloc(
            address: *mut c_void,
            size: usize,
            allocation_type: u32,
            protect: u32,
        ) -> *mut c_void;
        pub fn VirtualFree(address: *mut c_void, size: usize, free_type: u32) -> i32;
        pub fn VirtualLock(address: *mut c_void, size: usize) -> i32;
    }
}
