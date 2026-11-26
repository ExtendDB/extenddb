// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

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
/// succeeds means the store lost `memfd_secret`. Kernels with upstream
/// commit 97d34aa65c29 charge secret pages per user, so the test assumes
/// that no other process of this uid holds secret or io_uring pages near
/// the limit.
#[cfg(target_os = "linux")]
#[test]
fn linux_locks_with_memfd_secret_or_falls_back() {
    let expected = if platform::secret_fd(platform::page_size()).is_ok() {
        Protection::MemfdSecret
    } else {
        Protection::LockedPages
    };
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: a valid `rlimit` for `getrlimit` to fill.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &raw mut limit) },
        0
    );
    let (region, protection) = platform::lock(platform::page_size()).unwrap();
    assert_eq!(
        protection, expected,
        "soft memlock limit {} bytes",
        limit.rlim_cur
    );
    assert_eq!(region.len(), platform::page_size());
    let (_, fallback) = platform::lock_with(platform::page_size(), false).unwrap();
    assert_eq!(fallback, Protection::LockedPages);
}

/// A page the kernel refuses ends the pre-fault with a `read` error of
/// EFAULT and no signal, and the pages before it are written. The
/// mapping is two pages of a one-page `memfd_create` file: the kernel
/// answers a fault on the page past the end of the file with SIGBUS, as
/// it answers a refused `memfd_secret` page. With the file grown to two
/// pages the same pre-fault succeeds and zeroes every byte.
#[cfg(target_os = "linux")]
#[test]
fn prefault_reports_a_refused_page_as_a_read_error() {
    let page = platform::page_size();
    let len = 2 * page;
    // The raw call, because the glibc wrapper arrived in 2.27.
    // SAFETY: a NUL-terminated name and the documented flag.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_memfd_create,
            c"secmem-prefault-test".as_ptr(),
            libc::MFD_CLOEXEC,
        )
    };
    let fd = libc::c_int::try_from(fd).expect("a descriptor");
    assert!(fd >= 0, "memfd_create: {}", std::io::Error::last_os_error());
    let one_page = libc::off_t::try_from(page).unwrap();
    // SAFETY: `fd` is the descriptor opened above.
    assert_eq!(unsafe { libc::ftruncate(fd, one_page) }, 0);
    // SAFETY: a fresh shared mapping of two pages of the descriptor.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    assert_ne!(
        ptr,
        libc::MAP_FAILED,
        "mmap: {}",
        std::io::Error::last_os_error()
    );
    let ptr = ptr.cast::<u8>();
    // SAFETY: the first page is inside the file, so a write is allowed.
    unsafe { std::ptr::write_bytes(ptr, 0xA5, page) };

    // SAFETY: `ptr` is a mapping of `len` writable bytes, and no
    // reference covers it.
    let error = unsafe { platform::prefault_from_dev_zero(ptr, len) }.unwrap_err();
    assert_eq!(
        error,
        LockError {
            bytes: len,
            call: "read",
            errno: libc::EFAULT,
        },
        "{error}"
    );
    // SAFETY: the first page is mapped and inside the file.
    let first = unsafe { std::slice::from_raw_parts(ptr, page) };
    assert!(
        first.iter().all(|&b| b == 0),
        "the page before the refused one was written"
    );

    let two_pages = libc::off_t::try_from(len).unwrap();
    // SAFETY: `fd` is still open; the mapping now covers the file.
    assert_eq!(unsafe { libc::ftruncate(fd, two_pages) }, 0);
    // SAFETY: both pages are inside the file now.
    unsafe { std::ptr::write_bytes(ptr, 0xA5, len) };
    // SAFETY: as above; the slice `first` is no longer used.
    unsafe { platform::prefault_from_dev_zero(ptr, len) }.unwrap();
    // SAFETY: both pages are mapped and inside the file.
    let all = unsafe { std::slice::from_raw_parts(ptr, len) };
    assert!(all.iter().all(|&b| b == 0), "every byte zeroed");

    // SAFETY: the mapping and descriptor this test created.
    assert_eq!(unsafe { libc::munmap(ptr.cast(), len) }, 0);
    // SAFETY: as above.
    assert_eq!(unsafe { libc::close(fd) }, 0);
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
    let stderr = String::from_utf8_lossy(&output.stderr);
    // Shown with --nocapture or --show-output: the branch the child took.
    eprint!("{stderr}");
    assert!(
        output.status.success(),
        "child {}\nstdout:\n{}\nstderr:\n{stderr}",
        output.status,
        String::from_utf8_lossy(&output.stdout)
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
    eprintln!(
        "1 MiB store refused: call {}, errno {}",
        error.call, error.errno
    );
    assert_eq!(error.bytes, required_bytes(slots));
    assert!(
        matches!(error.call, "mlock" | "mmap"),
        "{}: {error}",
        error.call
    );
    assert_eq!(protection(), Protection::Unlocked, "nothing was set up");

    // 32 KiB of slots is within the limit.
    let small = Store::locked((32 * 1024) / SLOT_BYTES).unwrap();
    eprintln!("32 KiB store: {}", small.protection);
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
