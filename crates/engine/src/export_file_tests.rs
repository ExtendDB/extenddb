// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The export file is complete when the export answers, and an export that
//! does not complete leaves nothing at its path.

use super::*;
use extenddb_core::types::AttributeValue;

/// Exports per run of [`the_file_is_complete_when_the_export_answers`].
const EXPORTS: usize = 200;

/// A fresh directory under the temp dir, removed when dropped.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("eb-export-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn item(n: usize) -> Item {
    Item::from([
        ("pk".to_owned(), AttributeValue::S(format!("item-{n}"))),
        ("data".to_owned(), AttributeValue::S(format!("value-{n}"))),
    ])
}

fn items(n: usize) -> Vec<Item> {
    (0..n).map(item).collect()
}

/// The bytes the export writes for `items`.
fn bytes_of(items: &[Item]) -> usize {
    items
        .iter()
        .map(|i| {
            extenddb_core::backup_format::encode_item_line(i)
                .unwrap()
                .len()
        })
        .sum()
}

/// Create the export file as the handler does, with `create_new`, so a file
/// left at `path` by an earlier export makes this fail.
async fn create(path: &Path) -> tokio::fs::File {
    tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .expect("nothing is at the path")
}

type Page = Result<(Vec<Item>, Option<Item>), DynamoDbError>;

/// A page source that answers `pages` in order, each with a last key except
/// the final one.
fn pages(pages: Vec<Vec<Item>>) -> impl FnMut(Option<Item>) -> std::future::Ready<Page> {
    let mut pages = pages.into_iter().peekable();
    move |_| {
        let page = pages.next().expect("no page is asked for after the last");
        let more = pages.peek().is_some();
        std::future::ready(Ok((page, more.then(Item::new))))
    }
}

fn assert_no_file(path: &Path) {
    let found = std::fs::symlink_metadata(path);
    assert!(
        found
            .as_ref()
            .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
        "the export left {found:?} at its path"
    );
}

/// The file holds every line when the export answers. The last write runs on
/// the blocking pool, so an export that answers without waiting for it is
/// found short by a read made at once.
#[tokio::test]
async fn the_file_is_complete_when_the_export_answers() {
    let dir = Dir::new();
    let items = items(3);
    let expected = u64::try_from(bytes_of(&items)).unwrap();
    for n in 0..EXPORTS {
        let path = dir.join(&format!("export-{n}.json"));
        let mut file = create(&path).await;
        let count = write_export_file(&mut file, &path, 3, pages(vec![items.clone()]))
            .await
            .unwrap();
        let size = std::fs::metadata(&path).unwrap().len();
        assert_eq!(size, expected, "export {n} of {EXPORTS} was short");
        let lines = std::fs::read_to_string(&path).unwrap().lines().count();
        assert_eq!((count, lines), (3, 3), "export {n} of {EXPORTS}");
    }
}

/// An export that fails after writing lines (here on the item limit, passed
/// on the second page) leaves nothing at its path, so a second export to the
/// same path is not refused as an existing file.
#[tokio::test]
async fn an_export_over_the_item_limit_leaves_no_file_and_the_path_can_be_reused() {
    let dir = Dir::new();
    let path = dir.join("export.json");
    let pages_of_two = || pages(vec![items(2), items(2)]);

    let mut file = create(&path).await;
    let err = write_export_file(&mut file, &path, 3, pages_of_two())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(m)
            if m == "Export item count exceeds maximum (3)"),
        "{err:?}"
    );
    drop(file);
    assert_no_file(&path);

    let mut file = create(&path).await;
    let count = write_export_file(&mut file, &path, 4, pages_of_two())
        .await
        .unwrap();
    assert_eq!(count, 4);
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 4);
}

/// A scan that fails after a page was written leaves no file either.
#[tokio::test]
async fn an_export_whose_scan_fails_leaves_no_file() {
    let dir = Dir::new();
    let path = dir.join("export.json");
    let mut file = create(&path).await;
    let mut calls = 0;
    let err = write_export_file(&mut file, &path, 10, |_| {
        calls += 1;
        std::future::ready(if calls == 1 {
            Ok((items(2), Some(Item::new())))
        } else {
            Err(DynamoDbError::InternalServerError(
                "Internal server error".to_owned(),
            ))
        })
    })
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::InternalServerError(_)),
        "{err:?}"
    );
    assert_no_file(&path);
}

/// An export whose future is dropped part way, as when its client
/// disconnects, leaves no file.
#[tokio::test]
async fn a_dropped_export_leaves_no_file() {
    let dir = Dir::new();
    let path = dir.join("export.json");
    let mut file = create(&path).await;
    let second_page_asked = tokio::sync::Notify::new();
    let mut calls = 0;
    let export = write_export_file(&mut file, &path, 10, |_| {
        calls += 1;
        let first = calls == 1;
        let asked = &second_page_asked;
        async move {
            if first {
                Ok((items(2), Some(Item::new())))
            } else {
                asked.notify_one();
                std::future::pending().await
            }
        }
    });
    tokio::select! {
        result = export => panic!("the second page never answers, got {result:?}"),
        () = second_page_asked.notified() => {}
    }
    // `select!` has dropped the export's future.
    assert_no_file(&path);
}

#[cfg(unix)]
mod unix {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    /// Set in the child process [`run_in_child`] starts, to the name of the
    /// test it runs.
    const CHILD_TEST: &str = "EXTENDDB_TEST_CHILD";

    fn is_cannot_write(err: &DynamoDbError) -> bool {
        matches!(err, DynamoDbError::ValidationException(m) if m == "Cannot write export file")
    }

    /// Whether this process is the child [`run_in_child`] started for `name`.
    fn is_child(name: &str) -> bool {
        std::env::var(CHILD_TEST).is_ok_and(|n| n == name)
    }

    /// Run the test `name` again in a child process (this test binary with
    /// `--exact`) and assert that exactly that test ran and passed. For a test
    /// that changes process-wide state, here a resource limit, which must not
    /// reach the tests running beside it.
    fn run_in_child(name: &str) {
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([name, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_TEST, name)
            .output()
            .expect("spawn the child test process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("test result: ok. 1 passed"),
            "child {name} failed ({}):\n--- stdout\n{stdout}\n--- stderr\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Set this process's soft file size limit to `bytes`, or back to the
    /// hard limit for `None`, and ignore `SIGXFSZ`, so a write past the limit
    /// fails with `EFBIG` instead of ending the process. The hard limit is
    /// kept, and the soft limit may be raised up to it.
    fn limit_file_size(bytes: Option<u64>) {
        // SAFETY: plain libc calls on a valid, initialised struct; only the
        // child process this test runs in calls them.
        unsafe {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            let mut limit: libc::rlimit = std::mem::zeroed();
            assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &raw mut limit), 0);
            limit.rlim_cur = bytes.map_or(limit.rlim_max, |bytes| bytes as libc::rlim_t);
            assert_eq!(
                libc::setrlimit(libc::RLIMIT_FSIZE, &raw const limit),
                0,
                "setrlimit: {}",
                std::io::Error::last_os_error()
            );
        }
    }

    /// Set this process's soft limit on open files to `count`, keeping the
    /// hard limit, and return the soft limit it replaced.
    fn set_open_files_limit(count: libc::rlim_t) -> libc::rlim_t {
        // SAFETY: plain libc calls on a valid, initialised struct; only the
        // child process this test runs in calls them.
        unsafe {
            let mut limit: libc::rlimit = std::mem::zeroed();
            assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit), 0);
            let replaced = limit.rlim_cur;
            limit.rlim_cur = count;
            assert_eq!(
                libc::setrlimit(libc::RLIMIT_NOFILE, &raw const limit),
                0,
                "setrlimit: {}",
                std::io::Error::last_os_error()
            );
            replaced
        }
    }

    fn effective_uid_is_root() -> bool {
        // SAFETY: geteuid has no preconditions.
        unsafe { libc::geteuid() == 0 }
    }

    /// The warnings logged on this thread while the guard [`Warnings::capture`]
    /// returns is held, one line of `name=value` fields each.
    #[derive(Clone, Default)]
    struct Warnings(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

    impl Warnings {
        fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
            let warnings = Self::default();
            let guard = tracing::subscriber::set_default(warnings.clone());
            (warnings, guard)
        }

        fn lines(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl tracing::Subscriber for Warnings {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() == tracing::Level::WARN
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let mut line = String::new();
            event.record(&mut Fields(&mut line));
            self.0.lock().unwrap().push(line);
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// Writes an event's fields as `name=value` pairs.
    struct Fields<'a>(&'a mut String);

    impl tracing::field::Visit for Fields<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            use std::fmt::Write as _;
            let _ = write!(self.0, "{}={value:?} ", field.name());
        }
    }

    /// A full disk on the last line, without root or a mount: past
    /// `RLIMIT_FSIZE` a write fails with `EFBIG`, as one on a full
    /// filesystem fails with `ENOSPC`. The limit falls inside the last line,
    /// so the failing write is the last one, whose error tokio reports only
    /// to a flush: `sync_all` waits for that write but does not return its
    /// error. The export answers `Cannot write export file` and leaves no
    /// file. The test runs in a child process because the limit applies to
    /// the whole process.
    #[tokio::test]
    async fn a_write_that_fails_on_the_last_line_answers_cannot_write_and_leaves_no_file() {
        const NAME: &str = "export_file::tests::unix::\
            a_write_that_fails_on_the_last_line_answers_cannot_write_and_leaves_no_file";
        if !is_child(NAME) {
            run_in_child(NAME);
            return;
        }
        let dir = Dir::new();
        let path = dir.join("export.json");
        let items = items(3);
        let first_two = u64::try_from(bytes_of(&items[..2])).unwrap();
        let mut file = create(&path).await;
        limit_file_size(Some(first_two + 4));
        let err = write_export_file(&mut file, &path, 3, pages(vec![items]))
            .await
            .unwrap_err();
        assert!(is_cannot_write(&err), "{err:?}");
        assert_no_file(&path);
    }

    /// A write that fails before the last line, followed by writes that
    /// succeed, as when space is freed between two pages: the limit falls
    /// inside the second line of the first page and is raised again before
    /// the second page. tokio reports the failed write's error to the next
    /// write, and the export answers `Cannot write export file` and leaves no
    /// file. The flush reports only the last write's error, so an export that
    /// ignored the error of each write would answer with lines missing. The
    /// test runs in a child process because the limit applies to the whole
    /// process.
    #[tokio::test]
    async fn a_write_that_fails_before_the_last_line_answers_cannot_write_and_leaves_no_file() {
        const NAME: &str = "export_file::tests::unix::\
            a_write_that_fails_before_the_last_line_answers_cannot_write_and_leaves_no_file";
        if !is_child(NAME) {
            run_in_child(NAME);
            return;
        }
        let dir = Dir::new();
        let path = dir.join("export.json");
        let page = items(3);
        let inside_second_line = u64::try_from(bytes_of(&page[..1])).unwrap() + 4;
        let mut file = create(&path).await;
        let mut calls = 0;
        let err = write_export_file(&mut file, &path, 10, |_| {
            calls += 1;
            let first = calls == 1;
            limit_file_size(first.then_some(inside_second_line));
            std::future::ready(Ok((page.clone(), first.then(Item::new))))
        })
        .await
        .unwrap_err();
        assert!(is_cannot_write(&err), "{err:?}");
        assert_no_file(&path);
    }

    /// A directory without read permission (`0300`) cannot be opened for its
    /// sync, although the file can be created and written in it (write and
    /// search permission). The export logs a warning and answers with its
    /// file complete, without the directory sync. Root bypasses that
    /// permission, so the test is skipped as root.
    #[tokio::test]
    async fn a_directory_that_cannot_be_read_is_not_synced_and_the_export_completes() {
        if effective_uid_is_root() {
            eprintln!(
                "SKIP a_directory_that_cannot_be_read_is_not_synced_and_the_export_completes: \
                 running as root"
            );
            return;
        }
        let (warnings, _capture) = Warnings::capture();
        let dir = Dir::new();
        let write_only = dir.join("write-only");
        std::fs::create_dir(&write_only).unwrap();
        std::fs::set_permissions(&write_only, std::fs::Permissions::from_mode(0o300)).unwrap();
        let path = write_only.join("export.json");
        let items = items(3);
        let mut file = create(&path).await;
        let result = write_export_file(&mut file, &path, 3, pages(vec![items.clone()])).await;
        drop(file);
        // Search permission is enough to read the file by its path.
        let written = std::fs::read(&path).map(|bytes| bytes.len());
        // Restored before any assert, so `Dir` can remove the directory.
        std::fs::set_permissions(&write_only, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(result.unwrap(), 3);
        assert_eq!(written.unwrap(), bytes_of(&items));
        let logged = warnings.lines();
        assert!(
            logged.iter().any(|line| {
                line.contains("could not sync the directory of an export file")
                    && line.contains("Permission denied")
            }),
            "{logged:?}"
        );
    }

    fn os_error(code: i32) -> std::io::Error {
        std::io::Error::from_raw_os_error(code)
    }

    /// A directory that cannot be opened for its sync is left unsynced.
    #[test]
    fn a_directory_that_cannot_be_opened_is_not_synced() {
        for code in [libc::EACCES, libc::EPERM] {
            assert!(can_skip_dir_sync(DirStep::Open, &os_error(code)), "{code}");
        }
    }

    /// A directory whose filesystem has no fsync for directories is left
    /// unsynced.
    #[test]
    fn a_directory_without_an_fsync_is_not_synced() {
        for code in [libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP, libc::EBADF] {
            assert!(can_skip_dir_sync(DirStep::Fsync, &os_error(code)), "{code}");
        }
    }

    /// `EIO` and every other error of the directory sync fail the export,
    /// including an error that is accepted only at the other step and an
    /// error without a number.
    #[test]
    fn an_io_error_or_any_other_directory_sync_error_fails_the_export() {
        for (step, code) in [
            (DirStep::Open, libc::EIO),
            (DirStep::Fsync, libc::EIO),
            (DirStep::Open, libc::ENOENT),
            (DirStep::Open, libc::EINVAL),
            (DirStep::Fsync, libc::EACCES),
            (DirStep::Fsync, libc::ENOSPC),
        ] {
            assert!(!can_skip_dir_sync(step, &os_error(code)), "{step:?} {code}");
        }
        let no_number = std::io::Error::other("background task failed");
        assert!(!can_skip_dir_sync(DirStep::Fsync, &no_number));
    }

    /// Export A's file is removed by someone else while A runs (an operator
    /// who takes it for a leftover), and export B creates its own file at the
    /// same path. When A then fails, its guard finds that the path names
    /// another file and leaves it, so B answers with its file in place.
    #[tokio::test]
    async fn a_failing_export_leaves_the_file_of_another_export_at_its_path() {
        use tokio::sync::Notify;

        let dir = Dir::new();
        let path = dir.join("export.json");

        let a_paused = Notify::new();
        let a_resume = Notify::new();
        let a_done = Notify::new();
        let b_paused = Notify::new();
        let b_resume = Notify::new();

        let mut file_a = create(&path).await;
        let mut a_calls = 0;
        let export_a = write_export_file(&mut file_a, &path, 10, |_| {
            a_calls += 1;
            let first = a_calls == 1;
            let (paused, resume) = (&a_paused, &a_resume);
            async move {
                if first {
                    return Ok((items(2), Some(Item::new())));
                }
                paused.notify_one();
                resume.notified().await;
                Err(DynamoDbError::InternalServerError(
                    "Internal server error".to_owned(),
                ))
            }
        });
        let export_a = async {
            let result = export_a.await;
            a_done.notify_one();
            result
        };

        let mut b_lines = None;
        let other = async {
            a_paused.notified().await;
            std::fs::remove_file(&path).unwrap();
            let mut file_b = create(&path).await;
            let mut b_calls = 0;
            let export_b = write_export_file(&mut file_b, &path, 10, |_| {
                b_calls += 1;
                let first = b_calls == 1;
                let (paused, resume) = (&b_paused, &b_resume);
                async move {
                    if first {
                        return Ok((items(2), Some(Item::new())));
                    }
                    paused.notify_one();
                    resume.notified().await;
                    Ok((vec![item(9)], None))
                }
            });
            let driver = async {
                b_paused.notified().await;
                a_resume.notify_one();
                a_done.notified().await;
                b_resume.notify_one();
            };
            let (result_b, ()) = tokio::join!(export_b, driver);
            b_lines = std::fs::read_to_string(&path)
                .ok()
                .map(|s| s.lines().count());
            result_b
        };

        let (result_a, result_b) = tokio::join!(export_a, other);
        assert!(result_a.is_err(), "export A fails: {result_a:?}");
        assert_eq!(result_b.as_ref().ok(), Some(&3), "export B answers Ok(3)");
        assert_eq!(
            b_lines,
            Some(3),
            "export B answered Ok(3) but its path holds {b_lines:?} lines (None: no file)"
        );
    }

    /// Recording the file's device and inode takes a duplicate of its
    /// descriptor, which std places at 3 or above. With the soft limit on
    /// open files at 3 the duplicate fails, so the export answers `Cannot
    /// write export file` before it reads a page, and the guard, which then
    /// has no identity to compare, unlinks the path. The test runs in a child
    /// process because the limit applies to the whole process.
    #[tokio::test]
    async fn an_unreadable_file_identity_fails_the_export_before_any_page_and_leaves_no_file() {
        const NAME: &str = "export_file::tests::unix::\
            an_unreadable_file_identity_fails_the_export_before_any_page_and_leaves_no_file";
        if !is_child(NAME) {
            run_in_child(NAME);
            return;
        }
        let dir = Dir::new();
        let path = dir.join("export.json");
        let mut file = create(&path).await;
        let mut pages_asked = 0;
        let replaced = set_open_files_limit(3);
        let result = write_export_file(&mut file, &path, 10, |_| {
            pages_asked += 1;
            std::future::ready(Ok((items(2), None)))
        })
        .await;
        // Restored before any assert, so `Dir` can remove the directory.
        set_open_files_limit(replaced);
        let err = result.unwrap_err();
        assert!(is_cannot_write(&err), "{err:?}");
        assert_eq!(pages_asked, 0, "the export read a page");
        assert_no_file(&path);
    }

    /// The export's file is moved to another name while the export runs, and
    /// a symlink to it is put at the path. The guard compares the device and
    /// inode of the symlink itself, not of the file it points to, so the
    /// failed export leaves the symlink at the path.
    #[tokio::test]
    async fn a_symlink_put_at_the_path_of_a_failing_export_is_left() {
        let dir = Dir::new();
        let path = dir.join("export.json");
        let moved = dir.join("moved.json");
        let mut file = create(&path).await;
        let mut calls = 0;
        let result = write_export_file(&mut file, &path, 10, |_| {
            calls += 1;
            std::future::ready(if calls == 1 {
                Ok((items(2), Some(Item::new())))
            } else {
                std::fs::rename(&path, &moved).unwrap();
                std::os::unix::fs::symlink(&moved, &path).unwrap();
                Err(DynamoDbError::InternalServerError(
                    "Internal server error".to_owned(),
                ))
            })
        })
        .await;
        assert!(result.is_err(), "{result:?}");
        let left = std::fs::symlink_metadata(&path);
        assert!(
            left.as_ref()
                .is_ok_and(|meta| meta.file_type().is_symlink()),
            "the symlink is still at the path: {left:?}"
        );
    }

    /// When the guard's unlink fails, the failed export leaves its file at
    /// the path and the server logs a warning with the error. Here the
    /// directory loses write permission while the export runs, so the unlink
    /// fails with `EACCES`. Root bypasses that permission, so the test is
    /// skipped as root.
    #[tokio::test]
    async fn a_failed_export_whose_unlink_fails_leaves_its_file_and_logs_a_warning() {
        if effective_uid_is_root() {
            eprintln!(
                "SKIP a_failed_export_whose_unlink_fails_leaves_its_file_and_logs_a_warning: \
                 running as root"
            );
            return;
        }
        let (warnings, _capture) = Warnings::capture();
        let dir = Dir::new();
        let sub = dir.join("sub");
        std::fs::create_dir(&sub).unwrap();
        let path = sub.join("export.json");
        let mut file = create(&path).await;
        let mut calls = 0;
        let result = write_export_file(&mut file, &path, 10, |_| {
            calls += 1;
            std::future::ready(if calls == 1 {
                Ok((items(2), Some(Item::new())))
            } else {
                std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o500)).unwrap();
                Err(DynamoDbError::InternalServerError(
                    "Internal server error".to_owned(),
                ))
            })
        })
        .await;
        drop(file);
        let left = std::fs::symlink_metadata(&path);
        // Restored before any assert, so `Dir` can remove the directory.
        std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(result.is_err(), "{result:?}");
        assert!(left.is_ok(), "the file is still at its path: {left:?}");
        let logged = warnings.lines();
        assert!(
            logged.iter().any(|line| {
                line.contains("could not remove the file of a failed export")
                    && line.contains("Permission denied")
            }),
            "{logged:?}"
        );
    }
}
