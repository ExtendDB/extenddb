// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The restore spool of an S3 store: where a restore job downloads the
//! backup's files before it loads any of them.
//!
//! One restore owns one entry, `restore-<table id>/` under the spool
//! directory the server took at startup (`backup_store::take_spool`: owner
//! only, locked). The entry is created `0700` and every file in it `0600`
//! (the database and its sidecars are `0600`; the spool holds the same
//! data). A data file is written as `<index>.json.gz.partial`, synced,
//! renamed to `<index>.json.gz`, and only then marked `<index>.json.gz.verified`,
//! after the job has compared its digests with the manifest's; the marker
//! holds the file's sha256, and a file counts as verified only while the
//! marker names the digest the manifest in hand records for it, so a
//! replaced manifest does not have an old entry's files accepted. A
//! `.partial`, an unmarked file or a marker naming another digest is one a
//! crash or another manifest left, and the next run of the job removes it
//! and downloads the file again. The manifest and its sidecar are written to
//! the entry too, the same way (`<name>.partial`, synced, renamed), so a
//! crash during the write leaves the previous document or none and never a
//! truncated one, and a resume on the same host reads them from the spool
//! and verifies them again (a pair that fails verification is replaced by
//! the store's). An entry is this host's only while the record's
//! `spool_host` is this host's: a successor on another host finds no entry
//! of its own, and one whose own old entry survives from an earlier run of
//! the job (the record moved to another host and back) removes it before
//! use ([`Entry::open`]). The job removes its entry when it ends by any
//! path but the server's shutdown, which keeps it for a resume, and a job
//! that learnt the record is another's (its claim lost, its final write
//! matching no record), which leaves it to the sweep; the reconciler's sweep
//! ([`list_entries`], then [`sweep_entries`]) removes the entries it listed
//! before it read the records whose job is not RUNNING with this host's
//! `spool_host`, so an entry opened after the listing is never a candidate.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;

/// The prefix of an entry's directory name, followed by the table id.
const ENTRY_PREFIX: &str = "restore-";
/// The suffix of a file being downloaded.
const PARTIAL: &str = ".partial";
/// The suffix of the marker of a file whose digests the job verified.
const VERIFIED: &str = ".verified";

/// One restore's directory in the spool.
pub(crate) struct Entry {
    dir: PathBuf,
    /// Make the rename into place fail, so a test can see what a crash
    /// there leaves.
    #[cfg(test)]
    pub(crate) fail_rename: std::sync::atomic::AtomicBool,
}

impl Entry {
    /// The entry of the restore into table `table_id`, created `0700` when
    /// it does not exist. An existing entry is a resume's and is kept when
    /// `record_spool_host`, the `spool_host` the record carried before this
    /// job's claim, is `this_host`; otherwise the files in it were verified
    /// by a run whose record has since been another host's, and the entry
    /// is removed and created again, empty.
    pub(crate) fn open(
        spool: &Path,
        table_id: &str,
        record_spool_host: Option<&str>,
        this_host: &str,
    ) -> io::Result<Self> {
        let dir = spool.join(format!("{ENTRY_PREFIX}{table_id}"));
        if dir.is_dir() && record_spool_host != Some(this_host) {
            tracing::info!(
                "the spool entry {} is left from a run whose record moved to another host; it is \
                 removed and the files downloaded again",
                dir.display()
            );
            std::fs::remove_dir_all(&dir)?;
        }
        if !dir.is_dir() {
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&dir) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
        }
        Ok(Self {
            dir,
            #[cfg(test)]
            fail_rename: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The entry's directory.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// The path of data file `index` once downloaded and renamed.
    pub(crate) fn file_path(&self, index: u32) -> PathBuf {
        self.dir.join(format!("{index:06}.json.gz"))
    }

    fn partial_path(&self, index: u32) -> PathBuf {
        let mut name = self.file_path(index).into_os_string();
        name.push(PARTIAL);
        PathBuf::from(name)
    }

    fn verified_path(&self, index: u32) -> PathBuf {
        let mut name = self.file_path(index).into_os_string();
        name.push(VERIFIED);
        PathBuf::from(name)
    }

    /// Whether data file `index` is in the entry with a `.verified` marker
    /// naming `sha256`, the manifest's digest of the file: a file an earlier
    /// run of the job downloaded and verified against the same manifest.
    pub(crate) fn is_verified(&self, index: u32, sha256: &str) -> bool {
        self.file_path(index).is_file()
            && std::fs::read(self.verified_path(index)).is_ok_and(|marker| {
                marker.strip_suffix(b"\n").unwrap_or(&marker) == sha256.as_bytes()
            })
    }

    /// Write a document (the manifest, the sidecar) as `name` in the entry
    /// the way a data file is written: `<name>.partial`, `0600`, synced,
    /// then renamed over `name` and the directory synced. An existing
    /// document stands until the rename, so a crash during the write leaves
    /// it whole or leaves the new one, never a truncated file.
    pub(crate) async fn write_document(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        let path = self.dir.join(name);
        let mut partial = path.clone().into_os_string();
        partial.push(PARTIAL);
        let partial = PathBuf::from(partial);
        let mut file = open_private(&partial).await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        drop(file);
        #[cfg(test)]
        if self.fail_rename.load(std::sync::atomic::Ordering::Acquire) {
            return Err(io::Error::other("the process stopped at the rename"));
        }
        tokio::fs::rename(&partial, &path).await?;
        sync_dir(&self.dir).await
    }

    /// The bytes of document `name`, `None` when the entry has no such file.
    pub(crate) fn read_document(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
        match std::fs::read(self.dir.join(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Start downloading data file `index`: whatever an earlier attempt
    /// left (a `.partial`, an unmarked file, a stale marker) is removed, and
    /// the `.partial` is created `0600` for the caller to write.
    pub(crate) async fn begin_file(&self, index: u32) -> io::Result<tokio::fs::File> {
        for path in [
            self.verified_path(index),
            self.file_path(index),
            self.partial_path(index),
        ] {
            remove_if_present(&path).await?;
        }
        open_private(&self.partial_path(index)).await
    }

    /// Finish data file `index` after its digests matched: sync the
    /// `.partial`, rename it into place, then create and sync the
    /// `.verified` marker holding `sha256`, and sync the directory, in that
    /// order, so the marker exists only beside a complete file.
    pub(crate) async fn commit_file(
        &self,
        index: u32,
        mut file: tokio::fs::File,
        sha256: &str,
    ) -> io::Result<()> {
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        self.rename_into_place(index).await?;
        let mut marker = open_private(&self.verified_path(index)).await?;
        marker.write_all(sha256.as_bytes()).await?;
        marker.write_all(b"\n").await?;
        marker.sync_all().await?;
        drop(marker);
        sync_dir(&self.dir).await
    }

    /// The `.partial` renamed to the file's name.
    async fn rename_into_place(&self, index: u32) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_rename.load(std::sync::atomic::Ordering::Acquire) {
            return Err(io::Error::other("the process stopped at the rename"));
        }
        tokio::fs::rename(self.partial_path(index), self.file_path(index)).await
    }

    /// Remove the entry and everything in it; an entry already gone is not
    /// an error.
    pub(crate) async fn remove(&self) -> io::Result<()> {
        match tokio::fs::remove_dir_all(&self.dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Create or truncate `path` with mode `0600`.
async fn open_private(path: &Path) -> io::Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    options.open(path).await
}

async fn remove_if_present(path: &Path) -> io::Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Sync a directory so the entries it gained or lost are durable.
async fn sync_dir(dir: &Path) -> io::Result<()> {
    tokio::fs::File::open(dir).await?.sync_all().await
}

/// The table id of an entry's directory name, `None` for any other name in
/// the spool (the `.lock` file, a name an operator put there).
fn entry_table_id(name: &str) -> Option<&str> {
    let id = name.strip_prefix(ENTRY_PREFIX)?;
    (!id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
    .then_some(id)
}

/// The entries under `spool`, as `(table id, path)`: the reconciler's
/// sweep lists them before it reads the RUNNING records, so an entry a job
/// opens after this listing (its record committed after the listing too)
/// is never a candidate of the sweep that follows. Other names in the spool
/// (the `.lock` file, a name an operator put there, a file under an
/// entry-shaped name) are not listed.
pub(crate) fn list_entries(spool: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    let mut entries = Vec::new();
    for dir_entry in std::fs::read_dir(spool)? {
        let dir_entry = dir_entry?;
        let name = dir_entry.file_name();
        let Some(table_id) = name.to_str().and_then(entry_table_id) else {
            continue;
        };
        if !dir_entry.file_type()?.is_dir() {
            continue;
        }
        entries.push((table_id.to_owned(), dir_entry.path()));
    }
    Ok(entries)
}

/// The reconciler's sweep: remove every entry of `entries` (a
/// [`list_entries`] listing made before the records were read) whose table
/// id is not in `running` (the RUNNING restore targets whose `spool_host` is
/// this host's), and return the ids removed. An entry gone by the time of
/// its removal (its own job removed it between the listing and now) is done,
/// not an error, and not counted; one that cannot be removed is logged and
/// left for the next cycle.
pub(crate) fn sweep_entries(
    entries: Vec<(String, PathBuf)>,
    running: &HashSet<String>,
) -> Vec<String> {
    let mut removed = Vec::new();
    for (table_id, path) in entries {
        if running.contains(&table_id) {
            continue;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {
                tracing::info!(
                    "removed the spool entry of the restore into table id {table_id}: no \
                     RUNNING restore of this host's spool names it"
                );
                removed.push(table_id);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => tracing::error!(
                "could not remove the spool entry of table id {table_id}; the next cycle \
                 retries: {e}"
            ),
        }
    }
    removed
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A scratch directory of this test's own, removed when dropped.
    pub(crate) struct Scratch(PathBuf);

    impl Scratch {
        pub(crate) fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "extenddb-engine-test-{}",
                uuid::Uuid::new_v4().simple()
            ));
            std::fs::create_dir(&dir).unwrap();
            Self(dir)
        }

        pub(crate) fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    /// The entry is `0700`, its files `0600`; a committed file has its
    /// marker naming the digest, a `.partial` has none, a marker naming
    /// another digest does not verify the file, and `begin_file` removes
    /// what an earlier attempt left.
    #[tokio::test]
    async fn an_entry_is_private_and_a_marker_follows_the_rename() {
        let spool = Scratch::new();
        let entry = Entry::open(spool.path(), "tid-1", Some("host-a"), "host-a").unwrap();
        assert_eq!(mode(entry.dir()), 0o700);
        entry
            .write_document("extenddb-manifest.json", b"{}")
            .await
            .unwrap();
        assert_eq!(mode(&entry.dir().join("extenddb-manifest.json")), 0o600);
        assert_eq!(
            entry.read_document("extenddb-manifest.json").unwrap(),
            Some(b"{}".to_vec())
        );
        assert_eq!(entry.read_document("missing").unwrap(), None);

        let mut file = entry.begin_file(3).await.unwrap();
        file.write_all(b"bytes").await.unwrap();
        assert_eq!(mode(&entry.dir().join("000003.json.gz.partial")), 0o600);
        assert!(!entry.is_verified(3, "abc"));
        entry.commit_file(3, file, "abc").await.unwrap();
        assert!(entry.is_verified(3, "abc"));
        assert!(
            !entry.is_verified(3, "abd"),
            "the marker binds the digest, not the index"
        );
        assert_eq!(
            std::fs::read(entry.dir().join("000003.json.gz.verified")).unwrap(),
            b"abc\n"
        );
        assert_eq!(mode(&entry.file_path(3)), 0o600);
        assert_eq!(mode(&entry.dir().join("000003.json.gz.verified")), 0o600);
        assert!(!entry.dir().join("000003.json.gz.partial").exists());

        // A second attempt removes the file and its marker before it writes.
        let file = entry.begin_file(3).await.unwrap();
        assert!(!entry.is_verified(3, "abc"));
        assert!(!entry.file_path(3).exists());
        drop(file);
        // Reopening an existing entry of this host's record keeps it; one
        // whose record was another host's is emptied.
        let again = Entry::open(spool.path(), "tid-1", Some("host-a"), "host-a").unwrap();
        assert!(again.dir().join("000003.json.gz.partial").exists());
        let moved = Entry::open(spool.path(), "tid-1", Some("host-b"), "host-a").unwrap();
        assert!(moved.dir().is_dir());
        assert_eq!(mode(moved.dir()), 0o700);
        assert!(!moved.dir().join("000003.json.gz.partial").exists());
        assert!(!moved.dir().join("extenddb-manifest.json").exists());
        let unknown = Entry::open(spool.path(), "tid-1", None, "host-a");
        assert!(
            unknown.is_ok(),
            "a record with no spool_host has no entry to keep"
        );
        again.remove().await.unwrap();
        assert!(!again.dir().exists());
        again.remove().await.unwrap();
    }

    /// A crash at the rename leaves a `.partial` and no marker: the file is
    /// never taken for verified, so the marker is written only once the
    /// rename has landed.
    #[tokio::test]
    async fn the_marker_is_absent_until_the_file_is_renamed() {
        let spool = Scratch::new();
        let entry = Entry::open(spool.path(), "tid-2", Some("host-a"), "host-a").unwrap();
        let mut file = entry.begin_file(0).await.unwrap();
        file.write_all(b"bytes").await.unwrap();
        entry
            .fail_rename
            .store(true, std::sync::atomic::Ordering::Release);
        let err = entry.commit_file(0, file, "abc").await.unwrap_err();
        assert!(err.to_string().contains("at the rename"), "{err}");
        assert!(entry.dir().join("000000.json.gz.partial").exists());
        assert!(!entry.file_path(0).exists());
        assert!(!entry.dir().join("000000.json.gz.verified").exists());
        assert!(!entry.is_verified(0, "abc"));
    }

    /// The sweep removes the entries of restores not RUNNING on this host
    /// and leaves a live entry and every other name; an entry listed and
    /// then removed by its own job before the sweep reached it is not an
    /// error and not counted.
    #[test]
    fn the_sweep_removes_only_dead_entries() {
        let spool = Scratch::new();
        for name in [
            "restore-live",
            "restore-dead-1",
            "restore-gone",
            "other-dir",
        ] {
            std::fs::create_dir(spool.path().join(name)).unwrap();
        }
        std::fs::write(spool.path().join("restore-dead-1/000000.json.gz"), b"x").unwrap();
        std::fs::write(spool.path().join(".lock"), b"").unwrap();
        // A file, not a directory, under an entry-shaped name is left.
        std::fs::write(spool.path().join("restore-file"), b"").unwrap();
        let running: HashSet<String> = ["live".to_owned()].into_iter().collect();
        let mut listed = list_entries(spool.path()).unwrap();
        listed.sort();
        assert_eq!(
            listed.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            ["dead-1", "gone", "live"],
            "entries only, in whatever order the directory gives"
        );
        // Between the listing and the sweep, the job of `gone` removed its
        // entry.
        std::fs::remove_dir(spool.path().join("restore-gone")).unwrap();
        let mut removed = sweep_entries(listed, &running);
        removed.sort();
        assert_eq!(removed, ["dead-1"]);
        let mut left: Vec<String> = std::fs::read_dir(spool.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        assert_eq!(left, [".lock", "other-dir", "restore-file", "restore-live"]);
        assert_eq!(entry_table_id("restore-"), None);
        assert_eq!(entry_table_id("restore-a/b"), None);
        assert_eq!(entry_table_id("restore-ab_1-2"), Some("ab_1-2"));
    }

    /// A document is written beside the one it replaces and renamed over
    /// it: a write that stops at the rename leaves the earlier document
    /// whole and a `.partial` beside it; a write that completes leaves the
    /// new document and no `.partial`.
    #[tokio::test]
    async fn a_document_replaces_the_earlier_one_at_the_rename() {
        let spool = Scratch::new();
        let entry = Entry::open(spool.path(), "tid-3", Some("host-a"), "host-a").unwrap();
        entry
            .write_document("extenddb-manifest.json", b"first")
            .await
            .unwrap();
        entry
            .fail_rename
            .store(true, std::sync::atomic::Ordering::Release);
        let err = entry
            .write_document("extenddb-manifest.json", b"second")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("at the rename"), "{err}");
        assert_eq!(
            entry.read_document("extenddb-manifest.json").unwrap(),
            Some(b"first".to_vec()),
            "the earlier document stands until the rename"
        );
        assert_eq!(
            std::fs::read(entry.dir().join("extenddb-manifest.json.partial")).unwrap(),
            b"second"
        );
        assert_eq!(
            mode(&entry.dir().join("extenddb-manifest.json.partial")),
            0o600
        );
        entry
            .fail_rename
            .store(false, std::sync::atomic::Ordering::Release);
        entry
            .write_document("extenddb-manifest.json", b"third")
            .await
            .unwrap();
        assert_eq!(
            entry.read_document("extenddb-manifest.json").unwrap(),
            Some(b"third".to_vec())
        );
        assert!(!entry.dir().join("extenddb-manifest.json.partial").exists());
    }
}
