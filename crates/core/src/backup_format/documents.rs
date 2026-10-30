// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The documents a backup job writes besides its data files, built together
//! from one input: [`BackupDocuments::build`].

use super::FormatError;
use super::ddb_export::{
    ExportFileEntry, ExportSummaryManifest, trim_key_prefix, write_files_manifest,
};
use super::layout::{
    BackupArn, EXTENDDB_MANIFEST_FILE, FILES_MANIFEST_FILE, MANIFEST_CHECKSUM_FILE,
    SUMMARY_MANIFEST_FILE, backup_creation_date_time,
};
use super::manifest::{
    CURRENT_FORMAT_VERSION, DataFileEntry, ExtenddbManifest, ItemOrder, SnapshotDescriptor,
    SourceTableSchema, manifest_checksum,
};

/// The members of `extenddb-manifest.json` a backup job knows before it
/// writes its data files: every member except `format_version` (the
/// version this build writes), `backup_creation_date_time` (the instant in
/// the backup id), `backup_size_bytes` and `data_files` (from the files),
/// which [`BackupDocuments::build`] derives.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestHeader {
    /// The backup's ARN; its backup id carries the creation instant.
    pub backup_arn: BackupArn,
    /// See [`ExtenddbManifest::backup_name`].
    pub backup_name: String,
    /// See [`ExtenddbManifest::source_backend`].
    pub source_backend: String,
    /// See [`ExtenddbManifest::source_extenddb_version`].
    pub source_extenddb_version: String,
    /// See [`ExtenddbManifest::snapshot`].
    pub snapshot: SnapshotDescriptor,
    /// See [`ExtenddbManifest::item_order`].
    pub item_order: ItemOrder,
    /// See [`ExtenddbManifest::source_table`].
    pub source_table: SourceTableSchema,
}

/// `extenddb-manifest.json`, its sidecar, `manifest-files.json` and
/// `manifest-summary.json` for one backup, built by [`Self::build`] from one
/// header, one list of data files with their etags and one key prefix, so
/// the four agree: one item count, one `md5` per file, one prefix for every
/// key.
///
/// Write order: the data files first, then the three documents of
/// [`Self::documents_before_manifest`] in the order it returns them
/// (`manifest-files.json`, `manifest-summary.json`,
/// `extenddb-manifest.sha256`), then whatever the job checks before it
/// commits (that its claim on the backup still holds), then
/// [`Self::manifest_document`], `extenddb-manifest.json`, last.
/// [`Self::documents`] is the same four in that order for a caller with no
/// step between. The manifest's presence is the commit record; a prefix
/// with a manifest but no sidecar, or whose sidecar does not match, is
/// corrupt and is refused. The manifest is validated before any document is
/// built, so a job that writes in this order never stores the export
/// manifests of a manifest the reader refuses.
#[derive(Debug, Clone)]
pub struct BackupDocuments {
    manifest: ExtenddbManifest,
    manifest_bytes: Vec<u8>,
    sidecar: String,
    files_manifest: String,
    summary: String,
    key_prefix: String,
}

impl BackupDocuments {
    /// Build the documents of a backup whose data files are `data_files`,
    /// in the order a restore applies them, each with the `ETag` the store
    /// reported when the file was written under the backup's prefix (`None`
    /// on a store without one).
    ///
    /// Derived here and nowhere else: `backup_creation_date_time` from the
    /// backup id; `backup_size_bytes` as the sum of the files' `bytes`; the
    /// key prefix, `store_key_prefix` (the store's configured key prefix,
    /// empty when it has none; leading and trailing `/` dropped,
    /// [`trim_key_prefix`]) followed by [`BackupArn::store_prefix`], which
    /// is `s3Prefix` and the start of `manifestFilesS3Key` and of every
    /// `dataFileS3Key`. The store normalizes its configured prefix on its
    /// own (leading `/` dropped, one trailing `/` dropped, then each `/`
    /// separated component checked, an empty one refused), so for every
    /// prefix the store accepts the two rules give the same path and the
    /// keys here are the keys the store wrote. `bucket` is the summary's
    /// `s3Bucket`: the bucket name on S3, the root path on a filesystem
    /// store. The manifest is validated once ([`ExtenddbManifest::validate`],
    /// the reader's rule set).
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidManifest`] when the backup id carries no
    /// instant (a 13-digit or bare id) or the sizes overflow, the errors of
    /// [`ExtenddbManifest::validate`], and [`FormatError::Json`] when a
    /// document fails to serialize.
    pub fn build(
        header: ManifestHeader,
        data_files: Vec<(DataFileEntry, Option<String>)>,
        store_key_prefix: &str,
        bucket: &str,
    ) -> Result<Self, FormatError> {
        let (data_files, etags): (Vec<DataFileEntry>, Vec<Option<String>>) =
            data_files.into_iter().unzip();
        let ManifestHeader {
            backup_arn,
            backup_name,
            source_backend,
            source_extenddb_version,
            snapshot,
            item_order,
            source_table,
        } = header;
        let backup_creation_date_time = backup_creation_date_time(backup_arn.backup_id())
            .ok_or_else(|| {
                FormatError::InvalidManifest(format!(
                    "backup id {:?} does not carry the backup's creation instant",
                    backup_arn.backup_id()
                ))
            })?;
        let backup_size_bytes = data_files
            .iter()
            .try_fold(0u64, |sum, f| sum.checked_add(f.bytes))
            .and_then(|sum| i64::try_from(sum).ok())
            .ok_or_else(|| FormatError::InvalidManifest("data file sizes overflow".to_owned()))?;
        let backup_prefix = backup_arn.store_prefix();
        let backup_prefix = trim_key_prefix(&backup_prefix);
        let key_prefix = match trim_key_prefix(store_key_prefix) {
            "" => backup_prefix.to_owned(),
            store => format!("{store}/{backup_prefix}"),
        };
        let manifest = ExtenddbManifest {
            format_version: CURRENT_FORMAT_VERSION,
            backup_arn: backup_arn.to_arn_string(),
            backup_name,
            backup_creation_date_time,
            backup_size_bytes,
            source_backend,
            source_extenddb_version,
            snapshot,
            item_order,
            source_table,
            data_files,
        };
        let manifest_bytes = manifest.to_json_bytes()?;
        let sidecar = manifest_checksum(&manifest_bytes);
        let lines: Vec<ExportFileEntry> = manifest
            .data_files
            .iter()
            .zip(etags)
            .map(|(entry, etag)| ExportFileEntry::from_data_file(entry, &key_prefix, etag))
            .collect();
        let files_manifest = write_files_manifest(&lines)?;
        let summary =
            ExportSummaryManifest::for_backup(&manifest, bucket, &key_prefix)?.to_json()?;
        Ok(Self {
            manifest,
            manifest_bytes,
            sidecar,
            files_manifest,
            summary,
            key_prefix,
        })
    }

    /// The manifest the documents describe.
    #[must_use]
    pub fn manifest(&self) -> &ExtenddbManifest {
        &self.manifest
    }

    /// The bytes of `extenddb-manifest.json`, which the sidecar hashes.
    #[must_use]
    pub fn manifest_bytes(&self) -> &[u8] {
        &self.manifest_bytes
    }

    /// The content of `extenddb-manifest.sha256`.
    #[must_use]
    pub fn sidecar(&self) -> &str {
        &self.sidecar
    }

    /// The content of `manifest-files.json`.
    #[must_use]
    pub fn files_manifest(&self) -> &str {
        &self.files_manifest
    }

    /// The content of `manifest-summary.json`.
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// The backup's key prefix in the store, without a leading or trailing
    /// `/`: the summary's `s3Prefix`.
    #[must_use]
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    /// The three documents written before the manifest, as (name under the
    /// backup prefix, bytes) in the order they are written (type docs):
    /// `manifest-files.json`, `manifest-summary.json`,
    /// `extenddb-manifest.sha256`.
    #[must_use]
    pub fn documents_before_manifest(&self) -> [(&'static str, &[u8]); 3] {
        [
            (FILES_MANIFEST_FILE, self.files_manifest.as_bytes()),
            (SUMMARY_MANIFEST_FILE, self.summary.as_bytes()),
            (MANIFEST_CHECKSUM_FILE, self.sidecar.as_bytes()),
        ]
    }

    /// The document written last, `extenddb-manifest.json`, as (name under
    /// the backup prefix, bytes): the commit record (type docs).
    #[must_use]
    pub fn manifest_document(&self) -> (&'static str, &[u8]) {
        (EXTENDDB_MANIFEST_FILE, &self.manifest_bytes)
    }

    /// The four documents as (name under the backup prefix, bytes), in the
    /// order they are written: [`Self::documents_before_manifest`] then
    /// [`Self::manifest_document`].
    #[must_use]
    pub fn documents(&self) -> [(&'static str, &[u8]); 4] {
        let [files, summary, sidecar] = self.documents_before_manifest();
        [files, summary, sidecar, self.manifest_document()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup_format::{
        ExportSummaryManifest, parse_files_manifest, verify_manifest_checksum,
    };

    const V1: &[u8] = include_bytes!("../../testdata/extenddb-manifest/v1.json");
    const V1_SIDECAR: &str = include_str!("../../testdata/extenddb-manifest/v1.sha256");

    fn header(manifest: &ExtenddbManifest) -> ManifestHeader {
        ManifestHeader {
            backup_arn: manifest.backup_arn().unwrap(),
            backup_name: manifest.backup_name.clone(),
            source_backend: manifest.source_backend.clone(),
            source_extenddb_version: manifest.source_extenddb_version.clone(),
            snapshot: manifest.snapshot.clone(),
            item_order: manifest.item_order,
            source_table: manifest.source_table.clone(),
        }
    }

    /// `manifest`'s data files paired with `etags`, as `build` takes them.
    fn with_etags(
        manifest: &ExtenddbManifest,
        etags: &[Option<String>],
    ) -> Vec<(DataFileEntry, Option<String>)> {
        assert_eq!(manifest.data_files.len(), etags.len());
        manifest
            .data_files
            .iter()
            .cloned()
            .zip(etags.iter().cloned())
            .collect()
    }

    /// `manifest`'s data files with no etags.
    fn no_etags(manifest: &ExtenddbManifest) -> Vec<(DataFileEntry, Option<String>)> {
        manifest
            .data_files
            .iter()
            .cloned()
            .map(|entry| (entry, None))
            .collect()
    }

    /// Built from the golden manifest's header and files, the manifest is
    /// the fixture's bytes and the sidecar its sidecar, and the two export
    /// manifests agree with it and with each other: one prefix, derived
    /// from the ARN, for every key; one `md5` and item count per file, each
    /// etag on its file.
    #[test]
    fn the_four_documents_agree() {
        let golden = ExtenddbManifest::from_json_bytes(V1).unwrap();
        let etags = [Some("etag-1".to_owned()), None];
        let docs = BackupDocuments::build(
            header(&golden),
            with_etags(&golden, &etags),
            "root/",
            "backups-bucket",
        )
        .unwrap();
        assert_eq!(docs.manifest_bytes(), V1);
        assert_eq!(docs.sidecar(), V1_SIDECAR);
        assert_eq!(docs.manifest(), &golden);
        verify_manifest_checksum(docs.manifest_bytes(), docs.sidecar().as_bytes()).unwrap();
        let prefix = "root/123456789012/Music/01757500000123-0badcafe";
        assert_eq!(docs.key_prefix(), prefix);

        let lines = parse_files_manifest(docs.files_manifest()).unwrap();
        assert_eq!(lines.len(), golden.data_files.len());
        for ((line, entry), etag) in lines.iter().zip(&golden.data_files).zip(&etags) {
            assert_eq!(line.item_count, entry.item_count);
            assert_eq!(line.md5_checksum, entry.md5);
            assert_eq!(&line.etag, etag);
            assert_eq!(line.data_file_s3_key, format!("{prefix}/{}", entry.key));
        }
        let summary = ExportSummaryManifest::from_json(docs.summary()).unwrap();
        assert_eq!(summary.export_arn, golden.backup_arn);
        assert_eq!(summary.s3_bucket, "backups-bucket");
        assert_eq!(summary.s3_prefix, prefix);
        assert_eq!(
            summary.manifest_files_s3_key,
            format!("{prefix}/{FILES_MANIFEST_FILE}")
        );
        assert_eq!(summary.billed_size_bytes, golden.backup_size_bytes);
        assert_eq!(
            u64::try_from(summary.item_count).unwrap(),
            lines.iter().map(|l| l.item_count).sum::<u64>()
        );
        assert_eq!(summary.start_time, "2025-09-10T10:26:40.123Z");

        // The write order: the three documents, then the manifest, and the
        // four together are the same in that order.
        let before: Vec<&str> = docs
            .documents_before_manifest()
            .iter()
            .map(|(name, _)| *name)
            .collect();
        assert_eq!(
            before,
            [
                FILES_MANIFEST_FILE,
                SUMMARY_MANIFEST_FILE,
                MANIFEST_CHECKSUM_FILE
            ]
        );
        assert_eq!(docs.manifest_document(), (EXTENDDB_MANIFEST_FILE, V1));
        let all: Vec<(&str, &[u8])> = docs
            .documents_before_manifest()
            .into_iter()
            .chain([docs.manifest_document()])
            .collect();
        assert_eq!(docs.documents().to_vec(), all);
        assert_eq!(docs.documents()[2].1, V1_SIDECAR.as_bytes());
    }

    /// The summary's `itemCount` is the sum of the files' counts, not the
    /// source table's `item_count`, which is the catalog's figure and can
    /// differ from the snapshot's.
    #[test]
    fn summary_item_count_is_the_sum_of_the_files() {
        let golden = ExtenddbManifest::from_json_bytes(V1).unwrap();
        let sum: u64 = golden.data_files.iter().map(|f| f.item_count).sum();
        let mut header = header(&golden);
        header.source_table.item_count = i64::try_from(sum).unwrap() + 7;
        let docs = BackupDocuments::build(header, no_etags(&golden), "", "b").unwrap();
        let summary = ExportSummaryManifest::from_json(docs.summary()).unwrap();
        assert_eq!(u64::try_from(summary.item_count).unwrap(), sum);
        assert_ne!(summary.item_count, docs.manifest().source_table.item_count);
    }

    /// The prefix is one input: the store's key prefix in any spelling
    /// gives one key prefix, and every key of both export manifests starts
    /// with it. Leading `/` are dropped as trailing ones are, so a
    /// configured `/root/` gives the keys the store writes under `root/`.
    #[test]
    fn one_prefix_for_every_key() {
        let golden = ExtenddbManifest::from_json_bytes(V1).unwrap();
        let backup = "123456789012/Music/01757500000123-0badcafe";
        for (store, expected) in [
            ("", backup.to_owned()),
            ("/", backup.to_owned()),
            ("//", backup.to_owned()),
            ("root", format!("root/{backup}")),
            ("root//", format!("root/{backup}")),
            ("/root/", format!("root/{backup}")),
            ("/a", format!("a/{backup}")),
            ("//a//", format!("a/{backup}")),
            ("a/b/", format!("a/b/{backup}")),
            ("/a/b", format!("a/b/{backup}")),
        ] {
            let docs =
                BackupDocuments::build(header(&golden), no_etags(&golden), store, "b").unwrap();
            assert_eq!(docs.key_prefix(), expected, "{store:?}");
            let summary = ExportSummaryManifest::from_json(docs.summary()).unwrap();
            assert_eq!(summary.s3_prefix, expected);
            assert!(
                summary
                    .manifest_files_s3_key
                    .starts_with(&format!("{expected}/"))
            );
            for line in parse_files_manifest(docs.files_manifest()).unwrap() {
                assert!(
                    line.data_file_s3_key
                        .starts_with(&format!("{expected}/data/")),
                    "{store:?}: {}",
                    line.data_file_s3_key
                );
            }
        }
    }

    /// What the derived fields cannot be: a backup id without an instant
    /// and a file list the reader refuses are each refused before any
    /// document exists, and the size sum is refused where it leaves `i64`,
    /// not only where it leaves `u64`.
    #[test]
    fn inputs_the_reader_would_refuse_build_nothing() {
        let golden = ExtenddbManifest::from_json_bytes(V1).unwrap();
        let mut thirteen = header(&golden);
        thirteen.backup_arn = BackupArn::new(
            "aws",
            "us-east-1",
            "123456789012",
            "Music",
            "1757500000123-0badcafe",
        )
        .unwrap();
        let err = BackupDocuments::build(thirteen, no_etags(&golden), "", "b").unwrap_err();
        assert!(
            matches!(&err, FormatError::InvalidManifest(m) if m.contains("instant")),
            "{err}"
        );
        // Files out of order: the second file's range before the first's.
        let mut files = no_etags(&golden);
        files.reverse();
        assert!(BackupDocuments::build(header(&golden), files, "", "b").is_err());
        // A size total that overflows `u64`, and one that is `2^63`: in
        // `u64` but not in `i64`, so the sum itself is refused, not the
        // manifest it would have made.
        for (first, second) in [(u64::MAX, golden.data_files[1].bytes), (1 << 63, 0)] {
            let mut files = no_etags(&golden);
            files[0].0.bytes = first;
            files[1].0.bytes = second;
            let err = BackupDocuments::build(header(&golden), files, "", "b").unwrap_err();
            assert!(
                matches!(&err, FormatError::InvalidManifest(m)
                    if m == "data file sizes overflow"),
                "{first} + {second}: {err}"
            );
        }
    }
}
