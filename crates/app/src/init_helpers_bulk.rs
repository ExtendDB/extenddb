// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The commented bulk blocks of the config `extenddb init` writes.

/// Commented bulk `[limits]` keys written into the generated config, with the
/// defaults. Five of the defaults are DynamoDB's quotas, and the other keys are
/// extenddb's own. Kept in sync with `extenddb.sample.toml` and covered by a
/// test in this module.
pub(super) const BULK_LIMITS_TEMPLATE: &str = "\
# Bulk operations (StartBulkPutItems and the workers that drive it).
# DynamoDB's quotas are the defaults of max_concurrent_bulk_operations,
# the three max_bulk_input_* keys and max_bulk_client_token_chars. The
# other keys are ExtendDB's own.
# max_concurrent_bulk_operations = 5         # non-terminal operations per account in each PerformanceTier
# bulk_operation_retention_days = 30         # floored at 1; a purge pass runs at startup and daily
# bulk_operation_worker_concurrency = 4      # operations running at once per process; floored at 1
# bulk_operation_worker_per_account = 2      # of those, at most this many per account; floored at 1
# bulk_operation_lease_seconds = 60          # floored at 1
# bulk_put_batch_size = 25                   # rows per checkpoint; floored at 1, capped at 10000
# bulk_put_write_batch_rows = 25             # rows per storage transaction; capped at 100 and by bulk_put_batch_size
# max_bulk_input_bytes = 1099511627776       # ceiling on the listed (compressed) total per operation, under the tier quota (100 GB STANDARD, 1 TB ACCELERATED); above it the operation fails
# max_bulk_input_files = 50000               # enumerated object count per operation; above it the operation fails
# max_bulk_input_object_bytes = 5368709120   # one enumerated input object; above it the operation fails
# max_bulk_client_token_chars = 1024         # longest accepted ClientToken on StartBulkPutItems, in characters
# max_bulk_line_bytes = 4194304              # one input line; longer abandons the file
# max_bulk_item_count = 100000000            # rows read per operation; more fails the operation
# max_bulk_report_records = 1000000          # records kept per operation; more are dropped and logged
# max_bulk_report_bytes = 1073741824         # serialized bytes of records kept per operation; more are dropped and logged
# bulk_put_write_units_per_second = 80000    # ceiling on bulk writes per process, in 1 KB write units per second; 0 disables";

/// Commented `[bulk]` store block written into the generated config, the
/// same block `extenddb.sample.toml` carries. The feature is disabled until
/// an operator uncomments one of its two examples. Covered by a test in this
/// module.
pub(super) const BULK_STORE_TEMPLATE: &str = "\
# Bulk operations ([bulk]) are disabled by default (secure default). The bulk
# put operations read their input files and write their error reports through
# one store, independent of [import] and [export]. store selects it:
# \"filesystem\" or \"s3\".
#
# The filesystem store reads and writes beneath one root. The layout is
# {path}/{account id}/{S3Bucket}/...: the server creates {path} and the
# per-account directories; the caller creates the bucket directories beneath
# its own subtree and stages files there.
#
# Whoever can write beneath {path}/{account id} can feed data into any table
# that account starts a load on, so an account's subtree is as trusted as the
# mechanism that populates it.
#
# [bulk]
# store = \"filesystem\"
#
# [bulk.filesystem]
# path = \"/var/lib/extenddb/buckets\"
#
# The S3 store reads and writes the buckets the requests name, in AWS S3 or in
# an S3-compatible service at endpoint_url, with the credentials of the
# standard AWS credential chain (environment variables, the shared config and
# credentials files, web identity, container and instance credentials). This
# file holds no credentials. region defaults to [server] region; most
# S3-compatible services need force_path_style = true. An http:// endpoint_url
# must be loopback (127.x.y.z, ::1 or localhost) unless
# allow_insecure_endpoint = true, which accepts it and logs a warning at
# startup. Requests and responses to such an endpoint cross the network
# unencrypted.
#
# Every caller reaches the buckets the server's credentials reach, as far as
# its ExtendDB policies allow s3:ListBucket, s3:GetObject and s3:PutObject on
# them; dev mode evaluates no policy.
#
# [bulk]
# store = \"s3\"
#
# [bulk.s3]
# region = \"us-east-1\"
# endpoint_url = \"https://objects.example.internal:9000\"
# allow_insecure_endpoint = false
# force_path_style = true";

#[cfg(test)]
mod tests {
    use super::{BULK_LIMITS_TEMPLATE, BULK_STORE_TEMPLATE};

    /// Strip one comment marker (`# ` or a bare `#`) from a template line.
    fn uncomment(line: &str) -> &str {
        line.strip_prefix("# ")
            .or_else(|| line.strip_prefix('#'))
            .unwrap_or(line)
    }

    /// Each `[bulk]` block the generated config carries must uncomment to a
    /// section the config parser accepts, so the examples cannot drift from
    /// what the server reads. A block runs from a `# [bulk]` line to the
    /// next prose sentence (a line that uncomments to text starting with a
    /// capital letter); the prose between the blocks is dropped.
    #[test]
    fn bulk_store_template_uncomments_to_a_valid_section() {
        #[derive(serde::Deserialize)]
        struct Section {
            bulk: extenddb_config::BulkConfig,
        }
        let mut closed: Vec<String> = Vec::new();
        let mut current: Vec<&str> = Vec::new();
        for line in BULK_STORE_TEMPLATE.lines() {
            let text = uncomment(line);
            if line == "# [bulk]" {
                if !current.is_empty() {
                    closed.push(current.join("\n"));
                }
                current = vec![text];
            } else if text.starts_with(|c: char| c.is_ascii_uppercase()) {
                if !current.is_empty() {
                    closed.push(current.join("\n"));
                    current = Vec::new();
                }
            } else if !current.is_empty() {
                current.push(text);
            }
        }
        if !current.is_empty() {
            closed.push(current.join("\n"));
        }
        assert_eq!(closed.len(), 2, "one filesystem and one s3 example");

        let filesystem: Section = toml::from_str(&closed[0]).expect("filesystem block parses");
        assert_eq!(
            filesystem.bulk.store,
            extenddb_config::BulkStoreKind::Filesystem,
            "the first example is the filesystem store"
        );
        assert!(
            filesystem
                .bulk
                .filesystem
                .as_ref()
                .is_some_and(|fs| !fs.path.is_empty()),
            "the example names a filesystem path"
        );

        let s3: Section = toml::from_str(&closed[1]).expect("s3 block parses");
        assert_eq!(
            s3.bulk.store,
            extenddb_config::BulkStoreKind::S3,
            "the second example is the S3 store"
        );
        let section = s3.bulk.s3.expect("the example carries [bulk.s3]");
        assert_eq!(section.region.as_deref(), Some("us-east-1"));
        assert!(section.endpoint_url.is_some());
        assert!(!section.allow_insecure_endpoint);
        assert!(section.force_path_style);
        assert_eq!(section.validate(), Ok(Vec::new()), "the example validates");
    }

    /// The generated `[limits]` block must carry every bulk limit key. Taking
    /// the lines from the first key onward drops the prose, and parsing the
    /// uncommented keys confirms each is a valid TOML integer.
    #[test]
    fn bulk_limits_template_lists_every_bulk_key() {
        let body: String = BULK_LIMITS_TEMPLATE
            .lines()
            .skip_while(|l| !l.starts_with("# max_concurrent_bulk_operations"))
            .map(uncomment)
            .collect::<Vec<_>>()
            .join("\n");
        let value: toml::Value = toml::from_str(&body).expect("bulk limit keys parse as TOML");
        let table = value.as_table().expect("a table of keys");
        let expected = [
            "max_concurrent_bulk_operations",
            "bulk_operation_retention_days",
            "bulk_operation_worker_concurrency",
            "bulk_operation_worker_per_account",
            "bulk_operation_lease_seconds",
            "bulk_put_batch_size",
            "bulk_put_write_batch_rows",
            "max_bulk_input_bytes",
            "max_bulk_input_files",
            "max_bulk_input_object_bytes",
            "max_bulk_client_token_chars",
            "max_bulk_line_bytes",
            "max_bulk_item_count",
            "max_bulk_report_records",
            "max_bulk_report_bytes",
            "bulk_put_write_units_per_second",
        ];
        for key in expected {
            assert!(
                table.get(key).is_some_and(toml::Value::is_integer),
                "{key} is present with an integer default"
            );
        }
        assert_eq!(
            table.len(),
            expected.len(),
            "no bulk limit key is listed that the test does not name"
        );
    }
}
