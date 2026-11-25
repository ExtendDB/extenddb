// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_storage::key_commands::{self, NO_REVOCATION_NOTICE, Outcome, StatusReport, Summary};
use extenddb_storage::key_pass::{Opener, SecretListing, Survey, TableCounts};
use extenddb_storage::secret_keys::SecretTable;

/// The name of the key that `opener` stands for, from the names of the run's
/// keys: `[0]` the target, `[i + 1]` `Opener::Old(i)`, for example "the
/// catalog key" or "DEK 2".
fn key_name(names: &[String], opener: Opener) -> &str {
    let at = match opener {
        Opener::Target => 0,
        Opener::Old(i) => i + 1,
        Opener::None => return "no key",
    };
    names.get(at).map_or("an unnamed key", String::as_str)
}

/// One line per table: the stored secrets under each key, those that no key
/// opens, and, after a pass, how many it re-encrypted.
fn counts_lines(tables: &[(SecretTable, TableCounts); 2], names: &[String]) -> String {
    let mut out = String::new();
    for (table, counts) in tables {
        let mut parts = vec![format!(
            "{} under {}",
            counts.target,
            key_name(names, Opener::Target)
        )];
        for (i, n) in counts.old.iter().enumerate() {
            parts.push(format!("{n} under {}", key_name(names, Opener::Old(i))));
        }
        parts.push(format!("{} that no key opens", counts.unopenable));
        if counts.rewritten > 0 {
            parts.push(format!("{} re-encrypted", counts.rewritten));
        }
        out.push_str(&format!("  {}: {}\n", table.name(), parts.join(", ")));
    }
    out
}

/// One line per stored secret: its access key ID, owner, creation time and
/// key. Never the secret, and never the row's primary key.
fn listing_lines(listing: &[SecretListing], names: &[String]) -> String {
    listing
        .iter()
        .map(|s| {
            let created = s.created_at.map_or_else(
                || "unknown".to_owned(),
                |t| {
                    t.format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_else(|_| t.to_string())
                },
            );
            format!(
                "  {} {}: account {}, owner {}, created {created}, under {}\n",
                s.table.name(),
                s.access_key_id,
                s.account_id,
                s.principal_name,
                key_name(names, s.opener)
            )
        })
        .collect()
}

fn survey_text(survey: &Survey) -> String {
    let names = &survey.key_names;
    let mut out = format!("Stored secrets:\n{}", counts_lines(&survey.tables, names));
    if !survey.not_target.is_empty() {
        out.push_str(&format!(
            "Stored secrets under a key other than {}:\n{}",
            key_name(names, Opener::Target),
            listing_lines(&survey.not_target, names)
        ));
    }
    out
}

/// What `command` prints for `outcome`.
pub(super) fn outcome_text(command: &str, outcome: &Outcome) -> String {
    match outcome {
        Outcome::NothingToDo { kek_id, dek_id } => format!(
            "Nothing to do: the key ring is already in the target state (KEK id {kek_id}, DEK \
             id {dek_id}).\n"
        ),
        Outcome::Preview(preview) => {
            let mut out = format!("Nothing was written. `encryption-key {command} --yes` would:\n");
            for step in &preview.steps {
                out.push_str(&format!("  {step}\n"));
            }
            if let Some(survey) = &preview.survey {
                out.push_str(&survey_text(survey));
            }
            out
        }
        Outcome::Done(summary) => summary_text(command, summary),
    }
}

/// The summary of section 8, common rules: the KEK id, the DEK id, the
/// counts per table, the connection string row, the stored secrets that no
/// key opens, the `key_file` line and the notice.
fn summary_text(command: &str, summary: &Summary) -> String {
    let names = &summary.key_names;
    let mut out = format!(
        "=== extenddb encryption-key {command} complete ===\nKEK id: {}\nDEK id: {}\n{}",
        summary.kek_id,
        summary.dek_id,
        counts_lines(&summary.tables, names)
    );
    if let Some(rewritten) = summary.data_connection_rewritten {
        out.push_str(if rewritten {
            "The data_database_connection_string row was rewritten.\n"
        } else {
            "The data_database_connection_string row was not rewritten.\n"
        });
    }
    if summary.deleted_catalog_key {
        out.push_str("The encryption_key row was deleted.\n");
    }
    if !summary.deleted_access_key_ids.is_empty() {
        out.push_str(&format!(
            "Deleted access keys: {}\n",
            summary.deleted_access_key_ids.join(", ")
        ));
    }
    if !summary.unopenable.is_empty() {
        out.push_str(&format!(
            "Stored secrets that no key opens, kept as they are:\n{}",
            listing_lines(&summary.unopenable, names)
        ));
    }
    let notice = if command == "reset" {
        key_commands::reset_notice()
    } else {
        NO_REVOCATION_NOTICE.to_owned()
    };
    out.push_str(&format!(
        "Set this line in the [auth.encryption] section of the configuration:\n  {}\n{notice}\n",
        summary.key_file_line(),
    ));
    out
}

/// What `status` prints.
pub(super) fn status_text(report: &StatusReport) -> String {
    let mut out = format!(
        "Catalog key (encryption_key row): {}\n",
        if report.catalog_key_present {
            "present"
        } else {
            "none"
        }
    );
    match &report.ring {
        None => out.push_str("Key ring (encryption_key_ring row): none\n"),
        Some(status) => {
            let ring = &status.ring;
            let ids: Vec<String> = ring.dek_ids().iter().map(u64::to_string).collect();
            out.push_str(&format!(
                "Key ring (encryption_key_ring row): KEK source {}, KEK id {}, DEK ids {}, \
                 switch_from_catalog_key {}, retiring_catalog_key_id {}\n",
                ring.kek.source.name(),
                ring.kek.id,
                ids.join(", "),
                ring.switch_from_catalog_key,
                ring.retiring_catalog_key_id
                    .as_ref()
                    .map_or_else(|| "none".to_owned(), ToString::to_string)
            ));
            out.push_str(&match &status.given_kek_id {
                None => "  Not authenticated: no KEK file was given ([auth.encryption] key_file \
                         is not set).\n"
                    .to_owned(),
                Some(given) if *given != ring.kek.id => format!(
                    "  Not authenticated: the configured KEK has id {given}, and the ring names \
                     KEK id {}.\n",
                    ring.kek.id
                ),
                Some(_) if !status.failed_dek_ids.is_empty() => {
                    let failed: Vec<String> =
                        status.failed_dek_ids.iter().map(u64::to_string).collect();
                    format!(
                        "  Not authenticated: DEK ids {} fail to unwrap under the configured \
                         KEK; the ring has damage or a changed field.\n",
                        failed.join(", ")
                    )
                }
                Some(_) => "  Authenticated under the configured KEK.\n".to_owned(),
            });
        }
    }
    match &report.survey {
        Some(survey) => out.push_str(&survey_text(survey)),
        None => out.push_str("Stored secrets: not read, because no key opens them here.\n"),
    }
    out
}
