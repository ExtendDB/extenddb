// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the write batch fault classifier: which storage errors
//! leave the operation to resume and which end it.

use super::super::write::write_batch_fault_resumes;
use super::super::*;

fn internal(message: &str) -> StorageError {
    StorageError::Internal(message.to_owned())
}

#[test]
fn a_full_disk_and_too_many_connections_resume() {
    for code in ["53100", "53300"] {
        let error = internal(&format!("SQLSTATE {code}: insufficient resources"));
        assert!(
            write_batch_fault_resumes(&error),
            "SQLSTATE {code} leaves the operation to resume"
        );
    }
}

/// Out of memory, a configuration limit and the class's generic code can
/// recur on every attempt of the same batch (a transaction that takes more
/// locks than the server allows), so a resume would never end.
#[test]
fn resource_errors_a_batch_can_meet_every_time_stay_terminal() {
    for code in ["53000", "53200", "53400"] {
        let error = internal(&format!("SQLSTATE {code}: insufficient resources"));
        assert!(
            !write_batch_fault_resumes(&error),
            "SQLSTATE {code} ends the operation"
        );
    }
}

#[test]
fn a_disk_full_batch_error_as_postgres_renders_it_resumes() {
    let error = internal(
        "SQLSTATE 53100: error returned from database: could not extend file \
         \"base/16384/16385\": No space left on device",
    );
    assert!(write_batch_fault_resumes(&error));
}

#[test]
fn the_codes_that_already_resumed_still_resume() {
    for code in [
        "08000", "08006", "57P01", "57P02", "57P03", "57014", "55P03", "40001", "40P01",
    ] {
        let error = internal(&format!("SQLSTATE {code}: fault"));
        assert!(
            write_batch_fault_resumes(&error),
            "SQLSTATE {code} leaves the operation to resume"
        );
    }
}

#[test]
fn codes_outside_the_transient_classes_stay_terminal() {
    // 42P01 undefined table, 23505 unique violation, 22001 string too long,
    // 54000 program limit exceeded (a neighbour of class 53 that a retry
    // does not clear), 5 as a bare prefix.
    for code in ["42P01", "23505", "22001", "54000", "5"] {
        let error = internal(&format!("SQLSTATE {code}: fault"));
        assert!(
            !write_batch_fault_resumes(&error),
            "SQLSTATE {code} ends the operation"
        );
    }
}

#[test]
fn a_class_code_in_the_message_text_is_not_read() {
    // The code is read only from the fixed prefix, never from the text.
    let error = internal("error returned from database: SQLSTATE 53100 in the text");
    assert!(!write_batch_fault_resumes(&error));
}

#[test]
fn the_sqlite_busy_and_locked_form_resumes() {
    // SQLite's busy and locked result codes reach the runner as `Transient`
    // through the storage layer's error mapping.
    let error = StorageError::Transient(
        "error returned from database: (code: 5) database is locked".to_owned(),
    );
    assert!(write_batch_fault_resumes(&error));
}

#[test]
fn the_sqlite_full_form_carries_no_sqlstate_and_stays_terminal() {
    // SQLite reports a full disk as result code 13, which its storage layer
    // maps to `Internal` with sqlx's message and no SQLSTATE prefix. The
    // classifier keys on the variant and the prefix only, so this form ends
    // the operation.
    let error = internal("error returned from database: (code: 13) database or disk is full");
    assert!(!write_batch_fault_resumes(&error));
}
