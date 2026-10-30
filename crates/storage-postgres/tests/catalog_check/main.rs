// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb catalog-check` against the data relations the engine creates.
//!
//! The check compares the relations in the data database with the names the
//! catalog's ids give through the backend's own naming functions. These tests
//! build a deployment the way `init` leaves it (catalog and data in separate
//! databases), create a table with a GSI, an LSI and a vector index through
//! the engine, and run the check and the drop through the backend's
//! `DiagnosticsStore`, which is what the command calls.
//!
//! - `report`: what the check reports, for rows in every status, for a
//!   catalog without the data database setting, and for changes that land
//!   while it runs.
//! - `fix`: what the drop drops and keeps, against an index added while it
//!   waits (at both isolation levels), a relation another session holds or
//!   dropped first.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL
//! `postgres://user:pass@host:port`, no database); without it every test
//! reports a skip and passes. Each test creates its own databases and drops
//! them when its deployment is dropped, on a panic too.

mod fix;
mod report;
mod support;
