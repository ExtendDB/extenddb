// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `wasm-bindgen` surface exposed to the browser app.
//!
//! This module compiles only for `wasm32`. It is a thin wrapper over
//! [`crate::runtime::Engine`]: it installs a panic hook (so a wasm panic
//! reaches `console.error` instead of an opaque trap) and exposes two entry
//! points to JavaScript — construct the engine, and submit a `DynamoDB` request.
//! All engine logic lives in the portable `runtime` module, which the native
//! unit tests cover.

use wasm_bindgen::prelude::*;

use crate::runtime::Engine;

/// A handle to an in-browser `ExtendDB` engine, backed by in-memory storage.
///
/// Construct one with [`ExtendDb::create`], then call [`ExtendDb::submit`] for
/// each `DynamoDB` request.
#[wasm_bindgen]
pub struct ExtendDb {
    engine: Engine,
}

#[wasm_bindgen]
impl ExtendDb {
    /// Create an engine instance backed by a fresh in-browser in-memory `SQLite`
    /// database.
    ///
    /// Installs [`console_error_panic_hook`] once so a panic inside the wasm
    /// module surfaces as a readable `console.error` with a stack trace rather
    /// than an unhelpful `RuntimeError: unreachable`.
    ///
    /// Async because opening the database and running the catalog migrations is
    /// async; it resolves with the ready engine or rejects with an
    /// initialization error. In JavaScript: `const db = await ExtendDb.create();`
    ///
    /// # Errors
    ///
    /// Rejects with the initialization error as a string when the in-memory
    /// database cannot be opened or its catalog migrations fail.
    pub async fn create() -> Result<ExtendDb, JsValue> {
        console_error_panic_hook::set_once();
        let engine = Engine::new().await.map_err(|e| JsValue::from_str(&e))?;
        Ok(ExtendDb { engine })
    }

    /// Submit a `DynamoDB` request and resolve with the response as a JSON string.
    ///
    /// Returns a `Promise<string>`. `operation` is the operation name (the
    /// `X-Amz-Target` suffix, e.g. `"DescribeLimits"`); `body` is the request
    /// body as a JSON string (`"{}"` when the operation takes no parameters).
    ///
    /// Errors are delivered as values, not rejections: an engine error, a
    /// malformed body, or a request the browser build does not support resolves
    /// the promise with a DynamoDB-shaped error document carrying a `__type`
    /// member — the same JSON a real `DynamoDB` endpoint returns.
    ///
    /// The promise rejects only if the module traps. On `wasm32` a panic
    /// cannot unwind: it is an unrecoverable trap that aborts the instance
    /// permanently. The `submit` promise is left **pending** (it neither
    /// resolves nor rejects), and every later call traps as well. The instance
    /// cannot recover; a new one requires reloading the page. The panic hook
    /// writes the panic and its stack to `console.error` first.
    pub fn submit(&self, operation: String, body: String) -> js_sys::Promise {
        // Clone the cheap, Arc-backed engine into the future so it owns its
        // state and does not borrow `self` across the JS boundary (a borrowed
        // future would not be `'static`, which `future_to_promise` requires).
        let engine = self.engine.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            let response = engine.submit(&operation, &body).await;
            Ok(JsValue::from_str(&response))
        })
    }
}
