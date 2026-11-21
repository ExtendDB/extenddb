// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `wasm-bindgen` surface exposed to JavaScript.
//!
//! This module compiles only for `wasm32`. It is a thin wrapper over
//! [`crate::runtime::Engine`]: it installs a panic hook, which writes a
//! panic's message and stack to `console.error` before the trap that follows
//! it, and exposes two entry points to JavaScript — construct the engine, and
//! submit a `DynamoDB` request. All engine logic lives in the portable
//! `runtime` module, which the native unit tests cover.

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
    /// Installs [`console_error_panic_hook`] once, so that a panic inside the
    /// wasm module writes its message and a stack trace to `console.error`.
    /// The hook only logs: the panic still traps, and the trap surfaces as
    /// `RuntimeError: unreachable`.
    ///
    /// Async because opening the database and running the catalog migrations is
    /// async. In JavaScript: `const db = await ExtendDb.create();`. The promise
    /// resolves with the ready engine, or rejects as described under Errors. A
    /// panic during creation traps instead, and the promise then stays pending.
    /// One such panic comes from a host with no Web Crypto
    /// (`globalThis.crypto`, which Node 18 does not define in an ES module):
    /// the `uuid` crate panics when it cannot draw random bytes.
    ///
    /// # Errors
    ///
    /// Rejects with the initialization error as a string when the in-memory
    /// database cannot be opened or its catalog migrations or account setup
    /// fail.
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
    /// The promise never rejects. Errors are delivered as values: an engine
    /// error, a malformed body, a request the browser build does not support,
    /// or an `operation` or `body` that is not a JavaScript string resolves the
    /// promise with `ExtendDB`'s error document, a JSON object carrying a
    /// `__type` member. Both arguments are taken as any JavaScript value and
    /// checked here, so a number, `undefined` or an object answers a
    /// `ValidationException` instead of reaching the engine. A string is
    /// encoded to UTF-8 by the generated JavaScript glue's `TextEncoder`, which
    /// replaces an unpaired surrogate with U+FFFD.
    ///
    /// Call `submit` only on an object that [`ExtendDb::create`] returned and
    /// that has not been freed. The generated JavaScript passes the receiver's
    /// `__wbg_ptr` to the module, which checks only that it is not null, so a
    /// call on anything else is undefined: it can throw (an instance after
    /// `free()`, and an object made with `new ExtendDb()`, throw `Error: null
    /// pointer passed to rust`), trap, or read another instance's memory (an
    /// object that carries a live instance's `__wbg_ptr` answers from that
    /// instance's database).
    ///
    /// On an instance, a trap before `submit` returns its promise throws out of
    /// `submit` itself. When the module cannot allocate the copy of an argument
    /// in its linear memory, a release build traps there and the call throws
    /// `RuntimeError: unreachable`; a debug build throws `Error: invalid malloc
    /// request` instead.
    ///
    /// On `wasm32` a panic cannot unwind: it traps, and the trap throws out of
    /// the task that was running the request, so that request's promise stays
    /// **pending** (it neither resolves nor rejects). The instance is then in an
    /// undefined state: whatever the panicking request held when it stopped is
    /// never released, so a later call may answer, fail or hang. Reload the
    /// page to get a fresh instance. The panic hook writes the panic and its
    /// stack to `console.error` first.
    pub fn submit(&self, operation: &JsValue, body: &JsValue) -> js_sys::Promise {
        // `as_string` is `Some` only for a JavaScript string. It copies the
        // value into linear memory, so nothing below borrows a JS value.
        let operation = operation.as_string();
        let body = body.as_string();
        // Clone the cheap, Arc-backed engine into the future so it owns its
        // state and does not borrow `self` across the JS boundary (a borrowed
        // future would not be `'static`, which `future_to_promise` requires).
        let engine = self.engine.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            let response = match (operation, body) {
                (Some(operation), Some(body)) => engine.submit(&operation, &body).await,
                (None, _) => Engine::non_string_argument("operation"),
                (Some(_), None) => Engine::non_string_argument("body"),
            };
            Ok(JsValue::from_str(&response))
        })
    }
}
