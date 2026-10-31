// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Documentation pages — accessible without authentication.
//!
//! - `GET /console/docs` — index listing all documents by category.
//! - `GET /console/docs/{slug}` — rendered HTML doc in console layout.
//! - `GET /console/docs/{slug}/pdf` — PDF download.

use std::fmt::Write;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};

use crate::console::ConsoleState;
use crate::console::html;

/// Navigation bar for unauthenticated doc pages.
const NAV: &str = r#"<nav><span class="brand">extenddb Console</span><span class="spacer"></span><a href="/console/docs">Docs</a> <a href="/console/login">Login</a></nav>"#;

/// GET /console/docs — render the documentation index grouped by category.
pub async fn docs_page(State(state): State<Arc<ConsoleState>>) -> Response {
    let Some(ref store) = state.docs_store else {
        return Html(html::layout_full(
            "Documentation",
            NAV,
            r#"<div class="card card-warning"><p>Documentation is not available. The <code>docs_dir</code> setting is not configured or the directory is missing. See <code>extenddb.sample.toml</code> for details.</p></div>"#,
            None,
            None,
        )).into_response();
    };

    let mut content = String::with_capacity(4096);
    content.push_str(r#"<h1>Documentation</h1>
<div class="card card-warning">
<p style="margin:0;font-size:0.9rem"><strong>ExtendDB is not Amazon DynamoDB.</strong> It is a clean-room implementation that speaks the DynamoDB wire protocol and contains no DynamoDB source code, an open source project under the Apache License 2.0, maintained by engineers at Amazon Web Services. &ldquo;DynamoDB&rdquo; is a trademark of Amazon.com, Inc.</p>
</div>"#);

    let categories = [
        ("getting-started", "Getting Started &amp; Setup"),
        ("usage", "Usage"),
        ("architecture", "Architecture &amp; Design"),
        ("reference", "Reference"),
    ];

    for (cat_key, cat_display) in categories {
        let docs_in_cat: Vec<_> = store
            .entries()
            .iter()
            .filter(|d| d.category == cat_key)
            .collect();
        if docs_in_cat.is_empty() {
            continue;
        }
        let _ = write!(content, r#"<div class="card"><h2>{cat_display}</h2><ul>"#);
        for doc in docs_in_cat {
            let _ = write!(
                content,
                r#"<li><a href="/console/docs/{slug}">{title}</a> · <a href="/console/docs/{slug}/pdf" style="font-size:0.85rem;color:#666">PDF</a></li>"#,
                slug = html::escape(&doc.slug),
                title = html::escape(&doc.title),
            );
        }
        content.push_str("</ul></div>");
    }

    Html(html::layout_full(
        "Documentation",
        NAV,
        &content,
        None,
        None,
    ))
    .into_response()
}

/// GET /console/docs/{slug} — render a single document in the console layout.
pub async fn docs_view(
    State(state): State<Arc<ConsoleState>>,
    Path(slug): Path<String>,
) -> Response {
    let Some(ref store) = state.docs_store else {
        return (StatusCode::NOT_FOUND, "Documentation not available").into_response();
    };
    let Some(entry) = store.find(&slug) else {
        return (StatusCode::NOT_FOUND, "Document not found").into_response();
    };
    let Some(html_content) = store.read_html(&slug) else {
        return (StatusCode::NOT_FOUND, "Document file not found").into_response();
    };

    let mut content = String::with_capacity(html_content.len() + 256);
    let _ = write!(
        content,
        r#"<div style="margin-bottom:1rem"><a href="/console/docs">&larr; All Documents</a> · <a href="/console/docs/{slug}/pdf">Download PDF</a></div>"#,
        slug = html::escape(&entry.slug),
    );
    content.push_str(r#"<div class="card">"#);
    // HTML fragment is pre-rendered by build-docs.py — safe to embed directly.
    content.push_str(&html_content);
    content.push_str("</div>");
    Html(html::layout_full(&entry.title, NAV, &content, None, None)).into_response()
}

/// GET /console/docs/{slug}/pdf — serve the PDF from disk.
pub async fn docs_pdf(
    State(state): State<Arc<ConsoleState>>,
    Path(slug): Path<String>,
) -> Response {
    let Some(ref store) = state.docs_store else {
        return (StatusCode::NOT_FOUND, "Documentation not available").into_response();
    };
    let Some(entry) = store.find(&slug) else {
        return (StatusCode::NOT_FOUND, "Document not found").into_response();
    };
    let Some(pdf_bytes) = store.read_pdf(&slug) else {
        return (StatusCode::NOT_FOUND, "PDF file not found").into_response();
    };

    let filename = format!("extenddb-{}.pdf", entry.slug);
    (
        [
            (header::CONTENT_TYPE, "application/pdf"),
            (
                header::CONTENT_DISPOSITION,
                &format!("inline; filename=\"{filename}\""),
            ),
        ],
        pdf_bytes,
    )
        .into_response()
}
