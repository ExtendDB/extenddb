// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! HTML rendering helpers for the management console.
//!
//! All HTML is generated with Rust string formatting. No template engine
//! dependency. The console is a convenience tool, not a product — simple
//! server-rendered HTML with minimal CSS is sufficient.

use std::fmt::Write;

/// Escape HTML special characters to prevent XSS.
pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            _ => out.push(c),
        }
    }
    out
}

/// Wrap page content with CSRF token injection.
pub fn layout_csrf(title: &str, nav: &str, content: &str, csrf_token: &str) -> String {
    layout_full(title, nav, content, None, Some(csrf_token))
}

/// Wrap page content with version footer, CSRF token, and listen URL.
pub fn layout_with_version_csrf(
    title: &str,
    nav: &str,
    content: &str,
    version_info: Option<&str>,
    csrf_token: &str,
) -> String {
    layout_full_with_url(title, nav, content, version_info, Some(csrf_token), None)
}

/// Full layout with optional CSRF token injection.
pub fn layout_full(
    title: &str,
    nav: &str,
    content: &str,
    version_info: Option<&str>,
    csrf_token: Option<&str>,
) -> String {
    layout_full_with_url(title, nav, content, version_info, csrf_token, None)
}

/// Full layout with optional CSRF token, version footer, and listen URL.
pub fn layout_full_with_url(
    title: &str,
    nav: &str,
    content: &str,
    version_info: Option<&str>,
    csrf_token: Option<&str>,
    listen_url: Option<&str>,
) -> String {
    let csrf_meta = match csrf_token {
        Some(t) => format!(r#"<meta name="csrf-token" content="{}">"#, escape(t)),
        None => String::new(),
    };
    let csrf_script = if csrf_token.is_some() {
        r#"<script>
document.addEventListener('DOMContentLoaded',function(){
var t=document.querySelector('meta[name="csrf-token"]');
if(!t)return;
var v=t.getAttribute('content');
document.querySelectorAll('form').forEach(function(f){
if(f.method&&f.method.toLowerCase()==='post'){
var i=document.createElement('input');
i.type='hidden';i.name='_csrf';i.value=v;
f.appendChild(i);
}});
});
</script>"#
    } else {
        ""
    };
    let tls_line = match listen_url {
        Some(url) => format!(
            r#"Self-signed TLS certificate bound to <strong>{}</strong>"#,
            escape(url),
        ),
        None => String::new(),
    };
    let footer = match (version_info, listen_url) {
        (Some(v), Some(_)) => format!(
            r#"<footer style="max-width:960px;margin:2rem auto 1rem;padding:0 1rem;text-align:center;font-size:0.75rem;color:#999">{}<br>{tls_line}<br><a href="/console/docs" style="color:#999">Documentation</a></footer>"#,
            escape(v),
        ),
        (Some(v), None) => format!(
            r#"<footer style="max-width:960px;margin:2rem auto 1rem;padding:0 1rem;text-align:center;font-size:0.75rem;color:#999">{}<br><a href="/console/docs" style="color:#999">Documentation</a></footer>"#,
            escape(v),
        ),
        (None, Some(_)) => format!(
            r#"<footer style="max-width:960px;margin:2rem auto 1rem;padding:0 1rem;text-align:center;font-size:0.75rem;color:#999">{tls_line}<br><a href="/console/docs" style="color:#999">Documentation</a></footer>"#,
        ),
        (None, None) => r#"<footer style="max-width:960px;margin:2rem auto 1rem;padding:0 1rem;text-align:center;font-size:0.75rem;color:#999"><a href="/console/docs" style="color:#999">Documentation</a></footer>"#.to_owned(),
    };
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
{csrf_meta}
<title>{title} — extenddb Console</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:system-ui,-apple-system,sans-serif;line-height:1.5;color:#1a1a1a;background:#f5f5f5}}
nav{{background:#1a1a2e;color:#fff;padding:0.75rem 1.5rem;display:flex;align-items:center;gap:1.5rem}}
nav a{{color:#a0c4ff;text-decoration:none}}
nav a:hover{{text-decoration:underline}}
nav .brand{{font-weight:700;font-size:1.1rem;color:#fff}}
nav .spacer{{flex:1}}
main{{max-width:960px;margin:1.5rem auto;padding:0 1rem}}
h1{{font-size:1.5rem;margin-bottom:1rem}}
h2{{font-size:1.2rem;margin:1.5rem 0 0.5rem}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:4px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1)}}
th,td{{padding:0.5rem 0.75rem;text-align:left;border-bottom:1px solid #eee}}
th{{background:#f0f0f0;font-weight:600;font-size:0.85rem;text-transform:uppercase;letter-spacing:0.03em}}
a{{color:#2563eb}}
.btn{{display:inline-block;padding:0.4rem 0.8rem;border:none;border-radius:4px;cursor:pointer;font-size:0.9rem;text-decoration:none}}
.btn-primary{{background:#2563eb;color:#fff}}
.btn-danger{{background:#dc2626;color:#fff}}
.btn-sm{{padding:0.25rem 0.5rem;font-size:0.8rem}}
form.inline{{display:inline}}
input,textarea,select{{padding:0.4rem;border:1px solid #ccc;border-radius:4px;font-size:0.9rem}}
textarea{{font-family:monospace;width:100%;min-height:120px}}
label{{display:block;margin:0.5rem 0 0.2rem;font-weight:500;font-size:0.9rem}}
.card{{background:#fff;padding:1rem;border-radius:4px;box-shadow:0 1px 3px rgba(0,0,0,0.1);margin-bottom:1rem}}
.card-warning{{background:#fff8e1;border-left:4px solid #f9a825}}
.alert{{padding:0.75rem;border-radius:4px;margin-bottom:1rem}}
.alert-success{{background:#d1fae5;color:#065f46}}
.alert-error{{background:#fee2e2;color:#991b1b}}
.alert-info{{background:#dbeafe;color:#1e40af}}
.breadcrumb{{font-size:0.85rem;margin-bottom:1rem;color:#666}}
.breadcrumb a{{color:#2563eb}}
.secret-box{{background:#fef3c7;border:1px solid #f59e0b;padding:0.75rem;border-radius:4px;font-family:monospace;word-break:break-all;margin:0.5rem 0}}
</style>
</head>
<body>
{nav}
<main>
{content}
</main>
{footer}
{csrf_script}
</body>
</html>"#,
        title = escape(title),
        nav = nav,
        content = content,
        footer = footer,
        csrf_meta = csrf_meta,
        csrf_script = csrf_script,
    )
}

/// Build the navigation bar for an authenticated session.
pub fn nav_bar(identity_label: &str) -> String {
    format!(
        r#"<nav>
<span class="brand">extenddb Console</span>
<a href="/console">Dashboard</a>
<a href="/console/accounts">Accounts</a>
<a href="/console/metrics">Metrics</a>
<a href="/console/settings">Settings</a>
<a href="/console/cache">Cache</a>
<span class="spacer"></span>
<span>{identity}</span>
<form class="inline" method="post" action="/console/logout">
<button class="btn btn-sm" type="submit">Logout</button>
</form>
</nav>"#,
        identity = escape(identity_label),
    )
}

/// Build a breadcrumb trail from (label, url) pairs. Last item has no link.
pub fn breadcrumb(items: &[(&str, Option<&str>)]) -> String {
    let mut out = String::from(r#"<div class="breadcrumb">"#);
    for (i, (label, url)) in items.iter().enumerate() {
        if i > 0 {
            out.push_str(" &rsaquo; ");
        }
        if let Some(href) = url {
            let _ = write!(out, r#"<a href="{}">{}</a>"#, escape(href), escape(label));
        } else {
            out.push_str(&escape(label));
        }
    }
    out.push_str("</div>");
    out
}

/// Render a success alert.
pub fn alert_success(msg: &str) -> String {
    format!(r#"<div class="alert alert-success">{}</div>"#, escape(msg))
}

/// Render an error alert.
pub fn alert_error(msg: &str) -> String {
    format!(r#"<div class="alert alert-error">{}</div>"#, escape(msg))
}
