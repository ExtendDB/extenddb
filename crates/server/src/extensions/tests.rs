// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Mounting rules for thin-bin extension routers: a mounted router answers
//! under its prefix and behind the security layer; a reserved, nested,
//! duplicate, relative or root prefix is refused with the colliding route
//! named; the empty default mounts nothing.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use tower::ServiceExt;

use super::{
    ConsoleTab, ExtensionMountError, Extensions, body_hash, mount_extensions,
    validate_console_slug, validate_console_slugs, validate_extension_prefix,
    validate_extension_prefixes,
};

fn probe_router() -> Router {
    Router::new().route("/", get(|| async { StatusCode::OK }))
}

async fn send(app: Router, path: &str) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .uri(path)
            .body(Body::empty())
            .expect("request builds"),
    )
    .await
    .expect("router responds")
}

#[tokio::test]
async fn a_mounted_extension_answers_under_its_prefix_behind_the_security_layer() {
    let app = mount_extensions(
        Router::new(),
        vec![("/_extenddb/probe".to_owned(), probe_router())],
    )
    .expect("prefix mounts");
    let app = crate::apply_security_headers(app);

    let resp = send(app, "/_extenddb/probe").await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("x-content-type-options")
            .expect("security layer applied to the extension"),
        "nosniff"
    );
    assert_eq!(
        resp.headers().get("x-frame-options").expect("frame header"),
        "DENY"
    );
}

#[tokio::test]
async fn the_default_extensions_mount_nothing() {
    let app = mount_extensions(Router::new(), Extensions::default().http).expect("empty mounts");
    let app = crate::apply_security_headers(app);

    let resp = send(app, "/_extenddb/probe").await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[test]
fn a_reserved_prefix_is_refused_naming_the_conflict() {
    let err = validate_extension_prefix("/management", &[]).expect_err("must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("/management"),
        "message names the prefix: {msg}"
    );
}

#[test]
fn a_prefix_must_be_absolute_and_not_root() {
    assert!(matches!(
        validate_extension_prefix("relative", &[]),
        Err(ExtensionMountError::NotAbsolute(p)) if p == "relative"
    ));
    assert!(matches!(
        validate_extension_prefix("/", &[]),
        Err(ExtensionMountError::Root)
    ));
    assert!(validate_extension_prefix("/_extenddb/probe", &[]).is_ok());
}

#[test]
fn a_prefix_under_a_reserved_one_is_refused_naming_the_reserved_prefix() {
    for (under, reserved) in [
        ("/console/probe", "/console"),
        ("/console/login/x", "/console"),
        ("/management/accounts", "/management"),
        ("/health/live", "/health"),
        ("/metrics", "/metrics"),
    ] {
        match validate_extension_prefix(under, &[]) {
            Err(ExtensionMountError::Collision {
                prefix,
                conflicts_with,
            }) => {
                assert_eq!(prefix, under);
                assert_eq!(conflicts_with, reserved);
            }
            other => panic!("{under} must collide with {reserved}, got {other:?}"),
        }
    }
    // A prefix that shares the first characters only is not under it.
    assert!(validate_extension_prefix("/consoles", &[]).is_ok());
    assert!(validate_extension_prefix("/healthz", &[]).is_ok());
}

#[test]
fn a_duplicate_extension_prefix_is_refused() {
    let taken = vec!["/_extenddb/a".to_owned()];
    let err = validate_extension_prefix("/_extenddb/a", &taken).expect_err("must reject");
    assert!(matches!(
        err,
        ExtensionMountError::Collision { ref prefix, ref conflicts_with }
            if prefix == "/_extenddb/a" && conflicts_with == "/_extenddb/a"
    ));
}

#[tokio::test]
async fn a_refused_prefix_stops_the_mount_at_that_router() {
    let err = mount_extensions(
        Router::new(),
        vec![
            ("/_extenddb/a".to_owned(), probe_router()),
            ("/console/x".to_owned(), probe_router()),
        ],
    )
    .expect_err("the second prefix is refused");
    assert!(err.to_string().contains("/console/x"));
}

#[test]
fn body_hash_tells_bodies_apart_and_is_stable() {
    let a = body_hash(br#"{"TableName":"t","Key":{"pk":{"S":"1"}}}"#);
    let b = body_hash(br#"{"TableName":"t","Key":{"pk":{"S":"2"}}}"#);
    assert_ne!(a, b);
    assert_eq!(a, body_hash(br#"{"TableName":"t","Key":{"pk":{"S":"1"}}}"#));
}

#[test]
fn a_prefix_at_under_or_above_an_earlier_extension_is_refused_naming_it() {
    let taken = vec!["/_a/b".to_owned()];
    for prefix in ["/_a/b", "/_a/b/c", "/_a"] {
        match validate_extension_prefix(prefix, &taken) {
            Err(ExtensionMountError::Collision {
                prefix: p,
                conflicts_with,
            }) => {
                assert_eq!(p, prefix);
                assert_eq!(conflicts_with, "/_a/b");
            }
            other => panic!("{prefix} must collide with /_a/b, got {other:?}"),
        }
    }
    // A sibling and a prefix sharing only the first characters are fine.
    assert!(validate_extension_prefix("/_a/c", &taken).is_ok());
    assert!(validate_extension_prefix("/_a/bc", &taken).is_ok());
}

#[test]
fn a_prefix_with_a_trailing_slash_empty_segment_parameter_or_wildcard_is_refused() {
    for prefix in [
        "/_x/", "/_x//y", "/{id}", "/_x/{id}", "/{*rest}", "/_x/*", "/_x?y", "/_x#y", "/_x y",
        "/_x\ty",
    ] {
        assert!(
            matches!(
                validate_extension_prefix(prefix, &[]),
                Err(ExtensionMountError::BadShape(p)) if p == prefix
            ),
            "{prefix} must be refused as a bad shape"
        );
    }
    assert!(validate_extension_prefix("/_x/y-z_1.v2", &[]).is_ok());
    assert!(validate_extension_prefix("/_x/a:b", &[]).is_ok());
    assert!(validate_extension_prefix("/_x/%41", &[]).is_ok());
}

#[test]
fn validate_extension_prefixes_checks_the_list_as_the_mount_would() {
    let http = vec![
        ("/_a".to_owned(), probe_router()),
        ("/_a/b".to_owned(), probe_router()),
    ];
    let err = validate_extension_prefixes(&http).expect_err("the second prefix is under the first");
    assert!(err.to_string().contains("/_a/b"));
    assert!(err.to_string().contains("'/_a'"));
    assert!(validate_extension_prefixes(&[]).is_ok());
    assert!(
        validate_extension_prefixes(&[
            ("/_a".to_owned(), probe_router()),
            ("/_b".to_owned(), probe_router())
        ])
        .is_ok()
    );
}

#[test]
fn the_builders_fill_the_fields() {
    #[derive(Default)]
    struct Nop;
    impl super::RequestObserver for Nop {
        fn on_response(&self, _: super::RequestRecord) {}
    }
    let ext = Extensions::default()
        .with_router("/_a", probe_router())
        .with_router("/_b", probe_router())
        .with_request_observer(std::sync::Arc::new(Nop));
    assert_eq!(ext.http.len(), 2);
    assert_eq!(ext.http[0].0, "/_a");
    assert_eq!(ext.http[1].0, "/_b");
    assert!(ext.request_observer.is_some());
}

#[test]
fn a_console_tab_slug_is_one_lower_case_segment_not_reserved_and_not_taken() {
    assert!(validate_console_slug("chaos", &[]).is_ok());
    assert!(validate_console_slug("my-tab-2", &[]).is_ok());
    for slug in ["", "Chaos", "my tab", "a/b", "a_b", "tab.", "/x", "ä"] {
        assert!(
            matches!(validate_console_slug(slug, &[]), Err(ExtensionMountError::BadSlug(s)) if s == slug),
            "{slug:?} must be refused as a bad slug"
        );
    }
    for reserved in [
        "login",
        "logout",
        "docs",
        "metrics",
        "metrics-data",
        "settings",
        "cache",
        "accounts",
        "users",
        "groups",
        "roles",
        "policies",
        "access-keys",
    ] {
        match validate_console_slug(reserved, &[]) {
            Err(ExtensionMountError::ReservedSlug(s)) => assert_eq!(s, reserved),
            other => panic!("{reserved} must be reserved, got {other:?}"),
        }
    }
    let taken = vec!["chaos".to_owned()];
    assert!(matches!(
        validate_console_slug("chaos", &taken),
        Err(ExtensionMountError::RepeatedSlug(s)) if s == "chaos"
    ));
    assert!(validate_console_slug("chaos2", &taken).is_ok());
    assert_eq!(
        ExtensionMountError::ReservedSlug("login".to_owned()).to_string(),
        "console tab slug 'login' is a built-in console route"
    );
    assert_eq!(
        ExtensionMountError::RepeatedSlug("chaos".to_owned()).to_string(),
        "console tab slug 'chaos' is used by an earlier tab"
    );
}

#[test]
fn validate_console_slugs_checks_the_list_in_order() {
    let tabs = vec![
        ConsoleTab {
            label: "A".to_owned(),
            slug: "a".to_owned(),
            router: probe_router(),
        },
        ConsoleTab {
            label: "A again".to_owned(),
            slug: "a".to_owned(),
            router: probe_router(),
        },
    ];
    let err = validate_console_slugs(&tabs).expect_err("the second slug repeats the first");
    assert!(matches!(err, ExtensionMountError::RepeatedSlug(ref s) if s == "a"));
    assert!(validate_console_slugs(&[]).is_ok());
    let ext = Extensions::default().with_console_tab("Chaos", "chaos", probe_router());
    assert_eq!(ext.console_tabs.len(), 1);
    assert_eq!(ext.console_tabs[0].label, "Chaos");
    assert_eq!(ext.console_tabs[0].slug, "chaos");
    assert!(validate_console_slugs(&ext.console_tabs).is_ok());
}
