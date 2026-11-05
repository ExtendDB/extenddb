// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bulk put source/sink jail tests.

#[cfg(unix)]
use super::super::jail::{open_verified_account_dir, walk_open_input};
#[cfg(unix)]
use super::super::walk::{MAX_WALK_DEPTH, WalkBounds, walk_regular_files};
use super::*;

#[cfg(unix)]
#[tokio::test]
async fn list_skips_symlinked_files_and_does_not_descend_into_symlinked_dirs() {
    let root = Root::new();
    root.file(OWNER, "loads", "a.json", b"{}\n");
    let victim_file = root.file(OTHER, "loads", "secret.json", b"{}\n");
    let victim_dir = root.bucket_dir(OTHER, "loads");
    let own_bucket = root.bucket_dir(OWNER, "loads");
    std::os::unix::fs::symlink(&victim_file, own_bucket.join("b.json")).unwrap();
    std::os::unix::fs::symlink(&victim_dir, own_bucket.join("sub")).unwrap();

    let files = source(&root)
        .list(&at("loads"), "", u64::MAX)
        .await
        .unwrap();
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, vec!["a.json"], "only the regular file is enumerated");
}

#[tokio::test]
async fn list_with_a_missing_bucket_directory_is_empty() {
    let root = Root::new();
    root.bucket_dir(OWNER, "other-bucket");
    let files = source(&root)
        .list(&at("loads"), "", u64::MAX)
        .await
        .unwrap();
    assert!(files.is_empty());
}

/// The Start existence check never follows a symlink planted at the
/// bucket name: a link to another account's populated bucket, and a
/// link to a directory inside the caller's own subtree, both answer
/// `false`, the verdict a missing bucket gives. A check that resolved
/// the link would answer `true` for the foreign target and let the
/// operation proceed against it.
#[cfg(unix)]
#[tokio::test]
async fn bucket_exists_refuses_to_follow_a_planted_symlink() {
    let root = Root::new();
    // The foreign target genuinely exists and holds files, so a
    // `false` can only come from the symlink refusal, not absence.
    root.file(OTHER, "loads", "part-000.json", b"{}\n");
    let foreign = root.0.join(OTHER).join("loads");
    std::fs::create_dir_all(root.0.join(OWNER)).unwrap();
    std::os::unix::fs::symlink(&foreign, root.0.join(OWNER).join("loads")).unwrap();
    let exists = source(&root).bucket_exists(OWNER, "loads").await.unwrap();
    assert!(!exists, "a symlinked bucket name must answer false");

    // A link inside the caller's own subtree is refused the same way:
    // the check is on the directory entry itself, not its target.
    root.file(OWNER, "real-bucket", "part-000.json", b"{}\n");
    let own_target = root.0.join(OWNER).join("real-bucket");
    std::os::unix::fs::symlink(&own_target, root.0.join(OWNER).join("linked")).unwrap();
    let exists = source(&root).bucket_exists(OWNER, "linked").await.unwrap();
    assert!(!exists, "a symlink to an own directory must answer false");

    // The real directory itself answers true.
    let exists = source(&root)
        .bucket_exists(OWNER, "real-bucket")
        .await
        .unwrap();
    assert!(exists);
}

#[tokio::test]
async fn list_rejects_a_bucket_that_escapes_the_account_subtree() {
    let root = Root::new();
    // The victim's data genuinely exists, so a rejection can only come
    // from the jail and not from the path being absent.
    root.file(OTHER, "loads", "secret.json", b"{}\n");
    for bucket in [
        "..",
        &format!("../{OTHER}"),
        "a/b",
        &format!("../{OTHER}/loads"),
    ] {
        let err = source(&root)
            .list(&at(bucket), "", u64::MAX)
            .await
            .unwrap_err();
        assert!(
            matches!(err, SourceError::Jail(_)),
            "bucket {bucket:?} must be a jail violation, got {err:?}"
        );
    }
}

#[tokio::test]
async fn list_is_lexical_and_filters_by_prefix() {
    let root = Root::new();
    root.file(OWNER, "loads", "c.json", b"3");
    root.file(OWNER, "loads", "a.json", b"1");
    root.file(OWNER, "loads", "a/b.json", b"22");

    let all = source(&root)
        .list(&at("loads"), "", u64::MAX)
        .await
        .unwrap();
    let paths: Vec<&str> = all.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, vec!["a.json", "a/b.json", "c.json"]);
    assert_eq!(all[1].size, 2, "size comes from the entry metadata");

    let some = source(&root)
        .list(&at("loads"), "a", u64::MAX)
        .await
        .unwrap();
    let paths: Vec<&str> = some.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, vec!["a.json", "a/b.json"]);
}

#[tokio::test]
async fn a_bucket_with_one_matching_file_over_the_limit_returns_the_match_past_it() {
    let root = Root::new();
    for i in 0..4 {
        root.file(OWNER, "loads", &format!("part-{i}.json"), b"{}\n");
    }
    let files = source(&root)
        .list(&at("loads"), "", 3)
        .await
        .expect("four files over a limit of three are returned as four");
    assert_eq!(
        files.len(),
        4,
        "the match past the bound is kept so the caller can tell the bound was passed"
    );
}

#[tokio::test]
async fn a_bucket_with_exactly_the_limit_is_listed_whole() {
    let root = Root::new();
    for i in 0..3 {
        root.file(OWNER, "loads", &format!("part-{i}.json"), b"{}\n");
    }
    let files = source(&root).list(&at("loads"), "", 3).await.unwrap();
    assert_eq!(files.len(), 3);
}

/// Observed at the walk itself: collection stops the moment one more
/// match would exceed the limit, so the error arrives with at most
/// `max_files` entries collected, not after the whole bucket is read
/// into the list.
#[cfg(unix)]
#[test]
fn the_walk_collects_no_more_than_the_limit_before_failing() {
    let root = Root::new();
    for i in 0..10 {
        root.file(OWNER, "loads", &format!("part-{i}.json"), b"{}\n");
    }
    let bucket_dir = open_dir_handle(&root.0.join(OWNER).join("loads"));
    let mut out = Vec::new();
    let mut bounds = WalkBounds {
        max_files: 2,
        visit_ceiling: 40,
        visited: 0,
    };
    let err = walk_regular_files(&bucket_dir, "", "", 0, &mut bounds, &mut out)
        .expect_err("ten files over a limit of two must stop the walk");
    assert!(matches!(err, SourceError::TooManyFiles), "got {err:?}");
    assert_eq!(out.len(), 3, "collection stops one match past the limit");
}

/// Nesting deeper than `MAX_WALK_DEPTH` is refused before the walk
/// descends into it, so a chain of single-child directories cannot
/// grow the recursion beyond the cap however large the visit ceiling.
#[cfg(unix)]
#[test]
fn the_walk_refuses_directory_nesting_beyond_the_depth_cap() {
    let root = Root::new();
    let bucket_dir = root.0.join(OWNER).join("loads");
    let mut deep = bucket_dir.clone();
    for _ in 0..=MAX_WALK_DEPTH {
        deep.push("d");
    }
    std::fs::create_dir_all(&deep).expect("deep chain");
    std::fs::write(deep.join("part-000.json"), b"{}\n").expect("leaf file");
    let mut out = Vec::new();
    let mut bounds = WalkBounds {
        max_files: 10,
        visit_ceiling: u64::MAX,
        visited: 0,
    };
    let handle = open_dir_handle(&bucket_dir);
    let err = walk_regular_files(&handle, "", "", 0, &mut bounds, &mut out)
        .expect_err("a chain deeper than the cap must be refused");
    assert!(
        matches!(&err, SourceError::Jail(msg) if msg.contains("nesting exceeds")),
        "got {err:?}"
    );
    assert!(out.is_empty(), "nothing below the cap is enumerated");

    // One level shallower is walked to its leaf.
    let root = Root::new();
    let bucket_dir = root.0.join(OWNER).join("loads");
    let mut ok = bucket_dir.clone();
    for _ in 0..MAX_WALK_DEPTH {
        ok.push("d");
    }
    std::fs::create_dir_all(&ok).expect("chain at the cap");
    std::fs::write(ok.join("part-000.json"), b"{}\n").expect("leaf file");
    let mut out = Vec::new();
    let mut bounds = WalkBounds {
        max_files: 10,
        visit_ceiling: u64::MAX,
        visited: 0,
    };
    let handle = open_dir_handle(&bucket_dir);
    walk_regular_files(&handle, "", "", 0, &mut bounds, &mut out)
        .expect("a chain at the cap is walked");
    assert_eq!(out.len(), 1);
}

/// Driven through the handle walk itself: a directory entry that is a
/// symlink to a directory outside the account is neither descended
/// into nor counted, so the foreign names and sizes never enter the
/// enumeration however the walk is reached.
#[cfg(unix)]
#[test]
fn the_handle_walk_neither_descends_nor_counts_a_symlink_to_a_foreign_directory() {
    let root = Root::new();
    root.file(OWNER, "loads", "part-000.json", b"{}\n");
    // The foreign directory genuinely holds files, so an empty result
    // for it can only come from the walk refusing the link.
    root.file(OTHER, "loads", "stolen-0.json", b"{}\n");
    root.file(OTHER, "loads", "stolen-1.json", b"{}\n");
    let own_bucket = root.0.join(OWNER).join("loads");
    std::os::unix::fs::symlink(root.0.join(OTHER).join("loads"), own_bucket.join("sub")).unwrap();

    let handle = open_dir_handle(&own_bucket);
    let mut out = Vec::new();
    let mut bounds = WalkBounds {
        max_files: 100,
        visit_ceiling: 400,
        visited: 0,
    };
    walk_regular_files(&handle, "", "", 0, &mut bounds, &mut out).unwrap();
    let paths: Vec<&str> = out.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, vec!["part-000.json"], "only the regular file");
    assert_eq!(
        bounds.visited, 2,
        "the link is one visited entry; nothing beneath it is visited"
    );
}

/// The visited-entry ceiling bounds the walk over entries that can hold a
/// key under the prefix even when none of them is a match: more such
/// entries than four times the file limit ends it with the error rather
/// than reading the subtree to the end. Each empty directory below has a
/// name under the prefix, so each is visited and none yields a file.
#[tokio::test]
async fn the_visited_entry_ceiling_bounds_a_walk_over_entries_under_the_prefix_with_no_match() {
    let root = Root::new();
    for i in 0..9 {
        std::fs::create_dir_all(root.bucket_dir(OWNER, "loads").join(format!("match-{i}")))
            .unwrap();
    }
    let err = source(&root)
        .list(&at("loads"), "match-", 2)
        .await
        .expect_err("nine entries under the prefix over a ceiling of eight must be refused");
    assert!(matches!(err, SourceError::TooManyFiles), "got {err:?}");
}

/// Only entries that can hold a key under the prefix count toward the
/// visited-entry ceiling: files and directories beside the prefix, at the
/// bucket and at each level above the matches, are passed over uncounted,
/// so a bucket holding many times the ceiling in other entries still lists
/// the matches. The ceiling here is eight.
#[tokio::test]
async fn entries_outside_the_prefix_do_not_count_toward_the_visit_ceiling() {
    let root = Root::new();
    for i in 0..20 {
        root.file(OWNER, "loads", &format!("other-{i}.json"), b"{}\n");
        root.file(OWNER, "loads", &format!("dir-{i}/part.json"), b"{}\n");
        root.file(OWNER, "loads", &format!("in/other-{i}.json"), b"{}\n");
        root.file(OWNER, "loads", &format!("in/2026-{i}/part.json"), b"{}\n");
    }
    for i in 0..2 {
        root.file(OWNER, "loads", &format!("in/match-{i}.json"), b"{}\n");
    }
    let files = source(&root)
        .list(&at("loads"), "in/match-", 2)
        .await
        .expect("eighty entries beside the prefix are not counted");
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, vec!["in/match-0.json", "in/match-1.json"]);
}

/// The count quota still binds with entries beside the prefix: more
/// matches than the limit come back as the limit plus one, which the
/// runner ends `FAILED` with `S3ObjectsCountLimitExceeded`, rather than as
/// the ceiling error a Start refuses.
#[tokio::test]
async fn matches_over_the_limit_beside_many_other_entries_return_the_match_past_it() {
    let root = Root::new();
    for i in 0..50 {
        root.file(OWNER, "loads", &format!("other-{i}.json"), b"{}\n");
    }
    for i in 0..4 {
        root.file(OWNER, "loads", &format!("match-{i}.json"), b"{}\n");
    }
    let files = source(&root)
        .list(&at("loads"), "match-", 3)
        .await
        .expect("four matches over a limit of three are returned as four");
    assert_eq!(files.len(), 4);
}

/// One directory holding more matches than four times the limit: the walk
/// counts each entry when it examines it, after the matches before it, so
/// it ends at the limit plus one, as a listing does, and Start accepts the
/// operation for the runner to end `FAILED` with
/// `S3ObjectsCountLimitExceeded`, instead of refusing it at the ceiling.
#[tokio::test]
async fn a_directory_of_matches_over_the_ceiling_returns_the_match_past_the_limit() {
    let root = Root::new();
    for i in 0..63 {
        root.file(OWNER, "loads", &format!("in/match-{i:02}.json"), b"{}\n");
    }
    let files = source(&root)
        .list(&at("loads"), "in/", 10)
        .await
        .expect("63 matches over a limit of ten are returned as eleven");
    assert_eq!(files.len(), 11);
}

/// Pruning is what bounds the walk over a large non-matching subtree:
/// the directory is never opened. Proven by making it unreadable, so a
/// walk that entered it would answer a permission error instead of the
/// matches.
#[cfg(unix)]
#[tokio::test]
async fn a_directory_outside_the_prefix_is_never_opened() {
    use std::os::unix::fs::PermissionsExt;
    struct Restore(PathBuf);
    impl Drop for Restore {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        }
    }
    let root = Root::new();
    for i in 0..10_000 {
        root.file(OWNER, "loads", &format!("skip/other-{i}.json"), b"{}\n");
    }
    for i in 0..3 {
        root.file(OWNER, "loads", &format!("match/part-{i}.json"), b"{}\n");
    }
    let skip_dir = root.0.join(OWNER).join("loads").join("skip");
    std::fs::set_permissions(&skip_dir, std::fs::Permissions::from_mode(0o000)).unwrap();
    let _restore = Restore(skip_dir);

    let files = source(&root)
        .list(&at("loads"), "match/", 100)
        .await
        .unwrap();
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "match/part-0.json",
            "match/part-1.json",
            "match/part-2.json"
        ]
    );
}

/// A directory whose own name is a proper prefix of the requested
/// prefix is descended: the matches live below it.
#[tokio::test]
async fn a_directory_that_is_a_proper_prefix_of_the_prefix_is_descended() {
    let root = Root::new();
    root.file(OWNER, "loads", "sub/part-000.json", b"{}\n");
    root.file(OWNER, "loads", "sub/other.json", b"{}\n");
    let files = source(&root)
        .list(&at("loads"), "sub/part-", 10)
        .await
        .unwrap();
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, vec!["sub/part-000.json"]);
}

#[cfg(unix)]
#[tokio::test]
async fn open_refuses_a_fifo_swapped_in_after_enumeration() {
    let root = Root::new();
    let bucket = root.bucket_dir(OWNER, "loads");
    let status = std::process::Command::new("mkfifo")
        .arg(bucket.join("part-000.json"))
        .status()
        .unwrap();
    assert!(status.success(), "mkfifo must succeed for this test");

    let Err(err) = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json", 0),
            InputCodec::None,
        )
        .await
    else {
        panic!("a FIFO is not a regular file");
    };
    assert!(matches!(err, SourceError::NotRegularFile), "got {err:?}");
}

#[cfg(unix)]
#[tokio::test]
async fn open_refuses_a_symlink_swapped_in_after_enumeration() {
    let root = Root::new();
    let victim = root.file(OTHER, "loads", "secret.json", b"{\"stolen\":true}\n");
    let bucket = root.bucket_dir(OWNER, "loads");
    std::os::unix::fs::symlink(&victim, bucket.join("part-000.json")).unwrap();

    let Err(err) = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json", 16),
            InputCodec::None,
        )
        .await
    else {
        panic!("a symlink at the input path must be refused");
    };
    assert!(matches!(err, SourceError::Jail(_)), "got {err:?}");
}

/// The window after the account handle is taken: a symlink swapped in
/// at the final component fails the walk's own `openat`, because it
/// carries `O_NOFOLLOW`.
#[cfg(unix)]
#[test]
fn walk_refuses_a_symlink_swapped_in_after_the_account_handle_is_taken() {
    let root = Root::new();
    let target = root.file(OWNER, "loads", "part-000.json", b"{}\n");
    let victim = root.file(OTHER, "loads", "secret.json", b"{\"stolen\":true}\n");
    let roots = [root.root()];
    let account_dir = open_verified_account_dir(&root.0.join(OWNER), &roots, OWNER)
        .unwrap()
        .expect("the account directory exists");

    std::fs::remove_file(&target).unwrap();
    std::os::unix::fs::symlink(&victim, &target).unwrap();

    let err = walk_open_input(&account_dir, "loads", &["part-000.json"])
        .expect_err("a symlink swapped in after the handle is taken must be refused");
    assert!(matches!(err, SourceError::Jail(_)), "got {err:?}");
}

/// The same window with a FIFO: the open must neither block nor hand
/// the FIFO on, because it carries `O_NONBLOCK` and the handle's file
/// type is checked before the flag is cleared.
#[cfg(unix)]
#[tokio::test]
async fn walk_refuses_a_fifo_swapped_in_after_the_account_handle_is_taken() {
    let root = Root::new();
    let target = root.file(OWNER, "loads", "part-000.json", b"{}\n");
    let roots = [root.root()];
    let account_dir = open_verified_account_dir(&root.0.join(OWNER), &roots, OWNER)
        .unwrap()
        .expect("the account directory exists");
    std::fs::remove_file(&target).unwrap();
    let status = std::process::Command::new("mkfifo")
        .arg(&target)
        .status()
        .unwrap();
    assert!(status.success(), "mkfifo must succeed for this test");

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            walk_open_input(&account_dir, "loads", &["part-000.json"])
        }),
    )
    .await
    .expect("the open must not block on a FIFO with no writer")
    .unwrap();
    let err = result.expect_err("a FIFO swapped in after the handle is taken must be refused");
    assert!(matches!(err, SourceError::NotRegularFile), "got {err:?}");
}

/// A regular file renamed over the enumerated path stays within the
/// caller's subtree by construction of the walk, so it is served: the
/// jail guards containment, not file identity, and the enumerated size
/// still caps the read.
#[cfg(unix)]
#[tokio::test]
async fn a_regular_file_replaced_after_enumeration_is_served_within_the_jail() {
    let root = Root::new();
    let replacement = root.file(OWNER, "loads", "replacement.json", b"{\"b\":2}\n");
    let target = root.file(OWNER, "loads", "part-000.json", b"{\"a\":1}\n");
    let files = source(&root)
        .list(&at("loads"), "part-", u64::MAX)
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    std::fs::rename(&replacement, &target).unwrap();

    let mut reader = source(&root)
        .open(
            &at("loads"),
            &input_file("part-000.json", files[0].size),
            InputCodec::None,
        )
        .await
        .unwrap();
    let lines = read_all(&mut reader, 4096).await;
    assert_eq!(lines, vec!["{\"b\":2}"]);
}

/// The R-1 read-side window: after enumeration validates a path, an
/// ancestor directory of it is swapped for a symlink into a sibling
/// account's subtree. The walk resolves components from the verified
/// account handle, so the swapped component fails its own `openat`
/// instead of redirecting the read.
#[cfg(unix)]
#[tokio::test]
async fn open_refuses_an_ancestor_swapped_for_a_foreign_symlink_after_enumeration() {
    let root = Root::new();
    root.file(OWNER, "loads", "sub/part-000.json", b"{\"mine\":true}\n");
    // The foreign file genuinely exists at the redirected path, so a
    // refusal can only come from the walk and not from absence.
    root.file(OTHER, "loads", "sub/part-000.json", b"{\"stolen\":true}\n");
    let files = source(&root)
        .list(&at("loads"), "", u64::MAX)
        .await
        .unwrap();
    assert_eq!(files[0].path, "sub/part-000.json");

    let own_sub = root.0.join(OWNER).join("loads").join("sub");
    let moved = own_sub.with_file_name("sub-moved");
    std::fs::rename(&own_sub, &moved).unwrap();
    std::os::unix::fs::symlink(root.0.join(OTHER).join("loads").join("sub"), &own_sub).unwrap();

    let Err(err) = source(&root)
        .open(
            &at("loads"),
            &input_file("sub/part-000.json", files[0].size),
            InputCodec::None,
        )
        .await
    else {
        panic!("a swapped ancestor must be refused");
    };
    assert!(matches!(err, SourceError::Jail(_)), "got {err:?}");
}

/// An enumerated path is walked component by component, so a dot or
/// empty component is refused before any filesystem access however the
/// `SourceFile` was constructed.
#[tokio::test]
async fn open_refuses_dot_and_empty_components_in_an_enumerated_path() {
    let root = Root::new();
    root.file(OWNER, "loads", "sub/part-000.json", b"{}\n");
    for rel in [
        "../part-000.json",
        "./part-000.json",
        "sub/../part-000.json",
        "sub//part-000.json",
        "/etc/passwd",
        "",
    ] {
        let Err(err) = source(&root)
            .open(&at("loads"), &input_file(rel, 4), InputCodec::None)
            .await
        else {
            panic!("path {rel:?} must be refused");
        };
        assert!(
            matches!(err, SourceError::Jail(_)),
            "path {rel:?} must be a jail violation, got {err:?}"
        );
    }
}
