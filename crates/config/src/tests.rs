// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for `extenddb_config`.

use super::*;

#[test]
fn server_region_outside_the_arn_class_is_a_startup_warning() {
    let mut server = ServerConfig::default();
    assert!(server.startup_warnings().is_empty(), "default region");
    server.region = "LOCAL".to_owned();
    let warnings = server.startup_warnings();
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].title.contains("server.region"));
}

#[test]
fn expand_tilde_with_home() {
    let home = std::env::var("HOME").unwrap();
    assert_eq!(expand_tilde("~/foo/bar"), format!("{home}/foo/bar"));
    assert_eq!(expand_tilde("~"), home);
}

#[test]
fn expand_tilde_no_tilde() {
    assert_eq!(expand_tilde("/absolute/path"), "/absolute/path");
    assert_eq!(expand_tilde("relative/path"), "relative/path");
}

#[test]
fn expand_tilde_not_home_prefix() {
    // ~user should NOT be expanded (we only handle ~/...)
    assert_eq!(expand_tilde("~user/foo"), "~user/foo");
}

#[test]
fn pid_file_path_formats_port() {
    assert_eq!(
        pid_file_path("/run/extenddb", 18443),
        PathBuf::from("/run/extenddb/extenddb-18443.pid")
    );
}

/// Build a config from the same environment source `load` uses, fed
/// from the given variables instead of the process environment, over an
/// optional TOML file body.
fn config_from(file: &str, vars: &[(&str, &str)]) -> config::Config {
    let mut env = config::Map::new();
    for (name, value) in vars {
        env.insert((*name).to_owned(), (*value).to_owned());
    }
    config::Config::builder()
        .add_source(config::File::from_str(file, config::FileFormat::Toml))
        .add_source(environment_source().source(Some(env)))
        .build()
        .expect("sources build")
}

/// Issue #539: a list key set through the environment arrives as one
/// string and deserializes to the list the array form gives.
#[test]
fn import_and_export_paths_load_from_environment_variables() {
    let cfg = config_from(
        "",
        &[
            ("EXTENDDB__IMPORT__PATHS", "/data/in, /data/shared"),
            ("EXTENDDB__EXPORT__PATHS", "/data/out"),
        ],
    );
    let import: ImportExportPathConfig = cfg.get("import").expect("import section");
    assert_eq!(import.paths, vec!["/data/in", "/data/shared"]);
    let export: ImportExportPathConfig = cfg.get("export").expect("export section");
    assert_eq!(export.paths, vec!["/data/out"]);
}

/// The environment overrides the file's array, and an empty variable is an
/// empty list.
#[test]
fn an_environment_list_replaces_the_file_array() {
    let file = "[import]\npaths = [\"/from/file\"]\n[export]\npaths = [\"/from/file\"]\n";
    let cfg = config_from(
        file,
        &[
            ("EXTENDDB__IMPORT__PATHS", "/from/env"),
            ("EXTENDDB__EXPORT__PATHS", ""),
        ],
    );
    let import: ImportExportPathConfig = cfg.get("import").expect("import section");
    assert_eq!(import.paths, vec!["/from/env"]);
    let export: ImportExportPathConfig = cfg.get("export").expect("export section");
    assert!(export.paths.is_empty());
    // Without the variables the file array is read as written.
    let cfg = config_from(file, &[]);
    let import: ImportExportPathConfig = cfg.get("import").expect("import section");
    assert_eq!(import.paths, vec!["/from/file"]);
}

/// The `[storage.<backend>]` table is handed to the backend as raw TOML,
/// and an environment value reaches it as a string. The backends'
/// `string_coerce` fields exist for this shape; this pins it so the
/// backend-level string-form tests stay a faithful reproduction.
#[test]
fn storage_section_values_from_the_environment_arrive_as_strings() {
    let cfg = config_from(
        "[storage.sqlite]\npath = \":memory:\"\n",
        &[
            ("EXTENDDB__STORAGE__SQLITE__POOL_SIZE", "4"),
            (
                "EXTENDDB__STORAGE__CASSANDRA__CONTACT_POINTS",
                "a:9042,b:9042",
            ),
        ],
    );
    let storage: toml::Value = cfg.get("storage").expect("storage table");
    assert_eq!(
        storage["sqlite"]["pool_size"],
        toml::Value::String("4".to_owned())
    );
    assert_eq!(
        storage["cassandra"]["contact_points"],
        toml::Value::String("a:9042,b:9042".to_owned())
    );
}
