// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stubbed `Bootstrapper` for the S3 Object Annotations backend.
//!
//! Every method returns [`OpError::Internal`] annotated — both in the error
//! string and inline in the source — with the S3 Object Annotations API call a
//! real implementation would issue. This is the direct analog of PR #54's
//! Route 53 bootstrapper, which mapped each method to a Route 53 call
//! (`CreateHostedZone`, `ChangeResourceRecordSets`, …). Here the porting map
//! points at `CreateBucketMetadataConfiguration`,
//! `UpdateBucketMetadataAnnotationTableConfiguration`, `PutObjectAnnotation`,
//! `GetObjectAnnotation`, `ListObjectAnnotations`, and `DeleteObjectAnnotation`.
//!
//! Nothing here talks to S3. The crate exists to register the backend so that
//! `extenddb init --backend s3annotations` reaches a bootstrapper that hands the
//! operator a porting map instead of an "unknown backend" error.

use async_trait::async_trait;
use extenddb_storage::bootstrapper::{AdminBootstrapResult, Bootstrapper};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};

use crate::encoding::DEFAULT_TABLE_OBJECT_KEY;

/// The catalog schema version this (stubbed) backend would target.
const CATALOG_VERSION: &str = "s3annotations-0";

/// Stubbed bootstrapper for the S3 Object Annotations backend.
///
/// Holds the would-be connection coordinates so the display methods can render
/// something honest. No S3 client is constructed.
pub struct S3AnnotationsBootstrapper {
    /// The bucket whose sentinel object plays the role of the catalog "table".
    bucket: String,
    /// The sentinel object key (the "table"); see [`DEFAULT_TABLE_OBJECT_KEY`].
    table_object_key: String,
}

impl S3AnnotationsBootstrapper {
    /// Build a bootstrapper from the config file and CLI args.
    ///
    /// A real implementation would parse the bucket and region out of config;
    /// the stub records sensible placeholders so the lifecycle display commands
    /// have something to print. It never fails, because it never connects.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] only to match the factory signature; the stub
    /// always succeeds.
    pub async fn from_config(
        _config_path: &str,
        _cli_args: &[String],
    ) -> Result<Self, StorageError> {
        Ok(Self {
            bucket: "extenddb-annotations".to_owned(),
            table_object_key: DEFAULT_TABLE_OBJECT_KEY.to_owned(),
        })
    }
}

/// Build the standard "registered but unimplemented" error, naming the S3
/// Object Annotations API call a real implementation would issue. Mirrors the
/// porting-map error shape PR #54 used for Route 53.
fn todo_op(method: &str, maps_to: &str) -> OpError {
    OpError::Internal(format!(
        "{method}: S3 Annotations backend is registered but the relevant \
         operation is not yet implemented. Use --backend postgres, or wire this \
         method to the corresponding S3 Annotations API call (referenced inline \
         below). Maps to S3 Annotations {maps_to}."
    ))
}

#[async_trait]
impl Bootstrapper for S3AnnotationsBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        // S3 has no per-backend "app user"; access is IAM. Provisioning the
        // metadata substrate is the CreateBucketMetadataConfiguration call.
        Err(todo_op(
            "ensure_app_user",
            "CreateBucketMetadataConfiguration",
        ))
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        // No role grant exists; the closest provisioning step is enabling the
        // bucket's metadata configuration.
        Err(todo_op(
            "grant_app_role_to_admin",
            "CreateBucketMetadataConfiguration",
        ))
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        // CreateBucketMetadataConfiguration: enabling S3 Metadata is what
        // brings the Iceberg annotation table into existence.
        Err(todo_op(
            "create_catalog_db",
            "CreateBucketMetadataConfiguration",
        ))
    }

    async fn create_data_db(&self) -> OpResult<()> {
        // CreateBucketMetadataConfiguration on the data bucket.
        Err(todo_op(
            "create_data_db",
            "CreateBucketMetadataConfiguration",
        ))
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        // UpdateBucketMetadataAnnotationTableConfiguration: shape the annotation
        // table that backs the catalog (there is no DDL; you configure it).
        Err(todo_op(
            "run_catalog_migrations",
            "UpdateBucketMetadataAnnotationTableConfiguration",
        ))
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        // UpdateBucketMetadataAnnotationTableConfiguration on the data bucket.
        Err(todo_op(
            "run_data_migrations",
            "UpdateBucketMetadataAnnotationTableConfiguration",
        ))
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        // PutObjectAnnotation: write the data-bucket coordinates as an
        // annotation on the catalog sentinel object.
        Err(todo_op("record_data_connection", "PutObjectAnnotation"))
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        // PutObjectAnnotation: store the wrapped encryption key as an annotation.
        Err(todo_op("bootstrap_encryption_key", "PutObjectAnnotation"))
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        // PutObjectAnnotation: write the default account record.
        Err(todo_op("bootstrap_default_account", "PutObjectAnnotation"))
    }

    async fn bootstrap_admin_user(
        &self,
        _env_user: Option<&str>,
        _env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        // PutObjectAnnotation: write the initial admin user record.
        Err(todo_op("bootstrap_admin_user", "PutObjectAnnotation"))
    }

    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        // ListObjectAnnotations: the catalog is "initialized" iff the sentinel
        // object already carries its bootstrap annotations.
        Err(todo_op("is_catalog_initialized", "ListObjectAnnotations"))
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        // ListObjectAnnotations: enumerate the sentinel object's annotations.
        Err(todo_op("list_table_names", "ListObjectAnnotations"))
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        // GetObjectAnnotation: read the named annotation holding the data-bucket
        // coordinates.
        Err(todo_op("get_data_db_name", "GetObjectAnnotation"))
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        // DeleteObjectAnnotation: tear the catalog down annotation by annotation
        // (or delete the sentinel objects, which cascades to their annotations).
        Err(todo_op("drop_databases", "DeleteObjectAnnotation"))
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        // GetObjectAnnotation: read the named annotation holding the schema
        // version.
        Err(todo_op("read_catalog_version", "GetObjectAnnotation"))
    }

    fn expected_catalog_version(&self) -> String {
        CATALOG_VERSION.to_owned()
    }

    fn catalog_database_name(&self) -> String {
        // The sentinel object is the catalog "database".
        format!("s3://{}/{}", self.bucket, self.table_object_key)
    }

    fn endpoint_info(&self) -> String {
        format!("s3.amazonaws.com (bucket: {})", self.bucket)
    }

    fn catalog_connection_url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.table_object_key)
    }
}
