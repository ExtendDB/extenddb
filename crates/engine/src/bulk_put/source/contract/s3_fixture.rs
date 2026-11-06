// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The S3 store's fixture: the store's clients, built by the store's own
//! connect path for the service `EXTENDDB_TEST_S3_ENDPOINT` names, with an
//! interceptor that records every request they send; buckets named for the
//! case and created, filled and read through the same clients.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use aws_sdk_s3::primitives::ByteStream;

use super::{BulkSink, BulkSource, StoreFixture};
use crate::bulk_put::s3::{S3Clients, S3Sink, S3Source, connect_with};
use crate::bulk_put::store::S3StoreSettings;

const ENDPOINT: &str = "EXTENDDB_TEST_S3_ENDPOINT";
const REQUIRED: &str = "EXTENDDB_TEST_S3_REQUIRED";
const CREDENTIALS: [&str; 2] = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"];

/// One request the store's clients sent.
#[derive(Debug, Clone)]
pub(super) struct Sent {
    /// The method and the URI, for the failure messages.
    pub(super) request: String,
    /// The `x-amz-expected-bucket-owner` header, when it was sent.
    pub(super) owner: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Sent>>>);

impl Recorder {
    fn sent(&self) -> MutexGuard<'_, Vec<Sent>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Intercept for Recorder {
    fn name(&self) -> &'static str {
        "StoreContractRecorder"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request();
        let sent = Sent {
            request: format!("{} {}", request.method(), request.uri()),
            owner: request
                .headers()
                .get("x-amz-expected-bucket-owner")
                .map(str::to_owned),
        };
        self.sent().push(sent);
        Ok(())
    }
}

pub(super) struct S3Fixture {
    clients: Arc<S3Clients>,
    source: S3Source,
    sink: S3Sink,
    buckets: Mutex<Vec<String>>,
    recorder: Recorder,
}

fn is_set(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| !value.is_empty())
}

/// A bucket name of this run's, lowercase as S3 requires.
fn bucket_name(kind: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("contract-{kind}{}", &id[..20])
}

impl S3Fixture {
    /// The fixture for `case`, or `None` after a SKIP line when the S3
    /// service or its credentials are not given. With
    /// `EXTENDDB_TEST_S3_REQUIRED` set, a missing variable fails the case.
    pub(super) async fn connect(case: &str) -> Option<Self> {
        let missing: Vec<&str> = std::iter::once(ENDPOINT)
            .chain(CREDENTIALS)
            .filter(|name| !is_set(name))
            .collect();
        if !missing.is_empty() {
            let missing = missing.join(" and ");
            assert!(
                !is_set(REQUIRED),
                "{case}: {REQUIRED} is set but {missing} is not; the S3 store contract would \
                 pass without an S3 service"
            );
            eprintln!(
                "SKIP {case}: {missing} is not set, so there is no S3 service to run the case \
                 against"
            );
            return None;
        }
        let settings = S3StoreSettings {
            region: "us-east-1".to_owned(),
            endpoint_url: std::env::var(ENDPOINT).ok(),
            force_path_style: true,
        };
        let recorder = Recorder::default();
        let interceptor = recorder.clone();
        let clients = Arc::new(
            connect_with(&settings, move |builder| builder.interceptor(interceptor)).await,
        );
        Some(Self {
            source: S3Source {
                clients: Arc::clone(&clients),
            },
            sink: S3Sink {
                clients: Arc::clone(&clients),
            },
            clients,
            buckets: Mutex::new(Vec::new()),
            recorder,
        })
    }

    /// Every request the store's clients sent so far, the fixture's own
    /// included.
    pub(super) fn sent(&self) -> Vec<Sent> {
        self.recorder.sent().clone()
    }

    async fn remove_bucket(&self, bucket: &str) {
        let uploads = self
            .clients
            .call(bucket, |client| {
                client.list_multipart_uploads().bucket(bucket).send()
            })
            .await
            .map(|output| output.uploads().to_vec())
            .unwrap_or_default();
        for upload in uploads {
            let _ = self
                .clients
                .call(bucket, |client| {
                    client
                        .abort_multipart_upload()
                        .bucket(bucket)
                        .key(upload.key().unwrap_or_default())
                        .upload_id(upload.upload_id().unwrap_or_default())
                        .send()
                })
                .await;
        }
        for key in self.keys(bucket).await {
            let _ = self
                .clients
                .call(bucket, |client| {
                    client.delete_object().bucket(bucket).key(&key).send()
                })
                .await;
        }
        if let Err(error) = self
            .clients
            .call(bucket, |client| {
                client.delete_bucket().bucket(bucket).send()
            })
            .await
        {
            eprintln!(
                "bucket {bucket} was not removed: {}",
                aws_sdk_s3::error::DisplayErrorContext(&error)
            );
        }
    }
}

impl StoreFixture for S3Fixture {
    const S3: bool = true;

    fn source(&self) -> &dyn BulkSource {
        &self.source
    }

    fn sink(&self) -> &dyn BulkSink {
        &self.sink
    }

    async fn bucket(&self) -> String {
        let name = bucket_name("");
        self.clients
            .call(&name, |client| client.create_bucket().bucket(&name).send())
            .await
            .expect("bucket created");
        self.buckets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(name.clone());
        name
    }

    fn missing_bucket(&self) -> String {
        bucket_name("missing-")
    }

    async fn put(&self, bucket: &str, key: &str, body: &[u8]) {
        self.clients
            .call(bucket, |client| {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body.to_vec()))
                    .send()
            })
            .await
            .expect("object written");
    }

    async fn get(&self, bucket: &str, key: &str) -> Option<Vec<u8>> {
        match self
            .clients
            .call(bucket, |client| {
                client.get_object().bucket(bucket).key(key).send()
            })
            .await
        {
            Ok(output) => Some(output.body.collect().await.unwrap().to_vec()),
            Err(error)
                if error.as_service_error().is_some_and(
                    aws_sdk_s3::operation::get_object::GetObjectError::is_no_such_key,
                ) =>
            {
                None
            }
            Err(error) => panic!(
                "reading {key:?}: {}",
                aws_sdk_s3::error::DisplayErrorContext(&error)
            ),
        }
    }

    async fn keys(&self, bucket: &str) -> Vec<String> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let page = self
                .clients
                .call(bucket, |client| {
                    client
                        .list_objects_v2()
                        .bucket(bucket)
                        .set_continuation_token(token.clone())
                        .send()
                })
                .await
                .expect("bucket listed");
            keys.extend(
                page.contents()
                    .iter()
                    .filter_map(|o| o.key().map(str::to_owned)),
            );
            match page.next_continuation_token() {
                Some(next) if page.is_truncated() == Some(true) => token = Some(next.to_owned()),
                _ => break,
            }
        }
        keys.sort();
        keys
    }

    async fn pending(&self, bucket: &str) -> Vec<String> {
        let uploads = self
            .clients
            .call(bucket, |client| {
                client.list_multipart_uploads().bucket(bucket).send()
            })
            .await
            .expect("uploads listed");
        let mut keys: Vec<String> = uploads
            .uploads()
            .iter()
            .filter_map(|upload| upload.key().map(str::to_owned))
            .collect();
        keys.sort();
        keys
    }

    async fn cleanup(self) {
        let buckets =
            std::mem::take(&mut *self.buckets.lock().unwrap_or_else(PoisonError::into_inner));
        for bucket in buckets {
            self.remove_bucket(&bucket).await;
        }
    }
}
