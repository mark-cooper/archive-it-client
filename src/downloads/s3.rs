//! S3 destination for WASAPI downloads.
//!
//! Multipart-upload loop driven by [`aws_sdk_s3::Client`]. The base SDK
//! does not auto-manage multipart, so the `Sink` impl below explicitly
//! drives CreateMultipartUpload / UploadPart / CompleteMultipartUpload.
//!
//! # Checksum contract
//!
//! Every multipart upload is configured with server-side **crc64nvme**
//! (the AWS default). After completion the at-rest object carries a
//! crc64nvme — that's the integrity guarantee.
//!
//! When a sha1 is supplied via `WasapiFile::checksums.sha1`, we record
//! it on the object as user metadata so subsequent runs can compare it
//! for skip-on-match decisions. sha1 is optional; crc64nvme is not.
//!
//! # Skip rules
//!
//! - WASAPI sha1 is Some, object has metadata sha1: skip when they match.
//! - WASAPI sha1 is Some, object lacks metadata sha1: fall back to size.
//! - WASAPI sha1 is None: skip when the existing object's size matches
//!   `file.size`. Without a checksum, size is the only cheap server-side
//!   evidence that the upload would be redundant — re-uploading a
//!   multi-GB WARC every run is the alternative.
//!
//! # IAM permissions
//!
//! `prepare()` always issues HeadObject to drive the skip-on-existing rule,
//! so HeadObject is a hard requirement of this sink. The caller's S3
//! principal must allow:
//!
//! - `s3:GetObject` on the target key — HeadObject is gated on this.
//! - `s3:ListBucket` on the bucket — without it, S3 returns 403 (not 404)
//!   for a missing object, which we cannot distinguish from a real
//!   permission error and which therefore fails fresh uploads. With it,
//!   the missing case maps to `HeadObjectError::NotFound` and we proceed.
//! - `s3:PutObject` on the target key — covers CreateMultipartUpload,
//!   UploadPart, and CompleteMultipartUpload.
//! - `s3:AbortMultipartUpload` on the target key — used by `restart()`.
//!
//! # Aborted multipart uploads
//!
//! Interrupted downloads leave an in-progress multipart upload on S3.
//! We do not auto-abort on Drop or on permanent error: Rust has no
//! AsyncDrop and best-effort async cleanup from sync paths is brittle.
//! Configure an `AbortIncompleteMultipartUpload` lifecycle rule on the
//! target bucket to garbage-collect them.

use std::fmt;

use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart};
use sha1::{Digest, Sha1};

use crate::Error;
use crate::downloads::{DownloadLocation, Prepared, Sink, SinkFactory};
use crate::models::wasapi::WasapiFile;

// TODO: consider making the minimum part size configurable via a builder on
// `WasapiClient` or an `S3SinkConfig`. 8 MiB is a reasonable floor; S3's
// minimum (except the last part) is 5 MiB.
const MIN_PART_SIZE: usize = 8 * 1024 * 1024;
const MAX_PARTS: u64 = 10_000;
const SHA1_METADATA_KEY: &str = "sha1";

/// Uploads each file to `bucket` under `{prefix}{file.filename}`.
pub(crate) struct S3Dest {
    pub(crate) client: Client,
    pub(crate) bucket: String,
    pub(crate) prefix: Option<String>,
}

impl SinkFactory for S3Dest {
    type Sink = S3Sink;
    type Location = S3Location;

    async fn make(&mut self, file: &WasapiFile) -> Result<S3Sink, Error> {
        let key = match &self.prefix {
            Some(p) => format!("{p}{}", file.filename),
            None => file.filename.clone(),
        };
        let target = S3Location {
            bucket: self.bucket.clone(),
            key,
        };
        Ok(S3Sink::new(self.client.clone(), target))
    }
}

/// Identifier for an object in S3.
#[derive(Debug, Clone)]
pub struct S3Location {
    pub bucket: String,
    pub key: String,
}

impl DownloadLocation for S3Location {
    fn fmt_location(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}

impl fmt::Display for S3Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_location(f)
    }
}

pub(crate) struct S3Sink {
    client: Client,
    target: S3Location,
    part_size: usize,
    sha1_meta: Option<String>,
    state: SinkState,
}

enum SinkState {
    Idle,
    Uploading {
        upload_id: String,
        buffer: Vec<u8>,
        next_part_number: i32,
        parts: Vec<CompletedPart>,
    },
}

impl S3Sink {
    pub(crate) fn new(client: Client, target: S3Location) -> Self {
        Self {
            client,
            target,
            part_size: MIN_PART_SIZE,
            sha1_meta: None,
            state: SinkState::Idle,
        }
    }

    async fn create_multipart_upload(&self) -> Result<String, Error> {
        let mut req = self
            .client
            .create_multipart_upload()
            .bucket(&self.target.bucket)
            .key(&self.target.key)
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme);
        if let Some(s) = &self.sha1_meta {
            req = req.metadata(SHA1_METADATA_KEY, s);
        }
        let out = req.send().await.map_err(|e| Error::S3(Box::new(e)))?;
        out.upload_id
            .ok_or_else(|| Error::S3("create_multipart_upload returned no upload_id".into()))
    }
}

impl Sink for S3Sink {
    type Location = S3Location;

    async fn prepare(&mut self, file: &WasapiFile) -> Result<Prepared<Self::Location>, Error> {
        let existing = head_existing(&self.client, &self.target.bucket, &self.target.key).await?;
        if should_skip(file.checksums.sha1.as_deref(), file.size, existing.as_ref()) {
            return Ok(Prepared::Skip {
                location: self.target.clone(),
            });
        }

        // Reject zero-byte uploads only after the skip check, so an existing
        // zero-byte object that already matches can still be skipped.
        if file.size == 0 {
            return Err(Error::S3(
                format!(
                    "refusing to upload zero-byte file {} via multipart",
                    file.filename
                )
                .into(),
            ));
        }
        self.part_size = part_size_for(file.size);

        self.sha1_meta = file.checksums.sha1.clone();
        let upload_id = self.create_multipart_upload().await?;
        self.state = SinkState::Uploading {
            upload_id,
            buffer: Vec::with_capacity(self.part_size),
            next_part_number: 1,
            parts: Vec::new(),
        };
        Ok(Prepared::Resume {
            received: 0,
            partial_sha1: Sha1::new(),
        })
    }

    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Error> {
        let SinkState::Uploading {
            upload_id,
            buffer,
            next_part_number,
            parts,
        } = &mut self.state
        else {
            panic!("write_chunk before prepare");
        };
        buffer.extend_from_slice(chunk);
        while buffer.len() >= self.part_size {
            let part_bytes: Vec<u8> = buffer.drain(..self.part_size).collect();
            let part = upload_part(
                &self.client,
                &self.target,
                upload_id,
                *next_part_number,
                part_bytes,
            )
            .await?;
            parts.push(part);
            *next_part_number += 1;
        }
        Ok(())
    }

    async fn restart(&mut self) -> Result<(), Error> {
        let prev_upload_id = match &self.state {
            SinkState::Uploading { upload_id, .. } => Some(upload_id.clone()),
            SinkState::Idle => None,
        };
        if let Some(id) = prev_upload_id {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.target.bucket)
                .key(&self.target.key)
                .upload_id(id)
                .send()
                .await;
        }
        let upload_id = self.create_multipart_upload().await?;
        self.state = SinkState::Uploading {
            upload_id,
            buffer: Vec::with_capacity(self.part_size),
            next_part_number: 1,
            parts: Vec::new(),
        };
        Ok(())
    }

    async fn finalize(self) -> Result<Self::Location, Error> {
        let SinkState::Uploading {
            upload_id,
            buffer,
            next_part_number,
            mut parts,
        } = self.state
        else {
            panic!("finalize before prepare");
        };

        if !buffer.is_empty() {
            let part = upload_part(
                &self.client,
                &self.target,
                &upload_id,
                next_part_number,
                buffer,
            )
            .await?;
            parts.push(part);
        }

        if parts.is_empty() {
            return Err(Error::S3("finalize called with no parts uploaded".into()));
        }

        let multipart = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        let out = self
            .client
            .complete_multipart_upload()
            .bucket(&self.target.bucket)
            .key(&self.target.key)
            .upload_id(&upload_id)
            .multipart_upload(multipart)
            .send()
            .await
            .map_err(|e| Error::S3(Box::new(e)))?;

        if out.checksum_crc64_nvme.as_deref().unwrap_or("").is_empty() {
            return Err(Error::S3(
                "complete_multipart_upload returned no crc64nvme".into(),
            ));
        }

        Ok(self.target)
    }
}

async fn upload_part(
    client: &Client,
    target: &S3Location,
    upload_id: &str,
    part_number: i32,
    bytes: Vec<u8>,
) -> Result<CompletedPart, Error> {
    let out = client
        .upload_part()
        .bucket(&target.bucket)
        .key(&target.key)
        .upload_id(upload_id)
        .part_number(part_number)
        .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .map_err(|e| Error::S3(Box::new(e)))?;

    Ok(CompletedPart::builder()
        .set_e_tag(out.e_tag)
        .set_checksum_crc64_nvme(out.checksum_crc64_nvme)
        .part_number(part_number)
        .build())
}

#[derive(Debug)]
struct ExistingObject {
    sha1: Option<String>,
    size: u64,
}

async fn head_existing(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<ExistingObject>, Error> {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(out) => {
            let sha1 = out
                .metadata
                .as_ref()
                .and_then(|m| m.get(SHA1_METADATA_KEY))
                .cloned();
            let size = out.content_length.unwrap_or(0).max(0) as u64;
            Ok(Some(ExistingObject { sha1, size }))
        }
        Err(SdkError::ServiceError(e)) if matches!(e.err(), HeadObjectError::NotFound(_)) => {
            Ok(None)
        }
        Err(e) => Err(Error::S3(Box::new(e))),
    }
}

fn part_size_for(file_size: u64) -> usize {
    file_size.div_ceil(MAX_PARTS).max(MIN_PART_SIZE as u64) as usize
}

fn should_skip(
    wasapi_sha1: Option<&str>,
    file_size: u64,
    existing: Option<&ExistingObject>,
) -> bool {
    match (wasapi_sha1, existing) {
        (Some(expected), Some(obj)) => match obj.sha1.as_deref() {
            Some(s) => s == expected,
            None => obj.size == file_size,
        },
        (None, Some(obj)) => obj.size == file_size,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::wasapi::{Checksums, WasapiFile};
    use aws_credential_types::Credentials;
    use aws_sdk_s3::config::{BehaviorVersion, Region};
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;

    fn make_client(replay: &StaticReplayClient) -> Client {
        let cfg = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new("AKIA", "secret", None, None, "test"))
            .region(Region::new("us-east-1"))
            .http_client(replay.clone())
            .build();
        Client::from_conf(cfg)
    }

    fn target() -> S3Location {
        S3Location {
            bucket: "bucket".into(),
            key: "key".into(),
        }
    }

    fn placeholder_req() -> http::Request<SdkBody> {
        // StaticReplayClient does not validate the request unless
        // `assert_requests_match` is called, so a placeholder URI suffices.
        http::Request::builder()
            .method("GET")
            .uri("https://bucket.s3.us-east-1.amazonaws.com/key")
            .body(SdkBody::empty())
            .unwrap()
    }

    fn ok_with_body(body: &'static str) -> http::Response<SdkBody> {
        http::Response::builder()
            .status(200)
            .header("content-type", "application/xml")
            .body(SdkBody::from(body))
            .unwrap()
    }

    fn mk_file(filename: &str, size: u64, sha1: Option<&str>) -> WasapiFile {
        WasapiFile {
            filename: filename.into(),
            filetype: "warc".into(),
            checksums: Checksums {
                sha1: sha1.map(String::from),
                md5: None,
            },
            account: 1,
            size,
            collection: 1,
            crawl: None,
            crawl_time: None,
            crawl_start: None,
            store_time: "2025-01-01T00:00:00Z".into(),
            locations: vec![],
        }
    }

    const CREATE_MPU_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Bucket>bucket</Bucket>
   <Key>key</Key>
   <UploadId>upload-id-1</UploadId>
</InitiateMultipartUploadResult>"#;

    const COMPLETE_MPU_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Location>https://bucket.s3.us-east-1.amazonaws.com/key</Location>
   <Bucket>bucket</Bucket>
   <Key>key</Key>
   <ETag>"final-etag"</ETag>
   <ChecksumCRC64NVME>AAAAAAAAAAA=</ChecksumCRC64NVME>
</CompleteMultipartUploadResult>"#;

    const COMPLETE_MPU_BODY_NO_CRC: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Location>https://bucket.s3.us-east-1.amazonaws.com/key</Location>
   <Bucket>bucket</Bucket>
   <Key>key</Key>
   <ETag>"final-etag"</ETag>
</CompleteMultipartUploadResult>"#;

    fn upload_part_response() -> http::Response<SdkBody> {
        http::Response::builder()
            .status(200)
            .header("etag", "\"part-etag\"")
            .header("x-amz-checksum-crc64nvme", "AAAAAAAAAAA=")
            .body(SdkBody::empty())
            .unwrap()
    }

    fn head_404_response() -> http::Response<SdkBody> {
        http::Response::builder()
            .status(404)
            .body(SdkBody::empty())
            .unwrap()
    }

    fn head_200_response(size: u64, sha1: Option<&str>) -> http::Response<SdkBody> {
        let mut b = http::Response::builder()
            .status(200)
            .header("content-length", size.to_string());
        if let Some(s) = sha1 {
            b = b.header("x-amz-meta-sha1", s);
        }
        b.body(SdkBody::empty()).unwrap()
    }

    #[test]
    fn part_size_uses_minimum_for_small_files() {
        assert_eq!(part_size_for(1), MIN_PART_SIZE);
        assert_eq!(
            part_size_for((MIN_PART_SIZE as u64) * MAX_PARTS),
            MIN_PART_SIZE
        );
    }

    #[test]
    fn part_size_grows_to_stay_under_s3_part_limit() {
        let file_size = (MIN_PART_SIZE as u64) * MAX_PARTS + 1;
        assert_eq!(part_size_for(file_size), MIN_PART_SIZE + 1);
    }

    #[test]
    fn should_skip_when_wasapi_sha1_matches_object_metadata_sha1() {
        let existing = ExistingObject {
            sha1: Some("abc".into()),
            size: 100,
        };
        assert!(should_skip(Some("abc"), 100, Some(&existing)));
    }

    #[test]
    fn should_not_skip_when_wasapi_sha1_differs_from_object_metadata_sha1() {
        let existing = ExistingObject {
            sha1: Some("xxx".into()),
            size: 100,
        };
        assert!(!should_skip(Some("abc"), 100, Some(&existing)));
    }

    #[test]
    fn should_skip_when_wasapi_has_sha1_object_lacks_sha1_and_sizes_match() {
        let existing = ExistingObject {
            sha1: None,
            size: 100,
        };
        assert!(should_skip(Some("abc"), 100, Some(&existing)));
    }

    #[test]
    fn should_not_skip_when_wasapi_has_sha1_object_lacks_sha1_and_sizes_differ() {
        let existing = ExistingObject {
            sha1: None,
            size: 99,
        };
        assert!(!should_skip(Some("abc"), 100, Some(&existing)));
    }

    #[test]
    fn should_skip_when_wasapi_has_no_sha1_and_object_size_matches() {
        let existing = ExistingObject {
            sha1: None,
            size: 100,
        };
        assert!(should_skip(None, 100, Some(&existing)));
    }

    #[test]
    fn should_not_skip_when_wasapi_has_no_sha1_and_object_size_differs() {
        let existing = ExistingObject {
            sha1: None,
            size: 99,
        };
        assert!(!should_skip(None, 100, Some(&existing)));
    }

    #[test]
    fn should_not_skip_when_no_existing_object() {
        assert!(!should_skip(Some("abc"), 100, None));
        assert!(!should_skip(None, 100, None));
    }

    #[tokio::test]
    async fn head_existing_returns_none_on_404() {
        let replay = StaticReplayClient::new(vec![ReplayEvent::new(
            placeholder_req(),
            head_404_response(),
        )]);
        let result = head_existing(&make_client(&replay), "bucket", "key")
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn head_existing_returns_object_with_sha1_metadata() {
        let replay = StaticReplayClient::new(vec![ReplayEvent::new(
            placeholder_req(),
            head_200_response(42, Some("deadbeef")),
        )]);
        let obj = head_existing(&make_client(&replay), "bucket", "key")
            .await
            .unwrap()
            .expect("expected Some(ExistingObject)");
        assert_eq!(obj.sha1.as_deref(), Some("deadbeef"));
        assert_eq!(obj.size, 42);
    }

    #[tokio::test]
    async fn head_existing_returns_object_without_sha1_metadata() {
        let replay = StaticReplayClient::new(vec![ReplayEvent::new(
            placeholder_req(),
            head_200_response(100, None),
        )]);
        let obj = head_existing(&make_client(&replay), "bucket", "key")
            .await
            .unwrap()
            .expect("expected Some(ExistingObject)");
        assert!(obj.sha1.is_none());
        assert_eq!(obj.size, 100);
    }

    #[tokio::test]
    async fn prepare_skips_when_metadata_sha1_matches() {
        let replay = StaticReplayClient::new(vec![ReplayEvent::new(
            placeholder_req(),
            head_200_response(42, Some("abc123")),
        )]);
        let mut sink = S3Sink::new(make_client(&replay), target());
        let prepared = sink
            .prepare(&mk_file("foo.warc", 42, Some("abc123")))
            .await
            .unwrap();
        assert!(matches!(prepared, Prepared::Skip { .. }));
    }

    #[tokio::test]
    async fn prepare_skips_when_no_wasapi_sha1_and_object_size_matches() {
        let replay = StaticReplayClient::new(vec![ReplayEvent::new(
            placeholder_req(),
            head_200_response(100, None),
        )]);
        let mut sink = S3Sink::new(make_client(&replay), target());
        let prepared = sink.prepare(&mk_file("foo.warc", 100, None)).await.unwrap();
        assert!(matches!(prepared, Prepared::Skip { .. }));
    }

    #[tokio::test]
    async fn prepare_skips_zero_byte_when_existing_size_matches() {
        // Regression: zero-byte refusal must NOT fire when the existing
        // object already matches — that path was previously rejected before
        // the skip check ran.
        let replay = StaticReplayClient::new(vec![ReplayEvent::new(
            placeholder_req(),
            head_200_response(0, None),
        )]);
        let mut sink = S3Sink::new(make_client(&replay), target());
        let prepared = sink.prepare(&mk_file("zero.warc", 0, None)).await.unwrap();
        assert!(matches!(prepared, Prepared::Skip { .. }));
    }

    #[tokio::test]
    async fn prepare_rejects_zero_byte_when_no_existing_object() {
        let replay = StaticReplayClient::new(vec![ReplayEvent::new(
            placeholder_req(),
            head_404_response(),
        )]);
        let mut sink = S3Sink::new(make_client(&replay), target());
        let result = sink.prepare(&mk_file("zero.warc", 0, None)).await;
        assert!(matches!(result, Err(Error::S3(_))));
    }

    #[tokio::test]
    async fn sink_uploads_single_part_end_to_end() {
        let replay = StaticReplayClient::new(vec![
            ReplayEvent::new(placeholder_req(), head_404_response()),
            ReplayEvent::new(placeholder_req(), ok_with_body(CREATE_MPU_BODY)),
            ReplayEvent::new(placeholder_req(), upload_part_response()),
            ReplayEvent::new(placeholder_req(), ok_with_body(COMPLETE_MPU_BODY)),
        ]);
        let mut sink = S3Sink::new(make_client(&replay), target());
        sink.prepare(&mk_file("foo", 5, None)).await.unwrap();
        sink.write_chunk(b"hello").await.unwrap();
        let location = sink.finalize().await.unwrap();
        assert_eq!(location.bucket, "bucket");
        assert_eq!(location.key, "key");
    }

    #[tokio::test]
    async fn sink_uploads_multi_part_when_buffer_exceeds_part_size() {
        // Two full parts + a small trailing tail ⇒ three UploadPart calls.
        let part_size = MIN_PART_SIZE;
        let total = (part_size * 2) + 10;
        let replay = StaticReplayClient::new(vec![
            ReplayEvent::new(placeholder_req(), head_404_response()),
            ReplayEvent::new(placeholder_req(), ok_with_body(CREATE_MPU_BODY)),
            ReplayEvent::new(placeholder_req(), upload_part_response()),
            ReplayEvent::new(placeholder_req(), upload_part_response()),
            ReplayEvent::new(placeholder_req(), upload_part_response()),
            ReplayEvent::new(placeholder_req(), ok_with_body(COMPLETE_MPU_BODY)),
        ]);
        let mut sink = S3Sink::new(make_client(&replay), target());
        sink.prepare(&mk_file("big", total as u64, None))
            .await
            .unwrap();
        // Feed in multiple chunks that together cross both part boundaries.
        let chunk = vec![b'x'; part_size];
        sink.write_chunk(&chunk).await.unwrap();
        sink.write_chunk(&chunk).await.unwrap();
        sink.write_chunk(b"tail-bytes").await.unwrap();
        let location = sink.finalize().await.unwrap();
        assert_eq!(location.key, "key");
    }

    #[tokio::test]
    async fn finalize_errors_when_complete_response_lacks_crc() {
        let replay = StaticReplayClient::new(vec![
            ReplayEvent::new(placeholder_req(), head_404_response()),
            ReplayEvent::new(placeholder_req(), ok_with_body(CREATE_MPU_BODY)),
            ReplayEvent::new(placeholder_req(), upload_part_response()),
            ReplayEvent::new(placeholder_req(), ok_with_body(COMPLETE_MPU_BODY_NO_CRC)),
        ]);
        let mut sink = S3Sink::new(make_client(&replay), target());
        sink.prepare(&mk_file("foo", 5, None)).await.unwrap();
        sink.write_chunk(b"hello").await.unwrap();
        let err = sink.finalize().await.unwrap_err();
        assert!(matches!(err, Error::S3(_)));
    }
}
