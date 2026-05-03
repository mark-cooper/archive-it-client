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
//! it on the object as user metadata so [`head_sha1`] can return it on
//! later runs for skip-on-match decisions. sha1 is optional;
//! crc64nvme is not.

use std::fmt;

use aws_sdk_s3::Client;
use aws_sdk_s3::types::CompletedPart;

use crate::Error;
use crate::downloads::{DownloadLocation, Prepared, Sink};
use crate::models::wasapi::WasapiFile;

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

// TODO: consider making the minimum part size configurable via a builder on
// `WasapiClient` or an `S3SinkConfig`. 8 MiB is a reasonable floor; S3's
// minimum (except the last part) is 5 MiB.
const MIN_PART_SIZE: usize = 8 * 1024 * 1024;
const MAX_PARTS: u64 = 10_000;

pub(crate) struct S3Sink {
    #[allow(dead_code)]
    client: Client,
    #[allow(dead_code)]
    target: S3Location,
    #[allow(dead_code)]
    part_size: usize,
    #[allow(dead_code)]
    state: SinkState,
}

#[allow(dead_code)]
enum SinkState {
    /// Pre-prepare or post-skip.
    Idle,
    /// Multipart upload underway.
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
            state: SinkState::Idle,
        }
    }
}

impl Sink for S3Sink {
    type Location = S3Location;

    async fn prepare(&mut self, file: &WasapiFile) -> Result<Prepared<Self::Location>, Error> {
        self.part_size = part_size_for(file.size);

        // 1. Refuse zero-byte files (S3 multipart needs at least one part;
        //    callers wanting zero-byte support would need a PutObject path).
        //
        // 2. If `file.checksums.sha1` is Some AND `head_sha1` returns the
        //    matching value, return Skip { location: self.target.clone() }.
        //    Mirrors LocalSink: only sha1 matches count as "skip".
        //
        // 3. Otherwise call `client.create_multipart_upload()` configured
        //    for crc64nvme (and sha1 in metadata if present), stash the
        //    upload_id in self.state = Uploading { .. }.
        //
        // 4. Return Resume { received: 0, partial_sha1: Sha1::new() }.
        //    We never resume a previous interrupted MPU — that would
        //    require external persistence of upload_id + part list.
        todo!("S3Sink::prepare")
    }

    async fn write_chunk(&mut self, _chunk: &[u8]) -> Result<(), Error> {
        // Append to buffer. While buffer.len() >= self.part_size:
        //   - drain a part_size slice into a Bytes
        //   - client.upload_part(...) with the running part_number
        //   - push CompletedPart { e_tag, checksum_crc64nvme, part_number }
        //   - increment next_part_number
        //
        // Trailing < part_size remainder stays in buffer.
        todo!("S3Sink::write_chunk")
    }

    async fn restart(&mut self) -> Result<(), Error> {
        // Server returned 200 to our range request — discard everything.
        // abort_multipart_upload (best-effort, ignore error),
        // create_multipart_upload again, reset buffer/parts,
        // next_part_number = 1.
        todo!("S3Sink::restart")
    }

    async fn finalize(self) -> Result<Self::Location, Error> {
        // 1. Flush remaining buffer as the final part (last part has no
        //    minimum size). If both buffer and parts are empty we shouldn't
        //    be here — prepare() refuses zero-byte files.
        //
        // 2. client.complete_multipart_upload() with the parts list.
        //
        // 3. Verify the returned checksum_crc64nvme is non-empty. If it
        //    isn't, return Error::S3 wrapping a descriptive error —
        //    server-side crc64nvme is the at-rest integrity contract.
        //
        // 4. Engine has already verified our running sha1 against the
        //    WASAPI-supplied expected value before calling finalize, so
        //    by this point bytes have been content-verified end-to-end.
        //
        // 5. Return self.target.
        todo!("S3Sink::finalize")
    }
}

/// Look up the sha1 we may have previously stored on this object.
///
/// Returns `Some(sha1)` if the object exists and carries a recorded sha1,
/// `None` if the object is missing or no sha1 was attached.
#[allow(dead_code)]
async fn head_sha1(_client: &Client, _bucket: &str, _key: &str) -> Result<Option<String>, Error> {
    // client.head_object().bucket(_bucket).key(_key).send().await
    //   - NoSuchKey / 404 → Ok(None)
    //   - other error → Err(Error::S3(...))
    //   - ok → read sha1 out of metadata (the key we wrote in
    //     create_multipart_upload). If absent, Ok(None).
    todo!("head_sha1")
}

fn part_size_for(file_size: u64) -> usize {
    file_size.div_ceil(MAX_PARTS).max(MIN_PART_SIZE as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
