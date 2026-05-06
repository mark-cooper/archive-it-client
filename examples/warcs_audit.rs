// Match-check report against an S3 bucket using `warcs.csv` as the source
// of truth. For each (filename, sha1, size) row, HEAD the object and
// classify it the way `crate::downloads::s3::should_skip` would:
//   - matched (sha1):  object's sha1 metadata == WASAPI sha1
//   - matched (size):  no sha1 metadata, but content_length == WASAPI size
//   - unmatched:       object exists but neither matches (sink would re-upload)
//   - not found:       HEAD returned NotFound (sink would do a fresh upload)
//   - errored:         any other HEAD failure
//
// Rows in the not-found and unmatched buckets — i.e. anything the sink
// would (re-)upload — are written to `warcs_sync.csv` with the same schema
// as `warcs.csv` so `warcs_sync.rs` can drive a follow-up download for
// each.

use std::env;

use aws_config::BehaviorVersion;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_smithy_types::error::display::DisplayErrorContext;
use csv::{ReaderBuilder, WriterBuilder};
use futures::stream::{self, StreamExt};

const INVENTORY_PATH: &str = "warcs.csv";
const SYNC_PATH: &str = "warcs_sync.csv";
const FILENAME_COL: usize = 3;
const SIZE_COL: usize = 5;
const SHA1_COL: usize = 10;
const SHA1_METADATA_KEY: &str = "sha1";
const PROGRESS_EVERY: u64 = 1000;
const DEFAULT_CONCURRENCY: usize = 200;

enum Outcome {
    MatchedSha1,
    MatchedSize,
    Unmatched(csv::StringRecord, String),
    NotFound(csv::StringRecord, String),
    Errored(String),
    Skipped,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bucket = env::var("S3_BUCKET").expect("S3_BUCKET env var must be set");
    let key_prefix = env::var("S3_KEY_PREFIX").unwrap_or_default();
    let concurrency: usize = env::var("CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CONCURRENCY);

    let aws_cfg = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let s3 = aws_sdk_s3::Client::new(&aws_cfg);

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(INVENTORY_PATH)?;
    let header = rdr.headers()?.clone();
    let mut sync_writer = WriterBuilder::new().from_path(SYNC_PATH)?;
    sync_writer.write_record(&header)?;

    let mut matched_sha1 = 0_u64;
    let mut matched_size = 0_u64;
    let mut unmatched = 0_u64;
    let mut not_found = 0_u64;
    let mut errored = 0_u64;
    let mut skipped_rows = 0_u64;

    let bucket_ref = &bucket;
    let key_prefix_ref = &key_prefix;
    let s3_ref = &s3;

    let stream = stream::iter(rdr.records())
        .map(|record_result| async move {
            let record = record_result?;
            let filename = record.get(FILENAME_COL).unwrap_or("");
            let expected_sha1 = record.get(SHA1_COL).unwrap_or("");
            let size_str = record.get(SIZE_COL).unwrap_or("");
            if filename.is_empty() || size_str.is_empty() {
                return Ok::<Outcome, Box<dyn std::error::Error + Send + Sync>>(Outcome::Skipped);
            }
            let expected_size: u64 = size_str.parse()?;

            let key = format!("{key_prefix_ref}{filename}");
            let outcome = match s3_ref
                .head_object()
                .bucket(bucket_ref)
                .key(&key)
                .send()
                .await
            {
                Ok(out) => {
                    let existing_sha1 = out
                        .metadata
                        .as_ref()
                        .and_then(|m| m.get(SHA1_METADATA_KEY))
                        .map(String::as_str);
                    let existing_size = out.content_length.unwrap_or(0).max(0) as u64;
                    match existing_sha1 {
                        Some(s) if s == expected_sha1 => Outcome::MatchedSha1,
                        Some(s) => {
                            let msg = format!(
                                "unmatched (sha1 differs): s3://{bucket_ref}/{key} \
                                 (existing: {s}, expected: {expected_sha1})"
                            );
                            Outcome::Unmatched(record, msg)
                        }
                        None if existing_size == expected_size => Outcome::MatchedSize,
                        None => {
                            let msg = format!(
                                "unmatched (no sha1, size differs): s3://{bucket_ref}/{key} \
                                 (existing size: {existing_size}, expected: {expected_size})"
                            );
                            Outcome::Unmatched(record, msg)
                        }
                    }
                }
                Err(SdkError::ServiceError(e))
                    if matches!(e.err(), HeadObjectError::NotFound(_)) =>
                {
                    Outcome::NotFound(record, format!("not found: s3://{bucket_ref}/{key}"))
                }
                Err(e) => Outcome::Errored(format!(
                    "error: s3://{bucket_ref}/{key}: {}",
                    DisplayErrorContext(&e)
                )),
            };
            Ok(outcome)
        })
        .buffer_unordered(concurrency);

    tokio::pin!(stream);
    let mut done = 0_u64;
    while let Some(result) = stream.next().await {
        match result? {
            Outcome::MatchedSha1 => matched_sha1 += 1,
            Outcome::MatchedSize => matched_size += 1,
            Outcome::Unmatched(record, msg) => {
                unmatched += 1;
                sync_writer.write_record(&record)?;
                sync_writer.flush()?;
                println!("{msg}");
            }
            Outcome::NotFound(record, msg) => {
                not_found += 1;
                sync_writer.write_record(&record)?;
                sync_writer.flush()?;
                println!("{msg}");
            }
            Outcome::Errored(msg) => {
                errored += 1;
                println!("{msg}");
            }
            Outcome::Skipped => skipped_rows += 1,
        }

        done += 1;
        if done.is_multiple_of(PROGRESS_EVERY) {
            eprintln!(
                "[{done}] sha1={matched_sha1} size={matched_size} \
                 unmatched={unmatched} not_found={not_found} errored={errored}"
            );
        }
    }

    sync_writer.flush()?;
    eprintln!(
        "summary: {matched_sha1} matched (sha1), {matched_size} matched (size), \
         {unmatched} unmatched, {not_found} not found, {errored} errored \
         ({skipped_rows} rows skipped due to missing filename/size)"
    );

    Ok(())
}
