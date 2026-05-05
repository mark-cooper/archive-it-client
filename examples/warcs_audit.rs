// Match-check report against an S3 bucket using `warcs.csv` as the source
// of truth. For each (filename, sha1, size) row, HEAD the object and
// classify it the way `crate::downloads::s3::should_skip` would:
//   - matched (sha1):  object's sha1 metadata == WASAPI sha1
//   - matched (size):  no sha1 metadata, but content_length == WASAPI size
//   - unmatched:       object exists but neither matches (sink would re-upload)
//   - not found:       HEAD returned NotFound (sink would do a fresh upload)
//   - errored:         any other HEAD failure
//
// Rows in the not-found bucket are written to `warcs_not_found.csv` with
// the same schema as `warcs.csv` so a follow-up downloader can parse them
// the same way.

use std::env;

use aws_config::BehaviorVersion;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_smithy_types::error::display::DisplayErrorContext;
use csv::{ReaderBuilder, WriterBuilder};

const INVENTORY_PATH: &str = "warcs.csv";
const NOT_FOUND_PATH: &str = "warcs_not_found.csv";
const FILENAME_COL: usize = 3;
const SIZE_COL: usize = 5;
const SHA1_COL: usize = 10;
const SHA1_METADATA_KEY: &str = "sha1";
const PROGRESS_EVERY: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = env::var("S3_BUCKET").expect("S3_BUCKET env var must be set");
    let key_prefix = env::var("S3_KEY_PREFIX").unwrap_or_default();

    let aws_cfg = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let s3 = aws_sdk_s3::Client::new(&aws_cfg);

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(INVENTORY_PATH)?;
    let header = rdr.headers()?.clone();
    let mut not_found_writer = WriterBuilder::new().from_path(NOT_FOUND_PATH)?;
    not_found_writer.write_record(&header)?;

    let mut matched_sha1 = 0_u64;
    let mut matched_size = 0_u64;
    let mut unmatched = 0_u64;
    let mut not_found = 0_u64;
    let mut errored = 0_u64;
    let mut skipped_rows = 0_u64;

    for (i, record) in rdr.records().enumerate() {
        let record = record?;
        let filename = record.get(FILENAME_COL).unwrap_or("");
        let expected_sha1 = record.get(SHA1_COL).unwrap_or("");
        let size_str = record.get(SIZE_COL).unwrap_or("");
        if filename.is_empty() || size_str.is_empty() {
            skipped_rows += 1;
            continue;
        }
        let expected_size: u64 = size_str.parse()?;

        let key = format!("{key_prefix}{filename}");
        match s3.head_object().bucket(&bucket).key(&key).send().await {
            Ok(out) => {
                let existing_sha1 = out
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get(SHA1_METADATA_KEY))
                    .map(String::as_str);
                let existing_size = out.content_length.unwrap_or(0).max(0) as u64;
                match existing_sha1 {
                    Some(s) if s == expected_sha1 => matched_sha1 += 1,
                    Some(s) => {
                        unmatched += 1;
                        println!(
                            "unmatched (sha1 differs): s3://{bucket}/{key} \
                             (existing: {s}, expected: {expected_sha1})"
                        );
                    }
                    None if existing_size == expected_size => matched_size += 1,
                    None => {
                        unmatched += 1;
                        println!(
                            "unmatched (no sha1, size differs): s3://{bucket}/{key} \
                             (existing size: {existing_size}, expected: {expected_size})"
                        );
                    }
                }
            }
            Err(SdkError::ServiceError(e)) if matches!(e.err(), HeadObjectError::NotFound(_)) => {
                not_found += 1;
                not_found_writer.write_record(&record)?;
                not_found_writer.flush()?;
                println!("not found: s3://{bucket}/{key}");
            }
            Err(e) => {
                errored += 1;
                println!("error: s3://{bucket}/{key}: {}", DisplayErrorContext(&e));
            }
        }

        let done = i + 1;
        if done % PROGRESS_EVERY == 0 {
            eprintln!(
                "[{done}] sha1={matched_sha1} size={matched_size} \
                 unmatched={unmatched} not_found={not_found} errored={errored}"
            );
        }
    }

    not_found_writer.flush()?;
    eprintln!(
        "summary: {matched_sha1} matched (sha1), {matched_size} matched (size), \
         {unmatched} unmatched, {not_found} not found, {errored} errored \
         ({skipped_rows} rows skipped due to missing filename/size)"
    );

    Ok(())
}
