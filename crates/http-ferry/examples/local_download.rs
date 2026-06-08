use std::env;
use std::error::Error;
use std::time::Duration;

use futures_util::StreamExt;
use http_ferry::local::LocalDir;
use http_ferry::{Checksum, Download, Downloader, Outcome};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let url: Url = args.next().ok_or(usage())?.parse()?;
    let output_dir = args.next().ok_or(usage())?;
    let checksum = parse_checksum(&args.next().ok_or(usage())?)?;

    if args.next().is_some() {
        return Err(usage().into());
    }

    let client = reqwest::Client::builder().build()?;
    let download = source_file(&client, url, checksum).await?;
    let downloader = Downloader::builder(client)
        .max_attempts(3)
        .backoff(Duration::from_millis(250))
        .build();
    let items = futures_util::stream::iter([Ok(download)]);
    let destination = LocalDir::create_all(output_dir)?;

    let mut last_received = 0;
    let outcomes = http_ferry::drive_downloads(&downloader, items, destination);
    let mut outcomes = std::pin::pin!(outcomes);

    while let Some(outcome) = outcomes.next().await {
        match outcome {
            Outcome::Progress {
                received, total, ..
            } => {
                if received < last_received || received > total {
                    return Err(format!("invalid progress: {received} / {total}").into());
                }
                last_received = received;
                eprint!(
                    "\r{:.1}% ({received} / {total} bytes)",
                    percent(received, total)
                );
            }
            Outcome::Downloaded {
                location, verified, ..
            } => {
                eprintln!();
                if !verified {
                    return Err("download finished without checksum verification".into());
                }
                println!("downloaded and verified {}", location.display());
            }
            Outcome::Skipped { location, .. } => {
                println!(
                    "skipped {}; existing file already matches checksum",
                    location.display()
                );
            }
            Outcome::Failed { error, .. } | Outcome::StreamFailed { error } => {
                return Err(error.into());
            }
        }
    }

    Ok(())
}

async fn source_file(
    client: &reqwest::Client,
    url: Url,
    checksum: Checksum,
) -> Result<Download, Box<dyn Error>> {
    let response = client.head(url.clone()).send().await?.error_for_status()?;
    let size = response
        .content_length()
        .ok_or("source did not provide a Content-Length header")?;
    let name = file_name(&url)?.to_owned();

    Ok(Download {
        url,
        size,
        checksum: Some(checksum),
        name,
    })
}

fn file_name(url: &Url) -> Result<&str, Box<dyn Error>> {
    url.path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "source URL path must end with a file name".into())
}

fn parse_checksum(value: &str) -> Result<Checksum, Box<dyn Error>> {
    let (algorithm, hex) = value
        .split_once(':')
        .ok_or("checksum must look like sha1:<hex> or md5:<hex>")?;
    let hex = hex.to_ascii_lowercase();

    if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("checksum digest must be lowercase or uppercase hex".into());
    }

    match algorithm {
        "sha1" if hex.len() == 40 => Ok(Checksum::Sha1(hex)),
        "md5" if hex.len() == 32 => Ok(Checksum::Md5(hex)),
        "sha1" => Err("sha1 digest must be 40 hex characters".into()),
        "md5" => Err("md5 digest must be 32 hex characters".into()),
        _ => Err("checksum algorithm must be sha1 or md5".into()),
    }
}

fn percent(received: u64, total: u64) -> f64 {
    if total == 0 {
        100.0
    } else {
        (received as f64 / total as f64) * 100.0
    }
}

fn usage() -> &'static str {
    "usage: cargo run -p http-ferry --example local_download -- <url> <output-dir> <sha1:<hex>|md5:<hex>>"
}
