use std::path::PathBuf;
use std::pin::pin;

use archive_it_client::{DownloadOutcome, WasapiClient, WebdataQuery};
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user =
        std::env::var("ARCHIVE_IT_USERNAME").expect("ARCHIVE_IT_USERNAME env var must be set");
    let pass =
        std::env::var("ARCHIVE_IT_PASSWORD").expect("ARCHIVE_IT_PASSWORD env var must be set");

    let client = WasapiClient::new(user, pass)?;
    let dir = PathBuf::from("./warcs");
    // page_size: 1 keeps the listing roundtrip small — paired with the early
    // break below so the example exits after the first file. Real usage
    // would leave page_size at its default and iterate to completion.
    let query = WebdataQuery {
        collection: Some(4472),
        page_size: Some(1),
        ..Default::default()
    };

    let mut stream = pin!(client.download_collection(query, &dir));
    while let Some(outcome) = stream.try_next().await? {
        println!("{outcome}");
        // Example short-circuit: stop after the first file's terminal event
        // (Downloaded / DownloadedUnverified / Skipped / Failed) so the demo
        // doesn't pull the entire collection. Drop this break to download all.
        if !matches!(outcome, DownloadOutcome::Progress { .. }) {
            break;
        }
    }

    Ok(())
}
