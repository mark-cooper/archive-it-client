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
    let query = WebdataQuery {
        collection: Some(4472),
        page_size: Some(1),
        ..Default::default()
    };

    let mut stream = pin!(client.download_collection(query, &dir));
    match stream.try_next().await? {
        Some(DownloadOutcome::Downloaded { path, file }) => {
            println!("downloaded {} ({} bytes)", path.display(), file.size);
        }
        Some(DownloadOutcome::Skipped { path, .. }) => {
            println!("skipped {} (already present)", path.display());
        }
        None => println!("no files matched query"),
    }

    Ok(())
}
