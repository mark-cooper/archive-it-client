use archive_it_client::WasapiClient;
use futures::{StreamExt, TryStreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user =
        std::env::var("ARCHIVE_IT_USERNAME").expect("ARCHIVE_IT_USERNAME env var must be set");
    let pass =
        std::env::var("ARCHIVE_IT_PASSWORD").expect("ARCHIVE_IT_PASSWORD env var must be set");

    let client = WasapiClient::new(user, pass)?;
    let collection_id: u64 = 4472;

    // per-page call when you want pagination metadata (e.g. total count)
    let page = client.list_webdata(collection_id).await?;
    println!("collection {}: {} files total", collection_id, page.count);

    // streaming when you just want to iterate
    let sample: Vec<_> = client.webdata(collection_id).take(3).try_collect().await?;
    println!("\nfirst 3 files:");
    for f in &sample {
        println!("  {} ({} bytes)", f.filename, f.size);
    }

    Ok(())
}
