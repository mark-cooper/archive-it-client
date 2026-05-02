use archive_it_client::PartnerClient;
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("ARCHIVE_IT_USERNAME")?;
    let pass = std::env::var("ARCHIVE_IT_PASSWORD")?;

    let client = PartnerClient::new(user, pass)?;

    let total_warc_bytes = client
        .collections()
        .try_fold(0_u64, |sum, collection| async move {
            Ok(sum + collection.total_warc_bytes)
        })
        .await?;

    println!("total_warc_bytes: {total_warc_bytes}");

    Ok(())
}
