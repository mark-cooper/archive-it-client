use ait_client::PublicClient;
use futures::{StreamExt, TryStreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PublicClient::new()?;

    let accounts: Vec<_> = client.accounts().take(3).try_collect().await?;
    println!("first 3 partner accounts:");
    for a in &accounts {
        println!("  {} — {}", a.id, a.organization_name);
    }

    let account = client.get_account(484).await?;
    println!("\naccount 484: {}", account.organization_name);

    let collections: Vec<_> = client.collections(Some(484)).take(3).try_collect().await?;
    println!("\nfirst 3 public collections for account 484:");
    for c in &collections {
        println!("  {} — {}", c.id, c.name);
    }

    let coll = client.get_collection(2135).await?;
    println!(
        "\ncollection 2135: {} ({} active seeds)",
        coll.name, coll.num_active_seeds
    );

    Ok(())
}
