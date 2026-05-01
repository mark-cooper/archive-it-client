use ait_client::PartnerClient;
use futures::{StreamExt, TryStreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("AITU").expect("AITU env var must be set");
    let pass = std::env::var("AITP").expect("AITP env var must be set");

    let client = PartnerClient::new(user, pass)?;

    let me = client.self_account().await?;
    println!(
        "authenticated as account {} ({})",
        me.id, me.organization_name
    );
    println!("  account_type: {}", me.account_type);

    let collections: Vec<_> = client.collections().take(3).try_collect().await?;
    println!("\nfirst 3 of your collections:");
    for c in &collections {
        println!("  {} — {}", c.id, c.name);
    }

    Ok(())
}
