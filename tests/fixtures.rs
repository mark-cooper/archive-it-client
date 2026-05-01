use ait_client::models::partner::{Account, Collection};
use ait_client::models::public::{PublicAccount, PublicCollection};
use ait_client::models::wasapi::{Page, WasapiFile};

#[test]
fn deser_public_account() {
    let raw = include_str!("../fixtures/api_account_public.json");
    serde_json::from_str::<PublicAccount>(raw).unwrap();
}

#[test]
fn deser_authenticated_account() {
    let raw = include_str!("../fixtures/api_account_authenticated.json");
    serde_json::from_str::<Account>(raw).unwrap();
}

#[test]
fn deser_public_accounts_list() {
    let raw = include_str!("../fixtures/api_accounts_public.json");
    let accts: Vec<PublicAccount> = serde_json::from_str(raw).unwrap();
    assert!(!accts.is_empty());
}

#[test]
fn deser_authenticated_accounts_list() {
    let raw = include_str!("../fixtures/api_accounts_authenticated.json");
    let accts: Vec<Account> = serde_json::from_str(raw).unwrap();
    assert_eq!(accts.len(), 1);
}

#[test]
fn deser_public_collection() {
    let raw = include_str!("../fixtures/api_collection_public.json");
    serde_json::from_str::<PublicCollection>(raw).unwrap();
}

#[test]
fn deser_authenticated_collection() {
    let raw = include_str!("../fixtures/api_collection_authenticated.json");
    serde_json::from_str::<Collection>(raw).unwrap();
}

#[test]
fn deser_public_collections_list() {
    let raw = include_str!("../fixtures/api_collections_public.json");
    let _: Vec<PublicCollection> = serde_json::from_str(raw).unwrap();
}

#[test]
fn deser_authenticated_collections_list() {
    let raw = include_str!("../fixtures/api_collections_authenticated.json");
    let _: Vec<Collection> = serde_json::from_str(raw).unwrap();
}

#[test]
fn deser_wasapi_page() {
    let raw = include_str!("../fixtures/wasapi_collection.json");
    let page: Page<WasapiFile> = serde_json::from_str(raw).unwrap();
    assert!(page.count > 0);
    assert!(!page.files.is_empty());
}
