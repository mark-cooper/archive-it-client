use std::time::Duration;

use archive_it_client::models::wasapi::{Checksums, WasapiFile};
use archive_it_client::{
    Config, DownloadOutcome, Error, PageOpts, PartnerClient, PublicClient, WasapiClient,
    WebdataQuery,
};
use serde_json::json;
use sha1::{Digest, Sha1};
use tempfile::TempDir;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(server: &MockServer) -> Config {
    let mut cfg = Config::api();
    cfg.base_url = format!("{}/", server.uri());
    cfg.max_attempts = 1;
    cfg.timeout = Duration::from_secs(2);
    cfg.download_timeout = Duration::from_secs(2);
    cfg.backoff = Duration::from_millis(1);
    cfg
}

fn full_account() -> serde_json::Value {
    serde_json::from_str(include_str!("../fixtures/api_account_authenticated.json")).unwrap()
}

fn public_account_stub(id: u64) -> serde_json::Value {
    json!({
        "id": id,
        "organization_name": format!("org{id}"),
        "partner_description": null,
        "logo_blob": null,
        "partner_url": "x",
        "custom_crawl_schedules_visible": false,
    })
}

#[tokio::test]
async fn public_list_accounts_sends_pagination_params() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/account"))
        .and(query_param("limit", "50"))
        .and(query_param("offset", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(1)
        .mount(&server)
        .await;

    let client = PublicClient::with_config(config(&server)).unwrap();
    let result = client
        .list_accounts(PageOpts {
            limit: Some(50),
            offset: Some(100),
        })
        .await
        .unwrap();

    assert!(result.is_empty());
}

#[tokio::test]
async fn partner_attaches_basic_auth() {
    let server = MockServer::start().await;
    // "user:pass" base64-encoded
    Mock::given(method("GET"))
        .and(path("/account"))
        .and(header("authorization", "Basic dXNlcjpwYXNz"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([full_account()])))
        .expect(1)
        .mount(&server)
        .await;

    let client = PartnerClient::with_config("user", "pass", config(&server)).unwrap();
    let acct = client.my_account().await.unwrap();

    assert_eq!(acct.id, 0);
}

#[tokio::test]
async fn partner_caches_self_id_across_list_calls() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/account"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([full_account()])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/collection"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;

    let client = PartnerClient::with_config("u", "p", config(&server)).unwrap();
    client.list_collections(PageOpts::default()).await.unwrap();
    client.list_collections(PageOpts::default()).await.unwrap();

    let received = server.received_requests().await.unwrap();
    let account_hits = received
        .iter()
        .filter(|r| r.url.path() == "/account")
        .count();
    assert_eq!(account_hits, 1);
}

#[tokio::test]
async fn partner_get_collection_returns_some_when_in_user_list() {
    let server = MockServer::start().await;
    let collection: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/api_collection_authenticated.json"
    ))
    .unwrap();
    Mock::given(method("GET"))
        .and(path("/account"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([full_account()])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/collection"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([collection])))
        .mount(&server)
        .await;

    let client = PartnerClient::with_config("u", "p", config(&server)).unwrap();
    let found = client.get_collection(0).await.unwrap();

    assert!(found.is_some());
    assert_eq!(found.unwrap().id, 0);
}

#[tokio::test]
async fn partner_get_collection_returns_none_when_not_in_user_list() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/account"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([full_account()])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/collection"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(1)
        .mount(&server)
        .await;

    let client = PartnerClient::with_config("u", "p", config(&server)).unwrap();
    let found = client.get_collection(999).await.unwrap();

    assert!(found.is_none());
}

#[tokio::test]
async fn not_found_maps_to_error_and_does_not_retry() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/account/999"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let mut cfg = config(&server);
    cfg.max_attempts = 5;
    let client = PublicClient::with_config(cfg).unwrap();
    let err = client.get_account(999).await.unwrap_err();

    assert!(matches!(err, Error::NotFound(_)));
}

#[tokio::test]
async fn server_error_retries_then_surfaces_status() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/account"))
        .respond_with(ResponseTemplate::new(503))
        .expect(3)
        .mount(&server)
        .await;

    let mut cfg = config(&server);
    cfg.max_attempts = 3;
    let client = PublicClient::with_config(cfg).unwrap();
    let err = client.list_accounts(PageOpts::default()).await.unwrap_err();

    assert!(matches!(err, Error::Status(s) if s.as_u16() == 503));
}

#[tokio::test]
async fn public_accounts_stream_yields_across_pages() {
    use futures::TryStreamExt;

    let server = MockServer::start().await;
    let page1: Vec<_> = (0..100).map(public_account_stub).collect();
    let page2: Vec<_> = (100..150).map(public_account_stub).collect();

    Mock::given(method("GET"))
        .and(path("/account"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page1))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/account"))
        .and(query_param("offset", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page2))
        .expect(1)
        .mount(&server)
        .await;

    let client = PublicClient::with_config(config(&server)).unwrap();
    let all: Vec<_> = client.accounts().try_collect().await.unwrap();
    assert_eq!(all.len(), 150);
    assert_eq!(all[0].id, 0);
    assert_eq!(all[149].id, 149);
}

#[tokio::test]
async fn stream_dropped_mid_iteration_stops_paginating() {
    use futures::StreamExt;

    let server = MockServer::start().await;
    let page1: Vec<_> = (0..100).map(public_account_stub).collect();

    Mock::given(method("GET"))
        .and(path("/account"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page1))
        .mount(&server)
        .await;

    let client = PublicClient::with_config(config(&server)).unwrap();
    let mut stream = Box::pin(client.accounts());
    stream.next().await.unwrap().unwrap();
    drop(stream);

    let received = server.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);
}

#[tokio::test]
async fn stream_terminates_when_total_is_exact_page_multiple() {
    use futures::TryStreamExt;

    let server = MockServer::start().await;
    let page1: Vec<_> = (0..100).map(public_account_stub).collect();

    Mock::given(method("GET"))
        .and(path("/account"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page1))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/account"))
        .and(query_param("offset", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(1)
        .mount(&server)
        .await;

    let client = PublicClient::with_config(config(&server)).unwrap();
    let all: Vec<_> = client.accounts().try_collect().await.unwrap();
    assert_eq!(all.len(), 100);
}

#[tokio::test]
async fn stream_propagates_error_from_failed_page_fetch() {
    use futures::TryStreamExt;

    let server = MockServer::start().await;
    let page1: Vec<_> = (0..100).map(public_account_stub).collect();

    Mock::given(method("GET"))
        .and(path("/account"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page1))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/account"))
        .and(query_param("offset", "100"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let client = PublicClient::with_config(config(&server)).unwrap();
    let result: Result<Vec<_>, _> = client.accounts().try_collect().await;
    let err = result.unwrap_err();

    assert!(matches!(err, Error::Status(s) if s.as_u16() == 503));
}

#[tokio::test]
async fn wasapi_next_follows_server_url_then_returns_none() {
    let server = MockServer::start().await;
    let next_url = format!("{}/webdata?page=2", server.uri());

    Mock::given(method("GET"))
        .and(path("/webdata"))
        .and(query_param("collection", "4472"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 0, "next": next_url, "previous": null, "files": [],
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/webdata"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 0, "next": null, "previous": null, "files": [],
        })))
        .mount(&server)
        .await;

    let mut cfg = Config::wasapi();
    cfg.base_url = format!("{}/", server.uri());
    cfg.max_attempts = 1;
    let client = WasapiClient::with_config("u", "p", cfg).unwrap();

    let query = WebdataQuery {
        collection: Some(4472),
        ..Default::default()
    };
    let first = client.list_webdata(&query).await.unwrap();
    assert!(first.next.is_some());
    let second = client.list_webdata_next(&first).await.unwrap().unwrap();
    assert!(second.next.is_none());
    let third = client.list_webdata_next(&second).await.unwrap();
    assert!(third.is_none());
}

fn wasapi_config(server: &MockServer) -> Config {
    let mut cfg = Config::wasapi();
    cfg.base_url = format!("{}/", server.uri());
    cfg.max_attempts = 1;
    cfg.timeout = Duration::from_secs(2);
    cfg.download_timeout = Duration::from_secs(2);
    cfg.backoff = Duration::from_millis(1);
    cfg
}

#[tokio::test]
async fn wasapi_serializes_all_query_parameters() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/webdata"))
        .and(query_param("filename", "ARCHIVEIT-1.warc.gz"))
        .and(query_param("filetype", "warc"))
        .and(query_param("collection", "4472"))
        .and(query_param("crawl", "1234"))
        .and(query_param("crawl-time-after", "2025-01-01"))
        .and(query_param("crawl-time-before", "2025-12-31"))
        .and(query_param("crawl-start-after", "2024-01-01"))
        .and(query_param("crawl-start-before", "2024-12-31"))
        .and(query_param("page", "2"))
        .and(query_param("page_size", "500"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 0, "next": null, "previous": null, "files": [],
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = WasapiClient::with_config("u", "p", wasapi_config(&server)).unwrap();
    let query = WebdataQuery {
        filename: Some("ARCHIVEIT-1.warc.gz".into()),
        filetype: Some("warc".into()),
        collection: Some(4472),
        crawl: Some(1234),
        crawl_time_after: Some("2025-01-01".into()),
        crawl_time_before: Some("2025-12-31".into()),
        crawl_start_after: Some("2024-01-01".into()),
        crawl_start_before: Some("2024-12-31".into()),
        page: Some(2),
        page_size: Some(500),
    };
    client.list_webdata(&query).await.unwrap();
}

fn wasapi_file_at(server: &MockServer, content: &[u8]) -> WasapiFile {
    WasapiFile {
        filename: "ARCHIVEIT-1.warc.gz".into(),
        filetype: "warc".into(),
        checksums: Checksums {
            sha1: format!("{:x}", Sha1::digest(content)),
            md5: String::new(),
        },
        account: 1,
        size: content.len() as u64,
        collection: 4472,
        crawl: 1234,
        crawl_time: "2025-01-01T00:00:00Z".into(),
        crawl_start: "2025-01-01T00:00:00Z".into(),
        store_time: "2025-01-01T00:00:00Z".into(),
        locations: vec![format!("{}/warcs/foo.warc.gz", server.uri())],
    }
}

fn wasapi_download_client(server: &MockServer) -> WasapiClient {
    WasapiClient::with_config("u", "p", wasapi_config(server))
        .unwrap()
        .with_primary_location_src(server.uri())
}

#[tokio::test]
async fn download_writes_file_and_verifies_sha1() {
    let server = MockServer::start().await;
    let content = b"hello warc world";

    Mock::given(method("GET"))
        .and(path("/warcs/foo.warc.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(content.to_vec()))
        .expect(1)
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let out = dir.path().join("out.warc.gz");
    let client = wasapi_download_client(&server);
    client
        .download(wasapi_file_at(&server, content), &out)
        .await
        .unwrap();

    assert_eq!(std::fs::read(&out).unwrap(), content);
    assert!(!dir.path().join("out.warc.gz.part").exists());
}

#[tokio::test]
async fn download_returns_size_mismatch() {
    let server = MockServer::start().await;
    let content = b"actual content";

    Mock::given(method("GET"))
        .and(path("/warcs/foo.warc.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(content.to_vec()))
        .mount(&server)
        .await;

    let mut file = wasapi_file_at(&server, content);
    file.size = 9999;

    let dir = TempDir::new().unwrap();
    let out = dir.path().join("out.warc.gz");
    let err = wasapi_download_client(&server)
        .download(file, &out)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        Error::SizeMismatch {
            expected: 9999,
            actual: 14,
            ..
        }
    ));
    assert!(!out.exists());
}

#[tokio::test]
async fn download_returns_checksum_mismatch() {
    let server = MockServer::start().await;
    let content = b"some bytes";

    Mock::given(method("GET"))
        .and(path("/warcs/foo.warc.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(content.to_vec()))
        .mount(&server)
        .await;

    let mut file = wasapi_file_at(&server, content);
    file.checksums.sha1 = "0000000000000000000000000000000000000000".into();

    let dir = TempDir::new().unwrap();
    let out = dir.path().join("out.warc.gz");
    let err = wasapi_download_client(&server)
        .download(file, &out)
        .await
        .unwrap_err();

    assert!(matches!(err, Error::ChecksumMismatch { .. }));
    assert!(!out.exists());
}

#[tokio::test]
async fn download_returns_primary_location_missing() {
    let server = MockServer::start().await;
    let mut file = wasapi_file_at(&server, b"x");
    file.locations = vec!["https://other.example.com/warc".into()];

    let dir = TempDir::new().unwrap();
    let out = dir.path().join("out.warc.gz");
    let err = wasapi_download_client(&server)
        .download(file, &out)
        .await
        .unwrap_err();

    assert!(matches!(err, Error::PrimaryLocationMissing { .. }));
}

#[tokio::test]
async fn download_stream_yields_bytes() {
    use futures::TryStreamExt;

    let server = MockServer::start().await;
    let content = b"streaming content".to_vec();

    Mock::given(method("GET"))
        .and(path("/warcs/foo.warc.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(content.clone()))
        .mount(&server)
        .await;

    let stream = wasapi_download_client(&server)
        .download_stream(wasapi_file_at(&server, &content))
        .await
        .unwrap();
    let chunks: Vec<bytes::Bytes> = stream.try_collect().await.unwrap();
    let collected: Vec<u8> = chunks.iter().flat_map(|b| b.iter().copied()).collect();
    assert_eq!(collected, content);
}

fn wasapi_file_json(f: &WasapiFile) -> serde_json::Value {
    json!({
        "filename": f.filename,
        "filetype": f.filetype,
        "checksums": { "sha1": f.checksums.sha1, "md5": f.checksums.md5 },
        "account": f.account,
        "size": f.size,
        "collection": f.collection,
        "crawl": f.crawl,
        "crawl-time": f.crawl_time,
        "crawl-start": f.crawl_start,
        "store-time": f.store_time,
        "locations": f.locations,
    })
}

#[tokio::test]
async fn download_collection_writes_files_and_emits_downloaded() {
    use futures::TryStreamExt;

    let server = MockServer::start().await;
    let content = b"warc bytes";
    let file = wasapi_file_at(&server, content);

    Mock::given(method("GET"))
        .and(path("/webdata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 1,
            "next": null,
            "previous": null,
            "files": [wasapi_file_json(&file)],
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/warcs/foo.warc.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(content.to_vec()))
        .expect(1)
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let outcomes: Vec<_> = wasapi_download_client(&server)
        .download_collection(
            WebdataQuery {
                collection: Some(4472),
                ..Default::default()
            },
            dir.path(),
        )
        .try_collect()
        .await
        .unwrap();

    assert!(
        outcomes
            .iter()
            .any(|o| matches!(o, DownloadOutcome::Progress { .. }))
    );
    assert!(matches!(
        outcomes.last(),
        Some(DownloadOutcome::Downloaded { .. })
    ));
    assert_eq!(
        std::fs::read(dir.path().join("ARCHIVEIT-1.warc.gz")).unwrap(),
        content
    );
}

#[tokio::test]
async fn download_collection_progress_event_carries_received_and_total() {
    use futures::TryStreamExt;

    let server = MockServer::start().await;
    let content = b"warc bytes";
    let file = wasapi_file_at(&server, content);

    Mock::given(method("GET"))
        .and(path("/webdata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 1,
            "next": null,
            "previous": null,
            "files": [wasapi_file_json(&file)],
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/warcs/foo.warc.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(content.to_vec()))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let outcomes: Vec<_> = wasapi_download_client(&server)
        .download_collection(
            WebdataQuery {
                collection: Some(4472),
                ..Default::default()
            },
            dir.path(),
        )
        .try_collect()
        .await
        .unwrap();

    let progress = outcomes
        .iter()
        .find_map(|o| match o {
            DownloadOutcome::Progress {
                received, total, ..
            } => Some((*received, *total)),
            _ => None,
        })
        .expect("expected at least one Progress event");
    assert_eq!(progress.0, content.len() as u64);
    assert_eq!(progress.1, content.len() as u64);
}

#[tokio::test]
async fn download_collection_skips_existing_files_with_matching_sha1() {
    use futures::TryStreamExt;

    let server = MockServer::start().await;
    let content = b"warc bytes";
    let file = wasapi_file_at(&server, content);

    Mock::given(method("GET"))
        .and(path("/webdata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 1,
            "next": null,
            "previous": null,
            "files": [wasapi_file_json(&file)],
        })))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let target = dir.path().join("ARCHIVEIT-1.warc.gz");
    std::fs::write(&target, content).unwrap();

    let outcomes: Vec<_> = wasapi_download_client(&server)
        .download_collection(
            WebdataQuery {
                collection: Some(4472),
                ..Default::default()
            },
            dir.path(),
        )
        .try_collect()
        .await
        .unwrap();

    assert_eq!(outcomes.len(), 1);
    assert!(matches!(outcomes[0], DownloadOutcome::Skipped { .. }));

    let received = server.received_requests().await.unwrap();
    let warc_hits = received
        .iter()
        .filter(|r| r.url.path() == "/warcs/foo.warc.gz")
        .count();
    assert_eq!(warc_hits, 0);
}

#[tokio::test]
async fn download_collection_creates_missing_directory() {
    use futures::TryStreamExt;

    let server = MockServer::start().await;
    let content = b"warc bytes";
    let file = wasapi_file_at(&server, content);

    Mock::given(method("GET"))
        .and(path("/webdata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 1,
            "next": null,
            "previous": null,
            "files": [wasapi_file_json(&file)],
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/warcs/foo.warc.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(content.to_vec()))
        .mount(&server)
        .await;

    let root = TempDir::new().unwrap();
    let nested = root.path().join("a").join("b");
    assert!(!nested.exists());

    let _: Vec<_> = wasapi_download_client(&server)
        .download_collection(
            WebdataQuery {
                collection: Some(4472),
                ..Default::default()
            },
            &nested,
        )
        .try_collect()
        .await
        .unwrap();

    assert!(nested.is_dir());
    assert!(nested.join("ARCHIVEIT-1.warc.gz").exists());
}

#[tokio::test]
async fn wasapi_omits_unset_query_parameters() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/webdata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 0, "next": null, "previous": null, "files": [],
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = WasapiClient::with_config("u", "p", wasapi_config(&server)).unwrap();
    client.list_webdata(&WebdataQuery::default()).await.unwrap();

    let received = server.received_requests().await.unwrap();
    assert_eq!(received[0].url.query(), None);
}
