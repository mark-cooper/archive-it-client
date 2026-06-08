# http-ferry

A resumable, checksum-verified, streaming byte-transfer engine: pull bytes from
an HTTP source and push them into a pluggable sink, hashing as you go. One sink
ships in the box (local file); another (S3 multipart upload) lives behind the
`s3` feature. The caller's own item type rides through untouched, the way
`reqwest` hands your response back to you.

The crate knows nothing about any specific service — you bring the URLs, the
auth, and (optionally) your own sink. It was extracted from `archive-it-client`,
which uses it to download WASAPI WARCs to disk or S3.

The name: the HTTP side is the *source* and `Sink` is the *destination*, so the
crate ferries bytes from one to the other.

## What it does

- **Resumable downloads** over HTTP range requests, including the awkward case
  where a server ignores `Range` and replies `200` instead of `206` (the sink
  is restarted and the byte counter resets).
- **Integrity verification** with a pluggable [`Checksum`] (sha1 / md5). The
  engine hashes the stream with the matching algorithm and fails on mismatch.
- **Skip-on-exists**: a sink can report the destination already holds the file
  (by checksum, or by size when no checksum is supplied) and the engine yields
  `Skipped` without fetching a byte.
- **Progress + per-item error isolation**: a `Stream` of [`Outcome`] events; one
  bad file in a batch yields `Failed` and the stream continues.
- **Retry** with exponential backoff, both at request setup and mid-stream.

## Core concepts

| Type | Role |
|------|------|
| `Downloader` | Owns the HTTP client, retry policy, and optional request customization (where you inject auth). |
| `Source` | Trait an item implements to describe itself to the engine: `name`, `size`, `checksum`. The item rides through untouched. |
| `Download` | A ready-made `Source`: source `url`, destination `name`, `size`, and optional `checksum`. Use it when you don't have a domain type. |
| `Target<'a>` | The borrowed view a sink sees: `name`, `size`, `checksum`. No URL, no caller item — sinks are domain-agnostic. |
| `Sink` / `SinkFactory` | Where bytes go. Implement these to add a destination (disk, S3, GCS, a database BLOB…). |
| `Outcome<M, L>` | Per-item result stream: `Downloaded` / `Skipped` / `Progress` / `Failed` / `StreamFailed`. |
| `drive_downloads(..)` | Simple driver for `Download` items whose source URLs are already known. |
| `drive(..)` | General driver. Pulls `Source` items, resolves each source URL, builds a sink, runs the download. |

The engine reads `size`, `checksum`, and `name` from your item through the
`Source` trait; the item value itself is cloned into `Progress` events and
handed back in terminal `Outcome`s. `name()` also serves as the item's label in
`Outcome`'s `Display`. `Download` is a built-in `Source` for the common case
where each item already carries its own URL.

## Cargo features

- `s3` *(off by default)* — the S3 multipart-upload sink in the `s3` module. It
  pulls in the whole `aws-sdk-s3` dependency tree, so consumers who only
  download to disk don't pay for it. Enable with `features = ["s3"]`.

## Usage

Wire a `Downloader`, hand `drive_downloads` a stream of `Download`s and a
`SinkFactory`:

```rust,ignore
use std::time::Duration;
use futures_util::StreamExt;
use http_ferry::{Checksum, Download, Downloader, Outcome, local::LocalDir};

// 1. An HTTP layer. Request customization is optional; add it when you need
//    bearer tokens, basic auth, signed headers, etc.
let downloader = Downloader::builder(reqwest::Client::builder().build()?)
    .max_attempts(3)
    .backoff(Duration::from_millis(250))
    .build();

// 2. A stream of work items. `Download` is enough when each URL is known.
let items = futures_util::stream::iter(vec![Ok(Download {
    url: "https://example.com/files/report.bin".parse()?,
    size: 1_048_576,
    checksum: Some(Checksum::Sha1("da39a3ee…".into())),
    name: "report.bin".into(),
})]);

// 3. Drive it: write into ./out via the local sink. `create_all` makes the
//    destination dir up front (it must already exist).
let mut out = std::pin::pin!(http_ferry::drive_downloads(
    &downloader,
    items,
    LocalDir::create_all("./out")?,
));

while let Some(outcome) = out.next().await {
    match outcome {
        Outcome::Downloaded { location, verified, .. } => {
            println!("ok {} (verified={verified})", location.display());
        }
        Outcome::Progress { received, total, .. } => { /* update a bar */ }
        Outcome::Skipped { .. } => {}
        Outcome::Failed { error, .. } => eprintln!("file failed: {error}"),
        Outcome::StreamFailed { error } => eprintln!("fatal: {error}"),
    }
}
```

If requests need auth or other per-request setup, add a customizer:

```rust,ignore
let token = std::env::var("TOKEN")?;
let downloader = Downloader::builder(reqwest::Client::builder().build()?)
    .customize_request(move |req| req.bearer_auth(&token))
    .build();
```

For a complete one-file local-sink example with progress validation and required
checksum verification:

```sh
cargo run -p http-ferry --example local_download -- \
  https://example.com/large.bin ./out sha1:<40-hex-digest>
```

### Adding a destination

Implement [`Sink`] (per-file state machine) and [`SinkFactory`] (builds one sink
per item). The engine calls `prepare` once, then `write_chunk` repeatedly, then
`finalize` — or `restart` if the server forced a fresh download mid-stream.

```rust,ignore
use http_ferry::{Error, Hasher, Prepared, Sink, Target};

struct MemSink { name: String, buf: Vec<u8> }

impl Sink for MemSink {
    type Location = String; // identifies where the bytes landed

    async fn prepare(&mut self, target: Target<'_>) -> Result<Prepared<String>, Error> {
        // Inspect target.checksum / target.size to decide skip-vs-fetch.
        // Return a `Hasher` matching the expected checksum so resumed
        // downloads keep hashing from where they left off.
        Ok(Prepared::Resume { received: 0, partial: Hasher::for_checksum(target.checksum) })
    }

    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Error> {
        self.buf.extend_from_slice(chunk);
        Ok(())
    }

    async fn restart(&mut self) -> Result<(), Error> { self.buf.clear(); Ok(()) }

    async fn finalize(self) -> Result<String, Error> { Ok(self.name) }
}
```

`Location` types implement [`DownloadLocation`] so the engine can render where a
file went. The item type `M` already implements [`Source`], whose `name()`
supplies the filename used in log lines, so `Outcome`'s `Display` works for
free.

## Design notes

- **Auth is a closure, not a credential type.** `Downloader` never
  names a service's credential model. The builder works without customization,
  and advanced consumers can supply a `Fn(RequestBuilder) -> RequestBuilder`
  for bearer tokens, basic auth, signed headers, or anything else.
- **Known URLs use `Download` + `drive_downloads`.** This is the common path
  when each item already has a source URL.
- **Domain types implement `Source` and use `drive`.** Your item describes
  itself (`name`/`size`/`checksum`) and `drive` resolves its URL through a
  closure. Resolution can fail per item (yielding a non-fatal `Failed`) without
  tearing down the stream; a failure pulling the *next* item from the source
  yields a fatal `StreamFailed`.
- **Caller errors flow in through [`Error::Source`].** The resolver and the
  input item stream produce the *caller's* error type. The engine type-erases
  them through `Source(Box<dyn Error + Send + Sync>)`, so it never needs to know
  a consumer's domain errors; callers recover the original by `downcast`.
- **No auto-abort of interrupted uploads.** Rust has no `AsyncDrop`, so a sink
  that leaves server-side state (e.g. an S3 multipart upload) documents how to
  garbage-collect it rather than attempting brittle cleanup on drop. The S3 sink
  also defers `CreateMultipartUpload` to the first byte, so a source error
  before any data arrives leaves nothing behind.

[`Checksum`]: crate::Checksum
[`Outcome`]: crate::Outcome
[`Sink`]: crate::Sink
[`SinkFactory`]: crate::SinkFactory
[`DownloadLocation`]: crate::DownloadLocation
[`Source`]: crate::Source
[`Error::Source`]: crate::Error::Source
