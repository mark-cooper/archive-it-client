use std::fs::File;
use std::io::{BufWriter, Write};
use std::pin::pin;

use archive_it_client::wasapi::DEFAULT_WEBDATA_PAGE_SIZE;
use archive_it_client::{PartnerClient, WasapiClient, WebdataQuery};
use futures::TryStreamExt;

const OUTPUT_PATH: &str = "warcs.csv";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user =
        std::env::var("ARCHIVE_IT_USERNAME").expect("ARCHIVE_IT_USERNAME env var must be set");
    let pass =
        std::env::var("ARCHIVE_IT_PASSWORD").expect("ARCHIVE_IT_PASSWORD env var must be set");

    let partner = PartnerClient::new(user.clone(), pass.clone())?;
    let wasapi = WasapiClient::new(user, pass)?;
    let mut writer = BufWriter::new(File::create(OUTPUT_PATH)?);
    write_header(&mut writer)?;

    let mut collections = pin!(partner.collections());
    let mut collection_count = 0_u64;
    let mut warc_count = 0_u64;

    while let Some(collection) = collections.try_next().await? {
        let query = WebdataQuery {
            collection: Some(collection.id),
            filetype: Some("warc".to_owned()),
            page_size: Some(DEFAULT_WEBDATA_PAGE_SIZE),
            ..Default::default()
        };

        let mut page = wasapi.list_webdata(&query).await?;
        collection_count += 1;
        eprintln!(
            "collection {} ({}) -> {} reported WARC files",
            collection.id, collection.name, page.count
        );

        loop {
            for file in page.files.drain(..) {
                if file.filetype != "warc" {
                    continue;
                }

                let primary_location = wasapi
                    .primary_location(&file)
                    .unwrap_or_default()
                    .to_owned();
                let all_locations = file.locations.join(";");

                write_csv_record(
                    &mut writer,
                    [
                        collection.id.to_string(),
                        collection.name.clone(),
                        file.account.to_string(),
                        file.filename,
                        file.filetype,
                        file.size.to_string(),
                        opt_u64(file.crawl),
                        opt_string(file.crawl_time),
                        opt_string(file.crawl_start),
                        file.store_time,
                        opt_string(file.checksums.sha1),
                        opt_string(file.checksums.md5),
                        primary_location,
                        all_locations,
                    ],
                )?;
                warc_count += 1;
            }

            match wasapi.list_webdata_next(&page).await? {
                Some(next) => page = next,
                None => break,
            }
        }
    }

    writer.flush()?;
    eprintln!(
        "wrote {warc_count} WARC rows across {collection_count} collections to {OUTPUT_PATH}"
    );

    Ok(())
}

fn opt_string(value: Option<String>) -> String {
    value.unwrap_or_default()
}

fn opt_u64(value: Option<u64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn write_csv_field<W: Write>(writer: &mut W, value: &str) -> std::io::Result<()> {
    let needs_quotes = value
        .chars()
        .any(|ch| matches!(ch, ',' | '"' | '\n' | '\r'));

    if !needs_quotes {
        return writer.write_all(value.as_bytes());
    }

    writer.write_all(b"\"")?;
    for ch in value.chars() {
        if ch == '"' {
            writer.write_all(b"\"\"")?;
        } else {
            write!(writer, "{ch}")?;
        }
    }
    writer.write_all(b"\"")
}

fn write_csv_record<W, I>(writer: &mut W, columns: I) -> std::io::Result<()>
where
    W: Write,
    I: IntoIterator<Item = String>,
{
    let mut first = true;
    for column in columns {
        if !first {
            writer.write_all(b",")?;
        }
        first = false;
        write_csv_field(writer, &column)?;
    }
    writer.write_all(b"\n")
}

fn write_header<W: Write>(writer: &mut W) -> std::io::Result<()> {
    write_csv_record(
        writer,
        [
            "collection_id".to_owned(),
            "collection_name".to_owned(),
            "account_id".to_owned(),
            "filename".to_owned(),
            "filetype".to_owned(),
            "size_bytes".to_owned(),
            "crawl_id".to_owned(),
            "crawl_time".to_owned(),
            "crawl_start".to_owned(),
            "store_time".to_owned(),
            "sha1".to_owned(),
            "md5".to_owned(),
            "primary_location".to_owned(),
            "all_locations".to_owned(),
        ],
    )
}
