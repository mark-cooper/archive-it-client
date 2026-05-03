use std::path::{Path, PathBuf};

use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Error;
use crate::downloads::{Prepared, Sink};
use crate::models::wasapi::WasapiFile;

pub(crate) struct LocalSink {
    final_path: PathBuf,
    partial_path: PathBuf,
    out: Option<tokio::fs::File>,
}

impl LocalSink {
    pub(crate) fn new(final_path: PathBuf) -> Result<Self, Error> {
        let partial_path = partial_path(&final_path)?;
        Ok(Self {
            final_path,
            partial_path,
            out: None,
        })
    }
}

impl Sink for LocalSink {
    type Location = PathBuf;

    async fn prepare(&mut self, file: &WasapiFile) -> Result<Prepared<Self::Location>, Error> {
        if let Some(expected) = file.checksums.sha1.as_deref()
            && existing_sha1_matches(&self.final_path, expected).await?
        {
            return Ok(Prepared::Skip {
                location: self.final_path.clone(),
            });
        }

        let (received, partial_sha1) = examine_partial(&self.partial_path, file.size).await?;
        let out = if received > 0 {
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(&self.partial_path)
                .await?
        } else {
            tokio::fs::File::create(&self.partial_path).await?
        };
        self.out = Some(out);
        Ok(Prepared::Resume {
            received,
            partial_sha1,
        })
    }

    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Error> {
        let out = self
            .out
            .as_mut()
            .expect("write_chunk before prepare or after finalize");
        out.write_all(chunk).await?;
        Ok(())
    }

    async fn restart(&mut self) -> Result<(), Error> {
        let replacement = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.partial_path)
            .await?;
        self.out = Some(replacement);
        Ok(())
    }

    async fn finalize(mut self) -> Result<Self::Location, Error> {
        let out = self.out.take().expect("finalize without prepare");
        out.sync_all().await?;
        drop(out);
        tokio::fs::rename(&self.partial_path, &self.final_path).await?;
        Ok(self.final_path)
    }
}

async fn existing_sha1_matches(path: &Path, expected: &str) -> Result<bool, Error> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(false);
    }
    let mut hasher = Sha1::new();
    seed_hasher_from_file(path, &mut hasher).await?;
    Ok(format!("{:x}", hasher.finalize()) == expected)
}

async fn examine_partial(partial_path: &Path, expected_size: u64) -> Result<(u64, Sha1), Error> {
    if !tokio::fs::try_exists(partial_path).await? {
        return Ok((0, Sha1::new()));
    }
    let m = tokio::fs::metadata(partial_path).await?;
    if m.len() > expected_size {
        tokio::fs::remove_file(partial_path).await?;
        return Ok((0, Sha1::new()));
    }
    let mut hasher = Sha1::new();
    if m.len() > 0 {
        seed_hasher_from_file(partial_path, &mut hasher).await?;
    }
    Ok((m.len(), hasher))
}

async fn seed_hasher_from_file(path: &Path, hasher: &mut Sha1) -> Result<(), Error> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(())
}

fn partial_path(path: &Path) -> Result<PathBuf, Error> {
    let mut file_name = path
        .file_name()
        .ok_or_else(|| Error::InvalidDownloadPath { path: path.into() })?
        .to_os_string();
    file_name.push(".part");
    Ok(path.with_file_name(file_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_path_appends_part_suffix() {
        let result = partial_path(Path::new("/tmp/foo.warc.gz")).unwrap();
        assert_eq!(result, PathBuf::from("/tmp/foo.warc.gz.part"));
    }

    #[test]
    fn partial_path_rejects_path_with_no_filename() {
        let err = partial_path(Path::new("/")).unwrap_err();
        assert!(matches!(err, Error::InvalidDownloadPath { .. }));
    }
}
