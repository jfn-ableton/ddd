use actix_multipart::Multipart;
use actix_web::{web, App, HttpServer};
use clap::Parser;
use futures::{Stream, StreamExt, TryStreamExt};
use std::{
    iter::Extend,
    marker::{Send, Sync, Unpin},
    net::SocketAddr,
    path::Path,
};
use tokio::{fs, io::AsyncWriteExt};
use wax::{BuildError, Glob, Pattern};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Allows writing to this path (can be specified multiple times)
    #[clap(short = 'a', long = "allow", required(true))]
    allowed_paths: Vec<String>,

    /// Listen at this address/port (can be specified multiple times)
    #[clap(short = 'l', long = "listen", required(true))]
    listen_on: Vec<SocketAddr>,

    #[clap(long = "buffer-size", default_value_t = 4096)]
    buffer_size: usize,
}

async fn write_bytes_to<
    R: Stream<Item = Result<web::Bytes, E>> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
>(
    write_to: &Path,
    mut data: R,
    buffer_size: usize,
) -> anyhow::Result<()> {
    let mut out = fs::OpenOptions::new().write(true).open(write_to).await?;

    loop {
        let bytes = if let Some(bytes) = data.next().await {
            bytes
        } else {
            break;
        };
        let bytes = bytes?;
        for chunk in bytes.chunks(buffer_size) {
            // TODO: Do better batching here
            //            if chunk.iter().all(|b| *b == 0) {
            //                out.seek(io::SeekFrom::Current(i64::try_from(chunk.len())?))
            //                    .await?;
            //            } else {
            out.write_all(chunk).await?;
            //}
        }
    }

    Ok(out.flush().await?)
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let Args {
        allowed_paths,
        listen_on,
        buffer_size,
    } = Args::parse();

    let allowed_paths = allowed_paths
        .into_iter()
        .map(|path| {
            Glob::new(&*path)
                .map(Glob::into_owned)
                .map_err(BuildError::into_owned)
        })
        .collect::<Result<Vec<Glob<'static>>, _>>()?;
    let allowed_paths = wax::any::<'_, Glob<'static>, _>(allowed_paths)?;
    // This cannot cause space leaks as `main` is called only once.
    let allowed_paths: &'static wax::Any<'static> = Box::leak(Box::new(allowed_paths));

    Ok(HttpServer::new(move || {
        App::new().route(
            "/write",
            web::post().to(move |mut multipart: Multipart| async move {
                let path = multipart
                    .next()
                    .await
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Path needs to be specified before upload data",
                        )
                    })?
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                if path.name() != "path" {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Path needs to be specified before upload data",
                    ));
                }
                let mut out_path = vec![];
                path.try_for_each(|bytes| {
                    let out_path = &mut out_path;
                    out_path.extend(&*bytes);

                    async move { Ok(()) }
                })
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let path = out_path;
                let path = Path::new(
                    std::str::from_utf8(&*path)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
                );
                let body = multipart
                    .next()
                    .await
                    .ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "No data specified")
                    })?
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

                if allowed_paths.is_match(&*path) {
                    write_bytes_to(&*path, body, buffer_size)
                        .await
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                }

                Ok::<_, std::io::Error>(format!("Done!"))
            }),
        )
    })
    .bind(&listen_on[..])?
    .run()
    .await?)
}
