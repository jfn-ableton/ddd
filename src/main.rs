use actix_multipart::Multipart;
use actix_web::{web, App, HttpServer};
use async_compression::tokio::bufread::{
    BrotliDecoder, BzDecoder, DeflateDecoder, GzipDecoder, LzmaDecoder, XzDecoder, ZlibDecoder,
    ZstdDecoder,
};
use clap::Parser;
use futures::{
    future::{self, join, try_join, Either},
    FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt,
};
use std::{
    iter,
    marker::Unpin,
    net::SocketAddr,
    ops::DerefMut,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    fs,
    io::{self, AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::oneshot,
};
use wax::{BuildError, Glob, Pattern};

enum Compression {
    None,
    Brotli,
    Bz,
    Deflate,
    Gzip,
    Lzma,
    Xz,
    Zlib,
    Zstd,
}

impl Compression {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "none" => Some(Self::None),
            "brotli" => Some(Self::Brotli),
            "bz" => Some(Self::Bz),
            "deflate" => Some(Self::Deflate),
            "gzip" => Some(Self::Gzip),
            "lzma" => Some(Self::Lzma),
            "xz" => Some(Self::Xz),
            "zlib" => Some(Self::Zlib),
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }
}

const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

/// `ddd` - A simple web interface for overwriting a file or writing to a block device
/// on the server
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Allows writing to this path (can be specified multiple times)
    #[clap(short = 'a', long = "allow", required(true))]
    allowed_paths: Vec<String>,

    /// Listen at this address/port (can be specified multiple times)
    #[clap(short = 'l', long = "listen", required(true))]
    listen_on: Vec<SocketAddr>,

    /// Set the buffer size used to write to the block device. Consecutive runs
    /// of zeroes of this length will be skipped.
    #[clap(short = 'b', long = "buffer-size", default_value_t = DEFAULT_BUFFER_SIZE)]
    buffer_size: usize,

    /// Set the number of buffers to decode in parallel while writing to the output
    /// file.
    #[clap(short = 'p', long = "parallel", default_value_t = 16)]
    parallelism: usize,
}

async fn write_bytes_to<R: AsyncRead + Unpin>(
    write_to: &Path,
    mut data: R,
    buffer_size: usize,
    parallelism: usize,
) -> anyhow::Result<()> {
    use tokio::sync::mpsc;

    #[derive(Clone, Debug)]
    enum Task {
        Skip(usize),
        Write(Vec<u8>, usize),
    }

    let mut out = fs::OpenOptions::new().write(true).open(write_to).await?;
    let (buffer_tx, mut buffer_rx) = mpsc::channel(parallelism);
    let (task_tx, mut task_rx) = mpsc::channel::<Task>(parallelism);

    let mut opt_task_tx = Some(task_tx);

    let buffer_tx_ref = &buffer_tx;
    let mut run_tasks = Box::pin(
        future::try_join_all(
            iter::repeat(buffer_tx_ref)
                .take(parallelism)
                .map(|buffer_tx| buffer_tx.send(vec![0; buffer_size])),
        )
        .map_err(anyhow::Error::from)
        .and_then(move |_| async move {
            while let Some(task) = task_rx.recv().await {
                match task {
                    Task::Skip(num_bytes) => {
                        out.seek(io::SeekFrom::Current(i64::try_from(num_bytes)?))
                            .await?;
                    }
                    Task::Write(buffer, num_bytes) => {
                        out.write_all(&buffer[..num_bytes]).await?;
                        buffer_tx_ref.send(buffer).await?;
                    }
                }
            }

            Ok(out)
        }),
    );

    loop {
        let read_buffer = Box::pin(
            buffer_rx
                .recv()
                .map(|val| {
                    val.ok_or_else(|| {
                        anyhow::format_err!("Programmer error: Buffer list was dropped")
                    })
                })
                .and_then(|mut buffer: Vec<u8>| {
                    let opt_task_tx = &mut opt_task_tx;
                    let data = &mut data;

                    async move {
                        let task_tx = if let Some(task_tx) = opt_task_tx.take() {
                            task_tx
                        } else {
                            return Ok(());
                        };

                        let num_bytes = data.read(&mut buffer[..]).await?;
                        if num_bytes == 0 {
                            return Ok(());
                        }

                        if buffer[..num_bytes].iter().all(|b| *b == 0) {
                            let results = future::join(
                                task_tx.send(Task::Skip(num_bytes)),
                                buffer_tx_ref.send(buffer),
                            )
                            .await;
                            results.0?;
                            results.1?;
                        } else {
                            task_tx.send(Task::Write(buffer, num_bytes)).await?;
                        }

                        *opt_task_tx = Some(task_tx);

                        Ok(())
                    }
                }),
        );

        match future::try_select(read_buffer, run_tasks).await {
            Ok(Either::Left(((), rest_tasks))) => {
                run_tasks = rest_tasks;
            }
            Ok(Either::Right((mut out, _))) => {
                out.flush().await?;
                break;
            }
            Err(Either::Left((err, _))) | Err(Either::Right((err, _))) => return Err(err),
        }
    }

    Ok(())
}

struct ReadStream<S, B> {
    buf: Option<B>,
    consumed: usize,
    inner: Option<Pin<S>>,
}

impl<S, B, E> ReadStream<S, B>
where
    S: DerefMut,
    S::Target: Stream<Item = Result<B, E>>,
{
    pub fn new(inner: Pin<S>) -> Self {
        Self {
            buf: None,
            consumed: 0,
            inner: Some(inner),
        }
    }
}

impl<S, B, E> AsyncRead for ReadStream<S, B>
where
    S: DerefMut + Unpin,
    S::Target: Stream<Item = Result<B, E>>,
    B: AsRef<[u8]> + Unpin,
    E: Into<io::Error>,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let next_buf = match self.as_mut().poll_fill_buf(cx)? {
            Poll::Ready(next_buf) => next_buf,
            Poll::Pending => return Poll::Pending,
        };

        let len = next_buf.len().min(buf.remaining());
        buf.put_slice(&next_buf[..len]);
        self.consume(len);

        Poll::Ready(Ok(()))
    }
}

impl<S, B, E> AsyncBufRead for ReadStream<S, B>
where
    S: DerefMut + Unpin,
    S::Target: Stream<Item = Result<B, E>>,
    B: AsRef<[u8]> + Unpin,
    E: Into<io::Error>,
{
    fn poll_fill_buf<'a>(
        mut self: Pin<&'a mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<&'a [u8]>> {
        if self.inner.is_none() {
            return Poll::Ready(Ok(&[]));
        }

        // We already assert that `inner` is `Some` at the start, and
        // doing this "properly" means that `self.inner = None` causes
        // lifetime issues.
        let finished = {
            if self.buf.is_some() {
                false
            } else {
                match self.inner.as_mut().unwrap().as_mut().poll_next(cx) {
                    Poll::Ready(Some(next)) => {
                        self.buf = Some(next.map_err(Into::into)?);
                        self.consumed = 0;
                        false
                    }
                    Poll::Ready(None) => true,
                    Poll::Pending => return Poll::Pending,
                }
            }
        };

        if finished {
            self.inner = None;

            Poll::Ready(Ok(&[]))
        } else {
            let consumed = self.consumed;
            let buf = &self.into_ref().get_ref().buf.as_ref().unwrap().as_ref()[consumed..];

            Poll::Ready(Ok(buf))
        }
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.consumed += amt;

        let finished_buf = match &self.buf {
            Some(buf) if self.consumed >= buf.as_ref().len() => true,
            _ => false,
        };

        if finished_buf {
            self.buf = None;
        }
    }
}

fn to_io_err<E: Into<Box<dyn std::error::Error + Send + Sync>>>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

fn mk_io_err<E: Into<Box<dyn std::error::Error + Send + Sync>>>(
    e: E,
) -> impl FnOnce() -> io::Error {
    move || io::Error::new(io::ErrorKind::Other, e)
}

async fn byte_stream_to_string<B: AsRef<[u8]>, S: Stream<Item = io::Result<B>> + Unpin>(
    mut stream: S,
) -> io::Result<String> {
    let mut out = Vec::<u8>::new();

    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;

        out.extend(bytes.as_ref());
    }

    String::from_utf8(out).map_err(to_io_err)
}

async fn async_main() -> anyhow::Result<()> {
    let Args {
        allowed_paths,
        listen_on,
        buffer_size,
        parallelism,
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
                let multipart = &mut multipart;
                let (compression_field_tx, compression_field_rx) = oneshot::channel();
                let (path_field_tx, path_field_rx) = oneshot::channel();
                let (body_field_tx, body_field_rx) = oneshot::channel();

                let (mut compression_field_tx, mut path_field_tx, mut body_field_tx) = (
                    Some(compression_field_tx),
                    Some(path_field_tx),
                    Some(body_field_tx),
                );

                let decode_multipart = {
                    multipart
                        .map(move |val| val.map_err(to_io_err))
                        .try_for_each(move |field| {
                            let out = (|| {
                                match field.name() {
                                    "path" => {
                                        let _ = path_field_tx
                                            .take()
                                            .ok_or_else(mk_io_err("Multiple paths specified"))?
                                            .send(
                                                byte_stream_to_string(field.map_err(to_io_err))
                                                    .and_then(|bytes| async move {
                                                        Ok(PathBuf::from(bytes))
                                                    }),
                                            );
                                    }
                                    "compression" => {
                                        let _ = compression_field_tx
                                            .take()
                                            .ok_or_else(mk_io_err(
                                                "Multiple compression types specified",
                                            ))?
                                            .send(
                                                byte_stream_to_string(field.map_err(to_io_err))
                                                    .and_then(|value| async move {
                                                        Compression::from_str(&*value).ok_or_else(
                                                            mk_io_err("Unknown compression type"),
                                                        )
                                                    }),
                                            );
                                    }
                                    "body" => {
                                        let _ = body_field_tx
                                            .take()
                                            .ok_or_else(mk_io_err("Multiple bodies specified"))?
                                            .send(field);
                                    }
                                    // TODO: Should we ignore or fail on unknown fields?
                                    _ => {}
                                }

                                Ok::<_, io::Error>(())
                            })();

                            async move { out }
                        })
                };

                let (path, body, compression) = tokio::select!(
                    _ = decode_multipart => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "No body supplied",
                        ))
                    },
                    (path_body, compression) = join(
                        try_join(
                            path_field_rx
                                .map_err(to_io_err)
                                .try_flatten(),
                            body_field_rx
                                .map_err(to_io_err),
                        ),
                        compression_field_rx
                            .map_err(to_io_err)
                            .try_flatten(),
                    ) => {
                        let (path, body) = path_body?;

                        (path, body, compression)
                    },
                );
                let compression = compression.unwrap_or(Compression::None);

                let mut body = body.map_err(to_io_err);
                let body_reader = ReadStream::new(Pin::new(&mut body));

                let body: Pin<Box<dyn AsyncRead>> = match compression {
                    Compression::None => Box::pin(body_reader),
                    Compression::Brotli => Box::pin(BrotliDecoder::new(body_reader)),
                    Compression::Bz => Box::pin(BzDecoder::new(body_reader)),
                    Compression::Deflate => Box::pin(DeflateDecoder::new(body_reader)),
                    Compression::Gzip => Box::pin(GzipDecoder::new(body_reader)),
                    Compression::Lzma => Box::pin(LzmaDecoder::new(body_reader)),
                    Compression::Xz => Box::pin(XzDecoder::new(body_reader)),
                    Compression::Zlib => Box::pin(ZlibDecoder::new(body_reader)),
                    Compression::Zstd => Box::pin(ZstdDecoder::new(body_reader)),
                };

                if allowed_paths.is_match(&*path) {
                    write_bytes_to(&*path, body, buffer_size, parallelism)
                        .await
                        .map_err(to_io_err)?;
                }

                Ok::<_, io::Error>(format!("Done!"))
            }),
        )
    })
    .bind(&listen_on[..])?
    .run()
    .await?)
}

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main())
}
