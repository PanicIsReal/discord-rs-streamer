use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, stdin};
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Video,
    Audio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum IngestProtocol {
    #[default]
    Raw,
    Framed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestChunk {
    pub kind: MediaKind,
    pub payload: Bytes,
}

pub const DEFAULT_CHUNK_SIZE: usize = 16 * 1024;
pub const MAX_FRAMED_PACKET_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnixSocketSourceConfig {
    pub video_socket: PathBuf,
    pub audio_socket: Option<PathBuf>,
    pub read_chunk_size: usize,
    pub protocol: IngestProtocol,
}

impl UnixSocketSourceConfig {
    pub fn new(video_socket: impl Into<PathBuf>, audio_socket: Option<impl Into<PathBuf>>) -> Self {
        Self {
            video_socket: video_socket.into(),
            audio_socket: audio_socket.map(Into::into),
            read_chunk_size: DEFAULT_CHUNK_SIZE,
            protocol: IngestProtocol::Raw,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StdinSourceConfig {
    pub kind: MediaKind,
    pub read_chunk_size: usize,
    pub protocol: IngestProtocol,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GstreamerHarnessSpec {
    pub video_caps: String,
    pub audio_caps: String,
}

impl Default for GstreamerHarnessSpec {
    fn default() -> Self {
        Self {
            video_caps: "video/x-h264,stream-format=byte-stream,alignment=au".to_owned(),
            audio_caps: "audio/x-opus,channel-mapping-family=0".to_owned(),
        }
    }
}

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("framed packet length {0} exceeds maximum {MAX_FRAMED_PACKET_SIZE}")]
    FrameTooLarge(usize),
}

pub struct UnixSocketIngestServer {
    join_handles: Vec<JoinHandle<()>>,
}

impl UnixSocketIngestServer {
    pub async fn bind(
        config: UnixSocketSourceConfig,
        sender: mpsc::Sender<IngestChunk>,
    ) -> Result<Self, IngestError> {
        remove_stale_socket(&config.video_socket).await?;
        let video_listener = UnixListener::bind(&config.video_socket)?;

        let mut join_handles = vec![tokio::spawn(run_listener(
            MediaKind::Video,
            video_listener,
            sender.clone(),
            config.read_chunk_size,
            config.protocol,
        ))];

        if let Some(audio_socket) = &config.audio_socket {
            remove_stale_socket(audio_socket).await?;
            let audio_listener = UnixListener::bind(audio_socket)?;
            join_handles.push(tokio::spawn(run_listener(
                MediaKind::Audio,
                audio_listener,
                sender,
                config.read_chunk_size,
                config.protocol,
            )));
        }

        Ok(Self { join_handles })
    }

    pub async fn shutdown(self) {
        for handle in self.join_handles {
            handle.abort();
            let _ = handle.await;
        }
    }
}

pub fn spawn_stdin_ingest(
    config: StdinSourceConfig,
    sender: mpsc::Sender<IngestChunk>,
) -> JoinHandle<Result<(), IngestError>> {
    tokio::spawn(
        async move {
            read_reader(
                config.kind,
                stdin(),
                sender,
                config.read_chunk_size,
                config.protocol,
            )
            .await
        },
    )
}

async fn run_listener(
    kind: MediaKind,
    listener: UnixListener,
    sender: mpsc::Sender<IngestChunk>,
    read_chunk_size: usize,
    protocol: IngestProtocol,
) {
    while let Ok((stream, _)) = listener.accept().await {
        let sender = sender.clone();
        tokio::spawn(async move {
            if let Err(error) =
                read_reader(kind, stream, sender, read_chunk_size, protocol).await
            {
                warn!(?error, ?kind, "ingest reader exited");
            }
        });
    }
}

async fn read_reader<R>(
    kind: MediaKind,
    mut reader: R,
    sender: mpsc::Sender<IngestChunk>,
    read_chunk_size: usize,
    protocol: IngestProtocol,
) -> Result<(), IngestError>
where
    R: AsyncRead + Unpin,
{
    if protocol == IngestProtocol::Framed {
        return read_framed_reader(kind, reader, sender).await;
    }

    let mut buffer = vec![0; read_chunk_size.max(1)];
    loop {
        let bytes_read = reader.read(&mut buffer).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        send_chunk(
            &sender,
            kind,
            Bytes::copy_from_slice(&buffer[..bytes_read]).to_vec(),
        )
        .await?;
    }
}

async fn read_framed_reader<R>(
    kind: MediaKind,
    mut reader: R,
    sender: mpsc::Sender<IngestChunk>,
) -> Result<(), IngestError>
where
    R: AsyncRead + Unpin,
{
    loop {
        let mut length = [0_u8; 4];
        match reader.read_exact(&mut length).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error.into()),
        }

        let frame_len = u32::from_be_bytes(length) as usize;
        if frame_len == 0 {
            continue;
        }
        if frame_len > MAX_FRAMED_PACKET_SIZE {
            return Err(IngestError::FrameTooLarge(frame_len));
        }

        let mut payload = vec![0_u8; frame_len];
        reader.read_exact(&mut payload).await?;
        send_chunk(&sender, kind, payload).await?;
    }
}

async fn send_chunk(
    sender: &mpsc::Sender<IngestChunk>,
    kind: MediaKind,
    payload: Vec<u8>,
) -> Result<(), IngestError> {
    sender
        .send(IngestChunk {
            kind,
            payload: Bytes::from(payload),
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ingest channel closed"))?;
    Ok(())
}

async fn remove_stale_socket(path: &Path) -> Result<(), io::Error> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixStream;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn unix_socket_ingest_receives_video_and_audio() {
        let temp = tempdir().expect("tempdir");
        let video = temp.path().join("video.sock");
        let audio = temp.path().join("audio.sock");
        let (tx, mut rx) = mpsc::channel(8);

        let server = UnixSocketIngestServer::bind(
            UnixSocketSourceConfig {
                video_socket: video.clone(),
                audio_socket: Some(audio.clone()),
                read_chunk_size: 128,
                protocol: IngestProtocol::Raw,
            },
            tx,
        )
        .await
        .expect("bind");

        let mut video_stream = UnixStream::connect(video).await.expect("video connect");
        let mut audio_stream = UnixStream::connect(audio).await.expect("audio connect");
        video_stream
            .write_all(b"\x00\x00\x01video")
            .await
            .expect("write");
        audio_stream.write_all(b"opus").await.expect("write");

        let first = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("receive")
            .expect("chunk");
        let second = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("receive")
            .expect("chunk");

        assert_ne!(first.kind, second.kind);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn framed_ingest_preserves_packet_boundaries() {
        let temp = tempdir().expect("tempdir");
        let video = temp.path().join("video.sock");
        let (tx, mut rx) = mpsc::channel(8);

        let server = UnixSocketIngestServer::bind(
            UnixSocketSourceConfig {
                video_socket: video.clone(),
                audio_socket: None,
                read_chunk_size: 16,
                protocol: IngestProtocol::Framed,
            },
            tx,
        )
        .await
        .expect("bind");

        let mut video_stream = UnixStream::connect(video).await.expect("video connect");
        video_stream
            .write_all(&4_u32.to_be_bytes())
            .await
            .expect("length");
        video_stream.write_all(b"abcd").await.expect("payload");
        video_stream
            .write_all(&2_u32.to_be_bytes())
            .await
            .expect("length");
        video_stream.write_all(b"ef").await.expect("payload");

        let first = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("receive")
            .expect("chunk");
        let second = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("receive")
            .expect("chunk");

        assert_eq!(first.payload, Bytes::from_static(b"abcd"));
        assert_eq!(second.payload, Bytes::from_static(b"ef"));
        server.shutdown().await;
    }

    #[tokio::test]
    async fn framed_ingest_rejects_oversized_packets() {
        let temp = tempdir().expect("tempdir");
        let video = temp.path().join("video.sock");
        let (tx, _rx) = mpsc::channel(8);

        let server = UnixSocketIngestServer::bind(
            UnixSocketSourceConfig {
                video_socket: video.clone(),
                audio_socket: None,
                read_chunk_size: 16,
                protocol: IngestProtocol::Framed,
            },
            tx,
        )
        .await
        .expect("bind");

        let mut video_stream = UnixStream::connect(video).await.expect("video connect");
        video_stream
            .write_all(&((MAX_FRAMED_PACKET_SIZE as u32) + 1).to_be_bytes())
            .await
            .expect("length");

        tokio::time::sleep(Duration::from_millis(50)).await;
        server.shutdown().await;
    }
}
