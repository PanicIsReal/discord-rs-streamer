use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, stdin};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChunkTiming {
    pub capture_time_us: u64,
    pub duration_us: u64,
    pub is_keyframe: bool,
}

impl ChunkTiming {
    pub fn with_duration(duration_us: u64) -> Self {
        Self {
            duration_us,
            ..Self::default()
        }
    }

    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestChunk {
    pub kind: MediaKind,
    pub payload: Bytes,
    #[serde(default)]
    pub timing: ChunkTiming,
}

pub const DEFAULT_CHUNK_SIZE: usize = 16 * 1024;
pub const MAX_FRAMED_PACKET_SIZE: usize = 8 * 1024 * 1024;

const TIMED_FRAME_MAGIC: [u8; 4] = *b"DRSF";
const TIMED_FRAME_MAGIC_LEN: usize = 4;
const TIMED_FRAME_VERSION: u8 = 1;
const TIMED_FRAME_VERSION_INDEX: usize = TIMED_FRAME_MAGIC_LEN;
const TIMED_FRAME_KIND_INDEX: usize = TIMED_FRAME_VERSION_INDEX + 1;
const TIMED_FRAME_FLAGS_INDEX: usize = TIMED_FRAME_KIND_INDEX + 1;
const TIMED_FRAME_CAPTURE_TIME_INDEX: usize = TIMED_FRAME_FLAGS_INDEX + 1;
const TIMED_FRAME_DURATION_INDEX: usize = TIMED_FRAME_CAPTURE_TIME_INDEX + 8;
const TIMED_FRAME_HEADER_LEN: usize =
    TIMED_FRAME_DURATION_INDEX + 8;
const TIMED_FRAME_FLAG_KEYFRAME: u8 = 1 << 0;

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
    #[error("timed frame version {0} is not supported")]
    UnsupportedFrameVersion(u8),
    #[error("timed frame media kind {0} did not match expected {1:?}")]
    FrameKindMismatch(u8, MediaKind),
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
    tokio::spawn(async move {
        read_reader(
            config.kind,
            stdin(),
            sender,
            config.read_chunk_size,
            config.protocol,
        )
        .await
    })
}

pub async fn write_framed_chunk<W>(
    writer: &mut W,
    chunk: &IngestChunk,
) -> Result<(), IngestError>
where
    W: AsyncWrite + Unpin,
{
    let payload = encode_framed_payload(chunk);
    let payload_len = payload.len();
    if payload_len > MAX_FRAMED_PACKET_SIZE {
        return Err(IngestError::FrameTooLarge(payload_len));
    }

    writer.write_all(&(payload_len as u32).to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    Ok(())
}

pub fn encode_framed_payload(chunk: &IngestChunk) -> Vec<u8> {
    if chunk.timing.is_default() {
        return chunk.payload.to_vec();
    }

    let mut framed = Vec::with_capacity(TIMED_FRAME_HEADER_LEN + chunk.payload.len());
    framed.extend_from_slice(&TIMED_FRAME_MAGIC);
    framed.push(TIMED_FRAME_VERSION);
    framed.push(encode_media_kind(chunk.kind));
    framed.push(if chunk.timing.is_keyframe {
        TIMED_FRAME_FLAG_KEYFRAME
    } else {
        0
    });
    framed.extend_from_slice(&chunk.timing.capture_time_us.to_be_bytes());
    framed.extend_from_slice(&chunk.timing.duration_us.to_be_bytes());
    framed.extend_from_slice(&chunk.payload);
    framed
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
            if let Err(error) = read_reader(kind, stream, sender, read_chunk_size, protocol).await {
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
            IngestChunk {
                kind,
                payload: Bytes::copy_from_slice(&buffer[..bytes_read]),
                timing: ChunkTiming::default(),
            },
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
        let chunk = decode_framed_payload(kind, payload)?;
        send_chunk(&sender, chunk).await?;
    }
}

fn decode_framed_payload(expected_kind: MediaKind, payload: Vec<u8>) -> Result<IngestChunk, IngestError> {
    if payload.len() < TIMED_FRAME_HEADER_LEN || !payload.starts_with(&TIMED_FRAME_MAGIC) {
        return Ok(IngestChunk {
            kind: expected_kind,
            payload: Bytes::from(payload),
            timing: ChunkTiming::default(),
        });
    }

    let version = payload[4];
    if version != TIMED_FRAME_VERSION {
        return Err(IngestError::UnsupportedFrameVersion(version));
    }

    let encoded_kind = payload[TIMED_FRAME_KIND_INDEX];
    let kind = decode_media_kind(encoded_kind).ok_or(IngestError::FrameKindMismatch(
        encoded_kind,
        expected_kind,
    ))?;
    if kind != expected_kind {
        return Err(IngestError::FrameKindMismatch(encoded_kind, expected_kind));
    }

    let flags = payload[TIMED_FRAME_FLAGS_INDEX];
    let capture_time_us = u64::from_be_bytes(
        payload[TIMED_FRAME_CAPTURE_TIME_INDEX..TIMED_FRAME_DURATION_INDEX]
            .try_into()
            .expect("slice"),
    );
    let duration_us = u64::from_be_bytes(
        payload[TIMED_FRAME_DURATION_INDEX..TIMED_FRAME_HEADER_LEN]
            .try_into()
            .expect("slice"),
    );

    Ok(IngestChunk {
        kind,
        payload: Bytes::from(payload[TIMED_FRAME_HEADER_LEN..].to_vec()),
        timing: ChunkTiming {
            capture_time_us,
            duration_us,
            is_keyframe: flags & TIMED_FRAME_FLAG_KEYFRAME != 0,
        },
    })
}

fn encode_media_kind(kind: MediaKind) -> u8 {
    match kind {
        MediaKind::Video => 0,
        MediaKind::Audio => 1,
    }
}

fn decode_media_kind(encoded: u8) -> Option<MediaKind> {
    match encoded {
        0 => Some(MediaKind::Video),
        1 => Some(MediaKind::Audio),
        _ => None,
    }
}

async fn send_chunk(
    sender: &mpsc::Sender<IngestChunk>,
    chunk: IngestChunk,
) -> Result<(), IngestError> {
    sender
        .send(chunk)
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
    async fn framed_ingest_decodes_timed_metadata() {
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
        write_framed_chunk(
            &mut video_stream,
            &IngestChunk {
                kind: MediaKind::Video,
                payload: Bytes::from_static(b"frame"),
                timing: ChunkTiming {
                    capture_time_us: 42,
                    duration_us: 66_666,
                    is_keyframe: true,
                },
            },
        )
        .await
        .expect("write");

        let chunk = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("receive")
            .expect("chunk");

        assert_eq!(chunk.payload, Bytes::from_static(b"frame"));
        assert_eq!(chunk.timing.capture_time_us, 42);
        assert_eq!(chunk.timing.duration_us, 66_666);
        assert!(chunk.timing.is_keyframe);
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
