use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Context;
use media_ingest::{ChunkTiming, IngestChunk, MediaKind, write_framed_chunk};
use ogg::reading::async_api::PacketReader;
use tokio::{
    fs::File,
    io::{self, AsyncRead, AsyncReadExt},
    net::UnixStream,
    time::{Duration, sleep},
};
use tokio_stream::StreamExt;

const DEFAULT_OPUS_PACKET_DURATION_US: u64 = 20_000;

#[derive(Debug)]
pub(crate) struct MediaTimeline {
    started_at: Instant,
    state: Mutex<MediaTimelineState>,
}

#[derive(Debug, Default)]
struct MediaTimelineState {
    next_audio_capture_time_us: Option<u64>,
}

impl MediaTimeline {
    pub(crate) fn new() -> Self {
        Self {
            started_at: Instant::now(),
            state: Mutex::new(MediaTimelineState::default()),
        }
    }

    fn now_capture_time_us(&self) -> u64 {
        self.started_at.elapsed().as_micros() as u64
    }

    fn timed_chunk(&self, duration_us: u64, is_keyframe: bool) -> ChunkTiming {
        ChunkTiming {
            capture_time_us: self.now_capture_time_us(),
            duration_us,
            is_keyframe,
        }
    }

    fn audio_timing(&self, duration_us: u64) -> ChunkTiming {
        let duration_us = duration_us.max(1);
        let mut state = self.state.lock().expect("media timeline mutex poisoned");
        let capture_time_us = state
            .next_audio_capture_time_us
            .unwrap_or_else(|| self.now_capture_time_us());
        state.next_audio_capture_time_us = Some(capture_time_us.saturating_add(duration_us));
        ChunkTiming {
            capture_time_us,
            duration_us,
            is_keyframe: false,
        }
    }

    fn video_timing(&self, duration_us: u64, is_keyframe: bool) -> ChunkTiming {
        self.timed_chunk(duration_us, is_keyframe)
    }
}

pub(crate) async fn send_framed(
    socket: String,
    input: Option<PathBuf>,
    repeat: u32,
    interval_ms: u64,
) -> anyhow::Result<()> {
    let attempts = repeat.max(1);
    let payload = match input {
        Some(path) => tokio::fs::read(path).await?,
        None => {
            let mut payload = Vec::new();
            io::stdin().read_to_end(&mut payload).await?;
            payload
        }
    };
    let mut stream = UnixStream::connect(socket).await?;
    for attempt in 0..attempts {
        write_framed_chunk(
            &mut stream,
            &IngestChunk {
                kind: MediaKind::Video,
                payload: payload.clone().into(),
                timing: ChunkTiming::default(),
            },
        )
        .await?;
        if attempt + 1 < attempts && interval_ms > 0 {
            sleep(Duration::from_millis(interval_ms)).await;
        }
    }
    Ok(())
}

pub(crate) async fn send_ogg_opus(
    socket: String,
    input: PathBuf,
    interval_ms: u64,
) -> anyhow::Result<()> {
    let file = File::open(input).await?;
    let mut stream = UnixStream::connect(socket).await?;
    forward_ogg_opus_reader_to_stream(file, &mut stream, Some(interval_ms), None)
        .await
        .map(|_| ())
}

pub(crate) async fn send_h264_annex_b(
    socket: String,
    input: Option<PathBuf>,
    read_chunk_size: usize,
) -> anyhow::Result<()> {
    let mut stream = UnixStream::connect(socket).await?;
    let mut reader: Box<dyn AsyncRead + Unpin + Send> = match input {
        Some(path) => Box::new(File::open(path).await?),
        None => Box::new(io::stdin()),
    };

    forward_h264_annex_b_reader_to_stream(&mut reader, &mut stream, read_chunk_size, None, None)
        .await
        .map(|_| ())
}

pub(crate) async fn forward_ogg_opus_reader_to_stream<R>(
    reader: R,
    stream: &mut UnixStream,
    interval_ms: Option<u64>,
    timeline: Option<Arc<MediaTimeline>>,
) -> anyhow::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let mut packet_reader = PacketReader::new(reader);
    let mut header_packets_seen = 0_u8;
    let mut packets_written = 0_usize;

    while let Some(packet) = packet_reader.next().await.transpose()? {
        if packet.first_in_stream() || header_packets_seen < 2 {
            header_packets_seen = header_packets_seen.saturating_add(1);
            continue;
        }

        let timing = timeline
            .as_ref()
            .map(|timeline| timeline.audio_timing(DEFAULT_OPUS_PACKET_DURATION_US))
            .unwrap_or_default();
        write_framed_chunk(
            stream,
            &IngestChunk {
                kind: MediaKind::Audio,
                payload: packet.data.into(),
                timing,
            },
        )
        .await?;
        packets_written += 1;
        if let Some(ms) = interval_ms
            && ms > 0
        {
            sleep(Duration::from_millis(ms)).await;
        }
    }

    Ok(packets_written)
}

pub(crate) async fn forward_h264_annex_b_reader_to_stream<R>(
    mut reader: R,
    stream: &mut UnixStream,
    read_chunk_size: usize,
    timeline: Option<Arc<MediaTimeline>>,
    video_frame_duration_us: Option<u64>,
) -> anyhow::Result<usize>
where
    R: AsyncRead + Unpin,
{
    anyhow::ensure!(read_chunk_size > 0, "read_chunk_size must be positive");
    let mut read_buf = vec![0_u8; read_chunk_size];
    let mut parser = AnnexBAccessUnitParser::default();
    let mut access_units_written = 0_usize;

    loop {
        let bytes_read = reader.read(&mut read_buf).await?;
        if bytes_read == 0 {
            break;
        }

        let access_units = parser.push(&read_buf[..bytes_read]);
        for access_unit in access_units {
            write_h264_access_unit(
                stream,
                access_unit,
                timeline.as_ref(),
                video_frame_duration_us.unwrap_or_default(),
            )
            .await?;
            access_units_written += 1;
        }
    }

    if let Some(access_unit) = parser.finish() {
        write_h264_access_unit(
            stream,
            access_unit,
            timeline.as_ref(),
            video_frame_duration_us.unwrap_or_default(),
        )
        .await?;
        access_units_written += 1;
    }

    Ok(access_units_written)
}

async fn write_h264_access_unit(
    stream: &mut UnixStream,
    access_unit: Vec<u8>,
    timeline: Option<&Arc<MediaTimeline>>,
    video_frame_duration_us: u64,
) -> anyhow::Result<()> {
    let metadata = h264_access_unit_metadata(&access_unit);
    let timing = timeline
        .map(|timeline| {
            timeline.video_timing(
                video_frame_duration_us.max(1),
                metadata.is_keyframe,
            )
        })
        .unwrap_or_default();
    write_framed_chunk(
        stream,
        &IngestChunk {
            kind: MediaKind::Video,
            payload: access_unit.into(),
            timing,
        },
    )
    .await
    .context("failed to write h264 access unit")
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct H264AccessUnitMetadata {
    pub is_keyframe: bool,
    pub has_decoder_config: bool,
}

pub(crate) fn h264_access_unit_metadata(access_unit: &[u8]) -> H264AccessUnitMetadata {
    let mut metadata = H264AccessUnitMetadata::default();
    let mut index = 0;

    while let Some((start, start_code_len, nal_header)) = find_annex_b_start_code(access_unit, index)
    {
        let nal_type = nal_header & 0x1f;
        if nal_type == 5 {
            metadata.is_keyframe = true;
        }
        if matches!(nal_type, 7 | 8) {
            metadata.has_decoder_config = true;
        }
        index = start + start_code_len + 1;
    }

    metadata
}

#[derive(Default)]
pub(crate) struct AnnexBAccessUnitParser {
    buffer: Vec<u8>,
    frame_start: Option<usize>,
    scan_offset: usize,
}

impl AnnexBAccessUnitParser {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        self.buffer.extend_from_slice(bytes);
        self.drain_complete_access_units(false)
    }

    pub(crate) fn finish(&mut self) -> Option<Vec<u8>> {
        let mut frames = self.drain_complete_access_units(true);
        frames.pop()
    }

    fn drain_complete_access_units(&mut self, flush: bool) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();

        while let Some((start, start_code_len, nal_header)) =
            find_annex_b_start_code(&self.buffer, self.scan_offset)
        {
            let nal_type = nal_header & 0x1f;
            if nal_type == 9 {
                if let Some(frame_start) = self.frame_start
                    && start > frame_start
                {
                    frames.push(self.buffer[frame_start..start].to_vec());
                }
                self.frame_start = Some(start);
            } else if self.frame_start.is_none() {
                self.frame_start = Some(start);
            }

            self.scan_offset = start + start_code_len + 1;
        }

        if let Some(frame_start) = self.frame_start {
            if frame_start > 0 {
                self.buffer.drain(..frame_start);
                self.scan_offset = self.scan_offset.saturating_sub(frame_start);
                self.frame_start = Some(0);
            }
        } else if self.buffer.len() > 4 {
            let keep_from = self.buffer.len() - 4;
            self.buffer.drain(..keep_from);
            self.scan_offset = self.scan_offset.saturating_sub(keep_from);
        }

        if flush {
            if let Some(frame_start) = self.frame_start.take()
                && self.buffer.len() > frame_start
            {
                frames.push(self.buffer[frame_start..].to_vec());
            }
            self.buffer.clear();
            self.scan_offset = 0;
        }

        frames
    }
}

fn find_annex_b_start_code(bytes: &[u8], start_at: usize) -> Option<(usize, usize, u8)> {
    if bytes.len() < 4 {
        return None;
    }

    let mut index = start_at.min(bytes.len().saturating_sub(4));
    while index + 3 < bytes.len() {
        let (start_code_len, nal_header_index) = if bytes[index..].starts_with(&[0, 0, 1]) {
            (3, index + 3)
        } else if bytes[index..].starts_with(&[0, 0, 0, 1]) {
            (4, index + 4)
        } else {
            index += 1;
            continue;
        };

        if nal_header_index < bytes.len() {
            return Some((index, start_code_len, bytes[nal_header_index]));
        }

        return None;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::AnnexBAccessUnitParser;
    use super::MediaTimeline;
    use super::h264_access_unit_metadata;

    #[test]
    fn annex_b_parser_splits_on_aud_boundaries() {
        let mut parser = AnnexBAccessUnitParser::default();
        let stream = [
            0, 0, 0, 1, 0x09, 0xf0, 0, 0, 0, 1, 0x67, 0x64, 0, 0, 0, 1, 0x65, 0x88, 0x84, 0, 0, 0,
            1, 0x09, 0xf0, 0, 0, 0, 1, 0x61, 0x9a,
        ];

        let frames = parser.push(&stream);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], stream[..19].to_vec());

        let final_frame = parser.finish().expect("last frame");
        assert_eq!(final_frame, stream[19..].to_vec());
    }

    #[test]
    fn h264_metadata_detects_keyframes_and_decoder_config() {
        let access_unit = [
            0, 0, 0, 1, 0x67, 0x64, 0, 0, 0, 1, 0x68, 0xeb, 0, 0, 0, 1, 0x65, 0x88, 0x84,
        ];
        let metadata = h264_access_unit_metadata(&access_unit);
        assert!(metadata.is_keyframe);
        assert!(metadata.has_decoder_config);
    }

    #[test]
    fn audio_timing_advances_by_packet_duration() {
        let timeline = MediaTimeline::new();

        let first = timeline.audio_timing(20_000);
        let second = timeline.audio_timing(20_000);
        let third = timeline.audio_timing(40_000);

        assert_eq!(second.capture_time_us - first.capture_time_us, 20_000);
        assert_eq!(third.capture_time_us - second.capture_time_us, 20_000);
        assert_eq!(third.duration_us, 40_000);
    }
}
