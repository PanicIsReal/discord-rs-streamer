use std::path::PathBuf;

use anyhow::Context;
use ogg::reading::async_api::PacketReader;
use tokio::{
    fs::File,
    io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::{Duration, sleep},
};
use tokio_stream::StreamExt;

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
        write_framed_packet(&mut stream, &payload).await?;
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
    forward_ogg_opus_reader_to_stream(file, &mut stream, Some(interval_ms))
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

    forward_h264_annex_b_reader_to_stream(&mut reader, &mut stream, read_chunk_size)
        .await
        .map(|_| ())
}

pub(crate) async fn forward_ogg_opus_reader_to_stream<R>(
    reader: R,
    stream: &mut UnixStream,
    interval_ms: Option<u64>,
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

        write_framed_packet(stream, &packet.data).await?;
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
            write_framed_packet(stream, &access_unit).await?;
            access_units_written += 1;
        }
    }

    if let Some(access_unit) = parser.finish() {
        write_framed_packet(stream, &access_unit).await?;
        access_units_written += 1;
    }

    Ok(access_units_written)
}

pub(crate) async fn write_framed_packet(
    stream: &mut UnixStream,
    payload: &[u8],
) -> anyhow::Result<()> {
    let payload_len = u32::try_from(payload.len()).context("framed packet exceeds u32 size")?;
    stream.write_all(&payload_len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    Ok(())
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
}
