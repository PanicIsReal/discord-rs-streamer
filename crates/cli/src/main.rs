use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use daemon::{DaveInitRequest, IngestSource, SessionConnectRequest, StreamStartRequest};
use discord_gateway::StreamKind as GatewayStreamKind;
use media_ingest::{IngestProtocol, MediaKind};
use ogg::PacketReader;
use reqwest::RequestBuilder;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::{Duration, sleep},
};

#[derive(Debug, Parser)]
#[command(name = "discord-rs-streamer")]
#[command(about = "CLI for the discord-rs-streamer daemon")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7331")]
    daemon_url: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Session(SessionCommand),
    Stream(StreamCommand),
    Ingest(IngestCommand),
    Voice(VoiceCommand),
    Media(MediaCommand),
    Dave(DaveCommand),
    Health,
    Metrics,
}

#[derive(Debug, Parser)]
struct SessionCommand {
    #[command(subcommand)]
    command: SessionSubcommand,
}

#[derive(Debug, Subcommand)]
enum SessionSubcommand {
    Connect {
        #[arg(long)]
        token: String,
        #[arg(long)]
        guild_id: String,
        #[arg(long)]
        channel_id: String,
        #[arg(long, value_enum, default_value = "go-live")]
        stream_kind: StreamKindArg,
    },
    Media,
}

#[derive(Debug, Parser)]
struct StreamCommand {
    #[command(subcommand)]
    command: StreamSubcommand,
}

#[derive(Debug, Parser)]
struct VoiceCommand {
    #[command(subcommand)]
    command: VoiceSubcommand,
}

#[derive(Debug, Parser)]
struct IngestCommand {
    #[command(subcommand)]
    command: IngestSubcommand,
}

#[derive(Debug, Parser)]
struct MediaCommand {
    #[command(subcommand)]
    command: MediaSubcommand,
}

#[derive(Debug, Parser)]
struct DaveCommand {
    #[command(subcommand)]
    command: DaveSubcommand,
}

#[derive(Debug, Subcommand)]
enum StreamSubcommand {
    Start {
        #[arg(long)]
        source: IngestSourceArg,
        #[arg(long, default_value = "default")]
        source_name: String,
        #[arg(long)]
        video_socket: Option<String>,
        #[arg(long)]
        audio_socket: Option<String>,
        #[arg(long, value_enum)]
        stdin_media_kind: Option<MediaKindArg>,
        #[arg(long, value_enum)]
        ingest_protocol: Option<IngestProtocolArg>,
        #[arg(long)]
        pacing_bps: Option<usize>,
    },
    Stop,
}

#[derive(Debug, Subcommand)]
enum VoiceSubcommand {
    Connect,
    Health,
    State,
    Disconnect,
}

#[derive(Debug, Subcommand)]
enum IngestSubcommand {
    SendFramed {
        #[arg(long)]
        socket: String,
        #[arg(long)]
        input: Option<PathBuf>,
        #[arg(long, default_value_t = 1)]
        repeat: u32,
        #[arg(long, default_value_t = 0)]
        interval_ms: u64,
    },
    SendH264AnnexB {
        #[arg(long)]
        socket: String,
        #[arg(long)]
        input: Option<PathBuf>,
        #[arg(long, default_value_t = 65536)]
        read_chunk_size: usize,
    },
    SendOggOpus {
        #[arg(long)]
        socket: String,
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value_t = 20)]
        interval_ms: u64,
    },
}

#[derive(Debug, Subcommand)]
enum MediaSubcommand {
    Connect,
    Health,
    Disconnect,
}

#[derive(Debug, Subcommand)]
enum DaveSubcommand {
    Init {
        #[arg(long)]
        protocol_version: u16,
        #[arg(long)]
        user_id: u64,
        #[arg(long)]
        channel_id: u64,
    },
    State,
    KeyPackage,
}

#[derive(Debug, Clone, ValueEnum)]
enum StreamKindArg {
    GoLive,
    Camera,
}

#[derive(Debug, Clone, ValueEnum)]
enum IngestSourceArg {
    Unix,
    Stdin,
}

#[derive(Debug, Clone, ValueEnum)]
enum MediaKindArg {
    Video,
    Audio,
}

#[derive(Debug, Clone, ValueEnum)]
enum IngestProtocolArg {
    Raw,
    Framed,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let client = reqwest::Client::new();

    match args.command {
        Command::Session(session) => match session.command {
            SessionSubcommand::Connect {
                token,
                guild_id,
                channel_id,
                stream_kind,
            } => {
                let request = client
                    .post(format!("{}/v1/session/connect", args.daemon_url))
                    .json(&SessionConnectRequest {
                        token,
                        guild_id,
                        channel_id,
                        stream_kind: map_stream_kind(stream_kind),
                    });
                send_text_response(request).await?;
            }
            SessionSubcommand::Media => {
                let request = client.get(format!("{}/v1/session/media", args.daemon_url));
                send_text_response(request).await?;
            }
        },
        Command::Stream(stream) => match stream.command {
            StreamSubcommand::Start {
                source,
                source_name,
                video_socket,
                audio_socket,
                stdin_media_kind,
                ingest_protocol,
                pacing_bps,
            } => {
                let request = client
                    .post(format!("{}/v1/stream/start", args.daemon_url))
                    .json(&StreamStartRequest {
                        source_name,
                        source: map_source(source),
                        video_socket,
                        audio_socket,
                        stdin_media_kind: stdin_media_kind.map(map_media_kind),
                        ingest_protocol: ingest_protocol.map(map_ingest_protocol),
                        read_chunk_size: None,
                        pacing_bps,
                        pacing_window_ms: None,
                        max_queue_packets: None,
                    });
                send_text_response(request).await?;
            }
            StreamSubcommand::Stop => {
                let request = client.post(format!("{}/v1/stream/stop", args.daemon_url));
                send_text_response(request).await?;
            }
        },
        Command::Voice(voice) => match voice.command {
            VoiceSubcommand::Connect => {
                send_text_response(client.post(format!("{}/v1/voice/connect", args.daemon_url)))
                    .await?;
            }
            VoiceSubcommand::Health => {
                send_text_response(client.get(format!("{}/v1/voice/health", args.daemon_url)))
                    .await?;
            }
            VoiceSubcommand::State => {
                send_text_response(client.get(format!("{}/v1/voice/state", args.daemon_url)))
                    .await?;
            }
            VoiceSubcommand::Disconnect => {
                send_text_response(client.post(format!("{}/v1/voice/disconnect", args.daemon_url)))
                    .await?;
            }
        },
        Command::Ingest(ingest) => match ingest.command {
            IngestSubcommand::SendFramed {
                socket,
                input,
                repeat,
                interval_ms,
            } => {
                send_framed(socket, input, repeat, interval_ms).await?;
            }
            IngestSubcommand::SendH264AnnexB {
                socket,
                input,
                read_chunk_size,
            } => {
                send_h264_annex_b(socket, input, read_chunk_size).await?;
            }
            IngestSubcommand::SendOggOpus {
                socket,
                input,
                interval_ms,
            } => {
                send_ogg_opus(socket, input, interval_ms).await?;
            }
        },
        Command::Media(media) => match media.command {
            MediaSubcommand::Connect => {
                send_text_response(client.post(format!("{}/v1/media/connect", args.daemon_url)))
                    .await?;
            }
            MediaSubcommand::Health => {
                send_text_response(client.get(format!("{}/v1/media/health", args.daemon_url)))
                    .await?;
            }
            MediaSubcommand::Disconnect => {
                send_text_response(client.post(format!("{}/v1/media/disconnect", args.daemon_url)))
                    .await?;
            }
        },
        Command::Dave(dave) => match dave.command {
            DaveSubcommand::Init {
                protocol_version,
                user_id,
                channel_id,
            } => {
                let request = client
                    .post(format!("{}/v1/dave/init", args.daemon_url))
                    .json(&DaveInitRequest {
                        protocol_version,
                        user_id,
                        channel_id,
                        signing_private_key: None,
                        signing_public_key: None,
                    });
                send_text_response(request).await?;
            }
            DaveSubcommand::State => {
                let request = client.get(format!("{}/v1/dave/state", args.daemon_url));
                send_text_response(request).await?;
            }
            DaveSubcommand::KeyPackage => {
                let request = client.post(format!("{}/v1/dave/key-package", args.daemon_url));
                send_text_response(request).await?;
            }
        },
        Command::Health => {
            let request = client.get(format!("{}/v1/health", args.daemon_url));
            send_text_response(request).await?;
        }
        Command::Metrics => {
            let request = client.get(format!("{}/v1/metrics", args.daemon_url));
            send_text_response(request).await?;
        }
    }

    Ok(())
}

async fn send_text_response(request: RequestBuilder) -> anyhow::Result<()> {
    let response = request.send().await?;
    let status = response.status();
    let body = response.text().await?;
    println!("{body}");
    if !status.is_success() {
        anyhow::bail!("request failed with status {status}");
    }
    Ok(())
}

async fn send_framed(
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

async fn send_ogg_opus(socket: String, input: PathBuf, interval_ms: u64) -> anyhow::Result<()> {
    let file = std::fs::File::open(input)?;
    let mut reader = PacketReader::new(file);
    let mut stream = UnixStream::connect(socket).await?;
    let mut header_packets_seen = 0_u8;

    while let Some(packet) = reader.read_packet()? {
        if packet.first_in_stream() || header_packets_seen < 2 {
            header_packets_seen = header_packets_seen.saturating_add(1);
            continue;
        }

        write_framed_packet(&mut stream, &packet.data).await?;
        if interval_ms > 0 {
            sleep(Duration::from_millis(interval_ms)).await;
        }
    }

    Ok(())
}

async fn send_h264_annex_b(
    socket: String,
    input: Option<PathBuf>,
    read_chunk_size: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(read_chunk_size > 0, "read_chunk_size must be positive");
    let mut stream = UnixStream::connect(socket).await?;
    let mut reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> = match input {
        Some(path) => Box::new(tokio::fs::File::open(path).await?),
        None => Box::new(io::stdin()),
    };

    let mut read_buf = vec![0_u8; read_chunk_size];
    let mut parser = AnnexBAccessUnitParser::default();

    loop {
        let bytes_read = reader.read(&mut read_buf).await?;
        if bytes_read == 0 {
            break;
        }

        let access_units = parser.push(&read_buf[..bytes_read]);
        for access_unit in access_units {
            write_framed_packet(&mut stream, &access_unit).await?;
        }
    }

    if let Some(access_unit) = parser.finish() {
        write_framed_packet(&mut stream, &access_unit).await?;
    }

    Ok(())
}

async fn write_framed_packet(stream: &mut UnixStream, payload: &[u8]) -> anyhow::Result<()> {
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(payload).await?;
    Ok(())
}

fn map_stream_kind(value: StreamKindArg) -> GatewayStreamKind {
    match value {
        StreamKindArg::GoLive => GatewayStreamKind::GoLive,
        StreamKindArg::Camera => GatewayStreamKind::Camera,
    }
}

fn map_source(value: IngestSourceArg) -> IngestSource {
    match value {
        IngestSourceArg::Unix => IngestSource::Unix,
        IngestSourceArg::Stdin => IngestSource::Stdin,
    }
}

fn map_media_kind(value: MediaKindArg) -> MediaKind {
    match value {
        MediaKindArg::Video => MediaKind::Video,
        MediaKindArg::Audio => MediaKind::Audio,
    }
}

fn map_ingest_protocol(value: IngestProtocolArg) -> IngestProtocol {
    match value {
        IngestProtocolArg::Raw => IngestProtocol::Raw,
        IngestProtocolArg::Framed => IngestProtocol::Framed,
    }
}

#[derive(Default)]
struct AnnexBAccessUnitParser {
    buffer: Vec<u8>,
    frame_start: Option<usize>,
    scan_offset: usize,
}

impl AnnexBAccessUnitParser {
    fn push(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        self.buffer.extend_from_slice(bytes);
        self.drain_complete_access_units(false)
    }

    fn finish(&mut self) -> Option<Vec<u8>> {
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
