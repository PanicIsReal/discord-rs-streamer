mod ingest;
mod play;

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use daemon::{DaveInitRequest, IngestSource, SessionConnectRequest, StreamStartRequest};
use discord_gateway::StreamKind as GatewayStreamKind;
use media_ingest::{IngestProtocol, MediaKind};
use reqwest::RequestBuilder;

use crate::ingest::{send_framed, send_h264_annex_b, send_ogg_opus};

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
    Play(play::PlayCommand),
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
pub(crate) enum StreamKindArg {
    GoLive,
    Camera,
}

impl From<StreamKindArg> for GatewayStreamKind {
    fn from(value: StreamKindArg) -> Self {
        match value {
            StreamKindArg::GoLive => GatewayStreamKind::GoLive,
            StreamKindArg::Camera => GatewayStreamKind::Camera,
        }
    }
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
        Command::Play(play_command) => {
            play::run(play_command, args.daemon_url).await?;
        }
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
                        stream_kind: stream_kind.into(),
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
                        source: source.into(),
                        video_socket,
                        audio_socket,
                        stdin_media_kind: stdin_media_kind.map(Into::into),
                        ingest_protocol: ingest_protocol.map(Into::into),
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

impl From<IngestSourceArg> for IngestSource {
    fn from(value: IngestSourceArg) -> Self {
        match value {
            IngestSourceArg::Unix => IngestSource::Unix,
            IngestSourceArg::Stdin => IngestSource::Stdin,
        }
    }
}

impl From<MediaKindArg> for MediaKind {
    fn from(value: MediaKindArg) -> Self {
        match value {
            MediaKindArg::Video => MediaKind::Video,
            MediaKindArg::Audio => MediaKind::Audio,
        }
    }
}

impl From<IngestProtocolArg> for IngestProtocol {
    fn from(value: IngestProtocolArg) -> Self {
        match value {
            IngestProtocolArg::Raw => IngestProtocol::Raw,
            IngestProtocolArg::Framed => IngestProtocol::Framed,
        }
    }
}
