use std::{
    env,
    ffi::OsString,
    net::IpAddr,
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, anyhow, bail};
use clap::{ArgGroup, Parser};
use daemon::{IngestSource, SessionConnectRequest, StreamStartRequest};
use discord_gateway::StreamKind as GatewayStreamKind;
use media_ingest::IngestProtocol;
use reqwest::Client;
use serde_json::Value;
use tempfile::TempDir;
use tokio::{
    net::UnixStream,
    process::{Child, Command},
    signal,
    task::JoinHandle,
    time::{Duration, Instant, sleep},
};
use url::{Host, Url};

use crate::{
    StreamKindArg,
    ingest::{forward_h264_annex_b_reader_to_stream, forward_ogg_opus_reader_to_stream},
};

const DEFAULT_DAEMON_URL: &str = "http://127.0.0.1:7331";
const DEFAULT_DAEMON_BIND: &str = "127.0.0.1:7331";
const DEFAULT_PULSE_SOURCE: &str = "audio_output.monitor";
const DEFAULT_SOURCE_NAME_FILE: &str = "play-file";
const DEFAULT_SOURCE_NAME_X11: &str = "play-x11";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(60);
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(1);
const SOCKET_CONNECT_ATTEMPTS: usize = 60;
const SOCKET_CONNECT_INTERVAL: Duration = Duration::from_millis(250);
const ANNEX_B_READ_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, Parser)]
#[command(about = "Start a Discord stream from a local file or an X11/Pulse capture source")]
#[command(group(
    ArgGroup::new("source")
        .required(true)
        .args(["input", "x11_display"])
))]
pub(crate) struct PlayCommand {
    #[arg(long, help = "Discord user token. Falls back to DISCORD_TOKEN.")]
    token: Option<String>,
    #[arg(long, help = "Discord guild ID. Falls back to DISCORD_GUILD_ID.")]
    guild_id: Option<String>,
    #[arg(
        long,
        help = "Discord voice or stage channel ID. Falls back to DISCORD_CHANNEL_ID."
    )]
    channel_id: Option<String>,
    #[arg(
        long,
        value_enum,
        default_value = "go-live",
        help = "Discord stream type to publish."
    )]
    stream_kind: StreamKindArg,
    #[arg(
        long,
        group = "source",
        help = "Play a local media file through Discord."
    )]
    input: Option<PathBuf>,
    #[arg(long, group = "source", help = "Capture an X11 display such as :99.0.")]
    x11_display: Option<String>,
    #[arg(
        long,
        requires = "x11_display",
        conflicts_with = "input",
        help = "Pulse source name for X11 capture audio."
    )]
    pulse_source: Option<String>,
    #[arg(long, help = "Disable audio capture or playback.")]
    no_audio: bool,
    #[arg(
        long = "loop",
        requires = "input",
        conflicts_with = "x11_display",
        help = "Loop file input indefinitely."
    )]
    loop_input: bool,
    #[arg(long, default_value = DEFAULT_DAEMON_BIND, help = "Bind address for an auto-started local daemon.")]
    daemon_bind: String,
    #[arg(
        long,
        default_value = "ffmpeg",
        help = "Path to the ffmpeg binary used for capture/transcode."
    )]
    ffmpeg_bin: String,
    #[arg(long, default_value_t = 15, help = "Target video framerate.")]
    fps: u32,
    #[arg(long, default_value_t = 1280, help = "Output video width.")]
    width: u32,
    #[arg(long, default_value_t = 720, help = "Output video height.")]
    height: u32,
    #[arg(long, default_value_t = 2500, help = "Target video bitrate in kbps.")]
    video_bitrate_kbps: u32,
    #[arg(long, default_value_t = 128, help = "Target audio bitrate in kbps.")]
    audio_bitrate_kbps: u32,
    #[arg(
        long,
        default_value = "ultrafast",
        help = "x264 preset used for video encoding."
    )]
    x264_preset: String,
    #[arg(long, default_value_t = 2, help = "Seconds between forced keyframes.")]
    keyframe_interval_seconds: u32,
}

#[derive(Debug, Clone)]
enum PlaySource {
    File(PathBuf),
    X11 {
        display: String,
        pulse_source: Option<String>,
    },
}

impl PlaySource {
    fn x11_display(&self) -> Option<&str> {
        match self {
            Self::X11 { display, .. } => Some(display.as_str()),
            Self::File(_) => None,
        }
    }

    fn pulse_source(&self) -> Option<&str> {
        match self {
            Self::X11 { pulse_source, .. } => pulse_source.as_deref(),
            Self::File(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
struct PlayConfig {
    daemon_url: String,
    daemon_bind: String,
    token: String,
    guild_id: String,
    channel_id: String,
    stream_kind: GatewayStreamKind,
    ffmpeg_bin: String,
    source: PlaySource,
    no_audio: bool,
    loop_input: bool,
    fps: u32,
    width: u32,
    height: u32,
    video_bitrate_kbps: u32,
    audio_bitrate_kbps: u32,
    x264_preset: String,
    keyframe_interval_seconds: u32,
}

#[derive(Debug, Default)]
struct CleanupState {
    voice_connected: bool,
    media_connected: bool,
    stream_started: bool,
}

struct OwnedDaemon {
    child: Child,
}

struct PipelineTask {
    name: &'static str,
    child: Child,
    forwarder: Option<JoinHandle<anyhow::Result<usize>>>,
}

struct PlaybackPipeline {
    video: PipelineTask,
    audio: Option<PipelineTask>,
}

pub(crate) async fn run(command: PlayCommand, daemon_url: String) -> anyhow::Result<()> {
    let config = PlayConfig::from_command(command, daemon_url)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build HTTP client")?;

    let mut daemon = ensure_daemon_running(&client, &config).await?;
    let mut cleanup = CleanupState::default();
    let result = run_playback(&client, &config, &mut cleanup).await;
    cleanup_runtime(&client, &config.daemon_url, &mut cleanup, daemon.as_mut()).await;

    result
}

impl PlayConfig {
    fn from_command(command: PlayCommand, daemon_url: String) -> anyhow::Result<Self> {
        let daemon_url = if daemon_url.is_empty() {
            DEFAULT_DAEMON_URL.to_owned()
        } else {
            daemon_url
        };
        let daemon_bind = derive_daemon_bind(&daemon_url, &command.daemon_bind)?;
        Ok(Self {
            daemon_url,
            daemon_bind,
            token: required_arg(command.token, "DISCORD_TOKEN", "--token")?,
            guild_id: required_arg(command.guild_id, "DISCORD_GUILD_ID", "--guild-id")?,
            channel_id: required_arg(command.channel_id, "DISCORD_CHANNEL_ID", "--channel-id")?,
            stream_kind: command.stream_kind.into(),
            ffmpeg_bin: command.ffmpeg_bin,
            source: if let Some(input) = command.input {
                PlaySource::File(input)
            } else if let Some(display) = command.x11_display {
                PlaySource::X11 {
                    display,
                    pulse_source: if command.no_audio {
                        None
                    } else {
                        Some(
                            command
                                .pulse_source
                                .unwrap_or_else(|| DEFAULT_PULSE_SOURCE.to_owned()),
                        )
                    },
                }
            } else {
                bail!("either --input or --x11-display is required");
            },
            no_audio: command.no_audio,
            loop_input: command.loop_input,
            fps: command.fps.max(1),
            width: command.width.max(1),
            height: command.height.max(1),
            video_bitrate_kbps: command.video_bitrate_kbps.max(1),
            audio_bitrate_kbps: command.audio_bitrate_kbps.max(1),
            x264_preset: command.x264_preset,
            keyframe_interval_seconds: command.keyframe_interval_seconds.max(1),
        })
    }

    fn source_name(&self) -> &'static str {
        match self.source {
            PlaySource::File(_) => DEFAULT_SOURCE_NAME_FILE,
            PlaySource::X11 { .. } => DEFAULT_SOURCE_NAME_X11,
        }
    }

    fn audio_enabled(&self) -> bool {
        !self.no_audio
    }
}

async fn run_playback(
    client: &Client,
    config: &PlayConfig,
    cleanup: &mut CleanupState,
) -> anyhow::Result<()> {
    if let Some(display) = config.source.x11_display() {
        probe_ffmpeg_source(
            &config.ffmpeg_bin,
            "x11grab",
            display,
            "-frames:v",
            &format!("X11 display {display}"),
        )
        .await?;
        if let Some(pulse_source) = config.source.pulse_source() {
            probe_ffmpeg_source(
                &config.ffmpeg_bin,
                "pulse",
                pulse_source,
                "-frames:a",
                &format!("Pulse source {pulse_source}"),
            )
            .await?;
        }
    }

    connect_session(client, config).await?;
    cleanup.voice_connected = true;

    connect_media(client, &config.daemon_url).await?;
    cleanup.media_connected = true;

    let file_audio_present = match &config.source {
        PlaySource::File(input) if config.audio_enabled() => {
            file_has_audio_stream(config, input).await?
        }
        _ => false,
    };
    let socket_dir = TempDir::new().context("failed to create temporary socket directory")?;
    let video_socket = socket_dir.path().join("video.sock");
    let audio_socket = should_open_audio_socket(config, file_audio_present)
        .then(|| socket_dir.path().join("audio.sock"));

    start_stream(client, config, &video_socket, audio_socket.as_deref()).await?;
    cleanup.stream_started = true;
    wait_for_media_connected(client, &config.daemon_url).await?;

    let mut pipeline = spawn_pipeline(
        config,
        &video_socket,
        audio_socket.as_deref(),
        file_audio_present,
    )
    .await?;
    let outcome = tokio::select! {
        result = pipeline.wait() => result,
        _ = signal::ctrl_c() => Ok(()),
    };

    pipeline.terminate().await;
    outcome
}

async fn ensure_daemon_running(
    client: &Client,
    config: &PlayConfig,
) -> anyhow::Result<Option<OwnedDaemon>> {
    if daemon_healthy(client, &config.daemon_url).await {
        return Ok(None);
    }

    if !auto_start_allowed(&config.daemon_url)? {
        bail!("daemon at {} is unavailable", config.daemon_url);
    }

    let child = spawn_daemon(&config.daemon_bind).await?;
    wait_for_daemon_health(client, &config.daemon_url).await?;
    Ok(Some(OwnedDaemon { child }))
}

async fn spawn_daemon(bind: &str) -> anyhow::Result<Child> {
    let candidates = daemon_binary_candidates()?;
    let mut last_error = None;

    for candidate in candidates {
        let mut command = Command::new(&candidate);
        command
            .env("DISCORD_RS_STREAMER_BIND", bind)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        match command.spawn() {
            Ok(child) => return Ok(child),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                last_error = Some(anyhow!("{candidate:?}: {error}"));
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to spawn daemon binary {:?}", candidate));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("failed to locate daemon binary")))
}

async fn connect_session(client: &Client, config: &PlayConfig) -> anyhow::Result<()> {
    let response = client
        .post(format!("{}/v1/session/connect", config.daemon_url))
        .json(&SessionConnectRequest {
            token: config.token.clone(),
            guild_id: config.guild_id.clone(),
            channel_id: config.channel_id.clone(),
            stream_kind: config.stream_kind,
        })
        .send()
        .await
        .context("failed to connect session")?;
    ensure_success(response, "session connect").await
}

async fn connect_media(client: &Client, daemon_url: &str) -> anyhow::Result<()> {
    let response = client
        .post(format!("{daemon_url}/v1/media/connect"))
        .send()
        .await
        .context("failed to connect media")?;
    ensure_success(response, "media connect").await
}

async fn start_stream(
    client: &Client,
    config: &PlayConfig,
    video_socket: &Path,
    audio_socket: Option<&Path>,
) -> anyhow::Result<()> {
    let response = client
        .post(format!("{}/v1/stream/start", config.daemon_url))
        .json(&StreamStartRequest {
            source_name: config.source_name().to_owned(),
            source: IngestSource::Unix,
            video_socket: Some(video_socket.display().to_string()),
            audio_socket: audio_socket.map(|path| path.display().to_string()),
            stdin_media_kind: None,
            ingest_protocol: Some(IngestProtocol::Framed),
            read_chunk_size: None,
            pacing_bps: None,
            pacing_window_ms: None,
            max_queue_packets: None,
        })
        .send()
        .await
        .context("failed to start stream")?;
    ensure_success(response, "stream start").await
}

async fn wait_for_media_connected(client: &Client, daemon_url: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + HEALTH_TIMEOUT;
    loop {
        let response = client
            .get(format!("{daemon_url}/v1/media/health"))
            .send()
            .await
            .context("failed to query media health")?;
        if response.status().is_success() {
            let body: Value = response
                .json()
                .await
                .context("invalid media health response")?;
            let connected = body
                .pointer("/media/publisher/connection_state")
                .and_then(Value::as_str)
                == Some("Connected");
            if connected {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            bail!("media session did not reach connected state before timeout");
        }
        sleep(HEALTH_POLL_INTERVAL).await;
    }
}

async fn daemon_healthy(client: &Client, daemon_url: &str) -> bool {
    client
        .get(format!("{daemon_url}/v1/health"))
        .send()
        .await
        .map(|response| response.status().is_success())
        .unwrap_or(false)
}

async fn wait_for_daemon_health(client: &Client, daemon_url: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + HEALTH_TIMEOUT;
    loop {
        if daemon_healthy(client, daemon_url).await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("daemon at {daemon_url} did not become healthy before timeout");
        }
        sleep(HEALTH_POLL_INTERVAL).await;
    }
}

async fn cleanup_runtime(
    client: &Client,
    daemon_url: &str,
    cleanup: &mut CleanupState,
    daemon: Option<&mut OwnedDaemon>,
) {
    if cleanup.stream_started {
        let _ = client
            .post(format!("{daemon_url}/v1/stream/stop"))
            .send()
            .await;
        cleanup.stream_started = false;
    }
    if cleanup.media_connected {
        let _ = client
            .post(format!("{daemon_url}/v1/media/disconnect"))
            .send()
            .await;
        cleanup.media_connected = false;
    }
    if cleanup.voice_connected {
        let _ = client
            .post(format!("{daemon_url}/v1/voice/disconnect"))
            .send()
            .await;
        cleanup.voice_connected = false;
    }
    if let Some(daemon) = daemon {
        let _ = daemon.child.kill().await;
        let _ = daemon.child.wait().await;
    }
}

impl PlaybackPipeline {
    async fn wait(&mut self) -> anyhow::Result<()> {
        if let Some(audio) = self.audio.as_mut() {
            tokio::select! {
                video = self.video.wait() => {
                    video?;
                    audio.wait().await?;
                }
                audio_result = audio.wait() => {
                    audio_result?;
                    self.video.wait().await?;
                }
            }
        } else {
            self.video.wait().await?;
        }
        Ok(())
    }

    async fn terminate(&mut self) {
        self.video.terminate().await;
        if let Some(audio) = self.audio.as_mut() {
            audio.terminate().await;
        }
    }
}

impl PipelineTask {
    async fn wait(&mut self) -> anyhow::Result<()> {
        let forwarded = self
            .forwarder
            .take()
            .ok_or_else(|| anyhow!("{} forwarding task missing", self.name))?
            .await
            .map_err(|error| anyhow!("{} forwarding task failed: {error}", self.name))??;
        let status = self
            .child
            .wait()
            .await
            .with_context(|| format!("failed waiting for {} ffmpeg process", self.name))?;
        if !status.success() {
            bail!("{} ffmpeg process exited with status {status}", self.name);
        }
        if forwarded == 0 {
            bail!("{} ffmpeg process produced no packets", self.name);
        }
        Ok(())
    }

    async fn terminate(&mut self) {
        if let Some(forwarder) = self.forwarder.take() {
            forwarder.abort();
        }
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

async fn spawn_pipeline(
    config: &PlayConfig,
    video_socket: &Path,
    audio_socket: Option<&Path>,
    file_audio_present: bool,
) -> anyhow::Result<PlaybackPipeline> {
    let video_stream = connect_socket(video_socket).await?;
    let video = match &config.source {
        PlaySource::File(input) => {
            spawn_video_pipeline(
                build_file_video_args(config, input),
                &config.ffmpeg_bin,
                video_stream,
                "video",
            )
            .await?
        }
        PlaySource::X11 { display, .. } => {
            spawn_video_pipeline(
                build_x11_video_args(config, display),
                &config.ffmpeg_bin,
                video_stream,
                "video",
            )
            .await?
        }
    };

    let audio = spawn_audio_pipeline_if_needed(config, audio_socket, file_audio_present).await?;

    Ok(PlaybackPipeline { video, audio })
}

async fn spawn_audio_pipeline_if_needed(
    config: &PlayConfig,
    audio_socket: Option<&Path>,
    file_audio_present: bool,
) -> anyhow::Result<Option<PipelineTask>> {
    let Some(audio_socket) = audio_socket else {
        return Ok(None);
    };
    if !config.audio_enabled() {
        return Ok(None);
    }

    match &config.source {
        PlaySource::File(input) => {
            if !file_audio_present {
                return Ok(None);
            }
            let stream = connect_socket(audio_socket).await?;
            spawn_audio_pipeline(
                build_file_audio_args(config, input),
                &config.ffmpeg_bin,
                stream,
            )
            .await
            .map(Some)
        }
        PlaySource::X11 { pulse_source, .. } => {
            let stream = connect_socket(audio_socket).await?;
            spawn_audio_pipeline(
                build_pulse_audio_args(
                    config,
                    pulse_source.as_deref().unwrap_or(DEFAULT_PULSE_SOURCE),
                ),
                &config.ffmpeg_bin,
                stream,
            )
            .await
            .map(Some)
        }
    }
}

fn should_open_audio_socket(config: &PlayConfig, file_audio_present: bool) -> bool {
    match &config.source {
        PlaySource::File(_) => config.audio_enabled() && file_audio_present,
        PlaySource::X11 { .. } => config.audio_enabled(),
    }
}

async fn spawn_video_pipeline(
    args: Vec<OsString>,
    ffmpeg_bin: &str,
    mut stream: UnixStream,
    name: &'static str,
) -> anyhow::Result<PipelineTask> {
    let mut child = spawn_ffmpeg(ffmpeg_bin, args, name)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("{name} ffmpeg stdout was not piped"))?;
    let forwarder = tokio::spawn(async move {
        forward_h264_annex_b_reader_to_stream(stdout, &mut stream, ANNEX_B_READ_CHUNK_SIZE).await
    });
    Ok(PipelineTask {
        name,
        child,
        forwarder: Some(forwarder),
    })
}

async fn spawn_audio_pipeline(
    args: Vec<OsString>,
    ffmpeg_bin: &str,
    mut stream: UnixStream,
) -> anyhow::Result<PipelineTask> {
    let mut child = spawn_ffmpeg(ffmpeg_bin, args, "audio")?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("audio ffmpeg stdout was not piped"))?;
    let forwarder =
        tokio::spawn(
            async move { forward_ogg_opus_reader_to_stream(stdout, &mut stream, None).await },
        );
    Ok(PipelineTask {
        name: "audio",
        child,
        forwarder: Some(forwarder),
    })
}

fn spawn_ffmpeg(ffmpeg_bin: &str, args: Vec<OsString>, name: &str) -> anyhow::Result<Child> {
    let mut command = Command::new(ffmpeg_bin);
    command
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    command
        .spawn()
        .with_context(|| format!("failed to spawn {name} ffmpeg process"))
}

async fn connect_socket(path: &Path) -> anyhow::Result<UnixStream> {
    for attempt in 1..=SOCKET_CONNECT_ATTEMPTS {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(stream),
            Err(error) if attempt == SOCKET_CONNECT_ATTEMPTS => {
                return Err(error)
                    .with_context(|| format!("failed to connect to socket {}", path.display()));
            }
            Err(_) => sleep(SOCKET_CONNECT_INTERVAL).await,
        }
    }

    bail!("failed to connect to socket {}", path.display())
}

async fn file_has_audio_stream(config: &PlayConfig, input: &Path) -> anyhow::Result<bool> {
    let ffprobe_bin = ffprobe_binary(&config.ffmpeg_bin);
    let output = Command::new(ffprobe_bin)
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("a:0")
        .arg("-show_entries")
        .arg("stream=index")
        .arg("-of")
        .arg("csv=p=0")
        .arg(input)
        .output()
        .await
        .context("failed to probe input audio streams")?;

    if !output.status.success() {
        return Ok(false);
    }

    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

async fn probe_ffmpeg_source(
    ffmpeg_bin: &str,
    format: &str,
    input: &str,
    frames_flag: &str,
    label: &str,
) -> anyhow::Result<()> {
    let args = if format == "x11grab" {
        vec![
            "-loglevel",
            "error",
            "-video_size",
            "640x360",
            "-f",
            format,
            "-i",
            input,
            frames_flag,
            "1",
            "-f",
            "null",
            "-",
        ]
    } else {
        vec![
            "-loglevel",
            "error",
            "-f",
            format,
            "-i",
            input,
            frames_flag,
            "1",
            "-f",
            "null",
            "-",
        ]
    };

    let status = Command::new(ffmpeg_bin)
        .args(&args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .with_context(|| format!("failed to probe {label}"))?;

    if !status.success() {
        bail!("{label} is not readable");
    }

    Ok(())
}

fn build_file_video_args(config: &PlayConfig, input: &Path) -> Vec<OsString> {
    let mut args = common_video_args(config);
    let mut prefix = file_input_prefix(config, input);
    prefix.append(&mut args);
    prefix
}

fn build_file_audio_args(config: &PlayConfig, input: &Path) -> Vec<OsString> {
    let mut args = file_input_prefix(config, input);
    args.extend([
        OsString::from("-vn"),
        OsString::from("-map"),
        OsString::from("0:a:0"),
        OsString::from("-c:a"),
        OsString::from("libopus"),
        OsString::from("-b:a"),
        OsString::from(format!("{}k", config.audio_bitrate_kbps)),
        OsString::from("-f"),
        OsString::from("ogg"),
        OsString::from("pipe:1"),
    ]);
    args
}

fn build_x11_video_args(config: &PlayConfig, display: &str) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("-loglevel"),
        OsString::from("warning"),
        OsString::from("-framerate"),
        OsString::from(config.fps.to_string()),
        OsString::from("-f"),
        OsString::from("x11grab"),
        OsString::from("-i"),
        OsString::from(display),
    ];
    args.extend(common_video_args(config));
    args
}

fn build_pulse_audio_args(config: &PlayConfig, pulse_source: &str) -> Vec<OsString> {
    vec![
        OsString::from("-loglevel"),
        OsString::from("warning"),
        OsString::from("-f"),
        OsString::from("pulse"),
        OsString::from("-i"),
        OsString::from(pulse_source),
        OsString::from("-vn"),
        OsString::from("-c:a"),
        OsString::from("libopus"),
        OsString::from("-b:a"),
        OsString::from(format!("{}k", config.audio_bitrate_kbps)),
        OsString::from("-f"),
        OsString::from("ogg"),
        OsString::from("pipe:1"),
    ]
}

fn file_input_prefix(config: &PlayConfig, input: &Path) -> Vec<OsString> {
    let mut args = vec![OsString::from("-loglevel"), OsString::from("warning")];
    if config.loop_input {
        args.push(OsString::from("-stream_loop"));
        args.push(OsString::from("-1"));
    }
    args.push(OsString::from("-re"));
    args.push(OsString::from("-i"));
    args.push(input.as_os_str().to_owned());
    args
}

fn common_video_args(config: &PlayConfig) -> Vec<OsString> {
    let keyframe_interval_frames = config
        .fps
        .saturating_mul(config.keyframe_interval_seconds)
        .max(1);
    vec![
        OsString::from("-an"),
        OsString::from("-vf"),
        OsString::from(format!(
            "scale={}:{}:flags=lanczos,format=yuv420p",
            config.width, config.height
        )),
        OsString::from("-c:v"),
        OsString::from("libx264"),
        OsString::from("-preset"),
        OsString::from(config.x264_preset.clone()),
        OsString::from("-tune"),
        OsString::from("zerolatency"),
        OsString::from("-pix_fmt"),
        OsString::from("yuv420p"),
        OsString::from("-profile:v"),
        OsString::from("baseline"),
        OsString::from("-g"),
        OsString::from(keyframe_interval_frames.to_string()),
        OsString::from("-keyint_min"),
        OsString::from(keyframe_interval_frames.to_string()),
        OsString::from("-sc_threshold"),
        OsString::from("0"),
        OsString::from("-b:v"),
        OsString::from(format!("{}k", config.video_bitrate_kbps)),
        OsString::from("-maxrate"),
        OsString::from(format!("{}k", config.video_bitrate_kbps)),
        OsString::from("-bufsize"),
        OsString::from(format!("{}k", config.video_bitrate_kbps.saturating_mul(2))),
        OsString::from("-bsf:v"),
        OsString::from("h264_metadata=aud=insert"),
        OsString::from("-f"),
        OsString::from("h264"),
        OsString::from("pipe:1"),
    ]
}

fn ffprobe_binary(ffmpeg_bin: &str) -> PathBuf {
    let ffmpeg_path = Path::new(ffmpeg_bin);
    let file_name = ffmpeg_path.file_name().and_then(|name| name.to_str());
    if matches!(file_name, Some("ffmpeg")) {
        return ffmpeg_path.with_file_name("ffprobe");
    }
    PathBuf::from("ffprobe")
}

fn required_arg(value: Option<String>, env_key: &str, flag: &str) -> anyhow::Result<String> {
    required_arg_with_env(value, env::var(env_key).ok(), env_key, flag)
}

fn required_arg_with_env(
    value: Option<String>,
    env_value: Option<String>,
    env_key: &str,
    flag: &str,
) -> anyhow::Result<String> {
    value
        .or(env_value)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing required value for {flag} (or {env_key})"))
}

fn derive_daemon_bind(daemon_url: &str, requested_bind: &str) -> anyhow::Result<String> {
    let parsed = Url::parse(daemon_url).context("invalid --daemon-url")?;
    let Some(host) = parsed.host() else {
        return Ok(requested_bind.to_owned());
    };
    let Some(port) = parsed.port_or_known_default() else {
        return Ok(requested_bind.to_owned());
    };
    if requested_bind != DEFAULT_DAEMON_BIND {
        return Ok(requested_bind.to_owned());
    }
    Ok(match host {
        Host::Ipv4(address) => format!("{address}:{port}"),
        Host::Ipv6(address) => format!("[{address}]:{port}"),
        Host::Domain(domain) if domain.eq_ignore_ascii_case("localhost") => {
            format!("127.0.0.1:{port}")
        }
        Host::Domain(_) => requested_bind.to_owned(),
    })
}

fn auto_start_allowed(daemon_url: &str) -> anyhow::Result<bool> {
    let url = Url::parse(daemon_url).context("invalid --daemon-url")?;
    let Some(host) = url.host_str() else {
        return Ok(false);
    };
    if host.eq_ignore_ascii_case("localhost") {
        return Ok(true);
    }
    Ok(host
        .parse::<IpAddr>()
        .map(|address| address.is_loopback())
        .unwrap_or(false))
}

fn daemon_binary_candidates() -> anyhow::Result<Vec<PathBuf>> {
    let mut candidates = Vec::new();
    if let Ok(current_exe) = env::current_exe()
        && let Some(dir) = current_exe.parent()
    {
        candidates.push(dir.join("discord-rs-streamer-daemon"));
        candidates.push(dir.join("daemon"));
    }
    candidates.push(PathBuf::from("discord-rs-streamer-daemon"));
    candidates.push(PathBuf::from("daemon"));
    Ok(candidates)
}

async fn ensure_success(response: reqwest::Response, step: &str) -> anyhow::Result<()> {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if status.is_success() {
        return Ok(());
    }
    if body.is_empty() {
        bail!("{step} failed with status {status}");
    }
    bail!("{step} failed with status {status}: {body}");
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use clap::Parser;

    use super::{
        DEFAULT_DAEMON_BIND, DEFAULT_DAEMON_URL, PlayCommand, PlayConfig, PlaySource,
        build_file_audio_args, build_file_video_args, build_pulse_audio_args, build_x11_video_args,
        derive_daemon_bind, required_arg_with_env,
    };

    fn resolve(command: PlayCommand) -> PlayConfig {
        PlayConfig::from_command(command, DEFAULT_DAEMON_URL.to_owned()).expect("play config")
    }

    fn args_to_strings(args: Vec<OsString>) -> Vec<String> {
        args.into_iter()
            .map(|value| value.into_string().expect("utf8 arg"))
            .collect()
    }

    #[test]
    fn file_video_ffmpeg_args_include_realtime_loop_and_scaling() {
        let config = resolve(PlayCommand::parse_from([
            "discord-rs-streamer",
            "--input",
            "/tmp/sample.mp4",
            "--loop",
            "--token",
            "token",
            "--guild-id",
            "guild",
            "--channel-id",
            "channel",
        ]));
        let PlaySource::File(input) = &config.source else {
            panic!("expected file source");
        };

        let args = args_to_strings(build_file_video_args(&config, input));
        assert!(args.windows(2).any(|pair| pair == ["-stream_loop", "-1"]));
        assert!(args.windows(2).any(|pair| pair == ["-re", "-i"]));
        assert!(
            args.iter()
                .any(|value| value == "scale=1280:720:flags=lanczos,format=yuv420p")
        );
    }

    #[test]
    fn file_audio_ffmpeg_args_map_first_audio_stream() {
        let config = resolve(PlayCommand::parse_from([
            "discord-rs-streamer",
            "--input",
            "/tmp/sample.mp4",
            "--token",
            "token",
            "--guild-id",
            "guild",
            "--channel-id",
            "channel",
        ]));
        let PlaySource::File(input) = &config.source else {
            panic!("expected file source");
        };

        let args = args_to_strings(build_file_audio_args(&config, input));
        assert!(args.windows(2).any(|pair| pair == ["-map", "0:a:0"]));
        assert!(args.windows(2).any(|pair| pair == ["-c:a", "libopus"]));
    }

    #[test]
    fn x11_video_ffmpeg_args_use_capture_display() {
        let config = resolve(PlayCommand::parse_from([
            "discord-rs-streamer",
            "--x11-display",
            ":99.0",
            "--token",
            "token",
            "--guild-id",
            "guild",
            "--channel-id",
            "channel",
        ]));
        let PlaySource::X11 { display, .. } = &config.source else {
            panic!("expected x11 source");
        };

        let args = args_to_strings(build_x11_video_args(&config, display));
        assert!(args.windows(2).any(|pair| pair == ["-f", "x11grab"]));
        assert!(args.windows(2).any(|pair| pair == ["-i", ":99.0"]));
    }

    #[test]
    fn pulse_audio_ffmpeg_args_use_requested_source() {
        let config = resolve(PlayCommand::parse_from([
            "discord-rs-streamer",
            "--x11-display",
            ":99.0",
            "--pulse-source",
            "custom.monitor",
            "--token",
            "token",
            "--guild-id",
            "guild",
            "--channel-id",
            "channel",
        ]));
        let PlaySource::X11 {
            pulse_source: Some(pulse_source),
            ..
        } = &config.source
        else {
            panic!("expected pulse source");
        };

        let args = args_to_strings(build_pulse_audio_args(&config, pulse_source));
        assert!(args.windows(2).any(|pair| pair == ["-f", "pulse"]));
        assert!(args.windows(2).any(|pair| pair == ["-i", "custom.monitor"]));
    }

    #[test]
    fn play_command_rejects_multiple_sources() {
        let result = PlayCommand::try_parse_from([
            "discord-rs-streamer",
            "--input",
            "/tmp/sample.mp4",
            "--x11-display",
            ":99.0",
            "--token",
            "token",
            "--guild-id",
            "guild",
            "--channel-id",
            "channel",
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn play_command_rejects_loop_without_file_input() {
        let result = PlayCommand::try_parse_from([
            "discord-rs-streamer",
            "--x11-display",
            ":99.0",
            "--loop",
            "--token",
            "token",
            "--guild-id",
            "guild",
            "--channel-id",
            "channel",
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn required_arg_prefers_flag_value_over_env_fallback() {
        let resolved = required_arg_with_env(
            Some("flag-token".to_owned()),
            Some("env-token".to_owned()),
            "DISCORD_TOKEN",
            "--token",
        )
        .expect("resolved token");

        assert_eq!(resolved, "flag-token");
    }

    #[test]
    fn derive_daemon_bind_normalizes_localhost() {
        let bind = derive_daemon_bind("http://localhost:7331", DEFAULT_DAEMON_BIND)
            .expect("localhost bind");

        assert_eq!(bind, "127.0.0.1:7331");
    }

    #[test]
    fn derive_daemon_bind_wraps_ipv6_loopback() {
        let bind = derive_daemon_bind("http://[::1]:7443", DEFAULT_DAEMON_BIND).expect("ipv6 bind");

        assert_eq!(bind, "[::1]:7443");
    }

    #[test]
    fn derive_daemon_bind_preserves_explicit_override() {
        let bind =
            derive_daemon_bind("http://localhost:7331", "0.0.0.0:9000").expect("override bind");

        assert_eq!(bind, "0.0.0.0:9000");
    }
}
