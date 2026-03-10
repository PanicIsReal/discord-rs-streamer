#[cfg(feature = "webrtc-media")]
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
#[cfg(feature = "webrtc-media")]
use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
#[cfg(feature = "webrtc-media")]
use axum::body::Bytes;
use base64::{Engine as _, engine::general_purpose::STANDARD};
#[cfg(feature = "webrtc-media")]
use bytes::Bytes as PayloadBytes;
use dave::{
    DaveCommitWelcome, DaveControl, DaveError, DaveInitConfig, DaveMetadata, DaveProposalResult,
    DaveState, ManagedDaveSession, MediaKind as DaveMediaKind, ProposalOp,
};
use discord_gateway::{
    ConnectionState, DiscordGateway, GatewayError, GatewayHealth, GatewayMediaSession,
    RealDiscordGateway, SessionConfig, StreamKind,
};
use discord_transport::{
    MediaKind as TransportMediaKind, NullPacketSink, PacedPacketSender, PacerConfig, Packet,
    PacketSink, TransportMetrics,
};
use discord_voice::{
    StreamKind as VoiceStreamKind, VoiceServerInfo, VoiceSessionConfig, VoiceSessionController,
    VoiceSessionState,
    client::{VoiceGatewayClient, VoiceGatewayHealth},
};
#[cfg(feature = "webrtc-media")]
use discord_voice::{
    VoiceMediaKind, VoiceVideoConfig,
    publisher::{VoicePublisherConfig, VoicePublisherState},
    session::{VoiceMediaSession, VoiceMediaSessionHealth},
};
use media_ingest::{
    IngestChunk, IngestProtocol, MediaKind as IngestMediaKind, StdinSourceConfig,
    UnixSocketIngestServer, UnixSocketSourceConfig, spawn_stdin_ingest,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
#[cfg(feature = "webrtc-media")]
use tracing::warn;
use tracing::info;

#[cfg(feature = "webrtc-media")]
const MEDIA_CONNECT_MAX_ATTEMPTS: usize = 6;
#[cfg(feature = "webrtc-media")]
const MEDIA_CONNECT_RETRY_DELAY: Duration = Duration::from_secs(1);
// Discord media identify is accepted with DAVE protocol version 1 on the live
// stream path; keep this pinned until the local dave crate exposes a stable
// source-of-truth constant.
const MEDIA_MAX_DAVE_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConnectRequest {
    pub token: String,
    pub guild_id: String,
    pub channel_id: String,
    pub stream_kind: StreamKind,
}

impl From<SessionConnectRequest> for SessionConfig {
    fn from(request: SessionConnectRequest) -> Self {
        Self {
            token: request.token,
            guild_id: request.guild_id,
            channel_id: request.channel_id,
            stream_kind: request.stream_kind,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStartRequest {
    pub source_name: String,
    pub source: IngestSource,
    pub video_socket: Option<String>,
    pub audio_socket: Option<String>,
    pub stdin_media_kind: Option<IngestMediaKind>,
    pub ingest_protocol: Option<IngestProtocol>,
    pub read_chunk_size: Option<usize>,
    pub pacing_bps: Option<usize>,
    pub pacing_window_ms: Option<u64>,
    pub max_queue_packets: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaProfile {
    pub width: u32,
    pub height: u32,
    pub max_framerate: u32,
    pub max_bitrate_bps: u32,
    pub audio_frame_duration_ms: u32,
    pub keyframe_interval_seconds: u32,
}

impl Default for MediaProfile {
    fn default() -> Self {
        Self {
            width: 1280,
            height: 720,
            max_framerate: 30,
            max_bitrate_bps: 4_000_000,
            audio_frame_duration_ms: 20,
            keyframe_interval_seconds: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MediaConnectRequest {
    #[serde(default)]
    pub profile: MediaProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestSource {
    Unix,
    Stdin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResponse {
    pub discord: DiscordHealthResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMediaResponse {
    pub media: GatewayMediaSession,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamResponse {
    pub source_name: String,
    pub capture_running: bool,
    pub transport: TransportMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceSessionResponse {
    pub voice: VoiceGatewayHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceStateResponse {
    pub state: VoiceSessionState,
}

#[cfg(feature = "webrtc-media")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaSessionResponse {
    pub media: VoiceMediaSessionHealth,
    pub profile: MediaProfile,
    pub publish: PublishMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NekoHealthResponse {
    #[serde(rename = "loggedIn")]
    pub logged_in: bool,
    #[serde(rename = "websocketConnected")]
    pub websocket_connected: bool,
    #[serde(rename = "lastError", skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscordHealthResponse {
    pub enabled: bool,
    #[serde(rename = "loggedIn")]
    pub logged_in: bool,
    #[serde(rename = "joinedVoice")]
    pub joined_voice: bool,
    pub streaming: bool,
    #[serde(rename = "lastError", skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl From<GatewayHealth> for DiscordHealthResponse {
    fn from(value: GatewayHealth) -> Self {
        Self {
            enabled: true,
            logged_in: value.logged_in,
            joined_voice: value.joined_voice,
            streaming: value.streaming,
            last_error: value.last_error,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub neko: NekoHealthResponse,
    pub discord: DiscordHealthResponse,
    pub capture_running: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IngestMetrics {
    pub received_chunks: u64,
    pub received_bytes: u64,
}

#[cfg(feature = "webrtc-media")]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PublishMetrics {
    pub audio_queue_depth: usize,
    pub video_queue_depth: usize,
    pub startup_audio_depth: usize,
    pub startup_video_depth: usize,
    pub startup_has_video_config: bool,
    pub dropped_not_ready_audio: u64,
    pub dropped_not_ready_video: u64,
    pub dropped_overload_audio: u64,
    pub dropped_overload_video: u64,
    pub dropped_sink_video: u64,
    pub last_keyframe_age_ms: Option<u64>,
    pub audio_send_jitter_ms: Option<u64>,
    pub video_send_jitter_ms: Option<u64>,
    pub sink_video_quality: Option<u8>,
    pub sink_video_target_fps: Option<u32>,
    pub sink_video_paused: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub transport: TransportMetrics,
    pub ingest: IngestMetrics,
    pub discord_state: ConnectionState,
    #[cfg(feature = "webrtc-media")]
    pub publisher: Option<VoicePublisherState>,
    #[cfg(feature = "webrtc-media")]
    pub publish: PublishMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaveInitRequest {
    pub protocol_version: u16,
    pub user_id: u64,
    pub channel_id: u64,
    pub signing_private_key: Option<String>,
    pub signing_public_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryBodyRequest {
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryBodyResponse {
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaveProposalsRequest {
    pub operation: ProposalOp,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaveProposalsResponse {
    pub state: DaveState,
    #[serde(rename = "commitWelcome", skip_serializing_if = "Option::is_none")]
    pub commit_welcome: Option<BinaryCommitWelcome>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryCommitWelcome {
    pub commit: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub welcome: Option<String>,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Upstream(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
        };

        (
            status,
            Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response()
    }
}

type DynGateway = Arc<dyn DiscordGateway>;
type DynSink = dyn PacketSink;
type DynDave = dyn DaveControl;

type IngestTaskHandle = JoinHandle<Result<(), media_ingest::IngestError>>;

const DEFAULT_READ_CHUNK_SIZE: usize = media_ingest::DEFAULT_CHUNK_SIZE;
const INGEST_FORWARDER_CAPACITY: usize = 512;
const SERVICE_STATUS_OK: &str = "ok";
const SERVICE_STATUS_DEGRADED: &str = "degraded";
#[cfg(feature = "webrtc-media")]
const MAX_STARTUP_AUDIO_US: u64 = 120_000;
#[cfg(feature = "webrtc-media")]
const MAX_LIVE_AUDIO_US: u64 = 400_000;
#[cfg(feature = "webrtc-media")]
const MAX_VIDEO_QUEUE_PACKETS: usize = 64;
#[cfg(feature = "webrtc-media")]
const MEDIA_PLAYOUT_LEAD: Duration = Duration::from_millis(120);
#[cfg(feature = "webrtc-media")]
const MEDIA_RECOVERY_GRACE_PERIOD: Duration = Duration::from_millis(500);
#[cfg(feature = "webrtc-media")]
const MEDIA_RECOVERY_RETRY_INTERVAL: Duration = Duration::from_secs(2);

pub struct DaemonController {
    gateway: DynGateway,
    packet_sink: Arc<DynSink>,
    dave: Arc<DynDave>,
    neko_health: Arc<RwLock<NekoHealthResponse>>,
    last_error: Arc<RwLock<Option<String>>>,
    capture_running: Arc<AtomicBool>,
    ingest_metrics: Arc<IngestMetricState>,
    active_stream: Arc<Mutex<Option<ActiveStream>>>,
    active_voice: Arc<Mutex<Option<ActiveVoiceClient>>>,
    #[cfg(feature = "webrtc-media")]
    active_media: Arc<Mutex<Option<ActiveMediaSession>>>,
    #[cfg(feature = "webrtc-media")]
    publish_metrics: Arc<Mutex<PublishRuntimeState>>,
}

struct ActiveStream {
    source_name: String,
    transport: Option<PacedPacketSender<DynSink>>,
    ingest_forwarder: JoinHandle<()>,
    unix_server: Option<UnixSocketIngestServer>,
    stdin_task: Option<IngestTaskHandle>,
}

struct ActiveVoiceClient {
    client: VoiceGatewayClient<Arc<DynDave>>,
}

#[cfg(feature = "webrtc-media")]
struct ActiveMediaSession {
    session: VoiceMediaSession<Arc<DynDave>>,
    profile: MediaProfile,
}

#[cfg(feature = "webrtc-media")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VideoSinkPolicy {
    quality: Option<u8>,
    target_fps: Option<u32>,
}

#[derive(Default)]
struct IngestMetricState {
    received_chunks: AtomicU64,
    received_bytes: AtomicU64,
}

impl IngestMetricState {
    fn record(&self, chunk: &IngestChunk) {
        self.received_chunks.fetch_add(1, Ordering::Relaxed);
        self.received_bytes
            .fetch_add(chunk.payload.len() as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> IngestMetrics {
        IngestMetrics {
            received_chunks: self.received_chunks.load(Ordering::Relaxed),
            received_bytes: self.received_bytes.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "webrtc-media")]
#[derive(Debug, Default)]
struct PublishRuntimeState {
    snapshot: PublishMetrics,
    last_keyframe_at: Option<Instant>,
    last_audio_send_at: Option<Instant>,
    last_video_send_at: Option<Instant>,
    last_forwarded_video_capture_time_us: Option<u64>,
}

#[cfg(feature = "webrtc-media")]
impl PublishRuntimeState {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn snapshot(&self) -> PublishMetrics {
        let mut snapshot = self.snapshot.clone();
        snapshot.last_keyframe_age_ms = self
            .last_keyframe_at
            .map(|instant| instant.elapsed().as_millis() as u64);
        snapshot
    }
}

#[cfg(feature = "webrtc-media")]
fn record_publish_send_metrics(
    metrics: &mut PublishRuntimeState,
    chunk: &QueuedPublishChunk,
    now: Instant,
) {
    let expected = Duration::from_micros(chunk.duration_us.max(1));
    match chunk.kind {
        IngestMediaKind::Audio => {
            if let Some(previous) = metrics.last_audio_send_at {
                let actual = now.saturating_duration_since(previous);
                metrics.snapshot.audio_send_jitter_ms =
                    Some(actual.abs_diff(expected).as_millis() as u64);
            }
            metrics.last_audio_send_at = Some(now);
        }
        IngestMediaKind::Video => {
            if let Some(previous) = metrics.last_video_send_at {
                let actual = now.saturating_duration_since(previous);
                metrics.snapshot.video_send_jitter_ms =
                    Some(actual.abs_diff(expected).as_millis() as u64);
            }
            metrics.last_video_send_at = Some(now);
        }
    }
    if chunk.is_keyframe {
        metrics.last_keyframe_at = Some(now);
    }
}

impl DaemonController {
    pub fn new(gateway: DynGateway, packet_sink: Arc<DynSink>, dave: Arc<DynDave>) -> Self {
        Self {
            gateway,
            packet_sink,
            dave,
            neko_health: Arc::new(RwLock::new(NekoHealthResponse::default())),
            last_error: Arc::new(RwLock::new(None)),
            capture_running: Arc::new(AtomicBool::new(false)),
            ingest_metrics: Arc::new(IngestMetricState::default()),
            active_stream: Arc::new(Mutex::new(None)),
            active_voice: Arc::new(Mutex::new(None)),
            #[cfg(feature = "webrtc-media")]
            active_media: Arc::new(Mutex::new(None)),
            #[cfg(feature = "webrtc-media")]
            publish_metrics: Arc::new(Mutex::new(PublishRuntimeState::default())),
        }
    }

    pub fn prototype() -> Self {
        Self::new(
            Arc::new(RealDiscordGateway::new()),
            Arc::new(NullPacketSink),
            Arc::new(ManagedDaveSession::new()),
        )
    }

    pub async fn connect_session(
        &self,
        request: SessionConnectRequest,
    ) -> Result<SessionResponse, ApiError> {
        let response = self
            .gateway
            .connect(request.into())
            .await
            .map_err(map_gateway_error)?;
        Ok(SessionResponse {
            discord: response.into(),
        })
    }

    pub async fn session_media(&self) -> Result<SessionMediaResponse, ApiError> {
        self.gateway
            .media_session()
            .await
            .map(|media| SessionMediaResponse { media })
            .map_err(map_gateway_error)
    }

    pub async fn connect_voice(&self) -> Result<VoiceSessionResponse, ApiError> {
        let media = self
            .gateway
            .media_session()
            .await
            .map_err(map_gateway_error)?;
        let mut active = self.active_voice.lock().await;
        if active.is_some() {
            return Err(ApiError::Conflict(
                "voice session is already connected".to_owned(),
            ));
        }

        let client = VoiceGatewayClient::connect(
            VoiceServerInfo {
                endpoint: media.stream_endpoint.clone(),
                token: media.stream_token.clone(),
            },
            media_session_to_voice_config(&media),
            VoiceSessionController::new(Arc::clone(&self.dave)),
        )
        .await
        .map_err(|error| ApiError::Upstream(error.to_string()))?;

        let health = client.health().await;
        *active = Some(ActiveVoiceClient { client });
        Ok(VoiceSessionResponse { voice: health })
    }

    pub async fn voice_health(&self) -> Result<VoiceSessionResponse, ApiError> {
        let active = self.active_voice.lock().await;
        if let Some(active) = active.as_ref() {
            return Ok(VoiceSessionResponse {
                voice: active.client.health().await,
            });
        }
        drop(active);
        #[cfg(feature = "webrtc-media")]
        {
            let active_media = self.active_media.lock().await;
            if let Some(active_media) = active_media.as_ref() {
                return Ok(VoiceSessionResponse {
                    voice: active_media
                        .session
                        .health()
                        .await
                        .map_err(|error| ApiError::Upstream(error.to_string()))?
                        .voice,
                });
            }
        }
        Err(ApiError::BadRequest(
            "voice session is not connected".to_owned(),
        ))
    }

    pub async fn disconnect_voice(&self) -> Result<VoiceSessionResponse, ApiError> {
        let mut active = self.active_voice.lock().await;
        let Some(active) = active.take() else {
            return Err(ApiError::BadRequest(
                "voice session is not connected".to_owned(),
            ));
        };
        let health = active.client.health().await;
        active
            .client
            .close()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        Ok(VoiceSessionResponse { voice: health })
    }

    pub async fn voice_state(&self) -> Result<VoiceStateResponse, ApiError> {
        let active = self.active_voice.lock().await;
        if let Some(active) = active.as_ref() {
            return Ok(VoiceStateResponse {
                state: active.client.session_state().await,
            });
        }
        drop(active);
        #[cfg(feature = "webrtc-media")]
        {
            let active_media = self.active_media.lock().await;
            if let Some(active_media) = active_media.as_ref() {
                return Ok(VoiceStateResponse {
                    state: active_media
                        .session
                        .health()
                        .await
                        .map_err(|error| ApiError::Upstream(error.to_string()))?
                        .session_state,
                });
            }
        }
        Err(ApiError::BadRequest(
            "voice session is not connected".to_owned(),
        ))
    }

    #[cfg(feature = "webrtc-media")]
    pub async fn connect_media(
        &self,
        request: Option<MediaConnectRequest>,
    ) -> Result<MediaSessionResponse, ApiError> {
        let mut active = self.active_media.lock().await;
        if active.is_some() {
            return Err(ApiError::Conflict(
                "media session is already connected".to_owned(),
            ));
        }
        let profile = request.unwrap_or_default().profile;
        self.publish_metrics.lock().await.reset();
        let mut last_error = None;
        let mut active_media = None;

        for attempt in 1..=MEDIA_CONNECT_MAX_ATTEMPTS {
            let result = async {
                self.gateway
                    .start_stream()
                    .await
                    .map_err(|error| map_gateway_error(error).to_string())?;
                establish_media_session(&self.gateway, &self.dave, profile.clone()).await
            }
            .await;

            match result {
                Ok(session) => {
                    active_media = Some(session);
                    break;
                }
                Err(error) => {
                    let _ = self.gateway.stop_stream().await;
                    last_error = Some(error.clone());
                    if attempt < MEDIA_CONNECT_MAX_ATTEMPTS {
                        warn!(
                            attempt,
                            max_attempts = MEDIA_CONNECT_MAX_ATTEMPTS,
                            %error,
                            "media session establishment failed; retrying"
                        );
                        tokio::time::sleep(MEDIA_CONNECT_RETRY_DELAY).await;
                    }
                }
            }
        }

        let Some(active_media) = active_media else {
            return Err(ApiError::Upstream(last_error.unwrap_or_else(|| {
                "media session establishment failed".to_owned()
            })));
        };

        let health = active_media
            .session
            .health()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        let publish = self.publish_metrics.lock().await.snapshot();
        *active = Some(active_media);
        Ok(MediaSessionResponse {
            media: health,
            profile,
            publish,
        })
    }

    #[cfg(feature = "webrtc-media")]
    pub async fn media_health(&self) -> Result<MediaSessionResponse, ApiError> {
        let active = self.active_media.lock().await;
        let Some(active) = active.as_ref() else {
            return Err(ApiError::BadRequest(
                "media session is not connected".to_owned(),
            ));
        };
        Ok(MediaSessionResponse {
            media: active
                .session
                .health()
                .await
                .map_err(|error| ApiError::Upstream(error.to_string()))?,
            profile: active.profile.clone(),
            publish: self.publish_metrics.lock().await.snapshot(),
        })
    }

    #[cfg(feature = "webrtc-media")]
    pub async fn disconnect_media(&self) -> Result<MediaSessionResponse, ApiError> {
        let mut active = self.active_media.lock().await;
        let Some(active) = active.take() else {
            return Err(ApiError::BadRequest(
                "media session is not connected".to_owned(),
            ));
        };
        let health = active
            .session
            .health()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        active
            .session
            .close()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        let publish = self.publish_metrics.lock().await.snapshot();
        Ok(MediaSessionResponse {
            media: health,
            profile: active.profile,
            publish,
        })
    }

    pub async fn start_stream(
        &self,
        request: StreamStartRequest,
    ) -> Result<StreamResponse, ApiError> {
        self.assert_dave_session_initialized()?;

        let mut active = self.active_stream.lock().await;
        if active.is_some() {
            return Err(ApiError::Conflict("stream is already running".to_owned()));
        }

        let read_chunk_size = request.read_chunk_size.unwrap_or(DEFAULT_READ_CHUNK_SIZE);
        let pacing = build_pacer_config(&request);

        #[cfg(feature = "webrtc-media")]
        let media_mode = self.active_media.lock().await.is_some();
        #[cfg(not(feature = "webrtc-media"))]
        let media_mode = false;

        let transport =
            (!media_mode).then(|| PacedPacketSender::spawn(pacing, Arc::clone(&self.packet_sink)));
        let (tx, rx) = mpsc::channel(INGEST_FORWARDER_CAPACITY);
        let (unix_server, stdin_task) =
            build_ingest_source(&request, tx.clone(), read_chunk_size).await?;

        if !media_mode
            && let Err(error) = self.gateway.start_stream().await.map_err(map_gateway_error)
        {
            cleanup_failed_startup(unix_server, stdin_task, transport).await;
            return Err(error);
        }
        self.capture_running.store(true, Ordering::Relaxed);

        let gateway = Arc::clone(&self.gateway);
        let capture_running = Arc::clone(&self.capture_running);
        let last_error = Arc::clone(&self.last_error);
        let ingest_metrics = Arc::clone(&self.ingest_metrics);
        let dave = Arc::clone(&self.dave);
        let source_name = request.source_name;
        let transport_forwarder = transport.clone();
        #[cfg(feature = "webrtc-media")]
        let active_media = Arc::clone(&self.active_media);
        #[cfg(feature = "webrtc-media")]
        let publish_metrics = Arc::clone(&self.publish_metrics);
        let ingest_forwarder = tokio::spawn(async move {
            if let Err(error) = forward_ingest(
                rx,
                transport_forwarder,
                #[cfg(feature = "webrtc-media")]
                active_media,
                #[cfg(feature = "webrtc-media")]
                publish_metrics,
                ingest_metrics,
                dave,
                gateway,
                capture_running,
            )
            .await
            {
                *last_error.write().await = Some(error);
            }
        });

        let response_metrics = transport
            .as_ref()
            .map(PacedPacketSender::metrics)
            .unwrap_or_default();

        *active = Some(ActiveStream {
            source_name: source_name.clone(),
            transport,
            ingest_forwarder,
            unix_server,
            stdin_task,
        });

        info!(source = %source_name, "stream started");
        Ok(StreamResponse {
            source_name,
            capture_running: true,
            transport: response_metrics,
        })
    }

    fn assert_dave_session_initialized(&self) -> Result<(), ApiError> {
        let dave_state = self
            .dave
            .state()
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        if !dave_state.initialized {
            return Err(ApiError::BadRequest(
                "DAVE session must be initialized before starting the stream".to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn stop_stream(&self) -> Result<StreamResponse, ApiError> {
        let mut active = self.active_stream.lock().await;
        let Some(mut stream) = active.take() else {
            return Err(ApiError::Conflict("stream is not running".to_owned()));
        };

        stream.ingest_forwarder.abort();
        let _ = stream.ingest_forwarder.await;
        if let Some(server) = stream.unix_server.take() {
            server.shutdown().await;
        }
        if let Some(task) = stream.stdin_task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(transport) = stream.transport.take() {
            let _ = transport.close().await;
        }
        let _ = self.gateway.stop_stream().await;
        self.capture_running.store(false, Ordering::Relaxed);
        #[cfg(feature = "webrtc-media")]
        self.publish_metrics.lock().await.reset();
        let transport = TransportMetrics::default();
        drop(active);

        Ok(StreamResponse {
            source_name: stream.source_name,
            capture_running: false,
            transport,
        })
    }

    pub async fn health(&self) -> HealthResponse {
        let capture_running = self.capture_running.load(Ordering::Relaxed);
        let mut discord = DiscordHealthResponse::from(self.gateway.health().await);
        let last_error = self.last_error.read().await.clone();
        discord.last_error = last_error.or(discord.last_error);
        HealthResponse {
            status: service_status(capture_running).to_owned(),
            neko: self.neko_health.read().await.clone(),
            discord,
            capture_running,
        }
    }

    pub fn initialize_dave(&self, request: DaveInitRequest) -> Result<DaveState, ApiError> {
        self.dave
            .initialize(DaveInitConfig {
                protocol_version: request.protocol_version,
                user_id: request.user_id,
                channel_id: request.channel_id,
                signing_private_key: request.signing_private_key.map(decode_binary).transpose()?,
                signing_public_key: request.signing_public_key.map(decode_binary).transpose()?,
            })
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }

    pub fn dave_external_sender(&self, request: BinaryBodyRequest) -> Result<DaveState, ApiError> {
        let data = decode_binary(request.data)?;
        self.dave
            .set_external_sender(&data)
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }

    pub fn dave_key_package(&self) -> Result<BinaryBodyResponse, ApiError> {
        self.dave
            .create_key_package()
            .map(|data| BinaryBodyResponse {
                data: STANDARD.encode(data),
            })
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }

    pub fn dave_proposals(
        &self,
        request: DaveProposalsRequest,
    ) -> Result<DaveProposalsResponse, ApiError> {
        let proposals = decode_binary(request.data)?;
        self.dave
            .process_proposals_bundle(request.operation, &proposals, None)
            .map(DaveProposalsResponse::from)
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }

    pub fn dave_welcome(&self, request: BinaryBodyRequest) -> Result<DaveState, ApiError> {
        let welcome = decode_binary(request.data)?;
        self.dave
            .process_welcome(&welcome)
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }

    pub fn dave_commit(&self, request: BinaryBodyRequest) -> Result<DaveState, ApiError> {
        let commit = decode_binary(request.data)?;
        self.dave
            .process_commit(&commit)
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }

    pub fn dave_state(&self) -> Result<DaveState, ApiError> {
        self.dave
            .state()
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }

    pub async fn metrics(&self) -> MetricsResponse {
        let transport = {
            let active = self.active_stream.lock().await;
            active
                .as_ref()
                .and_then(|stream| stream.transport.as_ref().map(PacedPacketSender::metrics))
                .unwrap_or_default()
        };
        #[cfg(feature = "webrtc-media")]
        let publisher = {
            let active_media = self.active_media.lock().await;
            if let Some(active) = active_media.as_ref() {
                active
                    .session
                    .health()
                    .await
                    .ok()
                    .map(|health| health.publisher)
            } else {
                None
            }
        };
        let discord = self.gateway.health().await;
        MetricsResponse {
            transport,
            ingest: self.ingest_metrics.snapshot(),
            discord_state: discord.state,
            #[cfg(feature = "webrtc-media")]
            publisher,
            #[cfg(feature = "webrtc-media")]
            publish: self.publish_metrics.lock().await.snapshot(),
        }
    }
}

impl From<DaveCommitWelcome> for BinaryCommitWelcome {
    fn from(value: DaveCommitWelcome) -> Self {
        Self {
            commit: STANDARD.encode(value.commit),
            welcome: value.welcome.map(|welcome| STANDARD.encode(welcome)),
        }
    }
}

impl From<DaveProposalResult> for DaveProposalsResponse {
    fn from(value: DaveProposalResult) -> Self {
        Self {
            state: value.state,
            commit_welcome: value.commit_welcome.map(BinaryCommitWelcome::from),
        }
    }
}

#[cfg(feature = "webrtc-media")]
#[derive(Debug, Clone)]
struct QueuedPublishChunk {
    kind: IngestMediaKind,
    payload: PayloadBytes,
    capture_time_us: u64,
    duration_us: u64,
    is_keyframe: bool,
    is_decoder_config: bool,
}

#[cfg(feature = "webrtc-media")]
struct MediaPublishScheduler {
    metrics: Arc<Mutex<PublishRuntimeState>>,
    configured_video_fps: u32,
    playout_epoch: Option<Instant>,
    startup_mode: bool,
    startup_since: Option<Instant>,
    last_recovery_attempt_at: Option<Instant>,
    startup_video_config: Option<QueuedPublishChunk>,
    startup_audio: VecDeque<QueuedPublishChunk>,
    startup_video: VecDeque<QueuedPublishChunk>,
    audio_queue: VecDeque<QueuedPublishChunk>,
    video_queue: VecDeque<QueuedPublishChunk>,
    awaiting_resumed_video_keyframe: bool,
}

#[cfg(feature = "webrtc-media")]
impl MediaPublishScheduler {
    fn new(metrics: Arc<Mutex<PublishRuntimeState>>, configured_video_fps: u32) -> Self {
        Self {
            metrics,
            configured_video_fps: configured_video_fps.max(1),
            playout_epoch: None,
            startup_mode: true,
            startup_since: None,
            last_recovery_attempt_at: None,
            startup_video_config: None,
            startup_audio: VecDeque::new(),
            startup_video: VecDeque::new(),
            audio_queue: VecDeque::new(),
            video_queue: VecDeque::new(),
            awaiting_resumed_video_keyframe: false,
        }
    }

    async fn enqueue(&mut self, chunk: QueuedPublishChunk) {
        if self.playout_epoch.is_none() {
            self.playout_epoch = Some(
                Instant::now()
                    .checked_sub(Duration::from_micros(chunk.capture_time_us))
                    .and_then(|instant| instant.checked_add(MEDIA_PLAYOUT_LEAD))
                    .unwrap_or_else(Instant::now),
            );
        }

        if self.startup_mode {
            self.startup_since.get_or_insert_with(Instant::now);
            self.enqueue_startup(chunk).await;
        } else {
            self.enqueue_live(chunk).await;
        }
    }

    async fn flush_ready(
        &mut self,
        gateway: &DynGateway,
        dave: &Arc<DynDave>,
        active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
    ) -> Result<(), String> {
        let _ = self
            .transition_from_startup_if_ready(gateway, dave, active_media, false)
            .await?;

        while !self.startup_mode {
            let Some(due_at) = self.next_due_instant() else {
                break;
            };
            if due_at > Instant::now() {
                break;
            }
            let Some(chunk) = self.pop_next_chunk().await else {
                break;
            };
            if !self.apply_sink_policy(active_media, &chunk).await.map_err(|error| error.to_string())? {
                continue;
            }
            if let Err(error) = self.send_chunk(active_media, chunk.clone()).await {
                match error {
                    discord_voice::session::VoiceMediaSessionError::Publisher(
                        discord_voice::publisher::VoicePublisherError::TrackNotReady(_),
                    ) => {
                        self.record_not_ready_drop(chunk.kind).await;
                        self.enter_startup_mode();
                        self.enqueue_startup(chunk).await;
                    }
                    error => return Err(error.to_string()),
                }
            }
        }

        Ok(())
    }

    async fn drain_remaining(
        &mut self,
        gateway: &DynGateway,
        dave: &Arc<DynDave>,
        active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
    ) -> Result<(), String> {
        while self.has_pending() {
            if self.startup_mode {
                let became_live = self
                    .transition_from_startup_if_ready(gateway, dave, active_media, true)
                    .await?;
                if !became_live {
                    self.drop_startup_buffers().await;
                    break;
                }
                continue;
            }

            if let Some(due_at) = self.next_due_instant()
                && due_at > Instant::now()
            {
                tokio::time::sleep_until(tokio::time::Instant::from_std(due_at)).await;
            }
            self.flush_ready(gateway, dave, active_media).await?;
        }
        Ok(())
    }

    fn enter_startup_mode(&mut self) {
        self.startup_mode = true;
        self.startup_since.get_or_insert_with(Instant::now);
    }

    fn exit_startup_mode(&mut self) {
        self.startup_mode = false;
        self.startup_since = None;
        self.last_recovery_attempt_at = None;
    }

    async fn recover_media_if_stalled(
        &mut self,
        gateway: &DynGateway,
        dave: &Arc<DynDave>,
        active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
        force: bool,
    ) -> Result<(), String> {
        if !self.should_attempt_recovery(force) {
            return Ok(());
        }
        self.last_recovery_attempt_at = Some(Instant::now());
        reconnect_media_session(gateway, dave, active_media).await
    }

    fn should_attempt_recovery(&self, force: bool) -> bool {
        if !self.has_pending() {
            return false;
        }
        if let Some(last_attempt) = self.last_recovery_attempt_at
            && last_attempt.elapsed() < MEDIA_RECOVERY_RETRY_INTERVAL
        {
            return false;
        }
        if force {
            return true;
        }
        self.startup_since
            .is_some_and(|since| since.elapsed() >= MEDIA_RECOVERY_GRACE_PERIOD)
    }

    async fn transition_from_startup_if_ready(
        &mut self,
        gateway: &DynGateway,
        dave: &Arc<DynDave>,
        active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
        force_recovery: bool,
    ) -> Result<bool, String> {
        if !self.startup_mode {
            return Ok(false);
        }

        if media_session_is_ready(active_media).await? {
            self.exit_startup_mode();
            self.flush_startup_to_live().await;
            return Ok(true);
        }

        self.recover_media_if_stalled(gateway, dave, active_media, force_recovery)
            .await?;
        if media_session_is_ready(active_media).await? {
            self.exit_startup_mode();
            self.flush_startup_to_live().await;
            return Ok(true);
        }

        Ok(false)
    }

    fn has_pending(&self) -> bool {
        self.startup_video_config.is_some()
            || !self.startup_audio.is_empty()
            || !self.startup_video.is_empty()
            || !self.audio_queue.is_empty()
            || !self.video_queue.is_empty()
    }

    async fn enqueue_startup(&mut self, chunk: QueuedPublishChunk) {
        match chunk.kind {
            IngestMediaKind::Audio => {
                self.startup_audio.push_back(chunk);
                trim_audio_queue(
                    &mut self.startup_audio,
                    MAX_STARTUP_AUDIO_US,
                    &self.metrics,
                )
                .await;
            }
            IngestMediaKind::Video => {
                let has_video_context = chunk.is_decoder_config || !self.startup_video.is_empty();
                if chunk.is_decoder_config {
                    self.startup_video_config = Some(chunk.clone());
                }
                if chunk.is_keyframe {
                    self.startup_video.clear();
                    self.startup_video.push_back(chunk);
                } else if has_video_context {
                    self.startup_video.push_back(chunk);
                } else {
                    self.record_not_ready_drop(IngestMediaKind::Video).await;
                    self.update_snapshot().await;
                    return;
                }
                trim_video_queue(&mut self.startup_video, &self.metrics).await;
            }
        }
        self.update_snapshot().await;
    }

    async fn enqueue_live(&mut self, chunk: QueuedPublishChunk) {
        match chunk.kind {
            IngestMediaKind::Audio => {
                self.audio_queue.push_back(chunk);
                trim_audio_queue(&mut self.audio_queue, MAX_LIVE_AUDIO_US, &self.metrics).await;
            }
            IngestMediaKind::Video => {
                self.video_queue.push_back(chunk);
                trim_video_queue(&mut self.video_queue, &self.metrics).await;
            }
        }
        self.update_snapshot().await;
    }

    async fn flush_startup_to_live(&mut self) {
        if let Some(config) = self.startup_video_config.take() {
            self.video_queue.push_front(config);
        }
        self.video_queue.extend(self.startup_video.drain(..));
        self.audio_queue.extend(self.startup_audio.drain(..));
        self.update_snapshot().await;
    }

    async fn drop_startup_buffers(&mut self) {
        let mut metrics = self.metrics.lock().await;
        metrics.snapshot.dropped_not_ready_audio = metrics
            .snapshot
            .dropped_not_ready_audio
            .saturating_add(self.startup_audio.len() as u64);
        metrics.snapshot.dropped_not_ready_video = metrics
            .snapshot
            .dropped_not_ready_video
            .saturating_add(
                self.startup_video.len() as u64 + u64::from(self.startup_video_config.is_some()),
            );
        drop(metrics);
        self.startup_video_config = None;
        self.startup_audio.clear();
        self.startup_video.clear();
        self.update_snapshot().await;
    }

    fn next_due_instant(&self) -> Option<Instant> {
        let epoch = self.playout_epoch?;
        let audio_due = self
            .audio_queue
            .front()
            .and_then(|chunk| epoch.checked_add(Duration::from_micros(chunk.capture_time_us)));
        let video_due = self
            .video_queue
            .front()
            .and_then(|chunk| epoch.checked_add(Duration::from_micros(chunk.capture_time_us)));

        match (audio_due, video_due) {
            (Some(audio_due), Some(video_due)) => Some(audio_due.min(video_due)),
            (Some(next_due), None) | (None, Some(next_due)) => Some(next_due),
            (None, None) => None,
        }
    }

    async fn pop_next_chunk(&mut self) -> Option<QueuedPublishChunk> {
        let chunk = match (self.audio_queue.front(), self.video_queue.front()) {
            (Some(audio), Some(video)) if audio.capture_time_us <= video.capture_time_us => {
                self.audio_queue.pop_front()
            }
            (Some(_), Some(_)) => self.video_queue.pop_front(),
            (Some(_), None) => self.audio_queue.pop_front(),
            (None, Some(_)) => self.video_queue.pop_front(),
            (None, None) => None,
        };
        self.update_snapshot().await;
        chunk
    }

    async fn send_chunk(
        &mut self,
        active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
        chunk: QueuedPublishChunk,
    ) -> Result<(), discord_voice::session::VoiceMediaSessionError> {
        let mut active = active_media.lock().await;
        let Some(active) = active.as_mut() else {
            return Ok(());
        };
        active.session.send_media(
            to_voice_media_kind(chunk.kind),
            &chunk.payload,
            chunk.capture_time_us,
            chunk.duration_us,
        )?;

        let now = Instant::now();
        let mut metrics = self.metrics.lock().await;
        record_publish_send_metrics(&mut metrics, &chunk, now);
        drop(metrics);
        self.update_snapshot().await;
        Ok(())
    }

    async fn apply_sink_policy(
        &mut self,
        active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
        chunk: &QueuedPublishChunk,
    ) -> Result<bool, discord_voice::session::VoiceMediaSessionError> {
        if chunk.kind != IngestMediaKind::Video {
            return Ok(true);
        }

        let policy = current_video_sink_policy(active_media, self.configured_video_fps).await?;
        let should_video_be_active = !matches!(policy.target_fps, Some(0));
        let video_resumed = {
            let mut active = active_media.lock().await;
            if let Some(active) = active.as_mut() {
                active
                    .session
                    .set_video_active(should_video_be_active)
                    .await?
            } else {
                false
            }
        };
        {
            let mut metrics = self.metrics.lock().await;
            metrics.snapshot.sink_video_quality = policy.quality;
            metrics.snapshot.sink_video_target_fps = policy.target_fps;
            metrics.snapshot.sink_video_paused = !should_video_be_active;
        }
        if video_resumed {
            self.awaiting_resumed_video_keyframe = true;
        }

        let Some(target_fps) = policy.target_fps else {
            if self.awaiting_resumed_video_keyframe {
                if !chunk.is_keyframe {
                    self.record_sink_video_drop().await;
                    return Ok(false);
                }
                self.awaiting_resumed_video_keyframe = false;
            }
            return Ok(true);
        };
        if target_fps == 0 {
            self.awaiting_resumed_video_keyframe = true;
            self.record_sink_video_drop().await;
            return Ok(false);
        }
        if self.awaiting_resumed_video_keyframe {
            if !chunk.is_keyframe {
                self.record_sink_video_drop().await;
                return Ok(false);
            }
            self.awaiting_resumed_video_keyframe = false;
        }

        if chunk.is_keyframe {
            let mut metrics = self.metrics.lock().await;
            metrics.last_forwarded_video_capture_time_us = Some(chunk.capture_time_us);
            return Ok(true);
        }

        let min_interval_us = 1_000_000_u64 / u64::from(target_fps.max(1));
        let mut metrics = self.metrics.lock().await;
        if let Some(previous_capture_time_us) = metrics.last_forwarded_video_capture_time_us
            && chunk.capture_time_us.saturating_sub(previous_capture_time_us) < min_interval_us
        {
            metrics.snapshot.dropped_sink_video =
                metrics.snapshot.dropped_sink_video.saturating_add(1);
            return Ok(false);
        }
        metrics.last_forwarded_video_capture_time_us = Some(chunk.capture_time_us);
        Ok(true)
    }

    async fn update_snapshot(&self) {
        let mut metrics = self.metrics.lock().await;
        metrics.snapshot.audio_queue_depth = self.audio_queue.len();
        metrics.snapshot.video_queue_depth = self.video_queue.len();
        metrics.snapshot.startup_audio_depth = self.startup_audio.len();
        metrics.snapshot.startup_video_depth = self.startup_video.len();
        metrics.snapshot.startup_has_video_config = self.startup_video_config.is_some();
    }

    async fn record_not_ready_drop(&self, kind: IngestMediaKind) {
        let mut metrics = self.metrics.lock().await;
        match kind {
            IngestMediaKind::Audio => {
                metrics.snapshot.dropped_not_ready_audio =
                    metrics.snapshot.dropped_not_ready_audio.saturating_add(1);
            }
            IngestMediaKind::Video => {
                metrics.snapshot.dropped_not_ready_video =
                    metrics.snapshot.dropped_not_ready_video.saturating_add(1);
            }
        }
    }

    async fn record_sink_video_drop(&self) {
        let mut metrics = self.metrics.lock().await;
        metrics.snapshot.dropped_sink_video =
            metrics.snapshot.dropped_sink_video.saturating_add(1);
    }
}

async fn forward_ingest(
    mut receiver: mpsc::Receiver<IngestChunk>,
    transport: Option<PacedPacketSender<DynSink>>,
    #[cfg(feature = "webrtc-media")] active_media: Arc<Mutex<Option<ActiveMediaSession>>>,
    #[cfg(feature = "webrtc-media")] publish_metrics: Arc<Mutex<PublishRuntimeState>>,
    ingest_metrics: Arc<IngestMetricState>,
    dave: Arc<DynDave>,
    gateway: DynGateway,
    capture_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let mut sequence = 0_u64;
    #[cfg(feature = "webrtc-media")]
    let mut scheduler = MediaPublishScheduler::new(publish_metrics, 15);
    let result = async {
        #[cfg(feature = "webrtc-media")]
        enum IngestEvent {
            Received(Option<IngestChunk>),
            Tick,
        }

        loop {
            #[cfg(feature = "webrtc-media")]
            {
                scheduler.flush_ready(&gateway, &dave, &active_media).await?;
                let event = if let Some(due_at) = scheduler.next_due_instant() {
                    tokio::select! {
                        chunk = receiver.recv() => IngestEvent::Received(chunk),
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(due_at)) => IngestEvent::Tick,
                    }
                } else {
                    IngestEvent::Received(receiver.recv().await)
                };

                match event {
                    IngestEvent::Tick => continue,
                    IngestEvent::Received(Some(chunk)) => {
                        sequence += 1;
                        ingest_metrics.record(&chunk);
                        let protected = protect_chunk_for_media(&dave, sequence, chunk)
                            .map_err(|error| error.to_string())?;

                        if active_media.lock().await.is_some() {
                            scheduler.enqueue(protected).await;
                            scheduler.flush_ready(&gateway, &dave, &active_media).await?;
                            continue;
                        }

                        if let Some(transport) = transport.as_ref() {
                            let packet = publish_chunk_to_packet(sequence, &protected);
                            let _ = transport.try_enqueue(packet);
                        }
                    }
                    IngestEvent::Received(None) => break,
                }
            }

            #[cfg(not(feature = "webrtc-media"))]
            {
                let Some(chunk) = receiver.recv().await else {
                    break;
                };
                sequence += 1;
                ingest_metrics.record(&chunk);
                let protected = protect_chunk_without_media(&dave, sequence, chunk)
                    .map_err(|error: DaveError| error.to_string())?;
                if let Some(transport) = transport.as_ref() {
                    let packet = publish_chunk_to_packet(sequence, &protected);
                    let _ = transport.try_enqueue(packet);
                }
            }
        }
        #[cfg(feature = "webrtc-media")]
        scheduler
            .drain_remaining(&gateway, &dave, &active_media)
            .await?;
        Ok(())
    }
    .await;
    capture_running.store(false, Ordering::Relaxed);
    let _ = gateway.stop_stream().await;
    if let Some(transport) = transport {
        transport.close().await.map_err(|error| error.to_string())?;
    }
    result
}

#[cfg(feature = "webrtc-media")]
fn publish_chunk_to_packet(sequence: u64, chunk: &QueuedPublishChunk) -> Packet {
    Packet {
        kind: to_transport_media_kind(chunk.kind),
        sequence,
        timestamp_ms: chunk.capture_time_us / 1_000,
        payload: chunk.payload.clone(),
    }
}

#[cfg(not(feature = "webrtc-media"))]
fn publish_chunk_to_packet(sequence: u64, chunk: &IngestChunk) -> Packet {
    Packet {
        kind: to_transport_media_kind(chunk.kind),
        sequence,
        timestamp_ms: chunk.timing.capture_time_us / 1_000,
        payload: chunk.payload.clone(),
    }
}

#[cfg(feature = "webrtc-media")]
fn protect_chunk_for_media(
    dave: &Arc<DynDave>,
    sequence: u64,
    chunk: IngestChunk,
) -> Result<QueuedPublishChunk, DaveError> {
    let is_decoder_config =
        chunk.kind == IngestMediaKind::Video && payload_has_decoder_config(&chunk.payload);
    let kind = to_dave_media_kind(chunk.kind);
    let dave_state = dave.state()?;
    let payload = if dave_state.ready {
        dave.protect(
            kind,
            &DaveMetadata {
                sequence,
                timestamp_ms: chunk.timing.capture_time_us / 1_000,
            },
            chunk.payload,
        )?
    } else {
        chunk.payload
    };

    Ok(QueuedPublishChunk {
        kind: chunk.kind,
        payload,
        capture_time_us: chunk.timing.capture_time_us,
        duration_us: chunk.timing.duration_us,
        is_keyframe: chunk.timing.is_keyframe,
        is_decoder_config,
    })
}

#[cfg(not(feature = "webrtc-media"))]
fn protect_chunk_without_media(
    dave: &Arc<DynDave>,
    sequence: u64,
    chunk: IngestChunk,
) -> Result<IngestChunk, DaveError> {
    let kind = to_dave_media_kind(chunk.kind);
    let dave_state = dave.state()?;
    let payload = if dave_state.ready {
        dave.protect(
            kind,
            &DaveMetadata {
                sequence,
                timestamp_ms: chunk.timing.capture_time_us / 1_000,
            },
            chunk.payload,
        )?
    } else {
        chunk.payload
    };

    Ok(IngestChunk {
        kind: chunk.kind,
        payload,
        timing: chunk.timing,
    })
}

#[cfg(feature = "webrtc-media")]
async fn media_session_is_ready(
    active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
) -> Result<bool, String> {
    let active = active_media.lock().await;
    let Some(active) = active.as_ref() else {
        return Ok(false);
    };
    active.session.is_ready().map_err(|error| error.to_string())
}

#[cfg(feature = "webrtc-media")]
async fn current_video_sink_policy(
    active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
    default_video_fps: u32,
) -> Result<VideoSinkPolicy, discord_voice::session::VoiceMediaSessionError> {
    let active = active_media.lock().await;
    let Some(active) = active.as_ref() else {
        return Ok(VideoSinkPolicy {
            quality: None,
            target_fps: None,
        });
    };

    let configured_video_fps = active.profile.max_framerate.max(default_video_fps).max(1);
    let state = active.session.session_state().await;
    let quality = state
        .media_sink_wants
        .as_ref()
        .and_then(|payload| payload.data.get("any"))
        .and_then(serde_json::Value::as_u64)
        .map(|value| value.min(100) as u8);
    let target_fps = quality.and_then(|quality| sink_target_fps(configured_video_fps, quality));

    Ok(VideoSinkPolicy {
        quality,
        target_fps,
    })
}

#[cfg(feature = "webrtc-media")]
fn sink_target_fps(configured_video_fps: u32, quality: u8) -> Option<u32> {
    match quality {
        0 => Some(0),
        100 => None,
        _ => Some(((configured_video_fps.max(1) * u32::from(quality)).div_ceil(100)).max(1)),
    }
}

#[cfg(feature = "webrtc-media")]
async fn reconnect_media_session(
    gateway: &DynGateway,
    dave: &Arc<DynDave>,
    active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
) -> Result<(), String> {
    let previous = {
        let mut active = active_media.lock().await;
        active.take()
    };
    let Some(previous) = previous else {
        return Ok(());
    };
    let profile = previous.profile.clone();
    let _ = previous.session.close().await;
    let _ = gateway.stop_stream().await;
    gateway
        .start_stream()
        .await
        .map_err(|error| error.to_string())?;
    let rebuilt = establish_media_session(gateway, dave, profile).await?;
    let mut active = active_media.lock().await;
    *active = Some(rebuilt);
    Ok(())
}

#[cfg(feature = "webrtc-media")]
async fn establish_media_session(
    gateway: &DynGateway,
    dave: &Arc<DynDave>,
    profile: MediaProfile,
) -> Result<ActiveMediaSession, String> {
    let media = gateway.media_session().await.map_err(|error| error.to_string())?;
    info!(
        user_id = %media.user_id,
        guild_id = media.guild_id.as_deref(),
        channel_id = %media.channel_id,
        rtc_channel_id = %media.rtc_channel_id,
        stream_server_id = %media.stream_server_id,
        voice_session_id = %media.voice_session_id,
        stream_endpoint = %media.stream_endpoint,
        voice_endpoint = %media.voice_endpoint,
        stream_token_len = media.stream_token.len(),
        voice_token_len = media.voice_token.len(),
        "establishing discord media session"
    );
    let voice_client = VoiceGatewayClient::connect(
        VoiceServerInfo {
            endpoint: media.stream_endpoint.clone(),
            token: media.stream_token.clone(),
        },
        media_session_to_voice_config(&media),
        VoiceSessionController::new(Arc::clone(dave)),
    )
    .await
    .map_err(|error| error.to_string())?;
    let session = VoiceMediaSession::negotiate(
        voice_client,
        VoicePublisherConfig {
            audio_frame_duration_ms: profile.audio_frame_duration_ms.max(1),
            video_framerate: profile.max_framerate.max(1),
            ..VoicePublisherConfig::default()
        },
        Duration::from_secs(10),
        profile_to_video_config(&profile),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(ActiveMediaSession { session, profile })
}

#[cfg(feature = "webrtc-media")]
async fn trim_audio_queue(
    queue: &mut VecDeque<QueuedPublishChunk>,
    max_duration_us: u64,
    metrics: &Arc<Mutex<PublishRuntimeState>>,
) {
    let mut dropped = 0_u64;
    while total_duration_us(queue) > max_duration_us {
        if queue.pop_front().is_some() {
            dropped = dropped.saturating_add(1);
        }
    }
    if dropped == 0 {
        return;
    }

    let mut metrics = metrics.lock().await;
    metrics.snapshot.dropped_overload_audio = metrics
        .snapshot
        .dropped_overload_audio
        .saturating_add(dropped);
}

#[cfg(feature = "webrtc-media")]
async fn trim_video_queue(
    queue: &mut VecDeque<QueuedPublishChunk>,
    metrics: &Arc<Mutex<PublishRuntimeState>>,
) {
    let mut dropped = 0_u64;
    while queue.len() > MAX_VIDEO_QUEUE_PACKETS {
        let drop_index = queue.iter().position(|chunk| !chunk.is_keyframe).unwrap_or(0);
        queue.remove(drop_index);
        dropped = dropped.saturating_add(1);
    }
    if dropped == 0 {
        return;
    }
    let mut metrics = metrics.lock().await;
    metrics.snapshot.dropped_overload_video = metrics
        .snapshot
        .dropped_overload_video
        .saturating_add(dropped);
}

#[cfg(feature = "webrtc-media")]
fn total_duration_us(queue: &VecDeque<QueuedPublishChunk>) -> u64 {
    queue.iter().map(|chunk| chunk.duration_us.max(1)).sum()
}

#[cfg(feature = "webrtc-media")]
fn payload_has_decoder_config(payload: &PayloadBytes) -> bool {
    find_annex_b_nal_types(payload).any(|nal_type| matches!(nal_type, 7 | 8))
}

#[cfg(feature = "webrtc-media")]
fn find_annex_b_nal_types(payload: &[u8]) -> impl Iterator<Item = u8> + '_ {
    let mut index = 0_usize;
    std::iter::from_fn(move || {
        while index + 4 <= payload.len() {
            let (start_code_len, nal_header_index) = if payload[index..].starts_with(&[0, 0, 1]) {
                (3, index + 3)
            } else if payload[index..].starts_with(&[0, 0, 0, 1]) {
                (4, index + 4)
            } else {
                index += 1;
                continue;
            };
            index = nal_header_index.saturating_add(1);
            if nal_header_index < payload.len() {
                return Some(payload[nal_header_index] & 0x1f);
            }
            index = index.saturating_add(start_code_len);
        }
        None
    })
}

async fn cleanup_failed_startup(
    unix_server: Option<UnixSocketIngestServer>,
    stdin_task: Option<IngestTaskHandle>,
    transport: Option<PacedPacketSender<DynSink>>,
) {
    if let Some(server) = unix_server {
        server.shutdown().await;
    }
    if let Some(task) = stdin_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(transport) = transport {
        let _ = transport.close().await;
    }
}

pub fn build_router(controller: Arc<DaemonController>) -> Router {
    Router::new()
        .route("/v1/session/connect", post(connect_session))
        .route("/v1/session/media", get(session_media))
        .route("/v1/voice/connect", post(voice_connect))
        .route("/v1/voice/health", get(voice_health))
        .route("/v1/voice/state", get(voice_state))
        .route("/v1/voice/disconnect", post(voice_disconnect))
        .route("/v1/media/connect", post(media_connect))
        .route("/v1/media/health", get(media_health))
        .route("/v1/media/disconnect", post(media_disconnect))
        .route("/v1/stream/start", post(start_stream))
        .route("/v1/stream/stop", post(stop_stream))
        .route("/v1/dave/init", post(dave_init))
        .route("/v1/dave/external-sender", post(dave_external_sender))
        .route("/v1/dave/key-package", post(dave_key_package))
        .route("/v1/dave/proposals", post(dave_proposals))
        .route("/v1/dave/welcome", post(dave_welcome))
        .route("/v1/dave/commit", post(dave_commit))
        .route("/v1/dave/state", get(dave_state))
        .route("/v1/health", get(health))
        .route("/v1/metrics", get(metrics))
        .route("/healthz", get(healthz))
        .with_state(controller)
}

async fn connect_session(
    State(controller): State<Arc<DaemonController>>,
    Json(request): Json<SessionConnectRequest>,
) -> Result<Json<SessionResponse>, ApiError> {
    controller.connect_session(request).await.map(Json)
}

async fn session_media(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<SessionMediaResponse>, ApiError> {
    controller.session_media().await.map(Json)
}

async fn voice_connect(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<VoiceSessionResponse>, ApiError> {
    controller.connect_voice().await.map(Json)
}

async fn voice_health(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<VoiceSessionResponse>, ApiError> {
    controller.voice_health().await.map(Json)
}

async fn voice_state(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<VoiceStateResponse>, ApiError> {
    controller.voice_state().await.map(Json)
}

async fn voice_disconnect(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<VoiceSessionResponse>, ApiError> {
    controller.disconnect_voice().await.map(Json)
}

#[cfg(feature = "webrtc-media")]
async fn media_connect(
    State(controller): State<Arc<DaemonController>>,
    body: Bytes,
) -> Result<Json<MediaSessionResponse>, ApiError> {
    let request = parse_optional_media_connect_request(&body)?;
    controller.connect_media(request).await.map(Json)
}

#[cfg(not(feature = "webrtc-media"))]
async fn media_connect() -> Result<Json<serde_json::Value>, ApiError> {
    Err(ApiError::BadRequest(
        "daemon was built without webrtc-media support".to_owned(),
    ))
}

#[cfg(feature = "webrtc-media")]
async fn media_health(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<MediaSessionResponse>, ApiError> {
    controller.media_health().await.map(Json)
}

#[cfg(not(feature = "webrtc-media"))]
async fn media_health() -> Result<Json<serde_json::Value>, ApiError> {
    Err(ApiError::BadRequest(
        "daemon was built without webrtc-media support".to_owned(),
    ))
}

#[cfg(feature = "webrtc-media")]
async fn media_disconnect(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<MediaSessionResponse>, ApiError> {
    controller.disconnect_media().await.map(Json)
}

#[cfg(not(feature = "webrtc-media"))]
async fn media_disconnect() -> Result<Json<serde_json::Value>, ApiError> {
    Err(ApiError::BadRequest(
        "daemon was built without webrtc-media support".to_owned(),
    ))
}

async fn start_stream(
    State(controller): State<Arc<DaemonController>>,
    Json(request): Json<StreamStartRequest>,
) -> Result<Json<StreamResponse>, ApiError> {
    controller.start_stream(request).await.map(Json)
}

async fn stop_stream(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<StreamResponse>, ApiError> {
    controller.stop_stream().await.map(Json)
}

async fn health(State(controller): State<Arc<DaemonController>>) -> Json<HealthResponse> {
    Json(controller.health().await)
}

async fn healthz(State(controller): State<Arc<DaemonController>>) -> impl IntoResponse {
    let health = controller.health().await;
    let status = if health.capture_running {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(health))
}

async fn metrics(State(controller): State<Arc<DaemonController>>) -> Json<MetricsResponse> {
    Json(controller.metrics().await)
}

async fn dave_init(
    State(controller): State<Arc<DaemonController>>,
    Json(request): Json<DaveInitRequest>,
) -> Result<Json<DaveState>, ApiError> {
    controller.initialize_dave(request).map(Json)
}

async fn dave_external_sender(
    State(controller): State<Arc<DaemonController>>,
    Json(request): Json<BinaryBodyRequest>,
) -> Result<Json<DaveState>, ApiError> {
    controller.dave_external_sender(request).map(Json)
}

async fn dave_key_package(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<BinaryBodyResponse>, ApiError> {
    controller.dave_key_package().map(Json)
}

async fn dave_proposals(
    State(controller): State<Arc<DaemonController>>,
    Json(request): Json<DaveProposalsRequest>,
) -> Result<Json<DaveProposalsResponse>, ApiError> {
    controller.dave_proposals(request).map(Json)
}

async fn dave_welcome(
    State(controller): State<Arc<DaemonController>>,
    Json(request): Json<BinaryBodyRequest>,
) -> Result<Json<DaveState>, ApiError> {
    controller.dave_welcome(request).map(Json)
}

async fn dave_commit(
    State(controller): State<Arc<DaemonController>>,
    Json(request): Json<BinaryBodyRequest>,
) -> Result<Json<DaveState>, ApiError> {
    controller.dave_commit(request).map(Json)
}

async fn dave_state(
    State(controller): State<Arc<DaemonController>>,
) -> Result<Json<DaveState>, ApiError> {
    controller.dave_state().map(Json)
}

fn map_gateway_error(error: GatewayError) -> ApiError {
    match error {
        GatewayError::AlreadyConnected => ApiError::Conflict(error.to_string()),
        GatewayError::NotConnected => ApiError::BadRequest(error.to_string()),
        GatewayError::Message(_) => ApiError::Upstream(error.to_string()),
    }
}

fn media_session_to_voice_config(media: &GatewayMediaSession) -> VoiceSessionConfig {
    VoiceSessionConfig {
        server_id: media.stream_server_id.clone(),
        channel_id: media.rtc_channel_id.clone(),
        user_id: media.user_id.clone(),
        session_id: media.voice_session_id.clone(),
        token: media.stream_token.clone(),
        stream_kind: match media.stream_kind {
            StreamKind::GoLive => VoiceStreamKind::GoLive,
            StreamKind::Camera => VoiceStreamKind::Camera,
        },
        max_dave_protocol_version: MEDIA_MAX_DAVE_PROTOCOL_VERSION,
    }
}

#[cfg(feature = "webrtc-media")]
fn parse_optional_media_connect_request(body: &Bytes) -> Result<Option<MediaConnectRequest>, ApiError> {
    if body.is_empty() {
        return Ok(None);
    }

    serde_json::from_slice(body)
        .map(Some)
        .map_err(|error| ApiError::BadRequest(format!("invalid media connect payload: {error}")))
}

#[cfg(feature = "webrtc-media")]
fn profile_to_video_config(profile: &MediaProfile) -> VoiceVideoConfig {
    VoiceVideoConfig {
        width: profile.width.max(1),
        height: profile.height.max(1),
        max_framerate: profile.max_framerate.max(1),
        max_bitrate: profile.max_bitrate_bps.max(1),
    }
}

fn service_status(capture_running: bool) -> &'static str {
    if capture_running {
        SERVICE_STATUS_OK
    } else {
        SERVICE_STATUS_DEGRADED
    }
}

async fn build_ingest_source(
    request: &StreamStartRequest,
    tx: mpsc::Sender<IngestChunk>,
    read_chunk_size: usize,
) -> Result<(Option<UnixSocketIngestServer>, Option<IngestTaskHandle>), ApiError> {
    let ingest_protocol = request
        .ingest_protocol
        .unwrap_or(if cfg!(feature = "webrtc-media") {
            IngestProtocol::Framed
        } else {
            IngestProtocol::Raw
        });
    match &request.source {
        IngestSource::Unix => {
            let video_socket = request.video_socket.clone().ok_or_else(|| {
                ApiError::BadRequest("video_socket is required for unix ingest".to_owned())
            })?;
            let unix_server = UnixSocketIngestServer::bind(
                UnixSocketSourceConfig {
                    video_socket: PathBuf::from(video_socket),
                    audio_socket: request.audio_socket.clone().map(PathBuf::from),
                    read_chunk_size,
                    protocol: ingest_protocol,
                },
                tx,
            )
            .await
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;

            Ok((Some(unix_server), None))
        }
        IngestSource::Stdin => {
            let stdin_task = spawn_stdin_ingest(
                StdinSourceConfig {
                    kind: request.stdin_media_kind.unwrap_or(IngestMediaKind::Video),
                    read_chunk_size,
                    protocol: ingest_protocol,
                },
                tx,
            );

            Ok((None, Some(stdin_task)))
        }
    }
}

fn decode_binary(data: String) -> Result<Vec<u8>, ApiError> {
    STANDARD
        .decode(data)
        .map_err(|error| ApiError::BadRequest(format!("invalid base64 payload: {error}")))
}

fn build_pacer_config(request: &StreamStartRequest) -> PacerConfig {
    PacerConfig {
        bytes_per_second: request
            .pacing_bps
            .unwrap_or(PacerConfig::DEFAULT_BYTES_PER_SECOND),
        window_ms: request
            .pacing_window_ms
            .unwrap_or(PacerConfig::DEFAULT_WINDOW_MS),
        max_queue_packets: request
            .max_queue_packets
            .unwrap_or(PacerConfig::DEFAULT_MAX_QUEUE_PACKETS),
    }
}

fn to_dave_media_kind(kind: IngestMediaKind) -> DaveMediaKind {
    match kind {
        IngestMediaKind::Video => DaveMediaKind::Video,
        IngestMediaKind::Audio => DaveMediaKind::Audio,
    }
}

#[cfg(feature = "webrtc-media")]
fn to_voice_media_kind(kind: IngestMediaKind) -> VoiceMediaKind {
    match kind {
        IngestMediaKind::Video => VoiceMediaKind::Video,
        IngestMediaKind::Audio => VoiceMediaKind::Audio,
    }
}

fn to_transport_media_kind(kind: IngestMediaKind) -> TransportMediaKind {
    match kind {
        IngestMediaKind::Video => TransportMediaKind::Video,
        IngestMediaKind::Audio => TransportMediaKind::Audio,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::Body;
    use futures_util::{SinkExt, StreamExt};
    use http::{Method, Request};
    use tempfile::tempdir;
    use test_harness::{HarnessGateway, PassthroughDaveSession, sink};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::net::UnixStream;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, sleep, timeout};
    use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};
    use tower::util::ServiceExt;

    fn app_with_sink(packet_sink: Arc<DynSink>) -> Router {
        let controller = Arc::new(DaemonController::new(
            Arc::new(HarnessGateway::default()),
            packet_sink,
            Arc::new(PassthroughDaveSession),
        ));
        build_router(controller)
    }

    #[derive(Clone)]
    struct MockVoiceGateway {
        health: Arc<RwLock<GatewayHealth>>,
        media: GatewayMediaSession,
    }

    impl MockVoiceGateway {
        fn new(media: GatewayMediaSession) -> Self {
            Self {
                health: Arc::new(RwLock::new(GatewayHealth {
                    logged_in: true,
                    joined_voice: true,
                    streaming: true,
                    state: ConnectionState::Streaming,
                    last_error: None,
                    session: Some(SessionConfig {
                        token: "demo".to_owned(),
                        guild_id: media.guild_id.clone().unwrap_or_default(),
                        channel_id: media.channel_id.clone(),
                        stream_kind: media.stream_kind,
                    }),
                })),
                media,
            }
        }
    }

    #[async_trait]
    impl DiscordGateway for MockVoiceGateway {
        async fn connect(&self, _config: SessionConfig) -> Result<GatewayHealth, GatewayError> {
            Ok(self.health.read().await.clone())
        }

        async fn start_stream(&self) -> Result<GatewayHealth, GatewayError> {
            Ok(self.health.read().await.clone())
        }

        async fn stop_stream(&self) -> Result<GatewayHealth, GatewayError> {
            Ok(self.health.read().await.clone())
        }

        async fn disconnect(&self) -> Result<(), GatewayError> {
            Ok(())
        }

        async fn health(&self) -> GatewayHealth {
            self.health.read().await.clone()
        }

        async fn media_session(&self) -> Result<GatewayMediaSession, GatewayError> {
            Ok(self.media.clone())
        }
    }

    #[tokio::test]
    async fn connect_endpoint_reports_connected_state() {
        let app = app_with_sink(Arc::new(NullPacketSink));
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/session/connect")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SessionConnectRequest {
                            token: "demo".to_owned(),
                            guild_id: "1".to_owned(),
                            channel_id: "2".to_owned(),
                            stream_kind: StreamKind::GoLive,
                        })
                        .expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unix_ingest_drives_transport_metrics() {
        let packet_sink = sink();
        let app = app_with_sink(packet_sink.clone());
        let temp = tempdir().expect("tempdir");
        let video_socket = temp.path().join("video.sock");

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/session/connect")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SessionConnectRequest {
                            token: "demo".to_owned(),
                            guild_id: "1".to_owned(),
                            channel_id: "2".to_owned(),
                            stream_kind: StreamKind::GoLive,
                        })
                        .expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/stream/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&StreamStartRequest {
                            source_name: "unix-source".to_owned(),
                            source: IngestSource::Unix,
                            video_socket: Some(video_socket.to_string_lossy().into_owned()),
                            audio_socket: None,
                            stdin_media_kind: None,
                            ingest_protocol: Some(IngestProtocol::Raw),
                            read_chunk_size: Some(1024),
                            pacing_bps: Some(1_000_000),
                            pacing_window_ms: Some(1),
                            max_queue_packets: Some(32),
                        })
                        .expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let mut stream = UnixStream::connect(&video_socket).await.expect("connect");
        stream
            .write_all(b"\x00\x00\x00\x01frame")
            .await
            .expect("write");
        sleep(Duration::from_millis(100)).await;

        let sent = packet_sink.sent_packets().await;
        assert!(!sent.is_empty());

        let metrics = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(metrics.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn framed_unix_ingest_drives_transport_metrics() {
        let packet_sink = sink();
        let app = app_with_sink(packet_sink.clone());
        let temp = tempdir().expect("tempdir");
        let video_socket = temp.path().join("video.sock");

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/session/connect")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SessionConnectRequest {
                            token: "demo".to_owned(),
                            guild_id: "1".to_owned(),
                            channel_id: "2".to_owned(),
                            stream_kind: StreamKind::GoLive,
                        })
                        .expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/stream/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&StreamStartRequest {
                            source_name: "framed-unix-source".to_owned(),
                            source: IngestSource::Unix,
                            video_socket: Some(video_socket.to_string_lossy().into_owned()),
                            audio_socket: None,
                            stdin_media_kind: None,
                            ingest_protocol: Some(IngestProtocol::Framed),
                            read_chunk_size: Some(1024),
                            pacing_bps: Some(1_000_000),
                            pacing_window_ms: Some(1),
                            max_queue_packets: Some(32),
                        })
                        .expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let mut stream = UnixStream::connect(&video_socket).await.expect("connect");
        stream
            .write_all(&(5_u32.to_be_bytes()))
            .await
            .expect("write len");
        stream.write_all(b"frame").await.expect("write payload");
        sleep(Duration::from_millis(100)).await;

        let sent = packet_sink.sent_packets().await;
        assert!(!sent.is_empty());
        assert_eq!(sent[0].payload, bytes::Bytes::from_static(b"frame"));

        let metrics = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(metrics.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn healthz_is_503_until_stream_running() {
        let app = app_with_sink(Arc::new(NullPacketSink));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn dave_init_and_key_package_endpoints_work() {
        let app = app_with_sink(Arc::new(NullPacketSink));
        let init = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/dave/init")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&DaveInitRequest {
                            protocol_version: 1,
                            user_id: 55,
                            channel_id: 66,
                            signing_private_key: None,
                            signing_public_key: None,
                        })
                        .expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(init.status(), StatusCode::OK);

        let key_package = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/dave/key-package")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(key_package.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn session_media_endpoint_reports_active_media_session() {
        let app = app_with_sink(Arc::new(NullPacketSink));

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/session/connect")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SessionConnectRequest {
                            token: "demo".to_owned(),
                            guild_id: "1".to_owned(),
                            channel_id: "2".to_owned(),
                            stream_kind: StreamKind::GoLive,
                        })
                        .expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let temp = tempdir().expect("tempdir");
        let video_socket = temp.path().join("video.sock");
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/stream/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&StreamStartRequest {
                            source_name: "unix-source".to_owned(),
                            source: IngestSource::Unix,
                            video_socket: Some(video_socket.to_string_lossy().into_owned()),
                            audio_socket: None,
                            stdin_media_kind: None,
                            ingest_protocol: Some(IngestProtocol::Raw),
                            read_chunk_size: Some(1024),
                            pacing_bps: Some(1_000_000),
                            pacing_window_ms: Some(1),
                            max_queue_packets: Some(32),
                        })
                        .expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/session/media")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn voice_routes_connect_and_report_health() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (identify_tx, identify_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_async(stream).await.expect("ws");
            let identify = ws.next().await.expect("identify").expect("frame");
            let Message::Text(identify) = identify else {
                panic!("expected identify frame");
            };
            let payload: serde_json::Value =
                serde_json::from_str(identify.as_ref()).expect("identify json");
            assert_eq!(payload["op"], serde_json::json!(0));
            ws.send(Message::Text(
                serde_json::json!({
                    "op": 8,
                    "d": { "heartbeat_interval": 1000 }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("hello");
            let _ = identify_tx.send(());
            sleep(Duration::from_millis(50)).await;
        });

        let gateway = MockVoiceGateway::new(GatewayMediaSession {
            user_id: "1".to_owned(),
            guild_id: Some("1".to_owned()),
            channel_id: "2".to_owned(),
            rtc_channel_id: "3".to_owned(),
            stream_kind: StreamKind::GoLive,
            voice_session_id: "voice-session".to_owned(),
            voice_endpoint: "voice.example.test".to_owned(),
            voice_token: "voice-token".to_owned(),
            stream_key: "guild:1:2:1".to_owned(),
            stream_server_id: "stream-server".to_owned(),
            stream_endpoint: format!("ws://{addr}"),
            stream_token: "stream-token".to_owned(),
        });
        let controller = Arc::new(DaemonController::new(
            Arc::new(gateway),
            Arc::new(NullPacketSink),
            Arc::new(ManagedDaveSession::new()),
        ));
        let app = build_router(controller);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/voice/connect")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("voice connect");
        assert_eq!(response.status(), StatusCode::OK);

        identify_rx.await.expect("identify observed");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/voice/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("voice health");
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/voice/state")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("voice state");
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/voice/disconnect")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("voice disconnect");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[cfg(not(feature = "webrtc-media"))]
    #[tokio::test]
    async fn media_routes_fail_cleanly_without_feature() {
        let app = app_with_sink(Arc::new(NullPacketSink));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/media/connect")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("media connect");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/media/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("media health");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/media/disconnect")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("media disconnect");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn start_stream_rejects_missing_dave_session() {
        let controller = Arc::new(DaemonController::new(
            Arc::new(HarnessGateway::default()),
            Arc::new(NullPacketSink),
            Arc::new(ManagedDaveSession::new()),
        ));
        controller
            .connect_session(SessionConnectRequest {
                token: "demo".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            })
            .await
            .expect("connected");

        let error = controller
            .start_stream(StreamStartRequest {
                source_name: "unix-source".to_owned(),
                source: IngestSource::Unix,
                video_socket: Some("/tmp/discord-rs-streamer-test.sock".to_owned()),
                audio_socket: None,
                stdin_media_kind: None,
                ingest_protocol: Some(IngestProtocol::Raw),
                read_chunk_size: Some(1024),
                pacing_bps: Some(1_000_000),
                pacing_window_ms: Some(1),
                max_queue_packets: Some(32),
            })
            .await
            .expect_err("missing dave should fail");

        assert!(matches!(error, ApiError::BadRequest(_)));
    }

    #[tokio::test]
    async fn stop_stream_returns_without_deadlocking() {
        let controller = Arc::new(DaemonController::new(
            Arc::new(HarnessGateway::default()),
            sink(),
            Arc::new(PassthroughDaveSession),
        ));
        controller
            .connect_session(SessionConnectRequest {
                token: "demo".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            })
            .await
            .expect("connected");

        let temp = tempdir().expect("tempdir");
        let video_socket = temp.path().join("video.sock");
        controller
            .start_stream(StreamStartRequest {
                source_name: "unix-source".to_owned(),
                source: IngestSource::Unix,
                video_socket: Some(video_socket.to_string_lossy().into_owned()),
                audio_socket: None,
                stdin_media_kind: None,
                ingest_protocol: Some(IngestProtocol::Raw),
                read_chunk_size: Some(1024),
                pacing_bps: Some(1_000_000),
                pacing_window_ms: Some(1),
                max_queue_packets: Some(32),
            })
            .await
            .expect("stream started");

        let response = timeout(Duration::from_secs(2), controller.stop_stream())
            .await
            .expect("stop timeout")
            .expect("stop response");
        assert!(!response.capture_running);
    }

    #[cfg(feature = "webrtc-media")]
    #[test]
    fn media_connect_request_defaults_when_body_is_empty() {
        let request = parse_optional_media_connect_request(&Bytes::new()).expect("parse");
        assert!(request.is_none());
    }

    #[cfg(feature = "webrtc-media")]
    #[test]
    fn payload_decoder_config_detection_finds_sps_and_pps() {
        let payload = bytes::Bytes::from_static(&[
            0, 0, 0, 1, 0x67, 0x64, 0x00, 0x1f, 0, 0, 0, 1, 0x68, 0xeb, 0xef, 0x20,
        ]);
        assert!(payload_has_decoder_config(&payload));
    }

    #[cfg(feature = "webrtc-media")]
    #[tokio::test]
    async fn startup_video_buffer_keeps_newest_decodable_burst() {
        let metrics = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(Arc::clone(&metrics), 15);

        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Video,
                payload: bytes::Bytes::from_static(b"p-frame"),
                capture_time_us: 1,
                duration_us: 66_666,
                is_keyframe: false,
                is_decoder_config: false,
            })
            .await;
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Video,
                payload: bytes::Bytes::from_static(b"sps"),
                capture_time_us: 2,
                duration_us: 66_666,
                is_keyframe: false,
                is_decoder_config: true,
            })
            .await;
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Video,
                payload: bytes::Bytes::from_static(b"idr"),
                capture_time_us: 3,
                duration_us: 66_666,
                is_keyframe: true,
                is_decoder_config: false,
            })
            .await;
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Video,
                payload: bytes::Bytes::from_static(b"delta"),
                capture_time_us: 4,
                duration_us: 66_666,
                is_keyframe: false,
                is_decoder_config: false,
            })
            .await;

        assert_eq!(scheduler.startup_video.len(), 2);
        assert_eq!(
            scheduler.startup_video_config.as_ref().map(|chunk| chunk.payload.clone()),
            Some(bytes::Bytes::from_static(b"sps"))
        );
        let metrics = metrics.lock().await.snapshot();
        assert_eq!(metrics.dropped_not_ready_video, 1);
    }

    #[cfg(feature = "webrtc-media")]
    #[tokio::test]
    async fn drain_remaining_drops_startup_buffers_when_media_never_ready() {
        let metrics = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(Arc::clone(&metrics), 15);
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Audio,
                payload: bytes::Bytes::from_static(b"audio"),
                capture_time_us: 1,
                duration_us: 20_000,
                is_keyframe: false,
                is_decoder_config: false,
            })
            .await;
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Video,
                payload: bytes::Bytes::from_static(b"idr"),
                capture_time_us: 2,
                duration_us: 66_666,
                is_keyframe: true,
                is_decoder_config: false,
            })
            .await;

        let active_media = Arc::new(Mutex::new(None));
        let gateway: DynGateway = Arc::new(HarnessGateway::default());
        let dave: Arc<DynDave> = Arc::new(ManagedDaveSession::new());
        scheduler
            .drain_remaining(&gateway, &dave, &active_media)
            .await
            .expect("drain succeeds");

        assert!(!scheduler.has_pending());
        let metrics = metrics.lock().await.snapshot();
        assert_eq!(metrics.dropped_not_ready_audio, 1);
        assert_eq!(metrics.dropped_not_ready_video, 1);
    }

    #[cfg(feature = "webrtc-media")]
    #[tokio::test]
    async fn recovery_backoff_waits_for_startup_grace_period() {
        let metrics = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(Arc::clone(&metrics), 15);
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Audio,
                payload: bytes::Bytes::from_static(b"audio"),
                capture_time_us: 1,
                duration_us: 20_000,
                is_keyframe: false,
                is_decoder_config: false,
            })
            .await;

        assert!(!scheduler.should_attempt_recovery(false));

        scheduler.startup_since = Some(Instant::now() - MEDIA_RECOVERY_GRACE_PERIOD);
        assert!(scheduler.should_attempt_recovery(false));

        scheduler.last_recovery_attempt_at = Some(Instant::now());
        assert!(!scheduler.should_attempt_recovery(false));

        scheduler.last_recovery_attempt_at =
            Some(Instant::now() - MEDIA_RECOVERY_RETRY_INTERVAL);
        assert!(scheduler.should_attempt_recovery(false));
    }

    #[cfg(feature = "webrtc-media")]
    #[tokio::test]
    async fn forced_recovery_bypasses_grace_period_but_respects_retry_interval() {
        let metrics = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(Arc::clone(&metrics), 15);
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Video,
                payload: bytes::Bytes::from_static(b"idr"),
                capture_time_us: 1,
                duration_us: 66_666,
                is_keyframe: true,
                is_decoder_config: false,
            })
            .await;

        assert!(scheduler.should_attempt_recovery(true));

        scheduler.last_recovery_attempt_at = Some(Instant::now());
        assert!(!scheduler.should_attempt_recovery(true));

        scheduler.last_recovery_attempt_at =
            Some(Instant::now() - MEDIA_RECOVERY_RETRY_INTERVAL);
        assert!(scheduler.should_attempt_recovery(true));
    }

    #[cfg(feature = "webrtc-media")]
    #[tokio::test]
    async fn audio_burst_trim_keeps_publish_queue_bounded() {
        let metrics = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(Arc::clone(&metrics), 15);
        scheduler.startup_mode = false;

        for index in 0..10_u64 {
            scheduler
                .enqueue_live(QueuedPublishChunk {
                    kind: IngestMediaKind::Audio,
                    payload: bytes::Bytes::from_static(b"audio"),
                    capture_time_us: index * 20_000,
                    duration_us: 20_000,
                    is_keyframe: false,
                    is_decoder_config: false,
                })
                .await;
        }

        let metrics = metrics.lock().await.snapshot();
        assert_eq!(scheduler.audio_queue.len(), 10);
        assert_eq!(metrics.audio_queue_depth, 10);
        assert_eq!(metrics.dropped_overload_audio, 0);
    }

    #[cfg(feature = "webrtc-media")]
    #[tokio::test]
    async fn ready_transition_moves_startup_buffers_into_live_queues() {
        let metrics = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(Arc::clone(&metrics), 15);
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Audio,
                payload: bytes::Bytes::from_static(b"audio"),
                capture_time_us: 20_000,
                duration_us: 20_000,
                is_keyframe: false,
                is_decoder_config: false,
            })
            .await;
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Video,
                payload: bytes::Bytes::from_static(b"sps"),
                capture_time_us: 10_000,
                duration_us: 66_666,
                is_keyframe: false,
                is_decoder_config: true,
            })
            .await;
        scheduler
            .enqueue_startup(QueuedPublishChunk {
                kind: IngestMediaKind::Video,
                payload: bytes::Bytes::from_static(b"idr"),
                capture_time_us: 30_000,
                duration_us: 66_666,
                is_keyframe: true,
                is_decoder_config: false,
            })
            .await;

        scheduler.flush_startup_to_live().await;
        scheduler.startup_mode = false;

        assert!(scheduler.startup_audio.is_empty());
        assert!(scheduler.startup_video.is_empty());
        assert!(scheduler.startup_video_config.is_none());
        assert_eq!(scheduler.audio_queue.len(), 1);
        assert_eq!(scheduler.video_queue.len(), 2);
        assert_eq!(scheduler.video_queue.front().map(|chunk| chunk.payload.clone()), Some(bytes::Bytes::from_static(b"sps")));
        let metrics = metrics.lock().await.snapshot();
        assert_eq!(metrics.startup_audio_depth, 0);
        assert_eq!(metrics.startup_video_depth, 0);
        assert_eq!(metrics.audio_queue_depth, 1);
        assert_eq!(metrics.video_queue_depth, 2);
    }

    #[cfg(feature = "webrtc-media")]
    #[test]
    fn scheduler_prefers_earliest_capture_time_when_bursty() {
        let runtime = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(runtime, 15);
        scheduler.startup_mode = false;
        let base = Instant::now();
        scheduler.playout_epoch = Some(base + MEDIA_PLAYOUT_LEAD);
        scheduler.audio_queue.push_back(QueuedPublishChunk {
            kind: IngestMediaKind::Audio,
            payload: bytes::Bytes::from_static(b"audio"),
            capture_time_us: 20_000,
            duration_us: 20_000,
            is_keyframe: false,
            is_decoder_config: false,
        });
        scheduler.video_queue.push_back(QueuedPublishChunk {
            kind: IngestMediaKind::Video,
            payload: bytes::Bytes::from_static(b"video"),
            capture_time_us: 40_000,
            duration_us: 66_666,
            is_keyframe: true,
            is_decoder_config: false,
        });

        let next_due = scheduler.next_due_instant().expect("next due");
        assert!(next_due >= base + MEDIA_PLAYOUT_LEAD + Duration::from_micros(20_000));
        assert!(next_due <= base + MEDIA_PLAYOUT_LEAD + Duration::from_micros(40_000));
    }

    #[cfg(feature = "webrtc-media")]
    #[test]
    fn stalled_startup_mode_arms_media_recovery_after_grace_period() {
        let runtime = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(runtime, 15);
        scheduler.startup_audio.push_back(QueuedPublishChunk {
            kind: IngestMediaKind::Audio,
            payload: bytes::Bytes::from_static(b"audio"),
            capture_time_us: 0,
            duration_us: 20_000,
            is_keyframe: false,
            is_decoder_config: false,
        });
        scheduler.startup_since = Some(Instant::now() - MEDIA_RECOVERY_GRACE_PERIOD);
        assert!(scheduler.should_attempt_recovery(false));

        scheduler.last_recovery_attempt_at = Some(Instant::now());
        assert!(!scheduler.should_attempt_recovery(true));
    }

    #[cfg(feature = "webrtc-media")]
    #[test]
    fn exit_startup_mode_clears_recovery_markers() {
        let runtime = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(runtime, 15);
        scheduler.last_recovery_attempt_at = Some(Instant::now());
        scheduler.exit_startup_mode();

        assert!(!scheduler.startup_mode);
        assert!(scheduler.startup_since.is_none());
        assert!(scheduler.last_recovery_attempt_at.is_none());
    }

    #[cfg(feature = "webrtc-media")]
    #[test]
    fn mixed_audio_video_send_metrics_track_jitter_and_keyframe_age_inputs() {
        let base = Instant::now();
        let mut metrics = PublishRuntimeState::default();

        let audio_first = QueuedPublishChunk {
            kind: IngestMediaKind::Audio,
            payload: bytes::Bytes::from_static(b"audio-1"),
            capture_time_us: 0,
            duration_us: 20_000,
            is_keyframe: false,
            is_decoder_config: false,
        };
        let audio_second = QueuedPublishChunk {
            kind: IngestMediaKind::Audio,
            payload: bytes::Bytes::from_static(b"audio-2"),
            capture_time_us: 20_000,
            duration_us: 20_000,
            is_keyframe: false,
            is_decoder_config: false,
        };
        let video_first = QueuedPublishChunk {
            kind: IngestMediaKind::Video,
            payload: bytes::Bytes::from_static(b"video-1"),
            capture_time_us: 0,
            duration_us: 66_666,
            is_keyframe: true,
            is_decoder_config: true,
        };
        let video_second = QueuedPublishChunk {
            kind: IngestMediaKind::Video,
            payload: bytes::Bytes::from_static(b"video-2"),
            capture_time_us: 66_666,
            duration_us: 66_666,
            is_keyframe: false,
            is_decoder_config: false,
        };

        record_publish_send_metrics(&mut metrics, &audio_first, base);
        record_publish_send_metrics(&mut metrics, &audio_second, base + Duration::from_millis(25));
        record_publish_send_metrics(&mut metrics, &video_first, base + Duration::from_millis(30));
        record_publish_send_metrics(&mut metrics, &video_second, base + Duration::from_millis(120));

        assert_eq!(metrics.snapshot.audio_send_jitter_ms, Some(5));
        assert_eq!(metrics.snapshot.video_send_jitter_ms, Some(23));
        assert_eq!(metrics.last_audio_send_at, Some(base + Duration::from_millis(25)));
        assert_eq!(metrics.last_video_send_at, Some(base + Duration::from_millis(120)));
        assert_eq!(metrics.last_keyframe_at, Some(base + Duration::from_millis(30)));
    }

    #[cfg(feature = "webrtc-media")]
    #[test]
    fn sink_target_fps_scales_with_quality() {
        assert_eq!(sink_target_fps(15, 100), None);
        assert_eq!(sink_target_fps(15, 0), Some(0));
        assert_eq!(sink_target_fps(15, 50), Some(8));
        assert_eq!(sink_target_fps(15, 10), Some(2));
    }

    #[cfg(feature = "webrtc-media")]
    #[tokio::test]
    async fn record_sink_video_drop_updates_counter() {
        let metrics = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let scheduler = MediaPublishScheduler::new(Arc::clone(&metrics), 15);
        scheduler.record_sink_video_drop().await;

        let snapshot = metrics.lock().await.snapshot();
        assert_eq!(snapshot.dropped_sink_video, 1);
    }

    #[cfg(feature = "webrtc-media")]
    #[tokio::test]
    #[ignore = "long-running simulated scheduler soak"]
    async fn simulated_ten_minute_scheduler_soak_keeps_queues_bounded() {
        let metrics = Arc::new(Mutex::new(PublishRuntimeState::default()));
        let mut scheduler = MediaPublishScheduler::new(Arc::clone(&metrics), 15);
        scheduler.startup_mode = false;

        for second in 0..600_u64 {
            for audio_index in 0..50_u64 {
                scheduler
                    .enqueue_live(QueuedPublishChunk {
                        kind: IngestMediaKind::Audio,
                        payload: bytes::Bytes::from_static(b"audio"),
                        capture_time_us: second * 1_000_000 + audio_index * 20_000,
                        duration_us: 20_000,
                        is_keyframe: false,
                        is_decoder_config: false,
                    })
                    .await;
            }
            for video_index in 0..15_u64 {
                scheduler
                    .enqueue_live(QueuedPublishChunk {
                        kind: IngestMediaKind::Video,
                        payload: bytes::Bytes::from_static(b"video"),
                        capture_time_us: second * 1_000_000 + video_index * 66_666,
                        duration_us: 66_666,
                        is_keyframe: video_index == 0,
                        is_decoder_config: video_index == 0,
                    })
                    .await;
            }
        }

        let metrics = metrics.lock().await.snapshot();
        assert!(metrics.audio_queue_depth <= 20);
        assert!(metrics.video_queue_depth <= MAX_VIDEO_QUEUE_PACKETS);
        assert!(metrics.dropped_overload_audio > 0);
    }
}
