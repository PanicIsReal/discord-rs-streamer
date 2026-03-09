use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
#[cfg(feature = "webrtc-media")]
use std::time::Duration;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use dave::{
    DaveCommitWelcome, DaveControl, DaveInitConfig, DaveMetadata, DaveProposalResult, DaveState,
    ManagedDaveSession, MediaKind as DaveMediaKind, ProposalOp,
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
use tracing::info;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub transport: TransportMetrics,
    pub ingest: IngestMetrics,
    pub discord_state: ConnectionState,
    #[cfg(feature = "webrtc-media")]
    pub publisher: Option<VoicePublisherState>,
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
    pub async fn connect_media(&self) -> Result<MediaSessionResponse, ApiError> {
        let mut active = self.active_media.lock().await;
        if active.is_some() {
            return Err(ApiError::Conflict(
                "media session is already connected".to_owned(),
            ));
        }
        self.gateway
            .start_stream()
            .await
            .map_err(map_gateway_error)?;
        let media = self
            .gateway
            .media_session()
            .await
            .map_err(map_gateway_error)?;

        let voice_client = match VoiceGatewayClient::connect(
            VoiceServerInfo {
                endpoint: media.stream_endpoint.clone(),
                token: media.stream_token.clone(),
            },
            media_session_to_voice_config(&media),
            VoiceSessionController::new(Arc::clone(&self.dave)),
        )
        .await
        {
            Ok(client) => client,
            Err(error) => {
                let _ = self.gateway.stop_stream().await;
                return Err(ApiError::Upstream(error.to_string()));
            }
        };

        let session = match VoiceMediaSession::negotiate(
            voice_client,
            VoicePublisherConfig::default(),
            Duration::from_secs(10),
            VoiceVideoConfig {
                width: 1280,
                height: 720,
                max_framerate: 30,
                max_bitrate: 4_000_000,
            },
        )
        .await
        {
            Ok(session) => session,
            Err(error) => {
                let _ = self.gateway.stop_stream().await;
                return Err(ApiError::Upstream(error.to_string()));
            }
        };

        let health = session
            .health()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        *active = Some(ActiveMediaSession { session });
        Ok(MediaSessionResponse { media: health })
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
        Ok(MediaSessionResponse { media: health })
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
        let ingest_forwarder = tokio::spawn(async move {
            if let Err(error) = forward_ingest(
                rx,
                transport_forwarder,
                #[cfg(feature = "webrtc-media")]
                active_media,
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
            .process_proposals_bundle(request.operation, &proposals)
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
                active.session.health().await.ok().map(|health| health.publisher)
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

async fn forward_ingest(
    mut receiver: mpsc::Receiver<IngestChunk>,
    transport: Option<PacedPacketSender<DynSink>>,
    #[cfg(feature = "webrtc-media")] active_media: Arc<Mutex<Option<ActiveMediaSession>>>,
    ingest_metrics: Arc<IngestMetricState>,
    dave: Arc<DynDave>,
    gateway: DynGateway,
    capture_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let mut sequence = 0_u64;
    let result = async {
        while let Some(chunk) = receiver.recv().await {
            sequence += 1;
            ingest_metrics.record(&chunk);
            let kind = to_dave_media_kind(chunk.kind);
            let dave_state = dave.state().map_err(|error| error.to_string())?;
            let protected = if dave_state.ready {
                dave
                    .protect(
                        kind,
                        &DaveMetadata {
                            sequence,
                            timestamp_ms: sequence,
                        },
                        chunk.payload,
                    )
                    .map_err(|error| error.to_string())?
            } else {
                chunk.payload
            };

            let packet = Packet {
                kind: to_transport_media_kind(chunk.kind),
                sequence,
                timestamp_ms: sequence,
                payload: protected.clone(),
            };

            #[cfg(feature = "webrtc-media")]
            forward_chunk(
                &active_media,
                chunk.kind,
                packet,
                transport.as_ref(),
                &protected,
            )
            .await?;
            #[cfg(not(feature = "webrtc-media"))]
            forward_chunk(packet, transport.as_ref())?;
        }
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
async fn forward_chunk(
    active_media: &Arc<Mutex<Option<ActiveMediaSession>>>,
    kind: IngestMediaKind,
    packet: Packet,
    transport: Option<&PacedPacketSender<DynSink>>,
    protected_payload: &[u8],
) -> Result<(), String> {
    if let Some(active) = active_media.lock().await.as_mut() {
        active
            .session
            .send_media(to_voice_media_kind(kind), protected_payload)
            .map_err(|error| error.to_string())?;
        return Ok(());
    }

    if let Some(transport) = transport {
        let _ = transport.try_enqueue(packet);
    }
    Ok(())
}

#[cfg(not(feature = "webrtc-media"))]
fn forward_chunk(
    packet: Packet,
    transport: Option<&PacedPacketSender<DynSink>>,
) -> Result<(), String> {
    if let Some(transport) = transport {
        let _ = transport.try_enqueue(packet);
    }
    Ok(())
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
) -> Result<Json<MediaSessionResponse>, ApiError> {
    controller.connect_media().await.map(Json)
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
        channel_id: media.channel_id.clone(),
        user_id: media.user_id.clone(),
        session_id: media.voice_session_id.clone(),
        token: media.stream_token.clone(),
        stream_kind: match media.stream_kind {
            StreamKind::GoLive => VoiceStreamKind::GoLive,
            StreamKind::Camera => VoiceStreamKind::Camera,
        },
        max_dave_protocol_version: 1,
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
    let ingest_protocol = request.ingest_protocol.unwrap_or(if cfg!(feature = "webrtc-media") {
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
}
