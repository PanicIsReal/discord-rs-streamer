use std::env;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, warn};
use url::Url;

const DISCORD_API_BASE: &str = "https://discord.com/api";
const DISCORD_API_VERSION: u16 = 9;
const GATEWAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const GATEWAY_RECONNECT_DELAY: Duration = Duration::from_millis(250);
const STATE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const STATE_TIMEOUT: Duration = Duration::from_secs(20);
const VOICE_STATE_RESET_TIMEOUT: Duration = Duration::from_secs(3);
const DISCORD_USER_AGENT: &str = "discord-rs-streamer/0.1";
const INVALID_SESSION_ERROR: &str = "discord gateway invalid session";
const INVALID_SESSION_RESUMABLE_ERROR: &str = "discord gateway invalid session resumable";
const GATEWAY_EVENT_LOG_ENV: &str = "DISCORD_RS_STREAMER_GATEWAY_EVENT_LOG";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamKind {
    GoLive,
    Camera,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionConfig {
    pub token: String,
    pub guild_id: String,
    pub channel_id: String,
    pub stream_kind: StreamKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Streaming,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayHealth {
    pub logged_in: bool,
    pub joined_voice: bool,
    pub streaming: bool,
    pub state: ConnectionState,
    pub last_error: Option<String>,
    pub session: Option<SessionConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayMediaSession {
    pub user_id: String,
    pub guild_id: Option<String>,
    pub channel_id: String,
    pub rtc_channel_id: String,
    pub stream_kind: StreamKind,
    pub voice_session_id: String,
    pub voice_endpoint: String,
    pub voice_token: String,
    pub stream_key: String,
    pub stream_server_id: String,
    pub stream_endpoint: String,
    pub stream_token: String,
}

impl Default for GatewayHealth {
    fn default() -> Self {
        Self {
            logged_in: false,
            joined_voice: false,
            streaming: false,
            state: ConnectionState::Disconnected,
            last_error: None,
            session: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("discord session is not connected")]
    NotConnected,
    #[error("discord session already connected")]
    AlreadyConnected,
    #[error("{0}")]
    Message(String),
}

#[async_trait]
pub trait DiscordGateway: Send + Sync {
    async fn connect(&self, config: SessionConfig) -> Result<GatewayHealth, GatewayError>;
    async fn start_stream(&self) -> Result<GatewayHealth, GatewayError>;
    async fn stop_stream(&self) -> Result<GatewayHealth, GatewayError>;
    async fn disconnect(&self) -> Result<(), GatewayError>;
    async fn health(&self) -> GatewayHealth;
    async fn media_session(&self) -> Result<GatewayMediaSession, GatewayError>;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryGateway {
    state: Arc<RwLock<GatewayHealth>>,
}

impl InMemoryGateway {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DiscordGateway for InMemoryGateway {
    async fn connect(&self, config: SessionConfig) -> Result<GatewayHealth, GatewayError> {
        let mut state = self.state.write().await;
        if state.logged_in {
            return Err(GatewayError::AlreadyConnected);
        }

        state.logged_in = true;
        state.joined_voice = true;
        state.state = ConnectionState::Connected;
        state.last_error = None;
        state.session = Some(config);
        Ok(state.clone())
    }

    async fn start_stream(&self) -> Result<GatewayHealth, GatewayError> {
        let mut state = self.state.write().await;
        if !state.logged_in || !state.joined_voice {
            return Err(GatewayError::NotConnected);
        }
        state.streaming = true;
        state.state = ConnectionState::Streaming;
        Ok(state.clone())
    }

    async fn stop_stream(&self) -> Result<GatewayHealth, GatewayError> {
        let mut state = self.state.write().await;
        if !state.logged_in {
            return Err(GatewayError::NotConnected);
        }
        state.streaming = false;
        state.state = ConnectionState::Connected;
        Ok(state.clone())
    }

    async fn disconnect(&self) -> Result<(), GatewayError> {
        let mut state = self.state.write().await;
        *state = GatewayHealth::default();
        Ok(())
    }

    async fn health(&self) -> GatewayHealth {
        self.state.read().await.clone()
    }

    async fn media_session(&self) -> Result<GatewayMediaSession, GatewayError> {
        let state = self.state.read().await;
        let session = state.session.clone().ok_or(GatewayError::NotConnected)?;
        if !state.streaming {
            return Err(GatewayError::NotConnected);
        }
        Ok(GatewayMediaSession {
            user_id: "in-memory-user".to_owned(),
            guild_id: session_guild_id(&session).map(ToOwned::to_owned),
            channel_id: session.channel_id,
            rtc_channel_id: "stream-channel".to_owned(),
            stream_kind: session.stream_kind,
            voice_session_id: "voice-session".to_owned(),
            voice_endpoint: "voice.example.test".to_owned(),
            voice_token: "voice-token".to_owned(),
            stream_key: "guild:1:2:3".to_owned(),
            stream_server_id: "stream-server".to_owned(),
            stream_endpoint: "stream.example.test".to_owned(),
            stream_token: "stream-token".to_owned(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RealDiscordGateway {
    http: Client,
    state: Arc<RwLock<GatewayState>>,
    event_recorder: Option<Arc<GatewayEventRecorder>>,
    command_tx: Arc<Mutex<Option<mpsc::UnboundedSender<GatewayCommand>>>>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Debug, Clone, Default)]
struct GatewayState {
    health: GatewayHealth,
    self_user_id: Option<String>,
    gateway_session_id: Option<String>,
    resume_gateway_url: Option<String>,
    sequence: Option<u64>,
    pending_voice_rejoin: bool,
    pending_stream_restart: bool,
    voice_session_id: Option<String>,
    voice_endpoint: Option<String>,
    voice_token: Option<String>,
    stream_key: Option<String>,
    stream_server_id: Option<String>,
    stream_rtc_channel_id: Option<String>,
    stream_endpoint: Option<String>,
    stream_token: Option<String>,
}

#[derive(Debug)]
enum GatewayCommand {
    Send(Value),
    Close,
}

#[derive(Debug, Deserialize)]
struct GatewayInfoResponse {
    url: String,
}

#[derive(Debug, Clone)]
struct GatewayEventRecorder {
    path: Arc<str>,
}

#[derive(Debug, Clone, Serialize)]
struct GatewayStateSnapshot {
    logged_in: bool,
    joined_voice: bool,
    streaming: bool,
    state: ConnectionState,
    last_error: Option<String>,
    self_user_id: Option<String>,
    gateway_session_id: Option<String>,
    resume_gateway_url: Option<String>,
    sequence: Option<u64>,
    pending_voice_rejoin: bool,
    pending_stream_restart: bool,
    voice_session_id: Option<String>,
    voice_endpoint: Option<String>,
    voice_token: Option<String>,
    stream_key: Option<String>,
    stream_server_id: Option<String>,
    stream_rtc_channel_id: Option<String>,
    stream_endpoint: Option<String>,
    stream_token: Option<String>,
}

impl RealDiscordGateway {
    pub fn new() -> Self {
        Self {
            http: Client::builder()
                .user_agent(DISCORD_USER_AGENT)
                .build()
                .expect("http client"),
            state: Arc::new(RwLock::new(GatewayState::default())),
            event_recorder: GatewayEventRecorder::from_env().map(Arc::new),
            command_tx: Arc::new(Mutex::new(None)),
            task: Arc::new(Mutex::new(None)),
        }
    }

    async fn fetch_gateway_url(&self, token: &str) -> Result<Url, GatewayError> {
        let response = self
            .http
            .get(format!("{DISCORD_API_BASE}/v{DISCORD_API_VERSION}/gateway"))
            .header("authorization", token)
            .send()
            .await
            .map_err(|error| GatewayError::Message(format!("failed to fetch gateway: {error}")))?;
        let response = response.error_for_status().map_err(|error| {
            GatewayError::Message(format!("discord gateway lookup failed: {error}"))
        })?;
        let gateway = response
            .json::<GatewayInfoResponse>()
            .await
            .map_err(|error| {
                GatewayError::Message(format!("failed to decode gateway url: {error}"))
            })?;
        Url::parse(&format!(
            "{}/?v={}&encoding=json",
            gateway.url.trim_end_matches('/'),
            DISCORD_API_VERSION
        ))
        .map_err(|error| GatewayError::Message(format!("invalid gateway url: {error}")))
    }

    async fn send_command(&self, payload: Value) -> Result<(), GatewayError> {
        let sender = self.command_tx.lock().await.clone();
        let Some(sender) = sender else {
            return Err(GatewayError::NotConnected);
        };
        sender
            .send(GatewayCommand::Send(payload))
            .map_err(|_| GatewayError::Message("gateway command loop is unavailable".to_owned()))
    }

    async fn reset_runtime(&self) {
        if let Some(sender) = self.command_tx.lock().await.take() {
            let _ = sender.send(GatewayCommand::Close);
        }
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
    }

    async fn mark_error(&self, message: impl Into<String>) -> GatewayError {
        let message = message.into();
        let mut state = self.state.write().await;
        state.health.state = ConnectionState::Error;
        state.health.last_error = Some(message.clone());
        GatewayError::Message(message)
    }

    async fn wait_for<F>(
        &self,
        predicate: F,
        error_message: &'static str,
    ) -> Result<GatewayHealth, GatewayError>
    where
        F: Fn(&GatewayState) -> bool,
    {
        self.wait_for_with_timeout(STATE_TIMEOUT, predicate, error_message)
            .await
    }

    async fn wait_for_with_timeout<F>(
        &self,
        timeout_duration: Duration,
        predicate: F,
        error_message: &'static str,
    ) -> Result<GatewayHealth, GatewayError>
    where
        F: Fn(&GatewayState) -> bool,
    {
        let start = Instant::now();
        loop {
            let snapshot = self.state.read().await.clone();
            if predicate(&snapshot) {
                return Ok(snapshot.health);
            }
            if snapshot.health.state == ConnectionState::Error {
                return Err(GatewayError::Message(
                    snapshot
                        .health
                        .last_error
                        .unwrap_or_else(|| error_message.to_owned()),
                ));
            }
            if start.elapsed() >= timeout_duration {
                return Err(self
                    .mark_error(format!("{error_message} before timeout"))
                    .await);
            }
            sleep(STATE_POLL_INTERVAL).await;
        }
    }

    async fn update_state<F>(&self, mutate: F)
    where
        F: FnOnce(&mut GatewayState),
    {
        let mut state = self.state.write().await;
        mutate(&mut state);
    }
}

impl Default for RealDiscordGateway {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DiscordGateway for RealDiscordGateway {
    async fn connect(&self, config: SessionConfig) -> Result<GatewayHealth, GatewayError> {
        {
            let state = self.state.read().await;
            if state.health.logged_in && state.health.state != ConnectionState::Error {
                return Err(GatewayError::AlreadyConnected);
            }
        }

        self.reset_runtime().await;
        let gateway_url = self.fetch_gateway_url(&config.token).await?;
        self.connect_with_gateway_url(config, gateway_url).await
    }

    async fn start_stream(&self) -> Result<GatewayHealth, GatewayError> {
        let stream_commands = {
            let state = self.state.read().await;
            let (session, self_user_id) = stream_start_context(&state)?;
            build_stream_start_commands(session, self_user_id)
        };

        for command in stream_commands {
            self.send_command(command).await?;
        }

        self.wait_for(
            |state| {
                state.health.joined_voice
                    && state.health.streaming
                    && has_stream_session_resources(state)
            },
            "discord stream session did not become ready",
        )
        .await
    }

    async fn stop_stream(&self) -> Result<GatewayHealth, GatewayError> {
        let stream_key = {
            let state = self.state.read().await;
            if !state.health.logged_in {
                return Err(GatewayError::NotConnected);
            }
            state.stream_key.clone()
        };

        if let Some(stream_key) = stream_key {
            self.send_command(json!({
                "op": 19,
                "d": { "stream_key": stream_key }
            }))
            .await?;
        }

        self.update_state(|state| {
            state.health.streaming = false;
            state.health.state = ConnectionState::Connected;
            state.stream_key = None;
            state.stream_server_id = None;
            state.stream_rtc_channel_id = None;
            state.stream_endpoint = None;
            state.stream_token = None;
        })
        .await;

        Ok(self.state.read().await.health.clone())
    }

    async fn disconnect(&self) -> Result<(), GatewayError> {
        let session = self.state.read().await.health.session.clone();
        if let Some(session) = session {
            let _ = self
                .send_command(json!({
                    "op": 4,
                    "d": {
                        "guild_id": json_guild_id(&session.guild_id),
                        "channel_id": Value::Null,
                        "self_mute": true,
                        "self_deaf": false,
                        "self_video": false,
                    }
                }))
                .await;
        }
        self.reset_runtime().await;
        self.update_state(|state| *state = GatewayState::default())
            .await;
        Ok(())
    }

    async fn health(&self) -> GatewayHealth {
        self.state.read().await.health.clone()
    }

    async fn media_session(&self) -> Result<GatewayMediaSession, GatewayError> {
        let state = self.state.read().await;
        build_media_session(&state)
    }
}

impl RealDiscordGateway {
    async fn connect_with_gateway_url(
        &self,
        config: SessionConfig,
        gateway_url: Url,
    ) -> Result<GatewayHealth, GatewayError> {
        self.update_state(|state| {
            *state = GatewayState::default();
            state.health.state = ConnectionState::Connecting;
            state.health.last_error = None;
            state.health.session = Some(config.clone());
        })
        .await;

        let initial_stream = timeout(GATEWAY_CONNECT_TIMEOUT, connect_async(gateway_url.as_str()))
            .await
            .map_err(|_| {
                GatewayError::Message("timed out connecting to discord gateway".to_owned())
            })?
            .map_err(|error| {
                GatewayError::Message(format!("failed to connect to discord gateway: {error}"))
            })?;
        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<GatewayCommand>();
        *self.command_tx.lock().await = Some(command_tx);

        let state = Arc::clone(&self.state);
        let event_recorder = self.event_recorder.clone();
        let gateway_url_for_task = gateway_url.clone();
        let task = tokio::spawn(async move {
            let mut next_stream = Some(initial_stream);
            let mut use_resume = false;
            loop {
                let stream = match next_stream.take() {
                    Some(stream) => stream,
                    None => {
                        mark_gateway_reconnecting(&state, "discord gateway reconnecting").await;
                        sleep(GATEWAY_RECONNECT_DELAY).await;
                        let reconnect_url =
                            reconnect_gateway_url(&state, &gateway_url_for_task).await;
                        match timeout(GATEWAY_CONNECT_TIMEOUT, connect_async(reconnect_url.as_str()))
                            .await
                        {
                            Ok(Ok(stream)) => stream,
                            Ok(Err(error)) => {
                                mark_gateway_reconnecting(
                                    &state,
                                    format!("failed to reconnect to discord gateway: {error}"),
                                )
                                .await;
                                continue;
                            }
                            Err(_) => {
                                mark_gateway_reconnecting(
                                    &state,
                                    "timed out reconnecting to discord gateway",
                                )
                                .await;
                                continue;
                            }
                        }
                    }
                };
                let (stream, _) = stream;
                let (mut write, mut read) = stream.split();
                let mut heartbeat: Option<tokio::time::Interval> = None;
                let mut hello_payload = if use_resume {
                    build_resume_payload(&state).await
                } else {
                    None
                };
                let mut reconnect_with_resume = true;

                loop {
                    tokio::select! {
                        maybe_command = command_rx.recv() => {
                            match maybe_command {
                                Some(GatewayCommand::Send(payload)) => {
                                    if write.send(Message::Text(payload.to_string().into())).await.is_err() {
                                        mark_gateway_reconnecting(&state, "failed to write to discord gateway").await;
                                        break;
                                    }
                                }
                                Some(GatewayCommand::Close) | None => {
                                    let _ = write.close().await;
                                    return;
                                }
                            }
                        }
                        _ = async {
                            if let Some(interval) = heartbeat.as_mut() {
                                interval.tick().await;
                            }
                        }, if heartbeat.is_some() => {
                            let seq = state.read().await.sequence;
                            let heartbeat_payload = json!({ "op": 1, "d": seq });
                            if write.send(Message::Text(heartbeat_payload.to_string().into())).await.is_err() {
                                mark_gateway_reconnecting(&state, "failed to send heartbeat").await;
                                break;
                            }
                        }
                        maybe_message = read.next() => {
                            match maybe_message {
                                Some(Ok(message)) => {
                                    match message {
                                        Message::Text(text) => {
                                            if let Err(error) = handle_gateway_message(
                                                &state,
                                                event_recorder.as_deref(),
                                                &mut write,
                                                &mut heartbeat,
                                                &mut hello_payload,
                                                text.to_string(),
                                            ).await {
                                                match error.as_str() {
                                                    INVALID_SESSION_ERROR => {
                                                        prepare_gateway_reidentify(&state, error)
                                                            .await;
                                                        reconnect_with_resume = false;
                                                        break;
                                                    }
                                                    INVALID_SESSION_RESUMABLE_ERROR => {
                                                        mark_gateway_reconnecting(
                                                            &state, error,
                                                        )
                                                        .await;
                                                        reconnect_with_resume = true;
                                                        break;
                                                    }
                                                    _ => {
                                                        mark_gateway_reconnecting(&state, error)
                                                            .await;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        Message::Close(_) => {
                                            mark_gateway_reconnecting(&state, "discord gateway closed").await;
                                            break;
                                        }
                                        Message::Ping(payload) => {
                                            if write.send(Message::Pong(payload)).await.is_err() {
                                                mark_gateway_reconnecting(&state, "failed to answer ping").await;
                                                break;
                                            }
                                        }
                                        Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                                    }
                                }
                                Some(Err(error)) => {
                                    mark_gateway_reconnecting(&state, format!("discord gateway read failed: {error}")).await;
                                    break;
                                }
                                None => {
                                    mark_gateway_reconnecting(&state, "discord gateway ended").await;
                                    break;
                                }
                            }
                        }
                    }
                }
                use_resume = reconnect_with_resume;
            }
        });
        *self.task.lock().await = Some(task);

        self.wait_for(
            |state| state.health.logged_in,
            "discord gateway identify did not complete",
        )
        .await?;

        self.send_command(build_voice_leave_update()).await?;
        let _ = self
            .wait_for_with_timeout(
                VOICE_STATE_RESET_TIMEOUT,
                |state| !state.health.joined_voice && !has_voice_session_resources(state),
                "voice session did not clear after reset",
            )
            .await;

        self.send_command(build_voice_state_update(&config)).await?;

        self.wait_for(
            |state| {
                state.health.logged_in
                    && state.health.joined_voice
                    && has_voice_session_resources(state)
            },
            "voice session did not become ready",
        )
        .await
    }
}

impl GatewayEventRecorder {
    fn from_env() -> Option<Self> {
        let path = env::var(GATEWAY_EVENT_LOG_ENV).ok()?;
        if path.trim().is_empty() {
            return None;
        }
        Some(Self {
            path: Arc::<str>::from(path),
        })
    }

    async fn record(&self, entry: Value) {
        let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path.as_ref())
            .await
        else {
            return;
        };
        let Ok(line) = serde_json::to_vec(&entry) else {
            return;
        };
        let _ = file.write_all(&line).await;
        let _ = file.write_all(b"\n").await;
    }
}

async fn snapshot_gateway_state(state: &Arc<RwLock<GatewayState>>) -> GatewayStateSnapshot {
    let state = state.read().await;
    GatewayStateSnapshot {
        logged_in: state.health.logged_in,
        joined_voice: state.health.joined_voice,
        streaming: state.health.streaming,
        state: state.health.state,
        last_error: state.health.last_error.clone(),
        self_user_id: state.self_user_id.clone(),
        gateway_session_id: state.gateway_session_id.clone(),
        resume_gateway_url: state.resume_gateway_url.clone(),
        sequence: state.sequence,
        pending_voice_rejoin: state.pending_voice_rejoin,
        pending_stream_restart: state.pending_stream_restart,
        voice_session_id: redact_optional_secret(state.voice_session_id.as_deref()),
        voice_endpoint: state.voice_endpoint.clone(),
        voice_token: redact_optional_secret(state.voice_token.as_deref()),
        stream_key: state.stream_key.clone(),
        stream_server_id: state.stream_server_id.clone(),
        stream_rtc_channel_id: state.stream_rtc_channel_id.clone(),
        stream_endpoint: state.stream_endpoint.clone(),
        stream_token: redact_optional_secret(state.stream_token.as_deref()),
    }
}

fn gateway_event_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:03}Z", now.as_secs(), now.subsec_millis())
}

fn redact_optional_secret(value: Option<&str>) -> Option<String> {
    value.map(|_| "[redacted]".to_owned())
}

fn redact_gateway_value(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| {
                    let value = if is_secret_key(&key) {
                        Value::String("[redacted]".to_owned())
                    } else {
                        redact_gateway_value(value)
                    };
                    (key, value)
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(redact_gateway_value).collect()),
        other => other,
    }
}

fn is_secret_key(key: &str) -> bool {
    matches!(key, "token" | "voice_token" | "stream_token" | "session_id" | "resume_gateway_url")
        || key.ends_with("_token")
        || key.ends_with("_session_id")
}

async fn handle_gateway_message<S>(
    state: &Arc<RwLock<GatewayState>>,
    event_recorder: Option<&GatewayEventRecorder>,
    write: &mut S,
    heartbeat: &mut Option<tokio::time::Interval>,
    hello_payload: &mut Option<Value>,
    text: String,
) -> Result<(), String>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let payload: Value =
        serde_json::from_str(&text).map_err(|error| format!("invalid gateway payload: {error}"))?;
    if let Some(recorder) = event_recorder {
        recorder
            .record(json!({
                "ts": gateway_event_timestamp(),
                "kind": "gateway_incoming",
                "payload": redact_gateway_value(payload.clone()),
                "state": snapshot_gateway_state(state).await,
            }))
            .await;
    }
    let op = payload
        .get("op")
        .and_then(Value::as_i64)
        .ok_or_else(|| "gateway payload missing opcode".to_owned())?;

    if let Some(seq) = payload.get("s").and_then(Value::as_u64) {
        state.write().await.sequence = Some(seq);
    }

    match op {
        1 => {
            let seq = state.read().await.sequence;
            let heartbeat_payload = json!({ "op": 1, "d": seq });
            write
                .send(Message::Text(heartbeat_payload.to_string().into()))
                .await
                .map_err(|error| format!("failed to answer heartbeat request: {error}"))?;
        }
        10 => {
            let interval_ms = payload
                .get("d")
                .and_then(|value| value.get("heartbeat_interval"))
                .and_then(Value::as_u64)
                .ok_or_else(|| "gateway hello missing heartbeat interval".to_owned())?;
            *heartbeat = Some(tokio::time::interval(Duration::from_millis(interval_ms)));
            let identify = match hello_payload.take() {
                Some(payload) => payload,
                None => build_identify_payload(state).await?,
            };
            write
                .send(Message::Text(identify.to_string().into()))
                .await
                .map_err(|error| format!("failed to send identify: {error}"))?;
        }
        0 => {
            let event_type = payload
                .get("t")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let data = payload.get("d").cloned().unwrap_or(Value::Null);
            handle_dispatch_event(state, &event_type, data).await?;
            if let Some(recorder) = event_recorder {
                recorder
                    .record(json!({
                        "ts": gateway_event_timestamp(),
                        "kind": "gateway_dispatch_applied",
                        "event_type": event_type,
                        "state": snapshot_gateway_state(state).await,
                    }))
                    .await;
            }
            for follow_up in build_recovery_commands(state, &event_type).await {
                if let Some(recorder) = event_recorder {
                recorder
                    .record(json!({
                        "ts": gateway_event_timestamp(),
                        "kind": "gateway_recovery_command",
                        "event_type": event_type,
                        "payload": redact_gateway_value(follow_up.clone()),
                        "state": snapshot_gateway_state(state).await,
                    }))
                    .await;
                }
                write
                    .send(Message::Text(follow_up.to_string().into()))
                    .await
                    .map_err(|error| format!("failed to send gateway recovery command: {error}"))?;
            }
        }
        7 => return Err("discord requested reconnect".to_owned()),
        9 => {
            let resumable = payload.get("d").and_then(Value::as_bool).unwrap_or(false);
            let error = if resumable {
                INVALID_SESSION_RESUMABLE_ERROR
            } else {
                INVALID_SESSION_ERROR
            };
            return Err(error.to_owned());
        }
        11 => {}
        _ => debug!(opcode = op, "ignored discord gateway opcode"),
    }

    Ok(())
}

async fn handle_dispatch_event(
    state: &Arc<RwLock<GatewayState>>,
    event_type: &str,
    data: Value,
) -> Result<(), String> {
    match event_type {
        "READY" => {
            let mut state = state.write().await;
            state.self_user_id = data
                .get("user")
                .and_then(|value| value.get("id"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or(state.self_user_id.take());
            state.gateway_session_id = data
                .get("session_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            state.resume_gateway_url = data
                .get("resume_gateway_url")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            state.health.logged_in = true;
            restore_runtime_health(&mut state);
        }
        "RESUMED" => {
            let mut state = state.write().await;
            state.health.logged_in = true;
            restore_runtime_health(&mut state);
        }
        "VOICE_STATE_UPDATE" => {
            let mut state = state.write().await;
            let Some((session, self_user_id)) = active_session_for_user(&state) else {
                return Ok(());
            };
            if data.get("user_id").and_then(Value::as_str) != Some(self_user_id) {
                return Ok(());
            }
            if data.get("channel_id").and_then(Value::as_str) != Some(session.channel_id.as_str()) {
                prepare_voice_rejoin(&mut state);
                clear_voice_session(&mut state);
                return Ok(());
            }
            let next_voice_session_id = data
                .get("session_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            if next_voice_session_id != state.voice_session_id {
                prepare_voice_session_refresh(&mut state);
            }
            state.voice_session_id = next_voice_session_id;
            refresh_voice_state(&mut state);
        }
        "VOICE_SERVER_UPDATE" => {
            let mut state = state.write().await;
            let Some((session, _)) = active_session_for_user(&state) else {
                return Ok(());
            };
            if data.get("guild_id").and_then(Value::as_str) != session_guild_id(session) {
                return Ok(());
            }
            let next_voice_endpoint = data
                .get("endpoint")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let next_voice_token = data
                .get("token")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            if voice_server_details_changed(&state, next_voice_endpoint.as_deref(), next_voice_token.as_deref()) {
                prepare_voice_server_refresh(&mut state);
            }
            state.voice_endpoint = next_voice_endpoint;
            state.voice_token = next_voice_token;
            refresh_voice_state(&mut state);
        }
        "STREAM_CREATE" => {
            let mut state = state.write().await;
            let Some((session, self_user_id)) = active_session_for_user(&state) else {
                return Ok(());
            };
            let stream_key = data
                .get("stream_key")
                .and_then(Value::as_str)
                .ok_or_else(|| "stream create missing stream_key".to_owned())?;
            let parsed = parse_stream_key(stream_key)?;
            if !stream_event_targets_session(&parsed, session, self_user_id) {
                return Ok(());
            }
            clear_stream_session(&mut state);
            state.stream_key = Some(stream_key.to_owned());
            state.stream_server_id = data
                .get("rtc_server_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            state.stream_rtc_channel_id = data
                .get("rtc_channel_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            refresh_streaming_state(&mut state);
        }
        "STREAM_SERVER_UPDATE" => {
            let mut state = state.write().await;
            let Some((session, self_user_id)) = active_session_for_user(&state) else {
                return Ok(());
            };
            let stream_key = data
                .get("stream_key")
                .and_then(Value::as_str)
                .ok_or_else(|| "stream server update missing stream_key".to_owned())?;
            let parsed = parse_stream_key(stream_key)?;
            if !stream_event_targets_session(&parsed, session, self_user_id) {
                return Ok(());
            }
            if state.stream_key.as_deref() != Some(stream_key) {
                prepare_stream_session_refresh(&mut state);
            }
            state.stream_key = Some(stream_key.to_owned());
            state.stream_endpoint = data
                .get("endpoint")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            state.stream_token = data
                .get("token")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            refresh_streaming_state(&mut state);
        }
        _ => {}
    }
    Ok(())
}

async fn reconnect_gateway_url(state: &Arc<RwLock<GatewayState>>, default_url: &Url) -> Url {
    let state = state.read().await;
    let resume_url = state
        .resume_gateway_url
        .as_deref()
        .and_then(|url| {
            Url::parse(&format!(
                "{}/?v={DISCORD_API_VERSION}&encoding=json",
                url.trim_end_matches('/')
            ))
            .ok()
        });
    resume_url.unwrap_or_else(|| default_url.clone())
}

async fn build_resume_payload(state: &Arc<RwLock<GatewayState>>) -> Option<Value> {
    let state = state.read().await;
    let session = state.health.session.as_ref()?;
    let session_id = state.gateway_session_id.as_ref()?;
    let seq = state.sequence?;
    Some(json!({
        "op": 6,
        "d": {
            "token": session.token,
            "session_id": session_id,
            "seq": seq,
        }
    }))
}

async fn build_recovery_commands(
    state: &Arc<RwLock<GatewayState>>,
    event_type: &str,
) -> Vec<Value> {
    let mut state = state.write().await;
    match event_type {
        "READY" if state.pending_voice_rejoin => {
            let Some(session) = state.health.session.clone() else {
                clear_pending_rejoin_flags(&mut state);
                return Vec::new();
            };
            state.pending_voice_rejoin = false;
            vec![build_voice_state_update(&session)]
        }
        "VOICE_STATE_UPDATE" if state.pending_voice_rejoin && !state.health.joined_voice => {
            let Some(session) = state.health.session.clone() else {
                clear_pending_rejoin_flags(&mut state);
                return Vec::new();
            };
            vec![build_voice_state_update(&session)]
        }
        "VOICE_SERVER_UPDATE" if state.pending_stream_restart && state.health.joined_voice => {
            let Some(session) = state.health.session.clone() else {
                state.pending_stream_restart = false;
                return Vec::new();
            };
            let Some(self_user_id) = state.self_user_id.clone() else {
                return Vec::new();
            };
            state.pending_stream_restart = false;
            build_stream_start_commands(&session, &self_user_id)
        }
        "STREAM_SERVER_UPDATE" if state.pending_stream_restart && state.health.joined_voice && !state.health.streaming => {
            let Some(session) = state.health.session.clone() else {
                state.pending_stream_restart = false;
                return Vec::new();
            };
            let Some(self_user_id) = state.self_user_id.clone() else {
                return Vec::new();
            };
            state.pending_stream_restart = false;
            build_stream_start_commands(&session, &self_user_id)
        }
        _ => Vec::new(),
    }
}

fn stream_start_context(
    state: &GatewayState,
) -> Result<(&SessionConfig, &str), GatewayError> {
    let session = state.health.session.as_ref().ok_or_else(|| {
        GatewayError::Message("discord stream start requires an active session".to_owned())
    })?;
    let self_user_id = state.self_user_id.as_deref().ok_or_else(|| {
        GatewayError::Message("discord stream start is missing self user id".to_owned())
    })?;
    if !state.health.joined_voice {
        return Err(GatewayError::Message(
            "discord stream start requires a joined voice state".to_owned(),
        ));
    }
    Ok((session, self_user_id))
}

fn active_session_for_user(state: &GatewayState) -> Option<(&SessionConfig, &str)> {
    let session = state.health.session.as_ref()?;
    let self_user_id = state.self_user_id.as_deref()?;
    Some((session, self_user_id))
}

async fn build_identify_payload(
    state: &Arc<RwLock<GatewayState>>,
) -> Result<Value, String> {
    let session = {
        let state = state.read().await;
        state
            .health
            .session
            .as_ref()
            .cloned()
            .ok_or_else(|| "session missing during identify".to_owned())?
    };
    Ok(build_identify_payload_from_token(&session.token))
}

fn clear_pending_rejoin_flags(state: &mut GatewayState) {
    state.pending_voice_rejoin = false;
    state.pending_stream_restart = false;
}

fn voice_server_details_changed(
    state: &GatewayState,
    next_endpoint: Option<&str>,
    next_token: Option<&str>,
) -> bool {
    (state.voice_endpoint.as_deref(), state.voice_token.as_deref()) != (next_endpoint, next_token)
}

fn refresh_voice_state(state: &mut GatewayState) {
    state.health.joined_voice = has_voice_session_resources(state);
    if state.health.joined_voice {
        state.health.state = ConnectionState::Connected;
    }
}

fn refresh_streaming_state(state: &mut GatewayState) {
    state.health.streaming = has_stream_session_resources(state);
    if state.health.streaming {
        state.health.state = ConnectionState::Streaming;
    }
}

fn restore_runtime_health(state: &mut GatewayState) {
    state.health.last_error = None;
    state.health.state = if state.health.streaming {
        ConnectionState::Streaming
    } else if state.health.logged_in {
        ConnectionState::Connected
    } else {
        ConnectionState::Disconnected
    };
}

fn has_voice_session_resources(state: &GatewayState) -> bool {
    state.voice_session_id.is_some()
        && state.voice_endpoint.is_some()
        && state.voice_token.is_some()
}

fn has_stream_session_resources(state: &GatewayState) -> bool {
    state.stream_key.is_some()
        && state.stream_server_id.is_some()
        && state.stream_endpoint.is_some()
        && state.stream_token.is_some()
}

fn build_media_session(state: &GatewayState) -> Result<GatewayMediaSession, GatewayError> {
    let session = state.health.session.clone().ok_or_else(|| {
        GatewayError::Message("discord media session is missing login session".to_owned())
    })?;
    if !state.health.streaming {
        return Err(GatewayError::Message(
            "discord media session requires an active stream session".to_owned(),
        ));
    }

    Ok(GatewayMediaSession {
        user_id: state.self_user_id.clone().ok_or_else(|| {
            GatewayError::Message("discord media session is missing self user id".to_owned())
        })?,
        guild_id: session_guild_id(&session).map(ToOwned::to_owned),
        channel_id: session.channel_id,
        rtc_channel_id: state.stream_rtc_channel_id.clone().ok_or_else(|| {
            GatewayError::Message("discord media session is missing stream rtc channel id".to_owned())
        })?,
        stream_kind: session.stream_kind,
        voice_session_id: state.voice_session_id.clone().ok_or_else(|| {
            GatewayError::Message("discord media session is missing voice session id".to_owned())
        })?,
        voice_endpoint: state.voice_endpoint.clone().ok_or_else(|| {
            GatewayError::Message("discord media session is missing voice endpoint".to_owned())
        })?,
        voice_token: state.voice_token.clone().ok_or_else(|| {
            GatewayError::Message("discord media session is missing voice token".to_owned())
        })?,
        stream_key: state.stream_key.clone().ok_or_else(|| {
            GatewayError::Message("discord media session is missing stream key".to_owned())
        })?,
        stream_server_id: state.stream_server_id.clone().ok_or_else(|| {
            GatewayError::Message("discord media session is missing stream server id".to_owned())
        })?,
        stream_endpoint: state.stream_endpoint.clone().ok_or_else(|| {
            GatewayError::Message("discord media session is missing stream endpoint".to_owned())
        })?,
        stream_token: state.stream_token.clone().ok_or_else(|| {
            GatewayError::Message("discord media session is missing stream token".to_owned())
        })?,
    })
}

fn stream_event_targets_session(
    parsed: &ParsedStreamKey,
    session: &SessionConfig,
    self_user_id: &str,
) -> bool {
    parsed.channel_id == session.channel_id
        && parsed.user_id == self_user_id
        && parsed.guild_id.as_deref() == session_guild_id(session)
}

async fn mark_gateway_reconnecting(state: &Arc<RwLock<GatewayState>>, message: impl Into<String>) {
    let message = message.into();
    warn!(error = %message, "discord gateway reconnecting");
    let mut state = state.write().await;
    state.health.state = ConnectionState::Connecting;
    state.health.last_error = Some(message);
}

async fn prepare_gateway_reidentify(
    state: &Arc<RwLock<GatewayState>>,
    message: impl Into<String>,
) {
    let message = message.into();
    warn!(error = %message, "discord gateway re-identifying after invalid session");
    let mut state = state.write().await;
    state.pending_voice_rejoin = state.health.session.is_some();
    state.pending_stream_restart = state.health.streaming;
    clear_voice_session(&mut state);
    state.health.logged_in = false;
    state.health.state = ConnectionState::Connecting;
    state.health.last_error = Some(message);
    state.gateway_session_id = None;
    state.resume_gateway_url = None;
    state.sequence = None;
}

fn prepare_voice_rejoin(state: &mut GatewayState) {
    state.pending_voice_rejoin = state.health.session.is_some();
    state.pending_stream_restart = state.health.streaming;
}

fn prepare_voice_session_refresh(state: &mut GatewayState) {
    let should_restart_stream = state.health.streaming;
    state.voice_endpoint = None;
    state.voice_token = None;
    state.health.joined_voice = false;
    state.pending_stream_restart |= should_restart_stream;
    clear_stream_session(state);
}

fn prepare_voice_server_refresh(state: &mut GatewayState) {
    let should_restart_stream = state.health.streaming;
    state.pending_stream_restart |= should_restart_stream;
    clear_stream_session(state);
}

fn prepare_stream_session_refresh(state: &mut GatewayState) {
    state.pending_stream_restart |= state.health.streaming;
    clear_stream_session(state);
}

fn clear_stream_session(state: &mut GatewayState) {
    state.stream_key = None;
    state.stream_server_id = None;
    state.stream_rtc_channel_id = None;
    state.stream_endpoint = None;
    state.stream_token = None;
    state.health.streaming = false;
    state.health.state = derive_connected_state(state);
}

fn clear_voice_session(state: &mut GatewayState) {
    state.voice_session_id = None;
    state.voice_endpoint = None;
    state.voice_token = None;
    state.health.joined_voice = false;
    clear_stream_session(state);
    state.health.state = derive_connected_state(state);
}

fn derive_connected_state(state: &GatewayState) -> ConnectionState {
    if state.health.joined_voice || state.health.logged_in {
        ConnectionState::Connected
    } else {
        ConnectionState::Disconnected
    }
}

fn build_identify_payload_from_token(token: &str) -> Value {
    json!({
        "op": 2,
        "d": {
            "token": token,
            "capabilities": 0,
            "properties": {
                "os": "Windows",
                "browser": "Discord Client",
                "device": "discord-rs-streamer",
                "release_channel": "stable",
                "client_version": "1.0.9210",
                "os_version": "10.0.19045",
                "os_arch": "x64",
                "app_arch": "x64",
                "system_locale": "en-US",
                "has_client_mods": false,
                "browser_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) discord-rs-streamer/0.1",
                "browser_version": "35.3.0",
                "client_build_number": 455964,
                "native_build_number": 69976,
                "client_event_source": Value::Null,
                "client_app_state": "focused"
            },
            "compress": false,
            "presence": {
                "status": "online",
                "since": 0,
                "activities": [],
                "afk": false
            },
            "client_state": {
                "guild_versions": {}
            }
        }
    })
}

fn build_voice_state_update(session: &SessionConfig) -> Value {
    json!({
        "op": 4,
        "d": {
            "guild_id": json_guild_id(&session.guild_id),
            "channel_id": session.channel_id,
            "self_mute": false,
            "self_deaf": true,
            "self_video": matches!(session.stream_kind, StreamKind::Camera),
        }
    })
}

fn build_voice_leave_update() -> Value {
    json!({
        "op": 4,
        "d": {
            "guild_id": Value::Null,
            "channel_id": Value::Null,
            "self_mute": true,
            "self_deaf": false,
            "self_video": false,
        }
    })
}

fn build_stream_start_commands(session: &SessionConfig, self_user_id: &str) -> Vec<Value> {
    let guild_id = session_guild_id(session);
    let stream_type = stream_type_for_session(session);
    let stream_key = generate_stream_key(stream_type, guild_id, &session.channel_id, self_user_id);
    vec![
        json!({
            "op": 18,
            "d": {
                "type": stream_type,
                "guild_id": json_guild_id(&session.guild_id),
                "channel_id": session.channel_id,
                "preferred_region": Value::Null,
            }
        }),
        json!({
            "op": 22,
            "d": {
                "stream_key": stream_key,
                "paused": false,
            }
        }),
    ]
}

fn stream_type_for_session(session: &SessionConfig) -> &'static str {
    if session_guild_id(session).is_some() {
        "guild"
    } else {
        "call"
    }
}

fn session_guild_id(session: &SessionConfig) -> Option<&str> {
    (!session.guild_id.is_empty()).then_some(session.guild_id.as_str())
}

fn json_guild_id(guild_id: &str) -> Value {
    if guild_id.is_empty() {
        Value::Null
    } else {
        Value::String(guild_id.to_owned())
    }
}

fn generate_stream_key(
    stream_type: &str,
    guild_id: Option<&str>,
    channel_id: &str,
    user_id: &str,
) -> String {
    match guild_id {
        Some(guild_id) => format!("{stream_type}:{guild_id}:{channel_id}:{user_id}"),
        None => format!("{stream_type}:{channel_id}:{user_id}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedStreamKey {
    guild_id: Option<String>,
    channel_id: String,
    user_id: String,
}

fn parse_stream_key(stream_key: &str) -> Result<ParsedStreamKey, String> {
    let mut parts = stream_key.split(':');
    let Some(stream_type) = parts.next() else {
        return Err("stream key missing type".to_owned());
    };
    match stream_type {
        "guild" => {
            let guild_id = parse_stream_key_part(&mut parts, "guild id")?;
            let channel_id = parse_stream_key_part(&mut parts, "channel id")?;
            let user_id = parse_stream_key_part(&mut parts, "user id")?;
            Ok(ParsedStreamKey {
                guild_id: Some(guild_id.to_owned()),
                channel_id: channel_id.to_owned(),
                user_id: user_id.to_owned(),
            })
        }
        "call" => {
            let channel_id = parse_stream_key_part(&mut parts, "channel id")?;
            let user_id = parse_stream_key_part(&mut parts, "user id")?;
            Ok(ParsedStreamKey {
                guild_id: None,
                channel_id: channel_id.to_owned(),
                user_id: user_id.to_owned(),
            })
        }
        other => Err(format!("unsupported stream key type: {other}")),
    }
}

fn parse_stream_key_part<'a>(
    parts: &mut std::str::Split<'a, char>,
    label: &str,
) -> Result<&'a str, String> {
    parts
        .next()
        .ok_or_else(|| format!("stream key missing {label}"))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use futures::channel::mpsc;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};

    #[test]
    fn parse_guild_stream_key_extracts_parts() {
        let parsed = parse_stream_key("guild:1:2:3").expect("parsed");
        assert_eq!(parsed.guild_id.as_deref(), Some("1"));
        assert_eq!(parsed.channel_id, "2");
        assert_eq!(parsed.user_id, "3");
    }

    #[test]
    fn parse_call_stream_key_extracts_parts() {
        let parsed = parse_stream_key("call:2:3").expect("parsed");
        assert_eq!(parsed.guild_id, None);
        assert_eq!(parsed.channel_id, "2");
        assert_eq!(parsed.user_id, "3");
    }

    #[test]
    fn identify_payload_contains_token() {
        let payload = build_identify_payload_from_token("token");
        assert_eq!(payload["op"], json!(2));
        assert_eq!(payload["d"]["token"], json!("token"));
    }

    #[test]
    fn gateway_event_redaction_masks_secret_fields() {
        let value = redact_gateway_value(json!({
            "token": "secret-token",
            "session_id": "session-123",
            "nested": {
                "voice_token": "voice-secret",
                "keep": "value"
            }
        }));

        assert_eq!(value["token"], json!("[redacted]"));
        assert_eq!(value["session_id"], json!("[redacted]"));
        assert_eq!(value["nested"]["voice_token"], json!("[redacted]"));
        assert_eq!(value["nested"]["keep"], json!("value"));
    }

    #[tokio::test]
    async fn voice_dispatch_marks_session_joined() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.self_user_id = Some("42".to_owned());
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
        }

        handle_dispatch_event(
            &state,
            "VOICE_STATE_UPDATE",
            json!({
                "user_id": "42",
                "channel_id": "2",
                "session_id": "voice-session"
            }),
        )
        .await
        .expect("voice state");
        handle_dispatch_event(
            &state,
            "VOICE_SERVER_UPDATE",
            json!({
                "guild_id": "1",
                "endpoint": "voice.example.test",
                "token": "voice-token"
            }),
        )
        .await
        .expect("voice server");

        let snapshot = state.read().await;
        assert!(snapshot.health.joined_voice);
        assert_eq!(snapshot.health.state, ConnectionState::Connected);
    }

    #[tokio::test]
    async fn stream_dispatch_marks_session_streaming() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.self_user_id = Some("42".to_owned());
            guard.health.joined_voice = true;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
        }

        handle_dispatch_event(
            &state,
            "STREAM_CREATE",
            json!({
                "stream_key": "guild:1:2:42",
                "rtc_server_id": "99"
            }),
        )
        .await
        .expect("stream create");
        handle_dispatch_event(
            &state,
            "STREAM_SERVER_UPDATE",
            json!({
                "stream_key": "guild:1:2:42",
                "endpoint": "stream.example.test",
                "token": "stream-token"
            }),
        )
        .await
        .expect("stream server");

        let snapshot = state.read().await;
        assert!(snapshot.health.streaming);
        assert_eq!(snapshot.health.state, ConnectionState::Streaming);
    }

    #[tokio::test]
    async fn hello_sends_identify_payload() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
        }
        let (mut sink, mut stream) = mpsc::unbounded::<Message>();
        let mut heartbeat = None;
        let mut hello_payload = None;

        handle_gateway_message(
            &state,
            None,
            &mut sink,
            &mut heartbeat,
            &mut hello_payload,
            json!({"op":10,"d":{"heartbeat_interval":45000}}).to_string(),
        )
        .await
        .expect("hello");

        let sent = stream.next().await.expect("identify frame");
        let Message::Text(text) = sent else {
            panic!("expected text frame");
        };
        let payload: Value = serde_json::from_str(text.as_ref()).expect("json");
        assert_eq!(payload["op"], json!(2));
        assert!(heartbeat.is_some());
    }

    #[tokio::test]
    async fn opcode_one_triggers_immediate_heartbeat() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        state.write().await.sequence = Some(77);
        let (mut sink, mut stream) = mpsc::unbounded::<Message>();
        let mut heartbeat = None;
        let mut hello_payload = None;

        handle_gateway_message(
            &state,
            None,
            &mut sink,
            &mut heartbeat,
            &mut hello_payload,
            json!({"op":1,"d":null}).to_string(),
        )
        .await
        .expect("opcode 1");

        let sent = stream.next().await.expect("heartbeat frame");
        let Message::Text(text) = sent else {
            panic!("expected text frame");
        };
        let payload: Value = serde_json::from_str(text.as_ref()).expect("json");
        assert_eq!(payload, json!({"op":1,"d":77}));
    }

    #[tokio::test]
    async fn voice_state_update_clears_joined_voice_when_user_leaves_channel() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.self_user_id = Some("42".to_owned());
            guard.health.logged_in = true;
            guard.health.joined_voice = true;
            guard.health.streaming = true;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
            guard.voice_session_id = Some("voice-session".to_owned());
            guard.voice_endpoint = Some("voice.example.test".to_owned());
            guard.voice_token = Some("voice-token".to_owned());
            guard.stream_key = Some("guild:1:2:42".to_owned());
            guard.stream_endpoint = Some("stream.example.test".to_owned());
            guard.stream_token = Some("stream-token".to_owned());
        }

        handle_dispatch_event(
            &state,
            "VOICE_STATE_UPDATE",
            json!({
                "user_id": "42",
                "channel_id": null,
                "session_id": "voice-session"
            }),
        )
        .await
        .expect("voice leave");

        let snapshot = state.read().await;
        assert!(!snapshot.health.joined_voice);
        assert!(!snapshot.health.streaming);
        assert!(snapshot.voice_session_id.is_none());
        assert!(snapshot.stream_key.is_none());
        assert!(snapshot.pending_voice_rejoin);
        assert!(snapshot.pending_stream_restart);
    }

    #[tokio::test]
    async fn voice_state_refresh_clears_stale_voice_and_stream_resources() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.self_user_id = Some("42".to_owned());
            guard.health.logged_in = true;
            guard.health.joined_voice = true;
            guard.health.streaming = true;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
            guard.voice_session_id = Some("voice-session".to_owned());
            guard.voice_endpoint = Some("voice.example.test".to_owned());
            guard.voice_token = Some("voice-token".to_owned());
            guard.stream_key = Some("guild:1:2:42".to_owned());
            guard.stream_server_id = Some("stream-server".to_owned());
            guard.stream_endpoint = Some("stream.example.test".to_owned());
            guard.stream_token = Some("stream-token".to_owned());
        }

        handle_dispatch_event(
            &state,
            "VOICE_STATE_UPDATE",
            json!({
                "user_id": "42",
                "channel_id": "2",
                "session_id": "voice-session-2"
            }),
        )
        .await
        .expect("voice refresh");

        let snapshot = state.read().await;
        assert_eq!(snapshot.voice_session_id.as_deref(), Some("voice-session-2"));
        assert!(snapshot.voice_endpoint.is_none());
        assert!(snapshot.voice_token.is_none());
        assert!(!snapshot.health.joined_voice);
        assert!(snapshot.stream_key.is_none());
        assert!(snapshot.stream_server_id.is_none());
        assert!(snapshot.stream_endpoint.is_none());
        assert!(snapshot.stream_token.is_none());
        assert!(!snapshot.health.streaming);
        assert!(snapshot.pending_stream_restart);
    }

    #[tokio::test]
    async fn stream_create_with_new_key_clears_stale_stream_server_resources() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.self_user_id = Some("42".to_owned());
            guard.health.logged_in = true;
            guard.health.joined_voice = true;
            guard.health.streaming = true;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
            guard.stream_key = Some("guild:1:2:42".to_owned());
            guard.stream_server_id = Some("stream-server".to_owned());
            guard.stream_endpoint = Some("stream.example.test".to_owned());
            guard.stream_token = Some("stream-token".to_owned());
        }

        handle_dispatch_event(
            &state,
            "STREAM_CREATE",
            json!({
                "stream_key": "guild:1:2:42",
                "rtc_server_id": "stream-server-2"
            }),
        )
        .await
        .expect("stream create");

        let snapshot = state.read().await;
        assert_eq!(snapshot.stream_key.as_deref(), Some("guild:1:2:42"));
        assert_eq!(snapshot.stream_server_id.as_deref(), Some("stream-server-2"));
        assert!(snapshot.stream_endpoint.is_none());
        assert!(snapshot.stream_token.is_none());
        assert!(!snapshot.health.streaming);
    }

    #[tokio::test]
    async fn voice_server_refresh_clears_stale_stream_resources_and_marks_restart() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.self_user_id = Some("42".to_owned());
            guard.health.logged_in = true;
            guard.health.joined_voice = true;
            guard.health.streaming = true;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
            guard.voice_session_id = Some("voice-session".to_owned());
            guard.voice_endpoint = Some("voice.example.test".to_owned());
            guard.voice_token = Some("voice-token".to_owned());
            guard.stream_key = Some("guild:1:2:42".to_owned());
            guard.stream_server_id = Some("stream-server".to_owned());
            guard.stream_endpoint = Some("stream.example.test".to_owned());
            guard.stream_token = Some("stream-token".to_owned());
        }

        handle_dispatch_event(
            &state,
            "VOICE_SERVER_UPDATE",
            json!({
                "guild_id": "1",
                "endpoint": "voice2.example.test",
                "token": "voice-token-2"
            }),
        )
        .await
        .expect("voice server refresh");

        let snapshot = state.read().await;
        assert!(snapshot.health.joined_voice);
        assert!(!snapshot.health.streaming);
        assert_eq!(snapshot.voice_endpoint.as_deref(), Some("voice2.example.test"));
        assert_eq!(snapshot.voice_token.as_deref(), Some("voice-token-2"));
        assert!(snapshot.stream_key.is_none());
        assert!(snapshot.stream_server_id.is_none());
        assert!(snapshot.stream_endpoint.is_none());
        assert!(snapshot.stream_token.is_none());
        assert!(snapshot.pending_stream_restart);
    }

    #[tokio::test]
    async fn voice_server_refresh_without_changes_keeps_stream_resources() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.self_user_id = Some("42".to_owned());
            guard.health.logged_in = true;
            guard.health.joined_voice = true;
            guard.health.streaming = true;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
            guard.voice_session_id = Some("voice-session".to_owned());
            guard.voice_endpoint = Some("voice.example.test".to_owned());
            guard.voice_token = Some("voice-token".to_owned());
            guard.stream_key = Some("guild:1:2:42".to_owned());
            guard.stream_server_id = Some("stream-server".to_owned());
            guard.stream_endpoint = Some("stream.example.test".to_owned());
            guard.stream_token = Some("stream-token".to_owned());
        }

        handle_dispatch_event(
            &state,
            "VOICE_SERVER_UPDATE",
            json!({
                "guild_id": "1",
                "endpoint": "voice.example.test",
                "token": "voice-token"
            }),
        )
        .await
        .expect("voice server unchanged");

        let snapshot = state.read().await;
        assert!(snapshot.health.joined_voice);
        assert!(snapshot.health.streaming);
        assert_eq!(snapshot.stream_key.as_deref(), Some("guild:1:2:42"));
        assert_eq!(snapshot.stream_endpoint.as_deref(), Some("stream.example.test"));
        assert_eq!(snapshot.stream_token.as_deref(), Some("stream-token"));
        assert!(!snapshot.pending_stream_restart);
    }

    #[tokio::test]
    async fn stream_server_update_refreshes_token_and_endpoint_with_existing_server_id() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.self_user_id = Some("42".to_owned());
            guard.health.logged_in = true;
            guard.health.joined_voice = true;
            guard.health.streaming = true;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
            guard.stream_key = Some("guild:1:2:42".to_owned());
            guard.stream_server_id = Some("stream-server".to_owned());
            guard.stream_endpoint = Some("stream.example.test".to_owned());
            guard.stream_token = Some("stream-token".to_owned());
        }

        handle_dispatch_event(
            &state,
            "STREAM_SERVER_UPDATE",
            json!({
                "stream_key": "guild:1:2:42",
                "endpoint": "stream2.example.test",
                "token": "stream-token-2"
            }),
        )
        .await
        .expect("stream refresh");

        let snapshot = state.read().await;
        assert!(snapshot.health.streaming);
        assert_eq!(snapshot.stream_server_id.as_deref(), Some("stream-server"));
        assert_eq!(snapshot.stream_endpoint.as_deref(), Some("stream2.example.test"));
        assert_eq!(snapshot.stream_token.as_deref(), Some("stream-token-2"));
        assert!(!snapshot.pending_stream_restart);
    }

    #[tokio::test]
    async fn stream_server_update_without_server_id_is_not_streaming() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.self_user_id = Some("42".to_owned());
            guard.health.logged_in = true;
            guard.health.joined_voice = true;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
        }

        handle_dispatch_event(
            &state,
            "STREAM_SERVER_UPDATE",
            json!({
                "stream_key": "guild:1:2:42",
                "endpoint": "stream.example.test",
                "token": "stream-token"
            }),
        )
        .await
        .expect("stream update");

        let snapshot = state.read().await;
        assert!(!snapshot.health.streaming);
        assert!(snapshot.stream_server_id.is_none());
    }

    #[tokio::test]
    async fn pending_voice_rejoin_sends_voice_state_update_after_unexpected_leave() {
        let state = Arc::new(RwLock::new(GatewayState::default()));
        {
            let mut guard = state.write().await;
            guard.health.session = Some(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            });
            guard.pending_voice_rejoin = true;
        }

        let commands = build_recovery_commands(&state, "VOICE_STATE_UPDATE").await;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0]["op"], json!(4));
    }

    #[tokio::test]
    async fn media_session_requires_stream_resources_and_returns_snapshot() {
        let gateway = InMemoryGateway::new();
        gateway
            .connect(SessionConfig {
                token: "token".to_owned(),
                guild_id: "1".to_owned(),
                channel_id: "2".to_owned(),
                stream_kind: StreamKind::GoLive,
            })
            .await
            .expect("connected");
        assert!(matches!(
            gateway.media_session().await,
            Err(GatewayError::NotConnected)
        ));

        gateway.start_stream().await.expect("streaming");
        let media = gateway.media_session().await.expect("media session");
        assert_eq!(media.guild_id.as_deref(), Some("1"));
        assert_eq!(media.channel_id, "2");
        assert_eq!(media.stream_kind, StreamKind::GoLive);
    }

    #[tokio::test]
    async fn real_gateway_reconnects_with_resume_after_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let gateway_url =
            Url::parse(&format!("ws://{addr}/?v={DISCORD_API_VERSION}&encoding=json")).expect("url");
        let (resumed_tx, resumed_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept first");
            let mut ws = accept_async(stream).await.expect("ws first");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello first");

            let identify = ws.next().await.expect("identify").expect("frame");
            let Message::Text(identify) = identify else {
                panic!("expected identify");
            };
            let payload: Value = serde_json::from_str(identify.as_ref()).expect("identify json");
            assert_eq!(payload["op"], json!(2));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "READY",
                    "s": 1,
                    "d": {
                        "session_id": "gateway-session",
                        "resume_gateway_url": format!("ws://{addr}"),
                        "user": { "id": "42" }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ready");

            let payload = loop {
                let frame = ws.next().await.expect("voice state").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("voice state json");
                if payload["op"] == json!(4) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(4));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_STATE_UPDATE",
                    "s": 2,
                    "d": {
                        "user_id": "42",
                        "channel_id": "2",
                        "session_id": "voice-session"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice state update");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_SERVER_UPDATE",
                    "s": 3,
                    "d": {
                        "guild_id": "1",
                        "endpoint": "voice.example.test",
                        "token": "voice-token"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice server update");
            let _ = ws.close(None).await;

            let (stream, _) = listener.accept().await.expect("accept second");
            let mut ws = accept_async(stream).await.expect("ws second");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello second");

            let payload = loop {
                let frame = ws.next().await.expect("resume").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value = serde_json::from_str(text.as_ref()).expect("resume json");
                if payload["op"] == json!(6) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(6));
            assert_eq!(payload["d"]["session_id"], json!("gateway-session"));
            assert_eq!(payload["d"]["seq"], json!(3));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "RESUMED",
                    "s": 4,
                    "d": {}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("resumed");
            let _ = resumed_tx.send(());
            let _ = release_rx.await;
            let _ = ws.close(None).await;
        });

        let gateway = RealDiscordGateway::new();
        gateway
            .connect_with_gateway_url(
                SessionConfig {
                    token: "token".to_owned(),
                    guild_id: "1".to_owned(),
                    channel_id: "2".to_owned(),
                    stream_kind: StreamKind::GoLive,
                },
                gateway_url,
            )
            .await
            .expect("connected");

        resumed_rx.await.expect("resume observed");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let health = gateway.health().await;
            if health.logged_in
                && health.joined_voice
                && health.state == ConnectionState::Connected
                && health.last_error.is_none()
            {
                break;
            }
            assert!(Instant::now() < deadline, "gateway never reported resumed health");
            sleep(Duration::from_millis(25)).await;
        }

        let _ = release_tx.send(());
        gateway.disconnect().await.expect("disconnect");
    }

    #[tokio::test]
    async fn real_gateway_falls_back_to_identify_after_invalid_session() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let gateway_url =
            Url::parse(&format!("ws://{addr}/?v={DISCORD_API_VERSION}&encoding=json")).expect("url");
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let expected_stream_key = "guild:1:2:42".to_owned();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept first");
            let mut ws = accept_async(stream).await.expect("ws first");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello first");

            let identify = ws.next().await.expect("identify").expect("frame");
            let Message::Text(identify) = identify else {
                panic!("expected identify");
            };
            let payload: Value = serde_json::from_str(identify.as_ref()).expect("identify json");
            assert_eq!(payload["op"], json!(2));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "READY",
                    "s": 1,
                    "d": {
                        "session_id": "gateway-session",
                        "resume_gateway_url": format!("ws://{addr}"),
                        "user": { "id": "42" }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ready first");

            let payload = loop {
                let frame = ws.next().await.expect("voice state").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("voice state json");
                if payload["op"] == json!(4) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(4));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_STATE_UPDATE",
                    "s": 2,
                    "d": {
                        "user_id": "42",
                        "channel_id": "2",
                        "session_id": "voice-session"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice state update");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_SERVER_UPDATE",
                    "s": 3,
                    "d": {
                        "guild_id": "1",
                        "endpoint": "voice.example.test",
                        "token": "voice-token"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice server update");
            let payload = loop {
                let frame = ws.next().await.expect("stream create").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream create json");
                if payload["op"] == json!(18) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(18));
            let payload = loop {
                let frame = ws.next().await.expect("stream start").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream start json");
                if payload["op"] == json!(22) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(22));
            assert_eq!(payload["d"]["stream_key"], json!(expected_stream_key));
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_CREATE",
                    "s": 4,
                    "d": {
                        "stream_key": expected_stream_key,
                        "rtc_server_id": "stream-server",
                        "rtc_channel_id": "stream-channel"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream create event first");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_SERVER_UPDATE",
                    "s": 5,
                    "d": {
                        "stream_key": expected_stream_key,
                        "endpoint": "stream.example.test",
                        "token": "stream-token"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream server update first");
            let _ = ws.close(None).await;

            let (stream, _) = listener.accept().await.expect("accept second");
            let mut ws = accept_async(stream).await.expect("ws second");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello second");

            let payload = loop {
                let frame = ws.next().await.expect("resume").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value = serde_json::from_str(text.as_ref()).expect("resume json");
                if payload["op"] == json!(6) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(6));
            ws.send(Message::Text(json!({"op": 9, "d": false}).to_string().into()))
                .await
                .expect("invalid session");
            let _ = ws.close(None).await;

            let (stream, _) = listener.accept().await.expect("accept third");
            let mut ws = accept_async(stream).await.expect("ws third");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello third");

            let payload = loop {
                let frame = ws.next().await.expect("re-identify").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("re-identify json");
                if payload["op"] == json!(2) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(2));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "READY",
                    "s": 6,
                    "d": {
                        "session_id": "gateway-session-2",
                        "resume_gateway_url": format!("ws://{addr}"),
                        "user": { "id": "42" }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ready third");

            let payload = loop {
                let frame = ws.next().await.expect("voice state third").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("voice state third json");
                if payload["op"] == json!(4) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(4));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_STATE_UPDATE",
                    "s": 7,
                    "d": {
                        "user_id": "42",
                        "channel_id": "2",
                        "session_id": "voice-session-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice state update third");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_SERVER_UPDATE",
                    "s": 8,
                    "d": {
                        "guild_id": "1",
                        "endpoint": "voice.example.test",
                        "token": "voice-token-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice server update third");
            let payload = loop {
                let frame = ws.next().await.expect("stream create third").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream create third json");
                if payload["op"] == json!(18) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(18));
            let payload = loop {
                let frame = ws.next().await.expect("stream start third").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream start third json");
                if payload["op"] == json!(22) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(22));
            assert_eq!(payload["d"]["stream_key"], json!(expected_stream_key));
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_CREATE",
                    "s": 9,
                    "d": {
                        "stream_key": expected_stream_key,
                        "rtc_server_id": "stream-server-2",
                        "rtc_channel_id": "stream-channel-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream create event third");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_SERVER_UPDATE",
                    "s": 10,
                    "d": {
                        "stream_key": expected_stream_key,
                        "endpoint": "stream2.example.test",
                        "token": "stream-token-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream server update third");
            let _ = ready_tx.send(());
            let _ = release_rx.await;
            let _ = ws.close(None).await;
        });

        let gateway = RealDiscordGateway::new();
        gateway
            .connect_with_gateway_url(
                SessionConfig {
                    token: "token".to_owned(),
                    guild_id: "1".to_owned(),
                    channel_id: "2".to_owned(),
                    stream_kind: StreamKind::GoLive,
                },
                gateway_url,
            )
            .await
            .expect("connected");
        gateway.start_stream().await.expect("stream started");

        ready_rx.await.expect("re-identify observed");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let health = gateway.health().await;
            if health.logged_in
                && health.joined_voice
                && health.state == ConnectionState::Connected
                && health.last_error.is_none()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "gateway never reported recovered health after invalid session"
            );
            sleep(Duration::from_millis(25)).await;
        }

        let _ = release_tx.send(());
        gateway.disconnect().await.expect("disconnect");
    }

    #[tokio::test]
    async fn real_gateway_rejoins_voice_and_restarts_stream_after_unexpected_leave() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let gateway_url =
            Url::parse(&format!("ws://{addr}/?v={DISCORD_API_VERSION}&encoding=json")).expect("url");
        let (done_tx, done_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let expected_stream_key = "guild:1:2:42".to_owned();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_async(stream).await.expect("ws");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello");

            let identify = ws.next().await.expect("identify").expect("frame");
            let Message::Text(identify) = identify else {
                panic!("expected identify");
            };
            let payload: Value = serde_json::from_str(identify.as_ref()).expect("identify json");
            assert_eq!(payload["op"], json!(2));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "READY",
                    "s": 1,
                    "d": {
                        "session_id": "gateway-session",
                        "resume_gateway_url": format!("ws://{addr}"),
                        "user": { "id": "42" }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ready");

            let payload = loop {
                let frame = ws.next().await.expect("voice state").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value = serde_json::from_str(text.as_ref()).expect("voice json");
                if payload["op"] == json!(4) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(4));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_STATE_UPDATE",
                    "s": 2,
                    "d": {
                        "user_id": "42",
                        "channel_id": "2",
                        "session_id": "voice-session"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice state update");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_SERVER_UPDATE",
                    "s": 3,
                    "d": {
                        "guild_id": "1",
                        "endpoint": "voice.example.test",
                        "token": "voice-token"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice server update");
            let payload = loop {
                let frame = ws.next().await.expect("stream create").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream create json");
                if payload["op"] == json!(18) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(18));
            let payload = loop {
                let frame = ws.next().await.expect("stream start").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream start json");
                if payload["op"] == json!(22) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(22));
            assert_eq!(payload["d"]["stream_key"], json!(expected_stream_key));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_CREATE",
                    "s": 4,
                    "d": {
                        "stream_key": expected_stream_key,
                        "rtc_server_id": "stream-server",
                        "rtc_channel_id": "stream-channel"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream create event");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_SERVER_UPDATE",
                    "s": 5,
                    "d": {
                        "stream_key": expected_stream_key,
                        "endpoint": "stream.example.test",
                        "token": "stream-token"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream server update");

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_STATE_UPDATE",
                    "s": 6,
                    "d": {
                        "user_id": "42",
                        "channel_id": null,
                        "session_id": "voice-session"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice leave");

            let payload = loop {
                let frame = ws.next().await.expect("voice rejoin").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("voice rejoin json");
                if payload["op"] == json!(4) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(4));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_STATE_UPDATE",
                    "s": 7,
                    "d": {
                        "user_id": "42",
                        "channel_id": "2",
                        "session_id": "voice-session-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice rejoin update");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_SERVER_UPDATE",
                    "s": 8,
                    "d": {
                        "guild_id": "1",
                        "endpoint": "voice2.example.test",
                        "token": "voice-token-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice rejoin server");
            let payload = loop {
                let frame = ws.next().await.expect("stream recreate").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream recreate json");
                if payload["op"] == json!(18) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(18));
            let payload = loop {
                let frame = ws.next().await.expect("stream restart").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream restart json");
                if payload["op"] == json!(22) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(22));
            assert_eq!(payload["d"]["stream_key"], json!(expected_stream_key));
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_CREATE",
                    "s": 9,
                    "d": {
                        "stream_key": expected_stream_key,
                        "rtc_server_id": "stream-server-2",
                        "rtc_channel_id": "stream-channel-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream recreate event");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_SERVER_UPDATE",
                    "s": 10,
                    "d": {
                        "stream_key": expected_stream_key,
                        "endpoint": "stream2.example.test",
                        "token": "stream-token-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream restart update");
            let _ = done_tx.send(());
            let _ = release_rx.await;
            let _ = ws.close(None).await;
        });

        let gateway = RealDiscordGateway::new();
        gateway
            .connect_with_gateway_url(
                SessionConfig {
                    token: "token".to_owned(),
                    guild_id: "1".to_owned(),
                    channel_id: "2".to_owned(),
                    stream_kind: StreamKind::GoLive,
                },
                gateway_url,
            )
            .await
            .expect("connected");
        gateway.start_stream().await.expect("stream started");

        done_rx.await.expect("rejoin observed");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let health = gateway.health().await;
            if health.logged_in
                && health.joined_voice
                && health.streaming
                && health.state == ConnectionState::Streaming
                && health.last_error.is_none()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "gateway never reported recovered streaming health after voice leave"
            );
            sleep(Duration::from_millis(25)).await;
        }

        let media = gateway.media_session().await.expect("media session");
        assert_eq!(media.voice_session_id, "voice-session-2");
        assert_eq!(media.voice_endpoint, "voice2.example.test");
        assert_eq!(media.stream_server_id, "stream-server-2");
        assert_eq!(media.rtc_channel_id, "stream-channel-2");
        assert_eq!(media.stream_endpoint, "stream2.example.test");

        let _ = release_tx.send(());
        gateway.disconnect().await.expect("disconnect");
    }

    #[tokio::test]
    async fn real_gateway_restarts_stream_after_voice_server_rotation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let gateway_url =
            Url::parse(&format!("ws://{addr}/?v={DISCORD_API_VERSION}&encoding=json")).expect("url");
        let (done_tx, done_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let expected_stream_key = "guild:1:2:42".to_owned();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_async(stream).await.expect("ws");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello");

            let identify = ws.next().await.expect("identify").expect("frame");
            let Message::Text(identify) = identify else {
                panic!("expected identify");
            };
            let payload: Value = serde_json::from_str(identify.as_ref()).expect("identify json");
            assert_eq!(payload["op"], json!(2));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "READY",
                    "s": 1,
                    "d": {
                        "session_id": "gateway-session",
                        "resume_gateway_url": format!("ws://{addr}"),
                        "user": { "id": "42" }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ready");

            let payload = loop {
                let frame = ws.next().await.expect("voice state").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value = serde_json::from_str(text.as_ref()).expect("voice json");
                if payload["op"] == json!(4) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(4));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_STATE_UPDATE",
                    "s": 2,
                    "d": {
                        "user_id": "42",
                        "channel_id": "2",
                        "session_id": "voice-session"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice state update");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_SERVER_UPDATE",
                    "s": 3,
                    "d": {
                        "guild_id": "1",
                        "endpoint": "voice.example.test",
                        "token": "voice-token"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice server update");
            let payload = loop {
                let frame = ws.next().await.expect("stream create").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream create json");
                if payload["op"] == json!(18) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(18));
            let payload = loop {
                let frame = ws.next().await.expect("stream start").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream start json");
                if payload["op"] == json!(22) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(22));
            assert_eq!(payload["d"]["stream_key"], json!(expected_stream_key));

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_CREATE",
                    "s": 4,
                    "d": {
                        "stream_key": expected_stream_key,
                        "rtc_server_id": "stream-server",
                        "rtc_channel_id": "stream-channel"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream create event");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_SERVER_UPDATE",
                    "s": 5,
                    "d": {
                        "stream_key": expected_stream_key,
                        "endpoint": "stream.example.test",
                        "token": "stream-token"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream server update");

            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_SERVER_UPDATE",
                    "s": 6,
                    "d": {
                        "guild_id": "1",
                        "endpoint": "voice2.example.test",
                        "token": "voice-token-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice server rotation");

            let payload = loop {
                let frame = ws.next().await.expect("stream recreate").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream recreate json");
                if payload["op"] == json!(18) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(18));
            let payload = loop {
                let frame = ws.next().await.expect("stream restart").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value =
                    serde_json::from_str(text.as_ref()).expect("stream restart json");
                if payload["op"] == json!(22) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(22));
            assert_eq!(payload["d"]["stream_key"], json!(expected_stream_key));
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_CREATE",
                    "s": 7,
                    "d": {
                        "stream_key": expected_stream_key,
                        "rtc_server_id": "stream-server-2",
                        "rtc_channel_id": "stream-channel-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream recreate event");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "STREAM_SERVER_UPDATE",
                    "s": 8,
                    "d": {
                        "stream_key": expected_stream_key,
                        "endpoint": "stream2.example.test",
                        "token": "stream-token-2"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("stream restart update");
            let _ = done_tx.send(());
            let _ = release_rx.await;
            let _ = ws.close(None).await;
        });

        let gateway = RealDiscordGateway::new();
        gateway
            .connect_with_gateway_url(
                SessionConfig {
                    token: "token".to_owned(),
                    guild_id: "1".to_owned(),
                    channel_id: "2".to_owned(),
                    stream_kind: StreamKind::GoLive,
                },
                gateway_url,
            )
            .await
            .expect("connected");
        gateway.start_stream().await.expect("stream started");

        done_rx.await.expect("voice server rotation observed");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let health = gateway.health().await;
            if health.logged_in
                && health.joined_voice
                && health.streaming
                && health.state == ConnectionState::Streaming
                && health.last_error.is_none()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "gateway never reported recovered streaming health after voice server rotation"
            );
            sleep(Duration::from_millis(25)).await;
        }

        let media = gateway.media_session().await.expect("media session");
        assert_eq!(media.voice_endpoint, "voice2.example.test");
        assert_eq!(media.voice_token, "voice-token-2");
        assert_eq!(media.stream_server_id, "stream-server-2");
        assert_eq!(media.rtc_channel_id, "stream-channel-2");
        assert_eq!(media.stream_endpoint, "stream2.example.test");
        assert_eq!(media.stream_token, "stream-token-2");

        let _ = release_tx.send(());
        gateway.disconnect().await.expect("disconnect");
    }

    #[tokio::test]
    async fn real_gateway_retries_resume_when_invalid_session_is_resumable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let gateway_url =
            Url::parse(&format!("ws://{addr}/?v={DISCORD_API_VERSION}&encoding=json")).expect("url");
        let (done_tx, done_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept first");
            let mut ws = accept_async(stream).await.expect("ws first");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello first");
            let identify = ws.next().await.expect("identify").expect("frame");
            let Message::Text(identify) = identify else {
                panic!("expected identify");
            };
            let payload: Value = serde_json::from_str(identify.as_ref()).expect("identify json");
            assert_eq!(payload["op"], json!(2));
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "READY",
                    "s": 1,
                    "d": {
                        "session_id": "gateway-session",
                        "resume_gateway_url": format!("ws://{addr}"),
                        "user": { "id": "42" }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ready first");
            let payload = loop {
                let frame = ws.next().await.expect("voice state").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value = serde_json::from_str(text.as_ref()).expect("voice json");
                if payload["op"] == json!(4) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(4));
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_STATE_UPDATE",
                    "s": 2,
                    "d": {
                        "user_id": "42",
                        "channel_id": "2",
                        "session_id": "voice-session"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice state update");
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "VOICE_SERVER_UPDATE",
                    "s": 3,
                    "d": {
                        "guild_id": "1",
                        "endpoint": "voice.example.test",
                        "token": "voice-token"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("voice server update");
            let _ = ws.close(None).await;

            let (stream, _) = listener.accept().await.expect("accept second");
            let mut ws = accept_async(stream).await.expect("ws second");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello second");
            let payload = loop {
                let frame = ws.next().await.expect("resume second").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value = serde_json::from_str(text.as_ref()).expect("resume json");
                if payload["op"] == json!(6) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(6));
            ws.send(Message::Text(json!({"op": 9, "d": true}).to_string().into()))
                .await
                .expect("resumable invalid session");
            let _ = ws.close(None).await;

            let (stream, _) = listener.accept().await.expect("accept third");
            let mut ws = accept_async(stream).await.expect("ws third");
            ws.send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 10_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("hello third");
            let payload = loop {
                let frame = ws.next().await.expect("resume third").expect("frame");
                let Message::Text(text) = frame else {
                    continue;
                };
                let payload: Value = serde_json::from_str(text.as_ref()).expect("resume json");
                if payload["op"] == json!(6) {
                    break payload;
                }
            };
            assert_eq!(payload["op"], json!(6));
            ws.send(Message::Text(
                json!({
                    "op": 0,
                    "t": "RESUMED",
                    "s": 4,
                    "d": {}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("resumed");
            let _ = done_tx.send(());
            let _ = release_rx.await;
            let _ = ws.close(None).await;
        });

        let gateway = RealDiscordGateway::new();
        gateway
            .connect_with_gateway_url(
                SessionConfig {
                    token: "token".to_owned(),
                    guild_id: "1".to_owned(),
                    channel_id: "2".to_owned(),
                    stream_kind: StreamKind::GoLive,
                },
                gateway_url,
            )
            .await
            .expect("connected");

        done_rx.await.expect("resumable invalid-session flow");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let health = gateway.health().await;
            if health.logged_in
                && health.joined_voice
                && health.state == ConnectionState::Connected
                && health.last_error.is_none()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "gateway never reported resumed health after resumable invalid session"
            );
            sleep(Duration::from_millis(25)).await;
        }

        let _ = release_tx.send(());
        gateway.disconnect().await.expect("disconnect");
    }
}
