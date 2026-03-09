use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, warn};
use url::Url;

const DISCORD_API_BASE: &str = "https://discord.com/api";
const DISCORD_API_VERSION: u16 = 9;
const GATEWAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const STATE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const STATE_TIMEOUT: Duration = Duration::from_secs(20);
const DISCORD_USER_AGENT: &str = "discord-rs-streamer/0.1";

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
    voice_session_id: Option<String>,
    voice_endpoint: Option<String>,
    voice_token: Option<String>,
    stream_key: Option<String>,
    stream_server_id: Option<String>,
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

impl RealDiscordGateway {
    pub fn new() -> Self {
        Self {
            http: Client::builder()
                .user_agent(DISCORD_USER_AGENT)
                .build()
                .expect("http client"),
            state: Arc::new(RwLock::new(GatewayState::default())),
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
            if start.elapsed() >= STATE_TIMEOUT {
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

        self.update_state(|state| {
            *state = GatewayState::default();
            state.health.state = ConnectionState::Connecting;
            state.health.last_error = None;
            state.health.session = Some(config.clone());
        })
        .await;

        let (stream, _) = timeout(GATEWAY_CONNECT_TIMEOUT, connect_async(gateway_url.as_str()))
            .await
            .map_err(|_| {
                GatewayError::Message("timed out connecting to discord gateway".to_owned())
            })?
            .map_err(|error| {
                GatewayError::Message(format!("failed to connect to discord gateway: {error}"))
            })?;
        let (mut write, mut read) = stream.split();
        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<GatewayCommand>();
        *self.command_tx.lock().await = Some(command_tx);

        let state = Arc::clone(&self.state);
        let task = tokio::spawn(async move {
            let mut heartbeat: Option<tokio::time::Interval> = None;
            loop {
                tokio::select! {
                    maybe_command = command_rx.recv() => {
                        match maybe_command {
                            Some(GatewayCommand::Send(payload)) => {
                                if write.send(Message::Text(payload.to_string().into())).await.is_err() {
                                    set_disconnected_error(&state, "failed to write to discord gateway").await;
                                    break;
                                }
                            }
                            Some(GatewayCommand::Close) | None => {
                                let _ = write.close().await;
                                break;
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
                            set_disconnected_error(&state, "failed to send heartbeat").await;
                            break;
                        }
                    }
                    maybe_message = read.next() => {
                        match maybe_message {
                            Some(Ok(message)) => {
                                match message {
                                    Message::Text(text) => {
                                        if let Err(error) = handle_gateway_message(&state, &mut write, &mut heartbeat, text.to_string()).await {
                                            set_disconnected_error(&state, error).await;
                                            break;
                                        }
                                    }
                                    Message::Close(_) => {
                                        set_disconnected_error(&state, "discord gateway closed").await;
                                        break;
                                    }
                                    Message::Ping(payload) => {
                                        if write.send(Message::Pong(payload)).await.is_err() {
                                            set_disconnected_error(&state, "failed to answer ping").await;
                                            break;
                                        }
                                    }
                                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                                }
                            }
                            Some(Err(error)) => {
                                set_disconnected_error(&state, format!("discord gateway read failed: {error}")).await;
                                break;
                            }
                            None => {
                                set_disconnected_error(&state, "discord gateway ended").await;
                                break;
                            }
                        }
                    }
                }
            }
        });
        *self.task.lock().await = Some(task);

        self.wait_for(
            |state| state.health.logged_in,
            "discord gateway identify did not complete",
        )
        .await?;

        let voice_state_update = json!({
            "op": 4,
            "d": {
                "guild_id": json_guild_id(&config.guild_id),
                "channel_id": config.channel_id,
                "self_mute": false,
                "self_deaf": true,
                "self_video": matches!(config.stream_kind, StreamKind::Camera),
            }
        });
        self.send_command(voice_state_update).await?;

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

    async fn start_stream(&self) -> Result<GatewayHealth, GatewayError> {
        let (session, self_user_id, guild_id) = {
            let state = self.state.read().await;
            let Some(session) = state.health.session.clone() else {
                return Err(GatewayError::Message(
                    "discord stream start requires an active session".to_owned(),
                ));
            };
            let Some(self_user_id) = state.self_user_id.clone() else {
                return Err(GatewayError::Message(
                    "discord stream start is missing self user id".to_owned(),
                ));
            };
            if !state.health.joined_voice {
                return Err(GatewayError::Message(
                    "discord stream start requires a joined voice state".to_owned(),
                ));
            }
            let guild_id = session_guild_id(&session).map(ToOwned::to_owned);
            (session, self_user_id, guild_id)
        };

        let stream_type = if guild_id.is_some() { "guild" } else { "call" };
        let stream_key = generate_stream_key(
            stream_type,
            guild_id.as_deref(),
            &session.channel_id,
            &self_user_id,
        );

        self.send_command(json!({
            "op": 18,
            "d": {
                "type": stream_type,
                "guild_id": json_guild_id(&session.guild_id),
                "channel_id": session.channel_id,
                "preferred_region": Value::Null,
            }
        }))
        .await?;
        self.send_command(json!({
            "op": 22,
            "d": {
                "stream_key": stream_key,
                "paused": false,
            }
        }))
        .await?;

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

async fn handle_gateway_message<S>(
    state: &Arc<RwLock<GatewayState>>,
    write: &mut S,
    heartbeat: &mut Option<tokio::time::Interval>,
    text: String,
) -> Result<(), String>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let payload: Value =
        serde_json::from_str(&text).map_err(|error| format!("invalid gateway payload: {error}"))?;
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
            let identify = {
                let snapshot = state.read().await;
                let session = snapshot
                    .health
                    .session
                    .clone()
                    .ok_or_else(|| "session missing during identify".to_owned())?;
                build_identify_payload(&session.token)
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
        }
        7 => return Err("discord requested reconnect".to_owned()),
        9 => return Err("discord gateway invalid session".to_owned()),
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
            if state.health.joined_voice {
                state.health.state = ConnectionState::Connected;
            }
            state.health.last_error = None;
        }
        "VOICE_STATE_UPDATE" => {
            let mut state = state.write().await;
            let Some(session) = state.health.session.as_ref() else {
                return Ok(());
            };
            let Some(self_user_id) = state.self_user_id.as_deref() else {
                return Ok(());
            };
            if data.get("user_id").and_then(Value::as_str) != Some(self_user_id) {
                return Ok(());
            }
            if data.get("channel_id").and_then(Value::as_str) != Some(session.channel_id.as_str()) {
                clear_voice_session(&mut state);
                return Ok(());
            }
            state.voice_session_id = data
                .get("session_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            refresh_voice_state(&mut state);
        }
        "VOICE_SERVER_UPDATE" => {
            let mut state = state.write().await;
            let Some(session) = state.health.session.as_ref() else {
                return Ok(());
            };
            if data.get("guild_id").and_then(Value::as_str) != session_guild_id(session) {
                return Ok(());
            }
            state.voice_endpoint = data
                .get("endpoint")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            state.voice_token = data
                .get("token")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            refresh_voice_state(&mut state);
        }
        "STREAM_CREATE" => {
            let mut state = state.write().await;
            let Some(session) = state.health.session.as_ref() else {
                return Ok(());
            };
            let Some(self_user_id) = state.self_user_id.as_deref() else {
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
            state.stream_key = Some(stream_key.to_owned());
            state.stream_server_id = data
                .get("rtc_server_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            refresh_streaming_state(&mut state);
        }
        "STREAM_SERVER_UPDATE" => {
            let mut state = state.write().await;
            let Some(session) = state.health.session.as_ref() else {
                return Ok(());
            };
            let Some(self_user_id) = state.self_user_id.as_deref() else {
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

fn has_voice_session_resources(state: &GatewayState) -> bool {
    state.voice_session_id.is_some()
        && state.voice_endpoint.is_some()
        && state.voice_token.is_some()
}

fn has_stream_session_resources(state: &GatewayState) -> bool {
    state.stream_key.is_some() && state.stream_endpoint.is_some() && state.stream_token.is_some()
}

fn build_media_session(state: &GatewayState) -> Result<GatewayMediaSession, GatewayError> {
    let session = state
        .health
        .session
        .clone()
        .ok_or_else(|| GatewayError::Message("discord media session is missing login session".to_owned()))?;
    if !state.health.streaming {
        return Err(GatewayError::Message(
            "discord media session requires an active stream session".to_owned(),
        ));
    }

    Ok(GatewayMediaSession {
        user_id: state
            .self_user_id
            .clone()
            .ok_or_else(|| GatewayError::Message("discord media session is missing self user id".to_owned()))?,
        guild_id: session_guild_id(&session).map(ToOwned::to_owned),
        channel_id: session.channel_id,
        stream_kind: session.stream_kind,
        voice_session_id: state
            .voice_session_id
            .clone()
            .ok_or_else(|| GatewayError::Message("discord media session is missing voice session id".to_owned()))?,
        voice_endpoint: state
            .voice_endpoint
            .clone()
            .ok_or_else(|| GatewayError::Message("discord media session is missing voice endpoint".to_owned()))?,
        voice_token: state
            .voice_token
            .clone()
            .ok_or_else(|| GatewayError::Message("discord media session is missing voice token".to_owned()))?,
        stream_key: state
            .stream_key
            .clone()
            .ok_or_else(|| GatewayError::Message("discord media session is missing stream key".to_owned()))?,
        stream_server_id: state
            .stream_server_id
            .clone()
            .ok_or_else(|| GatewayError::Message("discord media session is missing stream server id".to_owned()))?,
        stream_endpoint: state
            .stream_endpoint
            .clone()
            .ok_or_else(|| GatewayError::Message("discord media session is missing stream endpoint".to_owned()))?,
        stream_token: state
            .stream_token
            .clone()
            .ok_or_else(|| GatewayError::Message("discord media session is missing stream token".to_owned()))?,
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

async fn set_disconnected_error(state: &Arc<RwLock<GatewayState>>, message: impl Into<String>) {
    let message = message.into();
    warn!(error = %message, "discord gateway session ended");
    let mut state = state.write().await;
    clear_voice_session(&mut state);
    state.health.logged_in = false;
    state.health.state = ConnectionState::Error;
    state.health.last_error = Some(message);
}

fn clear_voice_session(state: &mut GatewayState) {
    state.voice_session_id = None;
    state.voice_endpoint = None;
    state.voice_token = None;
    state.stream_key = None;
    state.stream_server_id = None;
    state.stream_endpoint = None;
    state.stream_token = None;
    state.health.joined_voice = false;
    state.health.streaming = false;
    state.health.state = if state.health.logged_in {
        ConnectionState::Connected
    } else {
        ConnectionState::Disconnected
    };
}

fn build_identify_payload(token: &str) -> Value {
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
    use super::*;
    use futures::channel::mpsc;
    use serde_json::json;
    use tokio_tungstenite::tungstenite::protocol::Message;

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
        let payload = build_identify_payload("token");
        assert_eq!(payload["op"], json!(2));
        assert_eq!(payload["d"]["token"], json!("token"));
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

        handle_gateway_message(
            &state,
            &mut sink,
            &mut heartbeat,
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

        handle_gateway_message(
            &state,
            &mut sink,
            &mut heartbeat,
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
}
