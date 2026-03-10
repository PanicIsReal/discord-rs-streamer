use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use dave::DaveControl;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    VoiceOutbound, VoiceServerInfo, VoiceSessionConfig, VoiceSessionController, VoiceSessionState,
};

const DEFAULT_VOICE_GATEWAY_VERSION: u8 = 8;
const VOICE_RECONNECT_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VoiceGatewayHealth {
    pub connected: bool,
    pub hello_received: bool,
    pub ready_received: bool,
    pub session_description_received: bool,
    pub heartbeat_rtt_ms: Option<u64>,
    pub reconnect_count: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Error)]
pub enum VoiceGatewayError {
    #[error("invalid voice endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("voice gateway connection failed: {0}")]
    Connection(String),
    #[error("voice gateway is closed")]
    Closed,
    #[error("voice protocol error: {0}")]
    Protocol(String),
}

#[derive(Debug)]
enum ClientCommand {
    Json(Value),
    Binary(Vec<u8>),
    Close,
}

pub struct VoiceGatewayClient<D> {
    controller: Arc<Mutex<VoiceSessionController<D>>>,
    health: Arc<RwLock<VoiceGatewayHealth>>,
    command_tx: mpsc::UnboundedSender<ClientCommand>,
    shutdown: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
}

impl<D> VoiceGatewayClient<D>
where
    D: DaveControl + Send + 'static,
{
    pub async fn connect(
        server: VoiceServerInfo,
        config: VoiceSessionConfig,
        controller: VoiceSessionController<D>,
    ) -> Result<Self, VoiceGatewayError> {
        let url = voice_gateway_url(&server.endpoint)?;
        let controller = Arc::new(Mutex::new(controller));
        controller
            .lock()
            .await
            .prepare_session(server.clone(), config.clone());
        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<ClientCommand>();
        let health = Arc::new(RwLock::new(VoiceGatewayHealth {
            ..VoiceGatewayHealth::default()
        }));
        let shutdown = Arc::new(AtomicBool::new(false));
        let health_for_task = Arc::clone(&health);
        let controller_for_task = Arc::clone(&controller);
        let shutdown_for_task = Arc::clone(&shutdown);
        let initial_stream = connect_async(url.as_str())
            .await
            .map_err(|error| VoiceGatewayError::Connection(error.to_string()))?;

        let task = tokio::spawn(async move {
            let mut next_stream = Some(initial_stream);
            let mut prefer_resume = false;
            loop {
                if shutdown_for_task.load(Ordering::Relaxed) {
                    return;
                }
                let stream = match next_stream.take() {
                    Some(stream) => stream,
                    None => {
                        mark_reconnecting(&health_for_task).await;
                        sleep(VOICE_RECONNECT_DELAY).await;
                        if shutdown_for_task.load(Ordering::Relaxed) {
                            return;
                        }
                        match connect_async(url.as_str()).await {
                            Ok(stream) => {
                                increment_reconnect_count(&health_for_task).await;
                                stream
                            }
                            Err(error) => {
                                set_client_error(
                                    &health_for_task,
                                    format!("voice gateway reconnect failed: {error}"),
                                )
                                .await;
                                continue;
                            }
                        }
                    }
                };

                let (stream, _) = stream;
                let (mut write, mut read) = stream.split();
                let mut heartbeat = None::<tokio::time::Interval>;
                let mut last_heartbeat_sent = None::<Instant>;

                let initial_payload = {
                    let controller = controller_for_task.lock().await;
                    if prefer_resume {
                        controller.resume(&config).or_else(|_| controller.identify(&config))
                    } else {
                        controller.identify(&config)
                    }
                };
                let initial_payload = match initial_payload {
                    Ok(payload) => payload,
                    Err(error) => {
                        set_client_error(&health_for_task, error.to_string()).await;
                        return;
                    }
                };

                log_outbound_opcode(&initial_payload, "voice gateway init");
                if write
                    .send(Message::Text(initial_payload.to_string().into()))
                    .await
                    .is_err()
                {
                    set_client_error(&health_for_task, "failed to send voice gateway init").await;
                    continue;
                }
                mark_connected(&health_for_task).await;

                loop {
                    tokio::select! {
                        maybe_command = command_rx.recv() => {
                            match maybe_command {
                                Some(ClientCommand::Json(payload)) => {
                                    log_outbound_opcode(&payload, "voice gateway outbound json");
                                    if write.send(Message::Text(payload.to_string().into())).await.is_err() {
                                        set_client_error(&health_for_task, "failed to send voice payload").await;
                                        break;
                                    }
                                }
                                Some(ClientCommand::Binary(payload)) => {
                                    if write.send(Message::Binary(payload.into())).await.is_err() {
                                        set_client_error(&health_for_task, "failed to send voice binary payload").await;
                                        break;
                                    }
                                }
                                Some(ClientCommand::Close) | None => {
                                    let _ = write.close().await;
                                    return;
                                }
                            }
                        }
                        _ = async {
                            if let Some(ticker) = heartbeat.as_mut() {
                                ticker.tick().await;
                            }
                        }, if heartbeat.is_some() => {
                            let payload = {
                                let controller = controller_for_task.lock().await;
                                controller.heartbeat()
                            };
                            last_heartbeat_sent = Some(Instant::now());
                            if write.send(Message::Text(payload.to_string().into())).await.is_err() {
                                set_client_error(&health_for_task, "failed to send voice heartbeat").await;
                                break;
                            }
                        }
                        maybe_message = read.next() => {
                            match maybe_message {
                                Some(Ok(message)) => {
                                    if let Err(error) = handle_message(
                                        message,
                                        &controller_for_task,
                                        &health_for_task,
                                        &mut heartbeat,
                                        &mut last_heartbeat_sent,
                                        &mut write,
                                    ).await {
                                        set_client_error(&health_for_task, error).await;
                                        break;
                                    }
                                }
                                Some(Err(error)) => {
                                    warn!(error = %error, "voice gateway read failed");
                                    set_client_error(&health_for_task, format!("voice gateway read failed: {error}")).await;
                                    break;
                                }
                                None => {
                                    warn!("voice gateway stream ended");
                                    set_client_error(&health_for_task, "voice gateway ended").await;
                                    break;
                                }
                            }
                        }
                    }
                }
                prefer_resume = true;
            }
        });

        Ok(Self {
            controller,
            health,
            command_tx,
            shutdown,
            task: Some(task),
        })
    }

    pub async fn health(&self) -> VoiceGatewayHealth {
        self.health.read().await.clone()
    }

    pub async fn session_state(&self) -> VoiceSessionState {
        self.controller.lock().await.state().clone()
    }

    pub async fn wait_for_hello(
        &self,
        timeout: Duration,
    ) -> Result<VoiceGatewayHealth, VoiceGatewayError> {
        self.wait_for(timeout, |health| health.hello_received).await
    }

    pub async fn wait_for_ready(
        &self,
        timeout: Duration,
    ) -> Result<VoiceGatewayHealth, VoiceGatewayError> {
        self.wait_for(timeout, |health| health.ready_received).await
    }

    pub async fn wait_for_session_description(
        &self,
        timeout: Duration,
    ) -> Result<VoiceGatewayHealth, VoiceGatewayError> {
        self.wait_for(timeout, |health| health.session_description_received)
            .await
    }

    pub async fn send_json(&self, payload: Value) -> Result<(), VoiceGatewayError> {
        self.command_tx
            .send(ClientCommand::Json(payload))
            .map_err(|_| VoiceGatewayError::Closed)
    }

    pub async fn send_binary(&self, payload: Vec<u8>) -> Result<(), VoiceGatewayError> {
        self.command_tx
            .send(ClientCommand::Binary(payload))
            .map_err(|_| VoiceGatewayError::Closed)
    }

    pub async fn send_select_protocol(&self, payload: Value) -> Result<(), VoiceGatewayError> {
        self.send_json(payload).await
    }

    pub async fn send_video_command(&self, payload: Value) -> Result<(), VoiceGatewayError> {
        self.send_json(payload).await
    }

    pub async fn send_speaking_command(&self, payload: Value) -> Result<(), VoiceGatewayError> {
        self.send_json(payload).await
    }

    pub async fn speaking_payload(&self, enabled: bool) -> Option<Value> {
        self.controller.lock().await.speaking_payload(enabled)
    }

    pub async fn set_streaming_announced(&self, announced: bool) -> Result<(), VoiceGatewayError> {
        self.controller
            .lock()
            .await
            .state_mut()
            .set_streaming_announced(announced);
        Ok(())
    }

    pub async fn close(mut self) -> Result<(), VoiceGatewayError> {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.command_tx.send(ClientCommand::Close);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        Ok(())
    }

    async fn wait_for(
        &self,
        timeout: Duration,
        predicate: impl Fn(&VoiceGatewayHealth) -> bool,
    ) -> Result<VoiceGatewayHealth, VoiceGatewayError> {
        let started = Instant::now();
        loop {
            let health = self.health().await;
            if predicate(&health) {
                return Ok(health);
            }
            if started.elapsed() >= timeout {
                return Err(VoiceGatewayError::Protocol(
                    health
                        .last_error
                        .unwrap_or_else(|| "voice gateway readiness timed out".to_owned()),
                ));
            }
            sleep(Duration::from_millis(25)).await;
        }
    }
}

async fn handle_message<S, D>(
    message: Message,
    controller: &Arc<Mutex<VoiceSessionController<D>>>,
    health: &Arc<RwLock<VoiceGatewayHealth>>,
    heartbeat: &mut Option<tokio::time::Interval>,
    last_heartbeat_sent: &mut Option<Instant>,
    write: &mut S,
) -> Result<(), String>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
    D: DaveControl + Send + 'static,
{
    match message {
        Message::Text(text) => {
            let payload: Value =
                serde_json::from_str(text.as_ref()).map_err(|error| error.to_string())?;
            log_inbound_opcode(&payload);
            if payload.get("op").and_then(Value::as_u64) == Some(8) {
                let interval_ms = payload
                    .get("d")
                    .and_then(|value| value.get("heartbeat_interval"))
                    .and_then(Value::as_u64)
                    .ok_or_else(|| "voice hello missing heartbeat interval".to_owned())?;
                *heartbeat = Some(interval(Duration::from_millis(interval_ms)));
                health.write().await.hello_received = true;
            }

            let outbound = controller
                .lock()
                .await
                .handle_json_message(&payload)
                .map_err(|error| error.to_string())?;
            update_health_from_op(&payload, health, last_heartbeat_sent).await;
            send_outbound(outbound, write).await?;
        }
        Message::Binary(payload) => {
            debug!(
                len = payload.len(),
                binary_opcode = payload.get(2).copied().unwrap_or_default(),
                "voice gateway inbound binary"
            );
            let outbound = controller
                .lock()
                .await
                .handle_binary_message(&payload)
                .map_err(|error| error.to_string())?;
            send_outbound(outbound, write).await?;
        }
        Message::Ping(payload) => {
            write
                .send(Message::Pong(payload))
                .await
                .map_err(|error| error.to_string())?;
        }
        Message::Close(frame) => {
            if let Some(frame) = frame.as_ref() {
                warn!(
                    code = u16::from(frame.code),
                    reason = %frame.reason,
                    "voice gateway close frame"
                );
            } else {
                warn!("voice gateway close frame without detail");
            }
            let detail = frame
                .map(|frame| {
                    format!(
                        "voice gateway closed: {} {}",
                        u16::from(frame.code),
                        frame.reason
                    )
                })
                .unwrap_or_else(|| "voice gateway closed".to_owned());
            return Err(detail);
        }
        Message::Pong(_) | Message::Frame(_) => {}
    }
    Ok(())
}

async fn update_health_from_op(
    payload: &Value,
    health: &Arc<RwLock<VoiceGatewayHealth>>,
    last_heartbeat_sent: &mut Option<Instant>,
) {
    let Some(opcode) = payload.get("op").and_then(Value::as_u64) else {
        return;
    };
    let mut health = health.write().await;
    match opcode {
        2 | 9 => health.ready_received = true,
        4 => health.session_description_received = true,
        6 => {
            health.heartbeat_rtt_ms = last_heartbeat_sent
                .take()
                .map(|sent| sent.elapsed().as_millis() as u64);
        }
        _ => {}
    }
}

async fn send_outbound<S>(outbound: Vec<VoiceOutbound>, write: &mut S) -> Result<(), String>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    for message in outbound {
        match message {
            VoiceOutbound::Json(payload) => {
                log_outbound_opcode(&payload, "voice gateway outbound json");
                write
                    .send(Message::Text(payload.to_string().into()))
                    .await
                    .map_err(|error| error.to_string())?
            }
            VoiceOutbound::Binary(payload) => write
                .send(Message::Binary(payload.into()))
                .await
                .map_err(|error| error.to_string())?,
        }
    }
    Ok(())
}

async fn set_client_error(health: &Arc<RwLock<VoiceGatewayHealth>>, message: impl Into<String>) {
    let message = message.into();
    debug!(%message, "voice gateway client error");
    let mut health = health.write().await;
    health.connected = false;
    health.last_error = Some(message);
}

async fn mark_connected(health: &Arc<RwLock<VoiceGatewayHealth>>) {
    let mut health = health.write().await;
    health.connected = true;
    health.last_error = None;
}

async fn mark_reconnecting(health: &Arc<RwLock<VoiceGatewayHealth>>) {
    let mut health = health.write().await;
    health.connected = false;
}

async fn increment_reconnect_count(health: &Arc<RwLock<VoiceGatewayHealth>>) {
    let mut health = health.write().await;
    health.reconnect_count = health.reconnect_count.saturating_add(1);
}

fn log_inbound_opcode(payload: &Value) {
    let opcode = payload.get("op").and_then(Value::as_u64);
    let seq = payload.get("seq").and_then(Value::as_u64);
    let data = payload.get("d");
    info!(
        opcode,
        seq,
        has_media_session_id = data
            .and_then(|value| value.get("media_session_id"))
            .is_some(),
        has_sdp = data
            .and_then(|value| value.get("sdp"))
            .and_then(|value| value.as_str())
            .map(|sdp| !sdp.is_empty())
            .unwrap_or(false),
        has_any_video_constraints = data
            .and_then(|value| value.get("any"))
            .and_then(|value| value.get("video"))
            .is_some(),
        has_streams = data
            .and_then(|value| value.get("streams"))
            .and_then(|value| value.as_array())
            .map(|streams| !streams.is_empty())
            .unwrap_or(false),
        "voice gateway inbound opcode"
    );
}

fn log_outbound_opcode(payload: &Value, context: &'static str) {
    let opcode = payload.get("op").and_then(Value::as_u64);
    let data = payload.get("d");
    info!(
        opcode,
        server_id = data
            .and_then(|value| value.get("server_id"))
            .and_then(|value| value.as_str()),
        channel_id = data
            .and_then(|value| value.get("channel_id"))
            .and_then(|value| value.as_str()),
        session_id = data
            .and_then(|value| value.get("session_id"))
            .and_then(|value| value.as_str()),
        token_len = data
            .and_then(|value| value.get("token"))
            .and_then(|value| value.as_str())
            .map(str::len),
        stream_type = data
            .and_then(|value| value.get("streams"))
            .and_then(|value| value.as_array())
            .and_then(|streams| streams.first())
            .and_then(|stream| stream.get("type"))
            .and_then(|value| value.as_str()),
        max_dave_protocol_version = data
            .and_then(|value| value.get("max_dave_protocol_version"))
            .and_then(|value| value.as_u64()),
        has_sdp = data
            .and_then(|value| value.get("sdp"))
            .and_then(|value| value.as_str())
            .map(|sdp| !sdp.is_empty())
            .unwrap_or(false),
        has_rtc_connection_id = data
            .and_then(|value| value.get("rtc_connection_id"))
            .is_some(),
        video_active = data
            .and_then(|value| value.get("streams"))
            .and_then(|value| value.as_array())
            .and_then(|streams| streams.first())
            .and_then(|stream| stream.get("active"))
            .and_then(|value| value.as_bool()),
        speaking = data
            .and_then(|value| value.get("speaking"))
            .and_then(|value| value.as_u64()),
        %context,
        "voice gateway outbound opcode"
    );
}

pub fn voice_gateway_url(endpoint: &str) -> Result<Url, VoiceGatewayError> {
    let normalized = if endpoint.starts_with("ws://") || endpoint.starts_with("wss://") {
        endpoint.to_owned()
    } else {
        format!(
            "wss://{}/?v={DEFAULT_VOICE_GATEWAY_VERSION}",
            endpoint.trim_end_matches('/')
        )
    };
    Url::parse(&normalized).map_err(|error| VoiceGatewayError::InvalidEndpoint(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use dave::ManagedDaveSession;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};

    use super::*;
    use crate::{StreamKind, VoiceBinaryOpcode, VoiceOpcode, VoiceServerInfo, VoiceSessionConfig};

    #[test]
    fn builds_voice_gateway_url_from_host() {
        let url = voice_gateway_url("voice.example.test").expect("url");
        assert_eq!(url.as_str(), "wss://voice.example.test/?v=8");
    }

    #[tokio::test]
    async fn client_identifies_and_processes_ready_flow() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_async(stream).await.expect("ws");
            let identify = ws.next().await.expect("identify frame").expect("identify");
            let Message::Text(identify) = identify else {
                panic!("expected identify text");
            };
            let payload: Value = serde_json::from_str(identify.as_ref()).expect("identify json");
            assert_eq!(payload["op"], json!(VoiceOpcode::Identify as u8));

            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::Hello as u8,
                    "d": { "heartbeat_interval": 25 }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("hello");
            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::Ready as u8,
                    "d": {
                        "ssrc": 111,
                        "ip": "127.0.0.1",
                        "port": 50000,
                        "modes": ["xsalsa20_poly1305"],
                        "streams": [{
                            "type": "video",
                            "rid": "100",
                            "quality": 100,
                            "ssrc": 222,
                            "rtx_ssrc": 333,
                            "active": true
                        }]
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ready");
            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::SessionDescription as u8,
                    "d": {
                        "audio_codec": "opus",
                        "video_codec": "H264",
                        "dave_protocol_version": 1,
                        "sdp": "v=0",
                        "media_session_id": 42
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("session description");

            let binary = loop {
                let frame = ws.next().await.expect("key package frame").expect("frame");
                match frame {
                    Message::Binary(binary) => break binary,
                    Message::Text(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(_) => panic!("voice client closed before key package"),
                }
            };
            assert_eq!(binary[0], VoiceBinaryOpcode::MlsKeyPackage as u8);
            let _ = ready_tx.send(());
            let _ = release_rx.await;
            let _ = ws.close(None).await;
        });

        let client = VoiceGatewayClient::connect(
            VoiceServerInfo {
                endpoint: format!("ws://{addr}"),
                token: "voice-token".to_owned(),
            },
            VoiceSessionConfig {
                server_id: "guild-1".to_owned(),
                channel_id: "2".to_owned(),
                user_id: "1".to_owned(),
                session_id: "session-1".to_owned(),
                token: "voice-token".to_owned(),
                stream_kind: StreamKind::GoLive,
                max_dave_protocol_version: 1,
            },
            VoiceSessionController::new(ManagedDaveSession::new()),
        )
        .await
        .expect("client");

        ready_rx.await.expect("server flow complete");
        let health = client.health().await;
        assert!(health.connected);
        assert!(health.hello_received);
        assert!(health.ready_received);
        assert!(health.session_description_received);
        client
            .wait_for_session_description(Duration::from_secs(1))
            .await
            .expect("session description ready");

        let state = client.session_state().await;
        assert_eq!(
            state.ready.as_ref().map(|ready| ready.audio_ssrc),
            Some(111)
        );
        assert_eq!(state.dave_protocol_version, 1);
        assert_eq!(
            state.protocol_ack.as_ref().map(|ack| ack.media_session_id),
            Some(Some(42))
        );
        let _ = release_tx.send(());
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn client_routes_manual_json_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let (message_tx, message_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_async(stream).await.expect("ws");
            let _ = ws.next().await;
            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::Hello as u8,
                    "d": { "heartbeat_interval": 10000 }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("hello");
            let payload = ws.next().await.expect("manual message").expect("frame");
            let Message::Text(payload) = payload else {
                panic!("expected text payload");
            };
            let payload: Value = serde_json::from_str(payload.as_ref()).expect("json");
            let _ = message_tx.send(payload);
        });

        let client = VoiceGatewayClient::connect(
            VoiceServerInfo {
                endpoint: format!("ws://{addr}"),
                token: "voice-token".to_owned(),
            },
            VoiceSessionConfig {
                server_id: "guild-1".to_owned(),
                channel_id: "2".to_owned(),
                user_id: "1".to_owned(),
                session_id: "session-1".to_owned(),
                token: "voice-token".to_owned(),
                stream_kind: StreamKind::GoLive,
                max_dave_protocol_version: 1,
            },
            VoiceSessionController::new(ManagedDaveSession::new()),
        )
        .await
        .expect("client");

        client
            .send_json(json!({
                "op": VoiceOpcode::Video as u8,
                "d": { "demo": true }
            }))
            .await
            .expect("send json");

        let payload = message_rx.await.expect("payload");
        assert_eq!(payload["op"], json!(VoiceOpcode::Video as u8));
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn client_reconnects_and_resumes_after_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let (done_tx, done_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept first");
            let mut ws = accept_async(stream).await.expect("ws first");
            let identify = ws.next().await.expect("identify").expect("frame");
            let Message::Text(identify) = identify else {
                panic!("expected identify");
            };
            let payload: Value = serde_json::from_str(identify.as_ref()).expect("identify json");
            assert_eq!(payload["op"], json!(VoiceOpcode::Identify as u8));
            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::Hello as u8,
                    "d": { "heartbeat_interval": 10000 }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("hello first");
            let _ = ws.close(None).await;

            let (stream, _) = listener.accept().await.expect("accept second");
            let mut ws = accept_async(stream).await.expect("ws second");
            let resume = ws.next().await.expect("resume").expect("frame");
            let Message::Text(resume) = resume else {
                panic!("expected resume");
            };
            let payload: Value = serde_json::from_str(resume.as_ref()).expect("resume json");
            assert_eq!(payload["op"], json!(VoiceOpcode::Resume as u8));
            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::Hello as u8,
                    "d": { "heartbeat_interval": 1000 }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("hello second");
            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::Resumed as u8,
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

        let client = VoiceGatewayClient::connect(
            VoiceServerInfo {
                endpoint: format!("ws://{addr}"),
                token: "voice-token".to_owned(),
            },
            VoiceSessionConfig {
                server_id: "guild-1".to_owned(),
                channel_id: "2".to_owned(),
                user_id: "1".to_owned(),
                session_id: "session-1".to_owned(),
                token: "voice-token".to_owned(),
                stream_kind: StreamKind::GoLive,
                max_dave_protocol_version: 1,
            },
            VoiceSessionController::new(ManagedDaveSession::new()),
        )
        .await
        .expect("client");

        done_rx.await.expect("resume flow complete");
        let mut health = client.health().await;
        let mut state = client.session_state().await;
        let deadline = Instant::now() + Duration::from_secs(2);
        while (!health.connected || health.reconnect_count == 0 || !state.resumed)
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
            health = client.health().await;
            state = client.session_state().await;
        }
        assert!(health.connected);
        assert_eq!(health.reconnect_count, 1);
        assert!(health.hello_received);

        assert!(state.resumed);
        let _ = release_tx.send(());
    }

    #[tokio::test]
    async fn wait_for_ready_survives_transient_disconnect_before_resume() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept first");
            let mut ws = accept_async(stream).await.expect("ws first");
            let identify = ws.next().await.expect("identify").expect("frame");
            let Message::Text(identify) = identify else {
                panic!("expected identify");
            };
            let payload: Value = serde_json::from_str(identify.as_ref()).expect("identify json");
            assert_eq!(payload["op"], json!(VoiceOpcode::Identify as u8));
            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::Hello as u8,
                    "d": { "heartbeat_interval": 1000 }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("hello first");
            let _ = ws.close(None).await;

            let (stream, _) = listener.accept().await.expect("accept second");
            let mut ws = accept_async(stream).await.expect("ws second");
            let resume = ws.next().await.expect("resume").expect("frame");
            let Message::Text(resume) = resume else {
                panic!("expected resume");
            };
            let payload: Value = serde_json::from_str(resume.as_ref()).expect("resume json");
            assert_eq!(payload["op"], json!(VoiceOpcode::Resume as u8));
            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::Hello as u8,
                    "d": { "heartbeat_interval": 1000 }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("hello second");
            ws.send(Message::Text(
                json!({
                    "op": VoiceOpcode::Ready as u8,
                    "d": {
                        "ssrc": 11,
                        "ip": "127.0.0.1",
                        "port": 5000,
                        "modes": ["aead_xchacha20_poly1305_rtpsize"],
                        "experiments": [],
                        "streams": [{
                            "ssrc": 22,
                            "rtx_ssrc": 33,
                            "rid": "100",
                            "quality": 100,
                            "active": false,
                            "max_bitrate": 4_000_000,
                            "max_framerate": 30
                        }]
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ready");
            let _ = ws.close(None).await;
        });

        let client = VoiceGatewayClient::connect(
            VoiceServerInfo {
                endpoint: format!("ws://{addr}"),
                token: "voice-token".to_owned(),
            },
            VoiceSessionConfig {
                server_id: "guild-1".to_owned(),
                channel_id: "2".to_owned(),
                user_id: "1".to_owned(),
                session_id: "session-1".to_owned(),
                token: "voice-token".to_owned(),
                stream_kind: StreamKind::GoLive,
                max_dave_protocol_version: 1,
            },
            VoiceSessionController::new(ManagedDaveSession::new()),
        )
        .await
        .expect("client");

        let health = client
            .wait_for_ready(Duration::from_secs(2))
            .await
            .expect("ready after resume");
        assert!(health.ready_received);
        assert!(health.reconnect_count >= 1);

        client.close().await.expect("close");
    }
}
