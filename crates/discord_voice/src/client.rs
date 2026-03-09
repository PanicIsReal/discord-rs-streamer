use std::sync::Arc;
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
use tracing::debug;
use url::Url;

use crate::{
    VoiceError, VoiceOutbound, VoiceServerInfo, VoiceSessionConfig, VoiceSessionController,
    VoiceSessionState,
};

const DEFAULT_VOICE_GATEWAY_VERSION: u8 = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VoiceGatewayHealth {
    pub connected: bool,
    pub hello_received: bool,
    pub ready_received: bool,
    pub session_description_received: bool,
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

        let (stream, _) = connect_async(url.as_str())
            .await
            .map_err(|error| VoiceGatewayError::Connection(error.to_string()))?;
        let (mut write, mut read) = stream.split();
        let (command_tx, mut command_rx) = mpsc::unbounded_channel::<ClientCommand>();
        let health = Arc::new(RwLock::new(VoiceGatewayHealth {
            connected: true,
            ..VoiceGatewayHealth::default()
        }));
        let health_for_task = Arc::clone(&health);
        let controller_for_task = Arc::clone(&controller);
        let identify = controller_for_task
            .lock()
            .await
            .identify(&config)
            .map_err(map_voice_error)?;

        let task = tokio::spawn(async move {
            let mut heartbeat = None::<tokio::time::Interval>;
            if write
                .send(Message::Text(identify.to_string().into()))
                .await
                .is_err()
            {
                set_client_error(&health_for_task, "failed to send voice identify").await;
                return;
            }

            loop {
                tokio::select! {
                    maybe_command = command_rx.recv() => {
                        match maybe_command {
                            Some(ClientCommand::Json(payload)) => {
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
                                break;
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
                                    &mut write,
                                ).await {
                                    set_client_error(&health_for_task, error).await;
                                    break;
                                }
                            }
                            Some(Err(error)) => {
                                set_client_error(&health_for_task, format!("voice gateway read failed: {error}")).await;
                                break;
                            }
                            None => {
                                set_client_error(&health_for_task, "voice gateway ended").await;
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            controller,
            health,
            command_tx,
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

    pub async fn set_streaming_announced(&self, announced: bool) -> Result<(), VoiceGatewayError> {
        self.controller
            .lock()
            .await
            .state_mut()
            .set_streaming_announced(announced);
        Ok(())
    }

    pub async fn close(mut self) -> Result<(), VoiceGatewayError> {
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
            if !health.connected {
                return Err(VoiceGatewayError::Protocol(
                    health
                        .last_error
                        .unwrap_or_else(|| "voice gateway disconnected".to_owned()),
                ));
            }
            if started.elapsed() >= timeout {
                return Err(VoiceGatewayError::Protocol(
                    "voice gateway readiness timed out".to_owned(),
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
            {
                let mut health = health.write().await;
                match payload.get("op").and_then(Value::as_u64) {
                    Some(2) => health.ready_received = true,
                    Some(4) => health.session_description_received = true,
                    _ => {}
                }
            }
            send_outbound(outbound, write).await?;
        }
        Message::Binary(payload) => {
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

async fn send_outbound<S>(outbound: Vec<VoiceOutbound>, write: &mut S) -> Result<(), String>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    for message in outbound {
        match message {
            VoiceOutbound::Json(payload) => write
                .send(Message::Text(payload.to_string().into()))
                .await
                .map_err(|error| error.to_string())?,
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

fn map_voice_error(error: VoiceError) -> VoiceGatewayError {
    VoiceGatewayError::Protocol(error.to_string())
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
                    "d": { "heartbeat_interval": 1000 }
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
}
