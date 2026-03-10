use std::collections::{HashMap, HashSet};

use dave::{DaveControl, DaveError, DaveInitConfig, DaveProposalResult, ProposalOp};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

pub mod client;
#[cfg(feature = "webrtc")]
pub mod publisher;
#[cfg(feature = "webrtc")]
pub mod session;

pub const AUDIO_MID: &str = "0";
pub const VIDEO_MID: &str = "1";
pub const OPUS_PAYLOAD_TYPE: u8 = 120;
pub const H264_PAYLOAD_TYPE: u8 = 101;
pub const H264_RTX_PAYLOAD_TYPE: u8 = 102;
pub const H265_PAYLOAD_TYPE: u8 = 103;
pub const H265_RTX_PAYLOAD_TYPE: u8 = 104;
pub const VP8_PAYLOAD_TYPE: u8 = 105;
pub const VP8_RTX_PAYLOAD_TYPE: u8 = 106;
pub const VP9_PAYLOAD_TYPE: u8 = 107;
pub const VP9_RTX_PAYLOAD_TYPE: u8 = 108;
pub const AV1_PAYLOAD_TYPE: u8 = 109;
pub const AV1_RTX_PAYLOAD_TYPE: u8 = 110;
const VOICE_OPCODE_READY: u8 = VoiceOpcode::Ready as u8;
const VOICE_OPCODE_SESSION_DESCRIPTION: u8 = VoiceOpcode::SessionDescription as u8;
const VOICE_OPCODE_HELLO: u8 = VoiceOpcode::Hello as u8;
const VOICE_OPCODE_HEARTBEAT_ACK: u8 = VoiceOpcode::HeartbeatAck as u8;
const VOICE_OPCODE_SPEAKING: u8 = VoiceOpcode::Speaking as u8;
const VOICE_OPCODE_RESUMED: u8 = VoiceOpcode::Resumed as u8;
const VOICE_OPCODE_CLIENTS_CONNECT: u8 = VoiceOpcode::ClientsConnect as u8;
const VOICE_OPCODE_CLIENT_DISCONNECT: u8 = VoiceOpcode::ClientDisconnect as u8;
const VOICE_OPCODE_MEDIA_SINK_WANTS: u8 = VoiceOpcode::MediaSinkWants as u8;
const VOICE_OPCODE_FLAGS: u8 = VoiceOpcode::Flags as u8;
const VOICE_OPCODE_PLATFORM: u8 = VoiceOpcode::Platform as u8;
const VOICE_OPCODE_DAVE_PREPARE_TRANSITION: u8 = VoiceOpcode::DavePrepareTransition as u8;
const VOICE_OPCODE_DAVE_EXECUTE_TRANSITION: u8 = VoiceOpcode::DaveExecuteTransition as u8;
const VOICE_OPCODE_DAVE_PREPARE_EPOCH: u8 = VoiceOpcode::DavePrepareEpoch as u8;
const VOICE_BINARY_OPCODE_EXTERNAL_SENDER: u8 = VoiceBinaryOpcode::MlsExternalSender as u8;
const VOICE_BINARY_OPCODE_PROPOSALS: u8 = VoiceBinaryOpcode::MlsProposals as u8;
const VOICE_BINARY_OPCODE_COMMIT_TRANSITION: u8 =
    VoiceBinaryOpcode::MlsAnnounceCommitTransition as u8;
const VOICE_BINARY_OPCODE_WELCOME: u8 = VoiceBinaryOpcode::MlsWelcome as u8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoiceOpcode {
    Identify = 0,
    SelectProtocol = 1,
    Ready = 2,
    Heartbeat = 3,
    SessionDescription = 4,
    Speaking = 5,
    HeartbeatAck = 6,
    Resume = 7,
    Hello = 8,
    Resumed = 9,
    ClientsConnect = 11,
    Video = 12,
    ClientDisconnect = 13,
    SessionUpdate = 14,
    MediaSinkWants = 15,
    VoiceBackendVersion = 16,
    ChannelOptionsUpdate = 17,
    Flags = 18,
    SpeedTest = 19,
    Platform = 20,
    DavePrepareTransition = 21,
    DaveExecuteTransition = 22,
    DaveTransitionReady = 23,
    DavePrepareEpoch = 24,
    MlsInvalidCommitWelcome = 31,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoiceBinaryOpcode {
    MlsExternalSender = 25,
    MlsKeyPackage = 26,
    MlsProposals = 27,
    MlsCommitWelcome = 28,
    MlsAnnounceCommitTransition = 29,
    MlsWelcome = 30,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoiceMediaKind {
    Audio,
    Video,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamKind {
    GoLive,
    Camera,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceSessionConfig {
    pub server_id: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_id: String,
    pub token: String,
    pub stream_kind: StreamKind,
    pub max_dave_protocol_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceServerInfo {
    pub endpoint: String,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamServerInfo {
    pub endpoint: String,
    pub token: String,
    pub stream_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDescriptor {
    pub rid: String,
    pub quality: u16,
    pub ssrc: u32,
    pub rtx_ssrc: u32,
    pub active: bool,
    #[serde(rename = "type")]
    pub media_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyPayload {
    pub audio_ssrc: u32,
    pub ip: String,
    pub port: u16,
    pub modes: Vec<String>,
    pub streams: Vec<StreamDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectProtocolAck {
    pub audio_codec: String,
    pub video_codec: String,
    pub dave_protocol_version: u16,
    pub sdp: String,
    pub media_session_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceAuxPayload {
    pub data: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceVideoConfig {
    pub width: u32,
    pub height: u32,
    pub max_framerate: u32,
    pub max_bitrate: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpeakingMode {
    Mic = 1,
    Soundshare = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceWebRtcConfig {
    pub local_sdp: String,
    pub rtc_connection_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VoiceSessionState {
    pub server_endpoint: Option<String>,
    pub server_token: Option<String>,
    pub session_id: Option<String>,
    pub seq_ack: Option<u16>,
    pub dave_protocol_version: u16,
    pub ready: Option<ReadyPayload>,
    pub protocol_ack: Option<SelectProtocolAck>,
    pub connected_users: HashSet<u64>,
    pub streaming_announced: bool,
    pub pending_transitions: HashMap<u16, u16>,
    pub resumed: bool,
    pub media_sink_wants: Option<VoiceAuxPayload>,
    pub flags: Option<VoiceAuxPayload>,
    pub platform: Option<VoiceAuxPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceOutbound {
    Json(Value),
    Binary(Vec<u8>),
}

#[derive(Debug, Error)]
pub enum VoiceError {
    #[error("voice session is missing server endpoint or token")]
    MissingServerInfo,
    #[error("voice session is missing identify resources")]
    MissingIdentifyResources,
    #[error("voice ready payload is missing stream descriptors")]
    MissingReadyStream,
    #[error("voice payload was invalid: {0}")]
    InvalidPayload(String),
    #[error("dave operation failed: {0}")]
    Dave(String),
}

pub struct VoiceSessionController<D> {
    dave: D,
    state: VoiceSessionState,
    config: Option<VoiceSessionConfig>,
}

impl<D> VoiceSessionController<D>
where
    D: DaveControl,
{
    pub fn new(dave: D) -> Self {
        Self {
            dave,
            state: VoiceSessionState::default(),
            config: None,
        }
    }

    pub fn state(&self) -> &VoiceSessionState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut VoiceSessionState {
        &mut self.state
    }

    pub fn prepare_session(&mut self, info: VoiceServerInfo, config: VoiceSessionConfig) {
        self.state.server_endpoint = Some(info.endpoint);
        self.state.server_token = Some(info.token);
        self.state.session_id = Some(config.session_id.clone());
        self.config = Some(config);
    }

    pub fn update_server(&mut self, info: VoiceServerInfo, session_id: String) {
        self.state.server_endpoint = Some(info.endpoint);
        self.state.server_token = Some(info.token);
        self.state.session_id = Some(session_id);
    }

    pub fn identify(&self, config: &VoiceSessionConfig) -> Result<Value, VoiceError> {
        if self.state.server_endpoint.is_none() || self.state.server_token.is_none() {
            return Err(VoiceError::MissingServerInfo);
        }
        if config.server_id.is_empty()
            || config.user_id.is_empty()
            || config.session_id.is_empty()
            || config.token.is_empty()
        {
            return Err(VoiceError::MissingIdentifyResources);
        }

        Ok(json!({
            "op": VoiceOpcode::Identify as u8,
            "d": {
                "server_id": config.server_id,
                "user_id": config.user_id,
                "session_id": config.session_id,
                "token": config.token,
                "video": true,
                "streams": [{
                    "type": identify_stream_type(config.stream_kind),
                    "rid":"100",
                    "quality":100
                }],
                "max_dave_protocol_version": config.max_dave_protocol_version,
            }
        }))
    }

    pub fn resume(&self, config: &VoiceSessionConfig) -> Result<Value, VoiceError> {
        if config.server_id.is_empty() || config.session_id.is_empty() || config.token.is_empty() {
            return Err(VoiceError::MissingIdentifyResources);
        }

        Ok(json!({
            "op": VoiceOpcode::Resume as u8,
            "d": {
                "server_id": config.server_id,
                "session_id": config.session_id,
                "token": config.token,
                "seq_ack": self.state.seq_ack.unwrap_or_default(),
            }
        }))
    }

    pub fn heartbeat(&self) -> Value {
        json!({
            "op": VoiceOpcode::Heartbeat as u8,
            "d": {
                "t": 0,
                "seq_ack": self.state.seq_ack.unwrap_or_default(),
            }
        })
    }

    pub fn select_protocol(&self, config: VoiceWebRtcConfig) -> Result<Value, VoiceError> {
        Ok(json!({
            "op": VoiceOpcode::SelectProtocol as u8,
            "d": {
                "protocol": "webrtc",
                "codecs": codecs(),
                "data": config.local_sdp,
                "sdp": config.local_sdp,
                "rtc_connection_id": config.rtc_connection_id,
            }
        }))
    }

    pub fn video_command(
        &self,
        config: VoiceVideoConfig,
        active: bool,
    ) -> Result<Value, VoiceError> {
        let Some(ready) = self.state.ready.as_ref() else {
            return Err(VoiceError::MissingReadyStream);
        };
        let Some(video_stream) = find_video_stream(ready) else {
            return Err(VoiceError::MissingReadyStream);
        };

        Ok(build_video_command(ready, video_stream, &config, active))
    }

    pub fn speaking_payload(&self, enabled: bool) -> Option<Value> {
        let stream_kind = self
            .config
            .as_ref()
            .map(|config| config.stream_kind)
            .unwrap_or(StreamKind::Camera);
        self.state.speaking_payload(stream_kind, enabled)
    }

    pub fn handle_json_message(
        &mut self,
        payload: &Value,
    ) -> Result<Vec<VoiceOutbound>, VoiceError> {
        let op = payload
            .get("op")
            .and_then(Value::as_u64)
            .ok_or_else(|| VoiceError::InvalidPayload("missing opcode".to_owned()))?;
        if let Some(seq) = payload.get("seq").and_then(Value::as_u64) {
            self.state.seq_ack = Some(seq as u16);
        }
        let data = payload.get("d").cloned().unwrap_or(Value::Null);
        match op as u8 {
            VOICE_OPCODE_READY => {
                self.state.ready = Some(parse_ready(&data)?);
                Ok(Vec::new())
            }
            VOICE_OPCODE_SESSION_DESCRIPTION => {
                self.state.protocol_ack = Some(parse_protocol_ack(&data)?);
                self.state.dave_protocol_version = self
                    .state
                    .protocol_ack
                    .as_ref()
                    .map(|ack| ack.dave_protocol_version)
                    .unwrap_or_default();
                self.reinitialize_dave_session(self.state.dave_protocol_version)?;
                Ok(vec![VoiceOutbound::Binary(self.key_package_message()?)])
            }
            VOICE_OPCODE_CLIENTS_CONNECT => {
                for user_id in parse_client_ids(&data, "user_ids")? {
                    self.state.connected_users.insert(user_id);
                }
                Ok(Vec::new())
            }
            VOICE_OPCODE_CLIENT_DISCONNECT => {
                let user_id = data
                    .get("user_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        VoiceError::InvalidPayload("client_disconnect missing user_id".to_owned())
                    })?
                    .parse::<u64>()
                    .map_err(|error| {
                        VoiceError::InvalidPayload(format!(
                            "invalid client_disconnect user_id: {error}"
                        ))
                    })?;
                self.state.connected_users.remove(&user_id);
                Ok(Vec::new())
            }
            VOICE_OPCODE_DAVE_PREPARE_TRANSITION => {
                let transition_id = read_u16(&data, "transition_id")?;
                let protocol_version = read_u16(&data, "protocol_version")?;
                self.state
                    .pending_transitions
                    .insert(transition_id, protocol_version);
                if transition_id == 0 {
                    self.execute_transition(transition_id);
                    Ok(Vec::new())
                } else {
                    Ok(vec![VoiceOutbound::Json(json!({
                        "op": VoiceOpcode::DaveTransitionReady as u8,
                        "d": { "transition_id": transition_id }
                    }))])
                }
            }
            VOICE_OPCODE_DAVE_EXECUTE_TRANSITION => {
                let transition_id = read_u16(&data, "transition_id")?;
                self.execute_transition(transition_id);
                Ok(Vec::new())
            }
            VOICE_OPCODE_DAVE_PREPARE_EPOCH => {
                let epoch = read_u16(&data, "epoch")?;
                let protocol_version = read_u16(&data, "protocol_version")?;
                if epoch == 1 {
                    self.state.dave_protocol_version = protocol_version;
                    self.reinitialize_dave_session(protocol_version)?;
                    Ok(vec![VoiceOutbound::Binary(self.key_package_message()?)])
                } else {
                    Ok(Vec::new())
                }
            }
            VOICE_OPCODE_RESUMED => {
                self.state.resumed = true;
                Ok(Vec::new())
            }
            VOICE_OPCODE_MEDIA_SINK_WANTS => {
                self.state.media_sink_wants = Some(VoiceAuxPayload { data });
                Ok(Vec::new())
            }
            VOICE_OPCODE_FLAGS => {
                self.state.flags = Some(VoiceAuxPayload { data });
                Ok(Vec::new())
            }
            VOICE_OPCODE_PLATFORM => {
                self.state.platform = Some(VoiceAuxPayload { data });
                Ok(Vec::new())
            }
            VOICE_OPCODE_HELLO | VOICE_OPCODE_HEARTBEAT_ACK | VOICE_OPCODE_SPEAKING => {
                Ok(Vec::new())
            }
            _ => {
                debug!(opcode = op, "ignoring unsupported voice opcode");
                Ok(Vec::new())
            }
        }
    }

    pub fn handle_binary_message(
        &mut self,
        payload: &[u8],
    ) -> Result<Vec<VoiceOutbound>, VoiceError> {
        if payload.len() < 3 {
            return Err(VoiceError::InvalidPayload(
                "binary payload too short".to_owned(),
            ));
        }
        self.state.seq_ack = Some(u16::from_be_bytes([payload[0], payload[1]]));
        let opcode = payload[2];
        match opcode {
            VOICE_BINARY_OPCODE_EXTERNAL_SENDER => {
                self.dave
                    .set_external_sender(&payload[3..])
                    .map_err(|error| VoiceError::Dave(error.to_string()))?;
                Ok(Vec::new())
            }
            VOICE_BINARY_OPCODE_PROPOSALS => {
                if payload.len() < 4 {
                    return Err(VoiceError::InvalidPayload(
                        "mls proposals missing operation".to_owned(),
                    ));
                }
                let operation = match payload[3] {
                    0 => ProposalOp::Append,
                    1 => ProposalOp::Revoke,
                    value => {
                        return Err(VoiceError::InvalidPayload(format!(
                            "unsupported proposal operation {value}"
                        )));
                    }
                };
                match self.dave.process_proposals_bundle(
                    operation,
                    &payload[4..],
                    self.expected_user_ids().as_deref(),
                ) {
                    Ok(result) => Ok(encode_commit_welcome(result)),
                    Err(error) => {
                        debug!(error = %error, "recovering from MLS proposals failure");
                        self.proposals_recovery()
                    }
                }
            }
            VOICE_BINARY_OPCODE_COMMIT_TRANSITION => {
                if payload.len() < 5 {
                    return Err(VoiceError::InvalidPayload(
                        "mls commit missing transition id".to_owned(),
                    ));
                }
                let transition_id = u16::from_be_bytes([payload[3], payload[4]]);
                self.handle_transition_result(
                    transition_id,
                    self.dave.process_commit(&payload[5..]).map(|_| ()),
                    "commit",
                )
            }
            VOICE_BINARY_OPCODE_WELCOME => {
                if payload.len() < 5 {
                    return Err(VoiceError::InvalidPayload(
                        "mls welcome missing transition id".to_owned(),
                    ));
                }
                let transition_id = u16::from_be_bytes([payload[3], payload[4]]);
                self.handle_transition_result(
                    transition_id,
                    self.dave.process_welcome(&payload[5..]).map(|_| ()),
                    "welcome",
                )
            }
            _ => {
                debug!(opcode, "ignoring unsupported voice binary opcode");
                Ok(Vec::new())
            }
        }
    }

    pub fn build_track_plan(&self) -> Result<TrackPlan, VoiceError> {
        self.state
            .track_plan()
            .ok_or(VoiceError::MissingReadyStream)
    }

    fn execute_transition(&mut self, transition_id: u16) {
        if let Some(protocol_version) = self.state.pending_transitions.remove(&transition_id) {
            self.state.dave_protocol_version = protocol_version;
        }
    }

    fn key_package_message(&self) -> Result<Vec<u8>, VoiceError> {
        let key_package = self
            .dave
            .create_key_package()
            .map_err(|error| VoiceError::Dave(error.to_string()))?;
        let mut message = Vec::with_capacity(key_package.len() + 1);
        message.push(VoiceBinaryOpcode::MlsKeyPackage as u8);
        message.extend_from_slice(&key_package);
        Ok(message)
    }

    fn invalid_commit_recovery(
        &self,
        transition_id: u16,
    ) -> Result<Vec<VoiceOutbound>, VoiceError> {
        self.reinitialize_dave_session(self.state.dave_protocol_version)?;
        Ok(vec![
            VoiceOutbound::Json(json!({
                "op": VoiceOpcode::MlsInvalidCommitWelcome as u8,
                "d": { "transition_id": transition_id }
            })),
            VoiceOutbound::Binary(self.key_package_message()?),
        ])
    }

    fn proposals_recovery(&self) -> Result<Vec<VoiceOutbound>, VoiceError> {
        self.reinitialize_dave_session(self.state.dave_protocol_version)?;
        Ok(vec![VoiceOutbound::Binary(self.key_package_message()?)])
    }

    fn handle_transition_result(
        &mut self,
        transition_id: u16,
        result: Result<(), DaveError>,
        recovery_label: &str,
    ) -> Result<Vec<VoiceOutbound>, VoiceError> {
        match result {
            Ok(_) => self.transition_ready_outbound(transition_id),
            Err(error) => {
                debug!(
                    error = %error,
                    transition_id,
                    recovery_label,
                    "recovering from MLS {recovery_label} failure"
                );
                self.invalid_commit_recovery(transition_id)
            }
        }
    }

    fn transition_ready_outbound(
        &mut self,
        transition_id: u16,
    ) -> Result<Vec<VoiceOutbound>, VoiceError> {
        if transition_id == 0 {
            return Ok(Vec::new());
        }

        self.state
            .pending_transitions
            .insert(transition_id, self.state.dave_protocol_version);

        Ok(vec![VoiceOutbound::Json(json!({
            "op": VoiceOpcode::DaveTransitionReady as u8,
            "d": { "transition_id": transition_id }
        }))])
    }

    fn reinitialize_dave_session(&self, protocol_version: u16) -> Result<(), VoiceError> {
        if protocol_version == 0 {
            return Ok(());
        }

        let config = self
            .config
            .as_ref()
            .ok_or_else(|| VoiceError::InvalidPayload("voice session config missing".to_owned()))?;
        let user_id = config.user_id.parse::<u64>().map_err(|error| {
            VoiceError::InvalidPayload(format!("voice user_id must be numeric: {error}"))
        })?;
        let channel_id = config.channel_id.parse::<u64>().map_err(|error| {
            VoiceError::InvalidPayload(format!("voice channel_id must be numeric: {error}"))
        })?;

        self.dave
            .initialize(DaveInitConfig {
                protocol_version,
                user_id,
                channel_id,
                signing_private_key: None,
                signing_public_key: None,
            })
            .map(|_| ())
            .map_err(|error| VoiceError::Dave(error.to_string()))
    }

    fn expected_user_ids(&self) -> Option<Vec<u64>> {
        let mut user_ids: Vec<u64> = self.state.connected_users.iter().copied().collect();
        if let Some(user_id) = self.session_user_id()
            && !user_ids.contains(&user_id)
        {
            user_ids.push(user_id);
        }
        (!user_ids.is_empty()).then_some(user_ids)
    }

    fn session_user_id(&self) -> Option<u64> {
        self.config
            .as_ref()
            .and_then(|config| config.user_id.parse::<u64>().ok())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackPlan {
    pub audio: TrackSpec,
    pub video: TrackSpec,
    pub rtx_ssrc: Option<u32>,
    pub rtx_payload_type: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrackKind {
    Audio,
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackSpec {
    pub kind: TrackKind,
    pub ssrc: u32,
    pub payload_type: u8,
    pub mid: String,
    pub codec: String,
}

impl VoiceSessionState {
    pub fn track_plan(&self) -> Option<TrackPlan> {
        let ready = self.ready.as_ref()?;
        let video_stream = find_video_stream(ready)?;
        Some(TrackPlan {
            audio: TrackSpec {
                kind: TrackKind::Audio,
                ssrc: ready.audio_ssrc,
                payload_type: OPUS_PAYLOAD_TYPE,
                mid: AUDIO_MID.to_owned(),
                codec: "opus".to_owned(),
            },
            video: TrackSpec {
                kind: TrackKind::Video,
                ssrc: video_stream.ssrc,
                payload_type: H264_PAYLOAD_TYPE,
                mid: VIDEO_MID.to_owned(),
                codec: "H264".to_owned(),
            },
            rtx_ssrc: Some(video_stream.rtx_ssrc),
            rtx_payload_type: Some(H264_RTX_PAYLOAD_TYPE),
        })
    }

    pub fn select_protocol_payload(
        &self,
        local_sdp: String,
        rtc_connection_id: Uuid,
    ) -> Option<Value> {
        self.ready.as_ref()?;
        Some(json!({
            "op": VoiceOpcode::SelectProtocol as u8,
            "d": {
                "protocol": "webrtc",
                "codecs": codecs(),
                "data": local_sdp,
                "sdp": local_sdp,
                "rtc_connection_id": rtc_connection_id,
            }
        }))
    }

    pub fn video_command_payload(&self, config: &VoiceVideoConfig, active: bool) -> Option<Value> {
        let ready = self.ready.as_ref()?;
        let video_stream = find_video_stream(ready)?;
        Some(build_video_command(ready, video_stream, config, active))
    }

    pub fn speaking_payload(&self, stream_kind: StreamKind, enabled: bool) -> Option<Value> {
        let ready = self.ready.as_ref()?;
        let speaking = if enabled {
            speaking_mode_for_stream(stream_kind) as u8
        } else {
            0
        };
        Some(json!({
            "op": VoiceOpcode::Speaking as u8,
            "d": {
                "delay": 0,
                "speaking": speaking,
                "ssrc": ready.audio_ssrc,
            }
        }))
    }

    pub fn set_streaming_announced(&mut self, announced: bool) {
        self.streaming_announced = announced;
    }
}

fn codecs() -> Vec<Value> {
    vec![
        json!({
            "name": "opus",
            "type": "audio",
            "clockRate": 48_000,
            "priority": 1000,
            "payload_type": OPUS_PAYLOAD_TYPE,
        }),
        json!({
            "name": "H264",
            "type": "video",
            "clockRate": 90_000,
            "priority": 1000,
            "payload_type": H264_PAYLOAD_TYPE,
            "rtx_payload_type": H264_RTX_PAYLOAD_TYPE,
            "encode": true,
            "decode": true,
        }),
        json!({
            "name": "H265",
            "type": "video",
            "clockRate": 90_000,
            "priority": 1000,
            "payload_type": H265_PAYLOAD_TYPE,
            "rtx_payload_type": H265_RTX_PAYLOAD_TYPE,
            "encode": true,
            "decode": true,
        }),
        json!({
            "name": "VP8",
            "type": "video",
            "clockRate": 90_000,
            "priority": 1000,
            "payload_type": VP8_PAYLOAD_TYPE,
            "rtx_payload_type": VP8_RTX_PAYLOAD_TYPE,
            "encode": true,
            "decode": true,
        }),
        json!({
            "name": "VP9",
            "type": "video",
            "clockRate": 90_000,
            "priority": 1000,
            "payload_type": VP9_PAYLOAD_TYPE,
            "rtx_payload_type": VP9_RTX_PAYLOAD_TYPE,
            "encode": true,
            "decode": true,
        }),
        json!({
            "name": "AV1",
            "type": "video",
            "clockRate": 90_000,
            "priority": 1000,
            "payload_type": AV1_PAYLOAD_TYPE,
            "rtx_payload_type": AV1_RTX_PAYLOAD_TYPE,
            "encode": true,
            "decode": true,
        }),
    ]
}

fn identify_stream_type(stream_kind: StreamKind) -> &'static str {
    match stream_kind {
        StreamKind::GoLive => "screen",
        StreamKind::Camera => "video",
    }
}

fn speaking_mode_for_stream(stream_kind: StreamKind) -> SpeakingMode {
    match stream_kind {
        StreamKind::GoLive => SpeakingMode::Soundshare,
        StreamKind::Camera => SpeakingMode::Mic,
    }
}

fn build_video_command(
    ready: &ReadyPayload,
    video_stream: &StreamDescriptor,
    config: &VoiceVideoConfig,
    active: bool,
) -> Value {
    if !active {
        return json!({
            "op": VoiceOpcode::Video as u8,
            "d": {
                "audio_ssrc": ready.audio_ssrc,
                "video_ssrc": 0,
                "rtx_ssrc": 0,
                "streams": []
            }
        });
    }

    json!({
        "op": VoiceOpcode::Video as u8,
        "d": {
            "audio_ssrc": ready.audio_ssrc,
            "video_ssrc": video_stream.ssrc,
            "rtx_ssrc": video_stream.rtx_ssrc,
            "streams": [{
                "type": "video",
                "rid": video_stream.rid,
                "ssrc": video_stream.ssrc,
                "active": true,
                "quality": video_stream.quality,
                "rtx_ssrc": video_stream.rtx_ssrc,
                "max_bitrate": config.max_bitrate,
                "max_framerate": config.max_framerate,
                "max_resolution": {
                    "type": "fixed",
                    "width": config.width,
                    "height": config.height,
                }
            }]
        }
    })
}

fn find_video_stream(ready: &ReadyPayload) -> Option<&StreamDescriptor> {
    ready
        .streams
        .iter()
        .find(|stream| stream.media_type == "video")
}

fn parse_ready(data: &Value) -> Result<ReadyPayload, VoiceError> {
    let audio_ssrc = data
        .get("ssrc")
        .and_then(Value::as_u64)
        .ok_or_else(|| VoiceError::InvalidPayload("ready missing ssrc".to_owned()))?
        as u32;
    let ip = data
        .get("ip")
        .and_then(Value::as_str)
        .ok_or_else(|| VoiceError::InvalidPayload("ready missing ip".to_owned()))?
        .to_owned();
    let port = data
        .get("port")
        .and_then(Value::as_u64)
        .ok_or_else(|| VoiceError::InvalidPayload("ready missing port".to_owned()))?
        as u16;
    let modes = data
        .get("modes")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let streams = data
        .get("streams")
        .and_then(Value::as_array)
        .ok_or_else(|| VoiceError::InvalidPayload("ready missing streams".to_owned()))?
        .iter()
        .map(parse_stream_descriptor)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ReadyPayload {
        audio_ssrc,
        ip,
        port,
        modes,
        streams,
    })
}

fn parse_stream_descriptor(data: &Value) -> Result<StreamDescriptor, VoiceError> {
    Ok(StreamDescriptor {
        rid: data
            .get("rid")
            .and_then(Value::as_str)
            .ok_or_else(|| VoiceError::InvalidPayload("stream missing rid".to_owned()))?
            .to_owned(),
        quality: data
            .get("quality")
            .and_then(Value::as_u64)
            .ok_or_else(|| VoiceError::InvalidPayload("stream missing quality".to_owned()))?
            as u16,
        ssrc: data
            .get("ssrc")
            .and_then(Value::as_u64)
            .ok_or_else(|| VoiceError::InvalidPayload("stream missing ssrc".to_owned()))?
            as u32,
        rtx_ssrc: data
            .get("rtx_ssrc")
            .and_then(Value::as_u64)
            .ok_or_else(|| VoiceError::InvalidPayload("stream missing rtx_ssrc".to_owned()))?
            as u32,
        active: data.get("active").and_then(Value::as_bool).unwrap_or(true),
        media_type: data
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("video")
            .to_owned(),
    })
}

fn parse_protocol_ack(data: &Value) -> Result<SelectProtocolAck, VoiceError> {
    Ok(SelectProtocolAck {
        audio_codec: data
            .get("audio_codec")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        video_codec: data
            .get("video_codec")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        dave_protocol_version: data
            .get("dave_protocol_version")
            .and_then(Value::as_u64)
            .unwrap_or_default() as u16,
        sdp: data
            .get("sdp")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        media_session_id: data.get("media_session_id").and_then(Value::as_u64),
    })
}

fn parse_client_ids(data: &Value, field: &str) -> Result<Vec<u64>, VoiceError> {
    let values = data
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| VoiceError::InvalidPayload(format!("{field} missing")))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| VoiceError::InvalidPayload(format!("{field} member must be string")))
                .and_then(|value| {
                    value.parse::<u64>().map_err(|error| {
                        VoiceError::InvalidPayload(format!("{field} member was invalid: {error}"))
                    })
                })
        })
        .collect()
}

fn read_u16(data: &Value, field: &str) -> Result<u16, VoiceError> {
    data.get(field)
        .and_then(Value::as_u64)
        .map(|value| value as u16)
        .ok_or_else(|| VoiceError::InvalidPayload(format!("{field} missing")))
}

fn encode_commit_welcome(result: DaveProposalResult) -> Vec<VoiceOutbound> {
    let Some(bundle) = result.commit_welcome else {
        return Vec::new();
    };
    let mut payload =
        Vec::with_capacity(1 + bundle.commit.len() + bundle.welcome.as_ref().map_or(0, Vec::len));
    payload.push(VoiceBinaryOpcode::MlsCommitWelcome as u8);
    payload.extend_from_slice(&bundle.commit);
    if let Some(welcome) = bundle.welcome {
        payload.extend_from_slice(&welcome);
    }
    vec![VoiceOutbound::Binary(payload)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use dave::{
        DaveError, DaveInitConfig, DaveMetadata, DaveSession, DaveSessionStatus, DaveState,
        ManagedDaveSession,
    };

    #[test]
    fn identify_payload_includes_streaming_fields() {
        let mut controller = VoiceSessionController::new(ManagedDaveSession::new());
        controller.update_server(
            VoiceServerInfo {
                endpoint: "voice.example.test".to_owned(),
                token: "voice-token".to_owned(),
            },
            "voice-session".to_owned(),
        );

        let payload = controller
            .identify(&VoiceSessionConfig {
                server_id: "guild-1".to_owned(),
                channel_id: "2".to_owned(),
                user_id: "user-1".to_owned(),
                session_id: "session-1".to_owned(),
                token: "voice-token".to_owned(),
                stream_kind: StreamKind::GoLive,
                max_dave_protocol_version: 1,
            })
            .expect("identify");

        assert_eq!(payload["op"], json!(VoiceOpcode::Identify as u8));
        assert!(payload["d"].get("channel_id").is_none());
        assert_eq!(payload["d"]["video"], json!(true));
        assert_eq!(payload["d"]["max_dave_protocol_version"], json!(1));
    }

    #[test]
    fn speaking_payload_uses_soundshare_for_go_live() {
        let dave = ManagedDaveSession::new();
        dave.initialize(DaveInitConfig {
            protocol_version: 1,
            user_id: 1,
            channel_id: 2,
            signing_private_key: None,
            signing_public_key: None,
        })
        .expect("dave init");
        let mut controller = VoiceSessionController::new(dave);
        controller.prepare_session(
            VoiceServerInfo {
                endpoint: "voice.example.test".to_owned(),
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
        );
        controller
            .handle_json_message(&json!({
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
            }))
            .expect("ready");

        let payload = controller.speaking_payload(true).expect("speaking payload");
        assert_eq!(payload["op"], json!(VoiceOpcode::Speaking as u8));
        assert_eq!(payload["d"]["ssrc"], json!(111));
        assert_eq!(payload["d"]["speaking"], json!(SpeakingMode::Soundshare as u8));
    }

    #[test]
    fn media_sink_wants_payload_is_preserved_in_state() {
        let mut controller = VoiceSessionController::new(ManagedDaveSession::new());
        controller
            .handle_json_message(&json!({
                "op": VoiceOpcode::MediaSinkWants as u8,
                "d": {
                    "any": {
                        "video": {
                            "max_framerate": 15
                        }
                    }
                }
            }))
            .expect("media sink wants");

        let stored = controller
            .state()
            .media_sink_wants
            .as_ref()
            .expect("stored payload");
        assert_eq!(stored.data["any"]["video"]["max_framerate"], json!(15));
    }

    #[test]
    fn ready_and_protocol_ack_build_track_plan() {
        let dave = ManagedDaveSession::new();
        dave.initialize(DaveInitConfig {
            protocol_version: 1,
            user_id: 1,
            channel_id: 2,
            signing_private_key: None,
            signing_public_key: None,
        })
        .expect("dave init");
        let mut controller = VoiceSessionController::new(dave);
        controller.prepare_session(
            VoiceServerInfo {
                endpoint: "voice.example.test".to_owned(),
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
        );
        controller
            .handle_json_message(&json!({
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
            }))
            .expect("ready");
        controller
            .handle_json_message(&json!({
                "op": VoiceOpcode::SessionDescription as u8,
                "d": {
                    "audio_codec": "opus",
                    "video_codec": "H264",
                    "dave_protocol_version": 1,
                    "sdp": "v=0",
                    "media_session_id": 42
                }
            }))
            .expect("ack");

        let plan = controller.build_track_plan().expect("track plan");
        assert_eq!(plan.audio.ssrc, 111);
        assert_eq!(plan.video.ssrc, 222);
        assert_eq!(plan.video.payload_type, H264_PAYLOAD_TYPE);
    }

    struct FakeDave;

    struct FakeDaveWithProposalFailure;

    impl DaveSession for FakeDave {
        fn protect(
            &self,
            _media_kind: dave::MediaKind,
            _metadata: &DaveMetadata,
            payload: Bytes,
        ) -> Result<Bytes, DaveError> {
            Ok(payload)
        }
    }

    impl DaveSession for FakeDaveWithProposalFailure {
        fn protect(
            &self,
            _media_kind: dave::MediaKind,
            _metadata: &DaveMetadata,
            payload: Bytes,
        ) -> Result<Bytes, DaveError> {
            Ok(payload)
        }
    }

    impl DaveControl for FakeDave {
        fn initialize(&self, _config: DaveInitConfig) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }

        fn set_external_sender(
            &self,
            _external_sender_data: &[u8],
        ) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }

        fn create_key_package(&self) -> Result<Vec<u8>, DaveError> {
            Ok(vec![1, 2, 3])
        }

        fn process_proposals(
            &self,
            _operation: ProposalOp,
            _proposals: &[u8],
            _recognized_user_ids: Option<&[u64]>,
        ) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }

        fn process_proposals_bundle(
            &self,
            _operation: ProposalOp,
            _proposals: &[u8],
            _recognized_user_ids: Option<&[u64]>,
        ) -> Result<DaveProposalResult, DaveError> {
            Ok(DaveProposalResult {
                state: DaveState {
                    initialized: true,
                    protocol_version: Some(1),
                    user_id: Some(1),
                    channel_id: Some(2),
                    ready: false,
                    status: DaveSessionStatus::Pending,
                    voice_privacy_code: None,
                    member_ids: None,
                },
                commit_welcome: Some(dave::DaveCommitWelcome {
                    commit: vec![9, 8, 7],
                    welcome: Some(vec![6, 5, 4]),
                }),
            })
        }

        fn process_welcome(&self, _welcome: &[u8]) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }

        fn process_commit(&self, _commit: &[u8]) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }

        fn state(&self) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }
    }

    impl DaveControl for FakeDaveWithProposalFailure {
        fn initialize(&self, _config: DaveInitConfig) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }

        fn set_external_sender(
            &self,
            _external_sender_data: &[u8],
        ) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }

        fn create_key_package(&self) -> Result<Vec<u8>, DaveError> {
            Ok(vec![4, 5, 6])
        }

        fn process_proposals(
            &self,
            _operation: ProposalOp,
            _proposals: &[u8],
            _recognized_user_ids: Option<&[u64]>,
        ) -> Result<DaveState, DaveError> {
            Err(DaveError::Message("proposal failure".to_owned()))
        }

        fn process_proposals_bundle(
            &self,
            _operation: ProposalOp,
            _proposals: &[u8],
            _recognized_user_ids: Option<&[u64]>,
        ) -> Result<DaveProposalResult, DaveError> {
            Err(DaveError::Message("proposal failure".to_owned()))
        }

        fn process_welcome(&self, _welcome: &[u8]) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }

        fn process_commit(&self, _commit: &[u8]) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }

        fn state(&self) -> Result<DaveState, DaveError> {
            Ok(DaveState::default())
        }
    }

    #[test]
    fn binary_proposals_emit_commit_welcome() {
        let mut controller = VoiceSessionController::new(FakeDave);
        let outbound = controller
            .handle_binary_message(&[0, 1, VoiceBinaryOpcode::MlsProposals as u8, 0, 0xaa])
            .expect("binary proposals");
        assert_eq!(outbound.len(), 1);
        match &outbound[0] {
            VoiceOutbound::Binary(payload) => {
                assert_eq!(payload[0], VoiceBinaryOpcode::MlsCommitWelcome as u8);
                assert_eq!(&payload[1..], &[9, 8, 7, 6, 5, 4]);
            }
            other => panic!("unexpected outbound: {other:?}"),
        }
    }

    #[test]
    fn binary_proposals_recover_with_fresh_key_package() {
        let mut controller = VoiceSessionController::new(FakeDaveWithProposalFailure);
        controller.prepare_session(
            VoiceServerInfo {
                endpoint: "voice.example.test".to_owned(),
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
        );
        controller.state_mut().dave_protocol_version = 1;

        let outbound = controller
            .handle_binary_message(&[0, 1, VoiceBinaryOpcode::MlsProposals as u8, 0, 0xaa])
            .expect("proposal recovery");
        assert_eq!(outbound, vec![VoiceOutbound::Binary(vec![26, 4, 5, 6])]);
    }
}
