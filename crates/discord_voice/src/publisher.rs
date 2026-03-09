use std::ffi::CString;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datachannel::{
    Codec, ConnectionState, DataChannelHandler, DataChannelInfo, Direction, IceCandidate,
    NalUnitSeparator, PacketizerInit, PeerConnectionHandler, Result as DcResult, RtcConfig,
    RtcDataChannel, RtcPeerConnection, RtcTrack, SdpType, SessionDescription, TrackHandler,
    TrackInit, configure_logging,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::Level;
use tracing::warn;

use crate::{AUDIO_MID, TrackPlan, TrackSpec, VIDEO_MID};

#[derive(Debug, Clone)]
pub struct VoicePublisherConfig {
    pub ice_servers: Vec<String>,
    pub bind_address: Option<String>,
    pub force_media_transport: bool,
    pub audio_frame_duration_ms: u32,
    pub video_framerate: u32,
}

impl Default for VoicePublisherConfig {
    fn default() -> Self {
        Self {
            ice_servers: vec!["stun:stun.l.google.com:19302".to_owned()],
            bind_address: None,
            force_media_transport: true,
            audio_frame_duration_ms: 20,
            video_framerate: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VoicePublisherState {
    pub local_description: Option<String>,
    pub remote_description: Option<String>,
    pub local_candidates: Vec<String>,
    pub connection_state: Option<String>,
    pub audio_track_open: bool,
    pub video_track_open: bool,
    pub audio_frames_sent: u64,
    pub video_frames_sent: u64,
    pub audio_bytes_sent: u64,
    pub video_bytes_sent: u64,
    pub last_send_error: Option<String>,
}

#[derive(Debug, Error)]
pub enum VoicePublisherError {
    #[error("failed to build peer connection: {0}")]
    PeerConnection(String),
    #[error("failed to configure track `{0}`: {1}")]
    Track(String, String),
    #[error("publisher lock poisoned")]
    LockPoisoned,
    #[error("track `{0}` is not ready for media yet")]
    TrackNotReady(String),
    #[error("remote sdp could not be parsed: {0}")]
    InvalidRemoteDescription(String),
    #[error("publisher did not reach ready state before timeout")]
    ReadyTimeout,
}

pub struct VoicePeerPublisher {
    track_plan: TrackPlan,
    peer_connection: Box<RtcPeerConnection<PublisherPeerConnectionHandler>>,
    audio_track: Box<RtcTrack<PublisherTrackHandler>>,
    video_track: Box<RtcTrack<PublisherTrackHandler>>,
    state: Arc<Mutex<VoicePublisherState>>,
    audio_timestamp_step: u32,
    video_timestamp_step: u32,
}

#[derive(Clone, Copy)]
enum PublisherTrackKind {
    Audio,
    Video,
}

impl VoicePeerPublisher {
    pub fn new(
        track_plan: &TrackPlan,
        config: VoicePublisherConfig,
    ) -> Result<Self, VoicePublisherError> {
        configure_logging(Level::INFO);

        let rtc_config = build_rtc_config(&config);
        let state = Arc::new(Mutex::new(VoicePublisherState::default()));
        let handler = PublisherPeerConnectionHandler {
            state: Arc::clone(&state),
        };
        let mut peer_connection = RtcPeerConnection::new(&rtc_config, handler)
            .map_err(|error| VoicePublisherError::PeerConnection(error.to_string()))?;
        let mut audio_track = add_track(&mut peer_connection, &track_plan.audio, &state)?;
        let mut video_track = add_track(&mut peer_connection, &track_plan.video, &state)?;
        configure_audio_packetizer(&mut audio_track, &track_plan.audio, &config)?;
        configure_video_packetizer(&mut video_track, &track_plan.video, &config)?;

        Ok(Self {
            track_plan: track_plan.clone(),
            peer_connection,
            audio_track,
            video_track,
            state,
            audio_timestamp_step: audio_timestamp_step(&config),
            video_timestamp_step: video_timestamp_step(&track_plan.video, &config),
        })
    }

    pub fn create_offer(&mut self) -> Result<SessionDescription, VoicePublisherError> {
        self.peer_connection
            .set_local_description(SdpType::Offer)
            .map_err(|error| VoicePublisherError::PeerConnection(error.to_string()))?;
        self.peer_connection.local_description().ok_or_else(|| {
            VoicePublisherError::PeerConnection("local description not produced".to_owned())
        })
    }

    pub fn apply_remote_answer(&mut self, sdp: &str) -> Result<(), VoicePublisherError> {
        let normalized = normalize_discord_answer_sdp(sdp, &self.track_plan);
        let parsed = datachannel::sdp::parse_sdp(&normalized, false)
            .map_err(|error| VoicePublisherError::InvalidRemoteDescription(error.to_string()))?;
        let description = SessionDescription {
            sdp: parsed,
            sdp_type: SdpType::Answer,
        };
        self.peer_connection
            .set_remote_description(&description)
            .map_err(|error| VoicePublisherError::PeerConnection(error.to_string()))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| VoicePublisherError::LockPoisoned)?;
        state.remote_description = Some(normalized);
        Ok(())
    }

    pub fn add_remote_candidate(
        &mut self,
        candidate: IceCandidate,
    ) -> Result<(), VoicePublisherError> {
        self.peer_connection
            .add_remote_candidate(&candidate)
            .map_err(|error| VoicePublisherError::PeerConnection(error.to_string()))
    }

    pub fn send_audio(&mut self, payload: &[u8]) -> Result<(), VoicePublisherError> {
        self.send_track(
            &mut self.audio_track,
            payload,
            self.audio_timestamp_step,
            PublisherTrackKind::Audio,
        )?;
        Ok(())
    }

    pub fn send_video(&mut self, payload: &[u8]) -> Result<(), VoicePublisherError> {
        self.send_track(
            &mut self.video_track,
            payload,
            self.video_timestamp_step,
            PublisherTrackKind::Video,
        )?;
        Ok(())
    }

    pub fn state(&self) -> Result<VoicePublisherState, VoicePublisherError> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| VoicePublisherError::LockPoisoned)
    }

    pub async fn wait_until_ready(&self, timeout: Duration) -> Result<(), VoicePublisherError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.is_ready()? {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(VoicePublisherError::ReadyTimeout);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn is_ready(&self) -> Result<bool, VoicePublisherError> {
        self.state
            .lock()
            .map(|state| publisher_ready(&state))
            .map_err(|_| VoicePublisherError::LockPoisoned)
    }

    fn send_track(
        &self,
        track: &mut RtcTrack<PublisherTrackHandler>,
        payload: &[u8],
        timestamp_step: u32,
        kind: PublisherTrackKind,
    ) -> Result<(), VoicePublisherError> {
        if !self.is_ready()? {
            return Err(VoicePublisherError::TrackNotReady(track_label(kind).to_owned()));
        }

        track
            .send(payload)
            .map_err(|error| self.track_error(track_label(kind), error))?;
        advance_track_timestamp(track, timestamp_step)
            .map_err(|error| self.track_error(track_label(kind), error))?;
        self.record_send(kind, payload.len())?;
        Ok(())
    }

    fn record_send(
        &self,
        kind: PublisherTrackKind,
        bytes: usize,
    ) -> Result<(), VoicePublisherError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| VoicePublisherError::LockPoisoned)?;
        match kind {
            PublisherTrackKind::Audio => {
                state.audio_frames_sent = state.audio_frames_sent.saturating_add(1);
                state.audio_bytes_sent =
                    state.audio_bytes_sent.saturating_add(bytes as u64);
            }
            PublisherTrackKind::Video => {
                state.video_frames_sent = state.video_frames_sent.saturating_add(1);
                state.video_bytes_sent =
                    state.video_bytes_sent.saturating_add(bytes as u64);
            }
        }
        state.last_send_error = None;
        Ok(())
    }

    fn track_error(
        &self,
        kind: &str,
        error: impl std::fmt::Display,
    ) -> VoicePublisherError {
        let message = error.to_string();
        if let Ok(mut state) = self.state.lock() {
            state.last_send_error = Some(format!("{kind}: {message}"));
        }
        VoicePublisherError::Track(kind.to_owned(), message)
    }
}

fn build_rtc_config(config: &VoicePublisherConfig) -> RtcConfig {
    let mut rtc_config = RtcConfig::new(&config.ice_servers);
    if let Some(bind_address) = config.bind_address.as_ref() {
        rtc_config = rtc_config.bind_address(bind_address);
    }
    if config.force_media_transport {
        rtc_config = rtc_config.force_media_transport();
    }
    rtc_config
}

fn add_track(
    peer_connection: &mut Box<RtcPeerConnection<PublisherPeerConnectionHandler>>,
    track: &TrackSpec,
    state: &Arc<Mutex<VoicePublisherState>>,
) -> Result<Box<RtcTrack<PublisherTrackHandler>>, VoicePublisherError> {
    peer_connection
        .add_track_ex(
            &TrackInit {
                direction: Direction::SendOnly,
                codec: codec_for(track),
                payload_type: i32::from(track.payload_type),
                ssrc: track.ssrc,
                mid: CString::new(track.mid.clone()).map_err(|error| {
                    VoicePublisherError::Track(track.mid.clone(), error.to_string())
                })?,
                name: None,
                msid: None,
                track_id: None,
                profile: track_profile(track)?,
            },
            PublisherTrackHandler {
                state: Arc::clone(state),
                kind: if track.mid == AUDIO_MID {
                    PublisherTrackKind::Audio
                } else {
                    PublisherTrackKind::Video
                },
            },
        )
        .map_err(|error| VoicePublisherError::Track(track.mid.clone(), error.to_string()))
}

fn codec_for(track: &TrackSpec) -> Codec {
    match track.codec.as_str() {
        "opus" => Codec::Opus,
        "H264" => Codec::H264,
        "VP8" => Codec::VP8,
        "VP9" => Codec::VP9,
        _ if track.mid == AUDIO_MID => Codec::Opus,
        _ if track.mid == VIDEO_MID => Codec::H264,
        _ => Codec::H264,
    }
}

fn track_profile(track: &TrackSpec) -> Result<Option<CString>, VoicePublisherError> {
    if matches!(codec_for(track), Codec::H264) {
        return CString::new(
            "profile-level-id=42e01f;packetization-mode=1;level-asymmetry-allowed=1",
        )
        .map(Some)
        .map_err(|error| VoicePublisherError::Track(track.mid.clone(), error.to_string()));
    }

    Ok(None)
}

fn packetizer_init(spec: &TrackSpec) -> PacketizerInit {
    PacketizerInit {
        ssrc: spec.ssrc,
        cname: spec.mid.clone(),
        payload_type: spec.payload_type,
        clock_rate: clock_rate_for(spec),
        sequence_number: initial_sequence_number(spec),
        timestamp: initial_timestamp(spec),
        max_fragment_size: 1200,
        nal_separator: NalUnitSeparator::StartSequence,
        playout_delay_id: 0,
        playout_delay_min: 0,
        playout_delay_max: 0,
    }
}

fn configure_audio_packetizer(
    track: &mut RtcTrack<PublisherTrackHandler>,
    spec: &TrackSpec,
    _config: &VoicePublisherConfig,
) -> Result<(), VoicePublisherError> {
    let init = packetizer_init(spec);
    track
        .set_opus_packetizer(&init)
        .map_err(|error| VoicePublisherError::Track(spec.mid.clone(), error.to_string()))?;
    try_chain_track("audio", "rtcp sr reporter", || track.chain_rtcp_sr_reporter());
    try_chain_track("audio", "rtcp nack responder", || {
        track.chain_rtcp_nack_responder(512)
    });
    Ok(())
}

fn configure_video_packetizer(
    track: &mut RtcTrack<PublisherTrackHandler>,
    spec: &TrackSpec,
    config: &VoicePublisherConfig,
) -> Result<(), VoicePublisherError> {
    let init = packetizer_init(spec);
    track
        .set_h264_packetizer(&init)
        .map_err(|error| VoicePublisherError::Track(spec.mid.clone(), error.to_string()))?;
    try_chain_track("video", "rtcp sr reporter", || track.chain_rtcp_sr_reporter());
    try_chain_track("video", "rtcp nack responder", || {
        track.chain_rtcp_nack_responder(1024)
    });
    let _ = config;
    Ok(())
}

fn advance_track_timestamp(
    track: &mut RtcTrack<PublisherTrackHandler>,
    step: u32,
) -> datachannel::Result<()> {
    let next = track.current_rtp_timestamp()?.wrapping_add(step);
    track.set_rtp_timestamp(next)
}

fn audio_timestamp_step(config: &VoicePublisherConfig) -> u32 {
    48_000_u32.saturating_mul(config.audio_frame_duration_ms.max(1)) / 1_000
}

fn video_timestamp_step(spec: &TrackSpec, config: &VoicePublisherConfig) -> u32 {
    clock_rate_for(spec) / config.video_framerate.max(1)
}

fn try_chain_track(
    kind: &str,
    chain_name: &str,
    action: impl FnOnce() -> datachannel::Result<()>,
) {
    if let Err(error) = action() {
        warn!(track = kind, chain = chain_name, %error, "publisher track chaining failed");
    }
}

fn clock_rate_for(spec: &TrackSpec) -> u32 {
    match spec.kind {
        crate::TrackKind::Audio => 48_000,
        crate::TrackKind::Video => 90_000,
    }
}

fn publisher_ready(state: &VoicePublisherState) -> bool {
    state.connection_state.as_deref() == Some("Connected")
        && state.audio_track_open
        && state.video_track_open
}

fn initial_sequence_number(spec: &TrackSpec) -> u16 {
    ((spec.ssrc >> 16) as u16) ^ (spec.ssrc as u16)
}

fn initial_timestamp(spec: &TrackSpec) -> u32 {
    spec.ssrc.rotate_left(7) ^ 0x4f1b_bcde
}

fn normalize_discord_answer_sdp(raw_sdp: &str, track_plan: &TrackPlan) -> String {
    let mut connection = String::new();
    let mut port = String::new();
    let mut ice_username = String::new();
    let mut ice_password = String::new();
    let mut fingerprint = String::new();
    let mut candidates = Vec::new();

    for line in raw_sdp.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if line.starts_with("c=") {
            connection = line.to_owned();
        } else if let Some(value) = line.strip_prefix("a=rtcp:") {
            port = value.split_whitespace().next().unwrap_or_default().to_owned();
        } else if line.starts_with("a=ice-ufrag:") {
            ice_username = line.to_owned();
        } else if line.starts_with("a=ice-pwd:") {
            ice_password = line.to_owned();
        } else if line.starts_with("a=fingerprint:") {
            fingerprint = line.to_owned();
        } else if line.starts_with("a=candidate:") {
            candidates.push(line.to_owned());
        }
    }

    let audio_payload = track_plan.audio.payload_type;
    let video_payload = track_plan.video.payload_type;
    let mut video_payloads = vec![video_payload.to_string()];
    if let Some(rtx_payload) = track_plan.rtx_payload_type {
        video_payloads.push(rtx_payload.to_string());
    }

    let lines = [
        "v=0".to_owned(),
        "o=- 0 0 IN IP4 127.0.0.1".to_owned(),
        "s=-".to_owned(),
        "t=0 0".to_owned(),
        format!("a=group:BUNDLE {} {}", track_plan.audio.mid, track_plan.video.mid),
        "a=ice-lite".to_owned(),
        build_audio_section(
            &port,
            &connection,
            &ice_username,
            &ice_password,
            &fingerprint,
            &candidates,
            audio_payload,
            &track_plan.audio.mid,
        ),
        build_video_section(
            &port,
            &connection,
            &ice_username,
            &ice_password,
            &fingerprint,
            &candidates,
            &video_payloads,
            video_payload,
            track_plan.rtx_payload_type,
            &track_plan.video.mid,
            &track_plan.video.codec,
        ),
    ];
    lines.join("\n")
}

#[allow(clippy::too_many_arguments)]
fn build_audio_section(
    port: &str,
    connection: &str,
    ice_username: &str,
    ice_password: &str,
    fingerprint: &str,
    candidates: &[String],
    payload_type: u8,
    mid: &str,
) -> String {
    let mut lines = vec![
        format!("m=audio {} UDP/TLS/RTP/SAVPF {}", fallback_port(port), payload_type),
        fallback_connection(connection),
        "a=extmap:1 urn:ietf:params:rtp-hdrext:ssrc-audio-level".to_owned(),
        "a=extmap:3 http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01"
            .to_owned(),
        "a=setup:passive".to_owned(),
        format!("a=mid:{mid}"),
        "a=maxptime:60".to_owned(),
        "a=recvonly".to_owned(),
        fallback_ice_username(ice_username),
        fallback_ice_password(ice_password),
        fallback_fingerprint(fingerprint),
        "a=rtcp-mux".to_owned(),
        format!("a=rtpmap:{payload_type} opus/48000/2"),
        format!("a=fmtp:{payload_type} minptime=10;useinbandfec=1;usedtx=1"),
        format!("a=rtcp-fb:{payload_type} transport-cc"),
        format!("a=rtcp-fb:{payload_type} nack"),
    ];
    lines.extend(candidates.iter().cloned());
    lines.join("\n")
}

#[allow(clippy::too_many_arguments)]
fn build_video_section(
    port: &str,
    connection: &str,
    ice_username: &str,
    ice_password: &str,
    fingerprint: &str,
    candidates: &[String],
    payload_types: &[String],
    payload_type: u8,
    rtx_payload_type: Option<u8>,
    mid: &str,
    codec: &str,
) -> String {
    let mut lines = vec![
        format!(
            "m=video {} UDP/TLS/RTP/SAVPF {}",
            fallback_port(port),
            payload_types.join(" ")
        ),
        fallback_connection(connection),
        "a=extmap:2 http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time".to_owned(),
        "a=extmap:3 http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01"
            .to_owned(),
        "a=extmap:14 urn:ietf:params:rtp-hdrext:toffset".to_owned(),
        "a=extmap:13 urn:3gpp:video-orientation".to_owned(),
        "a=extmap:5 http://www.webrtc.org/experiments/rtp-hdrext/playout-delay".to_owned(),
        "a=setup:passive".to_owned(),
        format!("a=mid:{mid}"),
        "a=recvonly".to_owned(),
        fallback_ice_username(ice_username),
        fallback_ice_password(ice_password),
        fallback_fingerprint(fingerprint),
        "a=rtcp-mux".to_owned(),
        format!("a=rtpmap:{payload_type} {codec}/90000"),
        format!("a=rtcp-fb:{payload_type} ccm fir"),
        format!("a=rtcp-fb:{payload_type} nack"),
        format!("a=rtcp-fb:{payload_type} nack pli"),
        format!("a=rtcp-fb:{payload_type} goog-remb"),
        format!("a=rtcp-fb:{payload_type} transport-cc"),
    ];
    if let Some(rtx_payload_type) = rtx_payload_type {
        lines.push(format!("a=rtpmap:{rtx_payload_type} rtx/90000"));
        lines.push(format!("a=fmtp:{rtx_payload_type} apt={payload_type}"));
    }
    lines.extend(candidates.iter().cloned());
    lines.join("\n")
}

fn fallback_port(port: &str) -> &str {
    if port.is_empty() { "9" } else { port }
}

fn fallback_connection(connection: &str) -> String {
    if connection.is_empty() {
        "c=IN IP4 0.0.0.0".to_owned()
    } else {
        connection.to_owned()
    }
}

fn fallback_ice_username(ice_username: &str) -> String {
    if ice_username.is_empty() {
        "a=ice-ufrag:discordrs".to_owned()
    } else {
        ice_username.to_owned()
    }
}

fn fallback_ice_password(ice_password: &str) -> String {
    if ice_password.is_empty() {
        "a=ice-pwd:discordrsdiscordrsdiscordrs".to_owned()
    } else {
        ice_password.to_owned()
    }
}

fn fallback_fingerprint(fingerprint: &str) -> String {
    if fingerprint.is_empty() {
        "a=fingerprint:sha-256 00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00".to_owned()
    } else {
        fingerprint.to_owned()
    }
}

struct PublisherTrackHandler {
    state: Arc<Mutex<VoicePublisherState>>,
    kind: PublisherTrackKind,
}

impl TrackHandler for PublisherTrackHandler {
    fn set_open_state(&self, open: bool) {
        if let Ok(mut state) = self.state.lock() {
            match self.kind {
                PublisherTrackKind::Audio => state.audio_track_open = open,
                PublisherTrackKind::Video => state.video_track_open = open,
            }
        }
    }

    fn on_open(&mut self) {
        self.set_open_state(true);
    }

    fn on_closed(&mut self) {
        self.set_open_state(false);
    }

    fn on_error(&mut self, err: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.last_send_error = Some(format!("{}: {err}", track_label(self.kind)));
        }
    }
}

fn track_label(kind: PublisherTrackKind) -> &'static str {
    match kind {
        PublisherTrackKind::Audio => "audio",
        PublisherTrackKind::Video => "video",
    }
}

#[derive(Default)]
struct PublisherDataChannelHandler;

impl DataChannelHandler for PublisherDataChannelHandler {}

struct PublisherPeerConnectionHandler {
    state: Arc<Mutex<VoicePublisherState>>,
}

impl PeerConnectionHandler for PublisherPeerConnectionHandler {
    type DCH = PublisherDataChannelHandler;

    fn data_channel_handler(&mut self, _info: DataChannelInfo) -> Self::DCH {
        PublisherDataChannelHandler
    }

    fn on_description(&mut self, sess_desc: SessionDescription) {
        if let Ok(mut state) = self.state.lock() {
            state.local_description = Some(sess_desc.sdp.to_string());
        }
    }

    fn on_candidate(&mut self, cand: IceCandidate) {
        if let Ok(mut state) = self.state.lock() {
            state.local_candidates.push(cand.candidate);
        }
    }

    fn on_connection_state_change(&mut self, state: ConnectionState) {
        if let Ok(mut publisher_state) = self.state.lock() {
            publisher_state.connection_state = Some(format!("{state:?}"));
            if matches!(
                state,
                ConnectionState::Closed | ConnectionState::Disconnected | ConnectionState::Failed
            ) {
                publisher_state.audio_track_open = false;
                publisher_state.video_track_open = false;
            }
        }
    }

    fn on_data_channel(&mut self, _data_channel: Box<RtcDataChannel<Self::DCH>>) {}
}

#[allow(dead_code)]
fn _assert_result_type<T>(value: DcResult<T>) -> DcResult<T> {
    value
}

#[cfg(test)]
mod tests {
    use super::{VoicePublisherState, publisher_ready};

    #[test]
    fn publisher_ready_requires_connected_and_open_tracks() {
        let base = VoicePublisherState::default();
        assert!(!publisher_ready(&base));

        let connected = VoicePublisherState {
            connection_state: Some("Connected".to_owned()),
            audio_track_open: true,
            video_track_open: true,
            ..VoicePublisherState::default()
        };
        assert!(publisher_ready(&connected));

        let missing_video = VoicePublisherState {
            video_track_open: false,
            ..connected.clone()
        };
        assert!(!publisher_ready(&missing_video));
    }
}
