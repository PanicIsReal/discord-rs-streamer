use std::time::Duration;

use dave::DaveControl;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

use crate::client::{VoiceGatewayClient, VoiceGatewayError, VoiceGatewayHealth};
use crate::publisher::{
    VoicePeerPublisher, VoicePublisherConfig, VoicePublisherError, VoicePublisherState,
};
use crate::{VoiceMediaKind, VoiceSessionState, VoiceVideoConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceMediaSessionHealth {
    pub voice: VoiceGatewayHealth,
    pub publisher: VoicePublisherState,
    pub session_state: VoiceSessionState,
}

#[derive(Debug, Error)]
pub enum VoiceMediaSessionError {
    #[error("{0}")]
    Gateway(#[from] VoiceGatewayError),
    #[error("{0}")]
    Publisher(#[from] VoicePublisherError),
    #[error("voice session is not ready for media negotiation")]
    NotReady,
}

pub struct VoiceMediaSession<D> {
    client: VoiceGatewayClient<D>,
    publisher: VoicePeerPublisher,
    video_config: VoiceVideoConfig,
    video_active: bool,
}

impl<D> VoiceMediaSession<D>
where
    D: DaveControl + Send + 'static,
{
    pub async fn negotiate(
        client: VoiceGatewayClient<D>,
        mut publisher_config: VoicePublisherConfig,
        timeout: Duration,
        video_config: VoiceVideoConfig,
    ) -> Result<Self, VoiceMediaSessionError> {
        client.wait_for_ready(timeout).await?;
        let state = client.session_state().await;
        debug!(
            has_ready = state.ready.is_some(),
            has_track_plan = state.track_plan().is_some(),
            "voice media ready received"
        );
        let inactive_video_command = state
            .video_command_payload(&video_config, false)
            .ok_or(VoiceMediaSessionError::NotReady)?;
        client.send_video_command(inactive_video_command).await?;
        let track_plan = state.track_plan().ok_or(VoiceMediaSessionError::NotReady)?;
        publisher_config.video_framerate = video_config.max_framerate.max(1);
        let mut publisher = VoicePeerPublisher::new(&track_plan, publisher_config)?;
        let offer = publisher.create_offer()?;

        let select_protocol = state
            .select_protocol_payload(offer.sdp.to_string(), Uuid::new_v4())
            .ok_or(VoiceMediaSessionError::NotReady)?;
        debug!("sending voice media select protocol");
        client.send_select_protocol(select_protocol).await?;
        client.wait_for_session_description(timeout).await?;

        let state = client.session_state().await;
        let answer_sdp = state
            .protocol_ack
            .as_ref()
            .map(|ack| ack.sdp.clone())
            .filter(|sdp| !sdp.is_empty())
            .ok_or(VoiceMediaSessionError::NotReady)?;
        debug!("received voice media session description");
        publisher.apply_remote_answer(&answer_sdp)?;

        let video_command = state
            .video_command_payload(&video_config, true)
            .ok_or(VoiceMediaSessionError::NotReady)?;
        debug!("sending active voice video command");
        client.send_video_command(video_command).await?;
        publisher.wait_until_ready(timeout).await?;
        let speaking_command = client
            .speaking_payload(true)
            .await
            .ok_or(VoiceMediaSessionError::NotReady)?;
        debug!("sending speaking voice command");
        client.send_speaking_command(speaking_command).await?;
        client.set_streaming_announced(true).await?;

        Ok(Self {
            client,
            publisher,
            video_config,
            video_active: true,
        })
    }

    pub async fn health(&self) -> Result<VoiceMediaSessionHealth, VoiceMediaSessionError> {
        Ok(VoiceMediaSessionHealth {
            voice: self.client.health().await,
            publisher: self.publisher.state()?,
            session_state: self.client.session_state().await,
        })
    }

    pub async fn session_state(&self) -> VoiceSessionState {
        self.client.session_state().await
    }

    pub async fn set_video_active(
        &mut self,
        active: bool,
    ) -> Result<bool, VoiceMediaSessionError> {
        if self.video_active == active {
            return Ok(false);
        }
        let state = self.client.session_state().await;
        let payload = state
            .video_command_payload(&self.video_config, active)
            .ok_or(VoiceMediaSessionError::NotReady)?;
        if active {
            debug!("resuming voice video command due to sink policy");
        } else {
            debug!("pausing voice video command due to sink policy");
        }
        self.client.send_video_command(payload).await?;
        self.video_active = active;
        Ok(true)
    }

    pub fn send_media(
        &mut self,
        kind: VoiceMediaKind,
        payload: &[u8],
        capture_time_us: u64,
        duration_us: u64,
    ) -> Result<(), VoiceMediaSessionError> {
        let result = match kind {
            VoiceMediaKind::Audio => self.publisher.send_audio(payload, capture_time_us, duration_us),
            VoiceMediaKind::Video => self.publisher.send_video(payload, capture_time_us, duration_us),
        };
        result.map_err(Into::into)
    }

    pub fn is_ready(&self) -> Result<bool, VoiceMediaSessionError> {
        self.publisher.is_ready().map_err(Into::into)
    }

    pub async fn close(self) -> Result<(), VoiceMediaSessionError> {
        let speaking_command = self.client.speaking_payload(false).await;
        if let Some(payload) = speaking_command {
            let _ = self.client.send_speaking_command(payload).await;
        }
        self.client.close().await.map_err(Into::into)
    }
}
