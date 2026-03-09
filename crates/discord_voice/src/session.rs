use std::time::Duration;

use dave::DaveControl;
use serde::{Deserialize, Serialize};
use thiserror::Error;
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
        let track_plan = state.track_plan().ok_or(VoiceMediaSessionError::NotReady)?;
        publisher_config.video_framerate = video_config.max_framerate.max(1);
        let mut publisher = VoicePeerPublisher::new(&track_plan, publisher_config)?;
        let offer = publisher.create_offer()?;

        let select_protocol = state
            .select_protocol_payload(offer.sdp.to_string(), Uuid::new_v4())
            .ok_or(VoiceMediaSessionError::NotReady)?;
        client.send_select_protocol(select_protocol).await?;
        client.wait_for_session_description(timeout).await?;

        let state = client.session_state().await;
        let answer_sdp = state
            .protocol_ack
            .as_ref()
            .map(|ack| ack.sdp.clone())
            .filter(|sdp| !sdp.is_empty())
            .ok_or(VoiceMediaSessionError::NotReady)?;
        publisher.apply_remote_answer(&answer_sdp)?;

        let video_command = state
            .video_command_payload(&video_config)
            .ok_or(VoiceMediaSessionError::NotReady)?;
        client.send_video_command(video_command).await?;

        Ok(Self { client, publisher })
    }

    pub async fn health(&self) -> Result<VoiceMediaSessionHealth, VoiceMediaSessionError> {
        Ok(VoiceMediaSessionHealth {
            voice: self.client.health().await,
            publisher: self.publisher.state()?,
            session_state: self.client.session_state().await,
        })
    }

    pub fn send_media(
        &mut self,
        kind: VoiceMediaKind,
        payload: &[u8],
    ) -> Result<(), VoiceMediaSessionError> {
        match kind {
            VoiceMediaKind::Audio => self.publisher.send_audio(payload).map_err(Into::into),
            VoiceMediaKind::Video => self.publisher.send_video(payload).map_err(Into::into),
        }
    }

    pub async fn close(self) -> Result<(), VoiceMediaSessionError> {
        self.client.close().await.map_err(Into::into)
    }
}
