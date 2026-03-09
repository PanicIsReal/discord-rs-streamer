use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dave::{
    DaveCommitWelcome, DaveControl, DaveError, DaveInitConfig, DaveMetadata, DaveProposalResult,
    DaveSession, DaveSessionStatus, DaveState, MediaKind as DaveMediaKind, ProposalOp,
};
use discord_gateway::{
    DiscordGateway, GatewayError, GatewayHealth, GatewayMediaSession, InMemoryGateway,
    SessionConfig,
};
use discord_transport::{MediaKind, Packet, PacketSink, TransportError};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct RecordingPacketSink {
    sent: Mutex<Vec<Packet>>,
}

impl RecordingPacketSink {
    pub async fn sent_packets(&self) -> Vec<Packet> {
        self.sent.lock().await.clone()
    }
}

#[async_trait]
impl PacketSink for RecordingPacketSink {
    async fn send(&self, packet: Packet) -> Result<(), TransportError> {
        self.sent.lock().await.push(packet);
        Ok(())
    }
}

pub fn sample_packet(kind: MediaKind, sequence: u64, size: usize) -> Packet {
    Packet {
        kind,
        sequence,
        timestamp_ms: sequence,
        payload: Bytes::from(vec![0; size]),
    }
}

fn dave_state_for(protocol_version: u16, user_id: u64, channel_id: u64) -> DaveState {
    DaveState {
        initialized: true,
        protocol_version: Some(protocol_version),
        user_id: Some(user_id),
        channel_id: Some(channel_id),
        ready: true,
        status: DaveSessionStatus::Active,
        voice_privacy_code: None,
        member_ids: None,
    }
}

#[derive(Default, Clone)]
pub struct HarnessGateway {
    inner: InMemoryGateway,
}

#[async_trait]
impl DiscordGateway for HarnessGateway {
    async fn connect(&self, config: SessionConfig) -> Result<GatewayHealth, GatewayError> {
        self.inner.connect(config).await
    }

    async fn start_stream(&self) -> Result<GatewayHealth, GatewayError> {
        self.inner.start_stream().await
    }

    async fn stop_stream(&self) -> Result<GatewayHealth, GatewayError> {
        self.inner.stop_stream().await
    }

    async fn disconnect(&self) -> Result<(), GatewayError> {
        self.inner.disconnect().await
    }

    async fn health(&self) -> GatewayHealth {
        self.inner.health().await
    }

    async fn media_session(&self) -> Result<GatewayMediaSession, GatewayError> {
        self.inner.media_session().await
    }
}

pub fn sink() -> Arc<RecordingPacketSink> {
    Arc::new(RecordingPacketSink::default())
}

#[derive(Default)]
pub struct PassthroughDaveSession;

impl DaveSession for PassthroughDaveSession {
    fn protect(
        &self,
        _media_kind: DaveMediaKind,
        _metadata: &DaveMetadata,
        payload: Bytes,
    ) -> Result<Bytes, DaveError> {
        Ok(payload)
    }
}

impl DaveControl for PassthroughDaveSession {
    fn initialize(&self, config: DaveInitConfig) -> Result<DaveState, DaveError> {
        Ok(dave_state_for(
            config.protocol_version,
            config.user_id,
            config.channel_id,
        ))
    }

    fn set_external_sender(&self, _external_sender_data: &[u8]) -> Result<DaveState, DaveError> {
        self.state()
    }

    fn create_key_package(&self) -> Result<Vec<u8>, DaveError> {
        Ok(vec![1, 2, 3])
    }

    fn process_proposals(
        &self,
        _operation: ProposalOp,
        _proposals: &[u8],
    ) -> Result<DaveState, DaveError> {
        self.state()
    }

    fn process_proposals_bundle(
        &self,
        _operation: ProposalOp,
        _proposals: &[u8],
    ) -> Result<DaveProposalResult, DaveError> {
        Ok(DaveProposalResult {
            state: self.state()?,
            commit_welcome: Some(DaveCommitWelcome {
                commit: vec![1, 2, 3],
                welcome: Some(vec![4, 5, 6]),
            }),
        })
    }

    fn process_welcome(&self, _welcome: &[u8]) -> Result<DaveState, DaveError> {
        self.state()
    }

    fn process_commit(&self, _commit: &[u8]) -> Result<DaveState, DaveError> {
        self.state()
    }

    fn state(&self) -> Result<DaveState, DaveError> {
        Ok(dave_state_for(1, 1, 1))
    }
}
