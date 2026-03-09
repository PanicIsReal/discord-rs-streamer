use std::borrow::Cow;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use davey::{
    Codec, DaveSession as VendorDaveSession, MediaType, ProposalsOperationType, SessionStatus,
    SigningKeyPair,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Video,
    Audio,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaveMetadata {
    pub sequence: u64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaveInitConfig {
    pub protocol_version: u16,
    pub user_id: u64,
    pub channel_id: u64,
    pub signing_private_key: Option<Vec<u8>>,
    pub signing_public_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProposalOp {
    Append,
    Revoke,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaveState {
    pub initialized: bool,
    pub protocol_version: Option<u16>,
    pub user_id: Option<u64>,
    pub channel_id: Option<u64>,
    pub ready: bool,
    pub status: DaveSessionStatus,
    pub voice_privacy_code: Option<String>,
    pub member_ids: Option<Vec<u64>>,
}

impl Default for DaveState {
    fn default() -> Self {
        Self {
            initialized: false,
            protocol_version: None,
            user_id: None,
            channel_id: None,
            ready: false,
            status: DaveSessionStatus::Inactive,
            voice_privacy_code: None,
            member_ids: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DaveCommitWelcome {
    pub commit: Vec<u8>,
    pub welcome: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaveProposalResult {
    pub state: DaveState,
    pub commit_welcome: Option<DaveCommitWelcome>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaveSessionStatus {
    Inactive,
    Pending,
    AwaitingResponse,
    Active,
}

impl From<SessionStatus> for DaveSessionStatus {
    fn from(value: SessionStatus) -> Self {
        match value {
            SessionStatus::INACTIVE => Self::Inactive,
            SessionStatus::PENDING => Self::Pending,
            SessionStatus::AWAITING_RESPONSE => Self::AwaitingResponse,
            SessionStatus::ACTIVE => Self::Active,
        }
    }
}

#[derive(Debug, Error)]
pub enum DaveError {
    #[error("missing DAVE session")]
    MissingSession,
    #[error("protocol version must be non-zero")]
    InvalidProtocolVersion,
    #[error("both signing keys must be provided together")]
    IncompleteKeyPair,
    #[error("{0}")]
    Message(String),
}

pub trait DaveSession: Send + Sync {
    fn protect(
        &self,
        media_kind: MediaKind,
        metadata: &DaveMetadata,
        payload: Bytes,
    ) -> Result<Bytes, DaveError>;
}

pub trait DaveControl: DaveSession + Send + Sync {
    fn initialize(&self, config: DaveInitConfig) -> Result<DaveState, DaveError>;
    fn set_external_sender(&self, external_sender_data: &[u8]) -> Result<DaveState, DaveError>;
    fn create_key_package(&self) -> Result<Vec<u8>, DaveError>;
    fn process_proposals(
        &self,
        operation: ProposalOp,
        proposals: &[u8],
    ) -> Result<DaveState, DaveError>;
    fn process_proposals_bundle(
        &self,
        operation: ProposalOp,
        proposals: &[u8],
    ) -> Result<DaveProposalResult, DaveError>;
    fn process_welcome(&self, welcome: &[u8]) -> Result<DaveState, DaveError>;
    fn process_commit(&self, commit: &[u8]) -> Result<DaveState, DaveError>;
    fn state(&self) -> Result<DaveState, DaveError>;
}

impl<T> DaveSession for Arc<T>
where
    T: DaveSession + ?Sized,
{
    fn protect(
        &self,
        media_kind: MediaKind,
        metadata: &DaveMetadata,
        payload: Bytes,
    ) -> Result<Bytes, DaveError> {
        (**self).protect(media_kind, metadata, payload)
    }
}

impl<T> DaveControl for Arc<T>
where
    T: DaveControl + ?Sized,
{
    fn initialize(&self, config: DaveInitConfig) -> Result<DaveState, DaveError> {
        (**self).initialize(config)
    }

    fn set_external_sender(&self, external_sender_data: &[u8]) -> Result<DaveState, DaveError> {
        (**self).set_external_sender(external_sender_data)
    }

    fn create_key_package(&self) -> Result<Vec<u8>, DaveError> {
        (**self).create_key_package()
    }

    fn process_proposals(
        &self,
        operation: ProposalOp,
        proposals: &[u8],
    ) -> Result<DaveState, DaveError> {
        (**self).process_proposals(operation, proposals)
    }

    fn process_proposals_bundle(
        &self,
        operation: ProposalOp,
        proposals: &[u8],
    ) -> Result<DaveProposalResult, DaveError> {
        (**self).process_proposals_bundle(operation, proposals)
    }

    fn process_welcome(&self, welcome: &[u8]) -> Result<DaveState, DaveError> {
        (**self).process_welcome(welcome)
    }

    fn process_commit(&self, commit: &[u8]) -> Result<DaveState, DaveError> {
        (**self).process_commit(commit)
    }

    fn state(&self) -> Result<DaveState, DaveError> {
        (**self).state()
    }
}

#[derive(Default)]
pub struct ManagedDaveSession {
    inner: Mutex<Option<VendorDaveSession>>,
}

impl ManagedDaveSession {
    pub fn new() -> Self {
        Self::default()
    }

    fn with_session<T>(
        &self,
        f: impl FnOnce(&mut VendorDaveSession) -> Result<T, DaveError>,
    ) -> Result<T, DaveError> {
        let mut guard = self.lock().map_err(map_dave_message)?;
        let Some(session) = guard.as_mut() else {
            return Err(DaveError::MissingSession);
        };
        f(session)
    }

    fn lock(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, Option<VendorDaveSession>>,
        std::sync::PoisonError<std::sync::MutexGuard<'_, Option<VendorDaveSession>>>,
    > {
        self.inner.lock()
    }
}

impl DaveSession for ManagedDaveSession {
    fn protect(
        &self,
        media_kind: MediaKind,
        _metadata: &DaveMetadata,
        payload: Bytes,
    ) -> Result<Bytes, DaveError> {
        self.with_session(|session| {
            let encoded = match media_kind {
                MediaKind::Audio => {
                    session.encrypt(MediaType::AUDIO, Codec::OPUS, payload.as_ref())
                }
                MediaKind::Video => {
                    session.encrypt(MediaType::VIDEO, Codec::H264, payload.as_ref())
                }
            }
            .map_err(map_dave_message)?;

            Ok(bytes_from_cow(encoded))
        })
    }
}

impl DaveControl for ManagedDaveSession {
    fn initialize(&self, config: DaveInitConfig) -> Result<DaveState, DaveError> {
        let protocol_version =
            NonZeroU16::new(config.protocol_version).ok_or(DaveError::InvalidProtocolVersion)?;
        let key_pair = match (config.signing_private_key, config.signing_public_key) {
            (None, None) => None,
            (Some(private), Some(public)) => Some(SigningKeyPair { private, public }),
            _ => return Err(DaveError::IncompleteKeyPair),
        };

        let session = VendorDaveSession::new(
            protocol_version,
            config.user_id,
            config.channel_id,
            key_pair.as_ref(),
        )
        .map_err(map_dave_message)?;

        {
            let mut guard = self.lock().map_err(map_dave_message)?;
            *guard = Some(session);
        }
        self.state()
    }

    fn set_external_sender(&self, external_sender_data: &[u8]) -> Result<DaveState, DaveError> {
        self.with_session(|session| {
            session
                .set_external_sender(external_sender_data)
                .map_err(map_dave_message)
        })?;
        self.state()
    }

    fn create_key_package(&self) -> Result<Vec<u8>, DaveError> {
        self.with_session(|session| session.create_key_package().map_err(map_dave_message))
    }

    fn process_proposals(
        &self,
        operation: ProposalOp,
        proposals: &[u8],
    ) -> Result<DaveState, DaveError> {
        self.process_proposals_bundle(operation, proposals)
            .map(|result| result.state)
    }

    fn process_proposals_bundle(
        &self,
        operation: ProposalOp,
        proposals: &[u8],
    ) -> Result<DaveProposalResult, DaveError> {
        let operation = proposal_operation(operation);
        let commit_welcome = self.with_session(|session| {
            session
                .process_proposals(operation, proposals, None)
                .map_err(map_dave_message)
                .map(|bundle| {
                    bundle.map(|bundle| DaveCommitWelcome {
                        commit: bundle.commit,
                        welcome: bundle.welcome,
                    })
                })
        })?;
        Ok(DaveProposalResult {
            state: self.state()?,
            commit_welcome,
        })
    }

    fn process_welcome(&self, welcome: &[u8]) -> Result<DaveState, DaveError> {
        self.with_session(|session| session.process_welcome(welcome).map_err(map_dave_message))?;
        self.state()
    }

    fn process_commit(&self, commit: &[u8]) -> Result<DaveState, DaveError> {
        self.with_session(|session| session.process_commit(commit).map_err(map_dave_message))?;
        self.state()
    }

    fn state(&self) -> Result<DaveState, DaveError> {
        let guard = self.lock().map_err(map_dave_message)?;
        let Some(session) = guard.as_ref() else {
            return Ok(DaveState::default());
        };

        Ok(DaveState {
            initialized: true,
            protocol_version: Some(session.protocol_version().get()),
            user_id: Some(session.user_id()),
            channel_id: Some(session.channel_id()),
            ready: matches!(session.status(), SessionStatus::ACTIVE),
            status: session.status().into(),
            voice_privacy_code: session.voice_privacy_code().map(ToOwned::to_owned),
            member_ids: session.get_user_ids(),
        })
    }
}

fn map_dave_message(error: impl std::fmt::Display) -> DaveError {
    DaveError::Message(error.to_string())
}

fn proposal_operation(operation: ProposalOp) -> ProposalsOperationType {
    match operation {
        ProposalOp::Append => ProposalsOperationType::APPEND,
        ProposalOp::Revoke => ProposalsOperationType::REVOKE,
    }
}

fn bytes_from_cow(data: Cow<'_, [u8]>) -> Bytes {
    match data {
        Cow::Borrowed(slice) => Bytes::copy_from_slice(slice),
        Cow::Owned(vec) => Bytes::from(vec),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initializes_and_creates_key_package() {
        let session = ManagedDaveSession::new();
        let state = session
            .initialize(DaveInitConfig {
                protocol_version: 1,
                user_id: 42,
                channel_id: 7,
                signing_private_key: None,
                signing_public_key: None,
            })
            .expect("init");
        assert!(state.initialized);
        assert_eq!(state.user_id, Some(42));
        assert_eq!(state.channel_id, Some(7));

        let package = session.create_key_package().expect("key package");
        assert!(!package.is_empty());
    }

    #[test]
    fn proposal_bundle_defaults_to_state_only() {
        let session = ManagedDaveSession::new();
        session
            .initialize(DaveInitConfig {
                protocol_version: 1,
                user_id: 42,
                channel_id: 7,
                signing_private_key: None,
                signing_public_key: None,
            })
            .expect("init");

        let result = session
            .process_proposals_bundle(ProposalOp::Append, &[0])
            .expect_err("invalid proposals should fail before creating a bundle");
        assert!(matches!(result, DaveError::Message(_)));
    }
}
