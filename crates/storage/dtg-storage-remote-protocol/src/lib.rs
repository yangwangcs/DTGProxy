#![forbid(unsafe_code)]

use core::fmt;

pub mod proto {
    tonic::include_proto!("dtgproxy.storage.v1");
}

pub const PROTOCOL_MAJOR: u32 = 1;
pub const PROTOCOL_MINOR: u32 = 0;
pub const CONTRACT_MAJOR: u32 = 1;
pub const CONTRACT_MINOR: u32 = 0;
pub const PAYLOAD_FORMAT_VERSION: u32 = 1;
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_MESSAGE_ITEMS: usize = 65_536;
pub const MAX_AUTH_CONTEXT_BYTES: usize = 4 * 1024;
pub const IDENTIFIER_BYTES: usize = 16;
pub const DIGEST_BYTES: usize = 32;
pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    ProtocolMajor { actual: u32 },
    ContractMajor { actual: u32 },
    UnsupportedMinor,
    MissingContext,
    InvalidRequestId,
    InvalidDeadline,
    MissingBinding,
    AuthContextTooLarge,
    InvalidResponseBudget,
    InvalidPayload(String),
}

impl ProtocolError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ProtocolMajor { .. } => "DTG-REMOTE-PROTOCOL-MAJOR",
            Self::ContractMajor { .. } => "DTG-REMOTE-CONTRACT-MAJOR",
            Self::UnsupportedMinor => "DTG-REMOTE-PROTOCOL-MINOR",
            Self::MissingContext => "DTG-REMOTE-CONTEXT",
            Self::InvalidRequestId => "DTG-REMOTE-REQUEST-ID",
            Self::InvalidDeadline => "DTG-REMOTE-DEADLINE",
            Self::MissingBinding => "DTG-REMOTE-BINDING",
            Self::AuthContextTooLarge => "DTG-REMOTE-AUTH-CONTEXT",
            Self::InvalidResponseBudget => "DTG-REMOTE-RESPONSE-BUDGET",
            Self::InvalidPayload(_) => "DTG-REMOTE-PAYLOAD",
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.code())
    }
}

impl std::error::Error for ProtocolError {}

pub fn bounded_payload(
    body: Vec<u8>,
    item_count: usize,
) -> Result<proto::BoundedPayload, ProtocolError> {
    if body.len() > MAX_MESSAGE_BYTES || item_count > MAX_MESSAGE_ITEMS {
        return Err(ProtocolError::InvalidPayload(
            "payload exceeds protocol bounds".into(),
        ));
    }
    Ok(proto::BoundedPayload {
        format_version: PAYLOAD_FORMAT_VERSION,
        declared_len: body.len() as u64,
        item_count: item_count as u32,
        checksum: blake3::hash(&body).as_bytes().to_vec(),
        body,
    })
}

pub fn validate_payload(payload: &proto::BoundedPayload) -> Result<(), ProtocolError> {
    if payload.format_version != PAYLOAD_FORMAT_VERSION
        || payload.declared_len != payload.body.len() as u64
        || payload.body.len() > MAX_MESSAGE_BYTES
        || payload.item_count as usize > MAX_MESSAGE_ITEMS
        || payload.checksum.as_slice() != blake3::hash(&payload.body).as_bytes()
    {
        return Err(ProtocolError::InvalidPayload(
            "payload version, length, count, or checksum is invalid".into(),
        ));
    }
    Ok(())
}

pub fn validate_context(
    context: Option<&proto::RequestContext>,
    now_ms: u64,
) -> Result<&proto::RequestContext, ProtocolError> {
    let context = context.ok_or(ProtocolError::MissingContext)?;
    if context.protocol_major != PROTOCOL_MAJOR {
        return Err(ProtocolError::ProtocolMajor {
            actual: context.protocol_major,
        });
    }
    if context.contract_major != CONTRACT_MAJOR {
        return Err(ProtocolError::ContractMajor {
            actual: context.contract_major,
        });
    }
    if context.protocol_minor > PROTOCOL_MINOR || context.contract_minor > CONTRACT_MINOR {
        return Err(ProtocolError::UnsupportedMinor);
    }
    if context.request_id.len() != IDENTIFIER_BYTES
        || context.request_id.iter().all(|byte| *byte == 0)
    {
        return Err(ProtocolError::InvalidRequestId);
    }
    if context.deadline_unix_ms <= now_ms {
        return Err(ProtocolError::InvalidDeadline);
    }
    if context.binding.is_none() {
        return Err(ProtocolError::MissingBinding);
    }
    if context.auth_context.len() > MAX_AUTH_CONTEXT_BYTES {
        return Err(ProtocolError::AuthContextTooLarge);
    }
    if context.max_response_bytes == 0
        || context.max_response_bytes as usize > MAX_MESSAGE_BYTES
        || context.max_response_items == 0
        || context.max_response_items as usize > MAX_MESSAGE_ITEMS
    {
        return Err(ProtocolError::InvalidResponseBudget);
    }
    Ok(context)
}
