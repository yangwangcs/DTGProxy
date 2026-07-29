use dtg_storage_remote_protocol::proto;

const AUTH_VERSION: u8 = 1;
const AUTH_CONTEXT_BYTES: usize = 33;

#[derive(Clone, Eq, PartialEq)]
pub struct RemoteAuthToken([u8; 32]);

impl RemoteAuthToken {
    #[must_use]
    pub const fn new(secret: [u8; 32]) -> Self {
        Self(secret)
    }

    pub(crate) const fn secret(&self) -> &[u8; 32] {
        &self.0
    }
}

impl core::fmt::Debug for RemoteAuthToken {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("RemoteAuthToken([redacted])")
    }
}

pub(crate) fn sign_context(context: &proto::RequestContext, token: &RemoteAuthToken) -> Vec<u8> {
    let mut result = Vec::with_capacity(AUTH_CONTEXT_BYTES);
    result.push(AUTH_VERSION);
    result.extend_from_slice(blake3::keyed_hash(token.secret(), &auth_message(context)).as_bytes());
    result
}

pub(crate) fn verify_context(context: &proto::RequestContext, token: &RemoteAuthToken) -> bool {
    if context.auth_context.len() != AUTH_CONTEXT_BYTES
        || context.auth_context.first().copied() != Some(AUTH_VERSION)
    {
        return false;
    }
    let expected = blake3::keyed_hash(token.secret(), &auth_message(context));
    constant_time_equal(&context.auth_context[1..], expected.as_bytes())
}

fn auth_message(context: &proto::RequestContext) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(b"dtg-storage-remote-auth-v1");
    bytes.extend_from_slice(&context.protocol_major.to_be_bytes());
    bytes.extend_from_slice(&context.protocol_minor.to_be_bytes());
    bytes.extend_from_slice(&context.contract_major.to_be_bytes());
    bytes.extend_from_slice(&context.contract_minor.to_be_bytes());
    bytes.extend_from_slice(&(context.request_id.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&context.request_id);
    bytes.extend_from_slice(&context.deadline_unix_ms.to_be_bytes());
    if let Some(binding) = context.binding.as_ref() {
        bytes.extend_from_slice(&(binding.binding_digest.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&binding.binding_digest);
        bytes.extend_from_slice(&(binding.credential_ref.len() as u64).to_be_bytes());
        bytes.extend_from_slice(binding.credential_ref.as_bytes());
    } else {
        bytes.extend_from_slice(&0_u64.to_be_bytes());
        bytes.extend_from_slice(&0_u64.to_be_bytes());
    }
    bytes.extend_from_slice(&context.max_response_bytes.to_be_bytes());
    bytes.extend_from_slice(&context.max_response_items.to_be_bytes());
    bytes
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use super::{RemoteAuthToken, sign_context, verify_context};
    use dtg_storage_remote_protocol::proto;

    fn context() -> proto::RequestContext {
        proto::RequestContext {
            protocol_major: 1,
            protocol_minor: 0,
            contract_major: 1,
            contract_minor: 0,
            request_id: vec![1; 16],
            deadline_unix_ms: 100,
            binding: Some(proto::Binding {
                binding_digest: vec![2; 32],
                credential_ref: "remote-reference".into(),
                ..proto::Binding::default()
            }),
            auth_context: Vec::new(),
            max_response_bytes: 1024,
            max_response_items: 16,
        }
    }

    #[test]
    fn token_is_bound_to_request_and_binding() {
        let token = RemoteAuthToken::new([7; 32]);
        let mut signed = context();
        signed.auth_context = sign_context(&signed, &token);
        assert!(verify_context(&signed, &token));

        signed.request_id[0] ^= 1;
        assert!(!verify_context(&signed, &token));
    }
}
