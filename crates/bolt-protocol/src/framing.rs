use crate::ProtocolError;

pub fn encode_chunks(payload: &[u8], max_chunk: usize) -> Result<Vec<u8>, ProtocolError> {
    if max_chunk == 0 || max_chunk > usize::from(u16::MAX) {
        return Err(ProtocolError::new(
            "DTG-BOLT-INVALID-CHUNK-LIMIT",
            0,
            "chunk size must be between 1 and 65535 bytes",
        ));
    }
    let chunk_count = payload.len().div_ceil(max_chunk);
    let framing_bytes = chunk_count
        .checked_add(1)
        .and_then(|count| count.checked_mul(2))
        .ok_or_else(|| {
            ProtocolError::new(
                "DTG-BOLT-LENGTH-OVERFLOW",
                0,
                "chunk framing length overflows usize",
            )
        })?;
    let mut framed = Vec::with_capacity(payload.len().saturating_add(framing_bytes));
    for chunk in payload.chunks(max_chunk) {
        let length = u16::try_from(chunk.len()).expect("chunk is bounded by u16");
        framed.extend_from_slice(&length.to_be_bytes());
        framed.extend_from_slice(chunk);
    }
    framed.extend_from_slice(&0_u16.to_be_bytes());
    Ok(framed)
}

#[derive(Clone, Debug)]
pub struct ChunkDecoder {
    max_message: usize,
    max_chunk: usize,
    pending: Vec<u8>,
    message: Vec<u8>,
    failed: bool,
}

impl ChunkDecoder {
    pub fn new(max_message: usize, max_chunk: usize) -> Result<Self, ProtocolError> {
        if max_message == 0 || max_chunk == 0 || max_chunk > usize::from(u16::MAX) {
            return Err(ProtocolError::new(
                "DTG-BOLT-INVALID-CHUNK-LIMIT",
                0,
                "message and chunk limits must be positive; chunk must fit u16",
            ));
        }
        Ok(Self {
            max_message,
            max_chunk,
            pending: Vec::new(),
            message: Vec::new(),
            failed: false,
        })
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Vec<u8>>, ProtocolError> {
        if self.failed {
            return Err(ProtocolError::new(
                "DTG-BOLT-FRAMING-FAILED",
                0,
                "chunk decoder is defunct after a framing error",
            ));
        }
        self.pending.extend_from_slice(bytes);
        let mut messages = Vec::new();
        loop {
            if self.pending.len() < 2 {
                break;
            }
            let length = usize::from(u16::from_be_bytes([self.pending[0], self.pending[1]]));
            if length == 0 {
                self.pending.drain(..2);
                messages.push(std::mem::take(&mut self.message));
                continue;
            }
            if length > self.max_chunk {
                return self.fail(
                    "DTG-BOLT-CHUNK-LIMIT",
                    "declared chunk exceeds the configured chunk limit",
                );
            }
            if self.message.len().saturating_add(length) > self.max_message {
                return self.fail(
                    "DTG-BOLT-MESSAGE-LIMIT",
                    "declared chunks exceed the configured message limit",
                );
            }
            if self.pending.len() < length + 2 {
                break;
            }
            self.message.extend_from_slice(&self.pending[2..2 + length]);
            self.pending.drain(..2 + length);
        }
        Ok(messages)
    }

    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.pending.is_empty() && self.message.is_empty()
    }

    fn fail<T>(&mut self, code: &'static str, message: &'static str) -> Result<T, ProtocolError> {
        self.failed = true;
        self.pending.clear();
        self.message.clear();
        Err(ProtocolError::new(code, 0, message))
    }
}
