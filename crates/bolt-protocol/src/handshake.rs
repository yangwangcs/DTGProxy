use crate::ProtocolError;

pub const BOLT_MAGIC: u32 = 0x6060_B017;
pub const HANDSHAKE_BYTES: usize = 20;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BoltVersion {
    major: u8,
    minor: u8,
    range: u8,
}

impl BoltVersion {
    #[must_use]
    pub const fn new(major: u8, minor: u8, range: u8) -> Self {
        Self {
            major,
            minor,
            range,
        }
    }

    #[must_use]
    pub const fn major(self) -> u8 {
        self.major
    }

    #[must_use]
    pub const fn minor(self) -> u8 {
        self.minor
    }

    #[must_use]
    pub const fn range(self) -> u8 {
        self.range
    }

    #[must_use]
    pub const fn encode(self) -> [u8; 4] {
        [0, self.range, self.minor, self.major]
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.major == 0 && self.minor == 0 && self.range == 0
    }

    #[must_use]
    fn includes(self, supported: Self) -> bool {
        self.major == supported.major
            && supported.range == 0
            && supported.minor <= self.minor
            && supported.minor >= self.minor.saturating_sub(self.range)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Handshake {
    proposals: [BoltVersion; 4],
}

impl Handshake {
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() != HANDSHAKE_BYTES {
            return Err(ProtocolError::new(
                "DTG-BOLT-INVALID-HANDSHAKE-LENGTH",
                bytes.len(),
                "Bolt handshake must contain exactly 20 bytes",
            ));
        }
        let magic = u32::from_be_bytes(bytes[..4].try_into().expect("four-byte magic"));
        if magic != BOLT_MAGIC {
            return Err(ProtocolError::new(
                "DTG-BOLT-INVALID-MAGIC",
                0,
                "invalid Bolt handshake magic",
            ));
        }
        let mut proposals = [BoltVersion::new(0, 0, 0); 4];
        for (index, proposal) in proposals.iter_mut().enumerate() {
            let start = 4 + index * 4;
            let encoded = &bytes[start..start + 4];
            if encoded[0] != 0 {
                return Err(ProtocolError::new(
                    "DTG-BOLT-INVALID-VERSION",
                    start,
                    "reserved version byte must be zero",
                ));
            }
            *proposal = BoltVersion::new(encoded[3], encoded[2], encoded[1]);
        }
        Ok(Self { proposals })
    }

    #[must_use]
    pub const fn proposals(&self) -> &[BoltVersion; 4] {
        &self.proposals
    }
}

#[must_use]
pub fn negotiate(proposals: &[BoltVersion], supported: &[BoltVersion]) -> Option<BoltVersion> {
    supported
        .iter()
        .copied()
        .filter(|supported| {
            !supported.is_zero()
                && proposals
                    .iter()
                    .copied()
                    .any(|proposal| proposal.includes(*supported))
        })
        .max_by_key(|version| (version.major, version.minor))
}
