#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Version(u64);

impl Version {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Digest32([u8; 32]);

impl Digest32 {
    pub const fn new(value: [u8; 32]) -> Self {
        Self(value)
    }

    pub const fn get(self) -> [u8; 32] {
        self.0
    }
}
