use crate::KernelError;

macro_rules! nonzero_u64_identifier {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
        pub struct $name(u64);

        impl $name {
            pub fn new(value: u64) -> Result<Self, KernelError> {
                (value != 0)
                    .then_some(Self(value))
                    .ok_or(KernelError::ZeroIdentifier)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }
    };
}

nonzero_u64_identifier!(ClusterId);
nonzero_u64_identifier!(GraphId);
nonzero_u64_identifier!(ShardId);
nonzero_u64_identifier!(ReplicaId);
nonzero_u64_identifier!(PlacementEpoch);
nonzero_u64_identifier!(BackendGeneration);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct TransactionId(u128);

impl TransactionId {
    pub fn new(value: u128) -> Result<Self, KernelError> {
        (value != 0)
            .then_some(Self(value))
            .ok_or(KernelError::ZeroIdentifier)
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}
