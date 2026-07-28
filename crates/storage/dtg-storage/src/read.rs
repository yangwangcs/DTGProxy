use std::future::Future;
use std::pin::Pin;

use dtg_kernel::{Digest32, TransactionTime};

use crate::{
    ApplyReceipt, CommittedShardBatch, EdgeId, EdgeVersion, LogicalMutation, ReplicaBinding,
    StorageError, VertexId, VertexVersion,
};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorageError>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadFence {
    binding: ReplicaBinding,
    applied_index: u64,
    capability_digest: Digest32,
}

impl ReadFence {
    pub fn new(binding: ReplicaBinding, applied_index: u64) -> Self {
        let capability_digest = binding.capability_digest();
        Self {
            binding,
            applied_index,
            capability_digest,
        }
    }

    pub fn with_capability_digest(
        binding: ReplicaBinding,
        applied_index: u64,
        capability_digest: Digest32,
    ) -> Self {
        Self {
            binding,
            applied_index,
            capability_digest,
        }
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn capability_digest(&self) -> Digest32 {
        self.capability_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexRead {
    id: VertexId,
    valid_at: i64,
    transaction_at: TransactionTime,
}

impl VertexRead {
    pub const fn new(id: VertexId, valid_at: i64, transaction_at: TransactionTime) -> Self {
        Self {
            id,
            valid_at,
            transaction_at,
        }
    }

    pub const fn id(&self) -> VertexId {
        self.id
    }

    pub const fn valid_at(&self) -> i64 {
        self.valid_at
    }

    pub const fn transaction_at(&self) -> TransactionTime {
        self.transaction_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeRead {
    id: EdgeId,
    valid_at: i64,
    transaction_at: TransactionTime,
}

impl EdgeRead {
    pub const fn new(id: EdgeId, valid_at: i64, transaction_at: TransactionTime) -> Self {
        Self {
            id,
            valid_at,
            transaction_at,
        }
    }

    pub const fn id(&self) -> EdgeId {
        self.id
    }

    pub const fn valid_at(&self) -> i64 {
        self.valid_at
    }

    pub const fn transaction_at(&self) -> TransactionTime {
        self.transaction_at
    }
}

macro_rules! history_request {
    ($name:ident, $id:ty, $version:ty) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name {
            id: $id,
            transaction_from: TransactionTime,
            transaction_through: TransactionTime,
            limit: u32,
        }

        impl $name {
            pub fn new(
                id: $id,
                transaction_from: TransactionTime,
                transaction_through: TransactionTime,
                limit: u32,
            ) -> Result<Self, StorageError> {
                if transaction_from > transaction_through || limit == 0 {
                    return Err(StorageError::InvalidMutation(
                        "history bounds must be ordered and nonempty".into(),
                    ));
                }
                Ok(Self {
                    id,
                    transaction_from,
                    transaction_through,
                    limit,
                })
            }

            pub const fn id(&self) -> $id {
                self.id
            }

            pub const fn limit(&self) -> u32 {
                self.limit
            }

            pub fn includes(&self, version: &$version) -> bool {
                self.transaction_from <= version.transaction_time()
                    && version.transaction_time() <= self.transaction_through
            }
        }
    };
}

history_request!(VertexHistoryRead, VertexId, VertexVersion);
history_request!(EdgeHistoryRead, EdgeId, EdgeVersion);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdjacencyDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdjacencyRead {
    vertex_id: VertexId,
    direction: AdjacencyDirection,
    valid_at: i64,
    transaction_at: TransactionTime,
    limit: u32,
}

impl AdjacencyRead {
    pub fn new(
        vertex_id: VertexId,
        direction: AdjacencyDirection,
        valid_at: i64,
        transaction_at: TransactionTime,
        limit: u32,
    ) -> Result<Self, StorageError> {
        if limit == 0 {
            return Err(StorageError::InvalidMutation(
                "adjacency limit must be nonzero".into(),
            ));
        }
        Ok(Self {
            vertex_id,
            direction,
            valid_at,
            transaction_at,
            limit,
        })
    }

    pub fn matches(&self, edge: &EdgeVersion) -> bool {
        let direction_matches = match self.direction {
            AdjacencyDirection::Outgoing => edge.source() == self.vertex_id,
            AdjacencyDirection::Incoming => edge.target() == self.vertex_id,
            AdjacencyDirection::Both => {
                edge.source() == self.vertex_id || edge.target() == self.vertex_id
            }
        };
        direction_matches
            && edge.valid_time().start() <= self.valid_at
            && self.valid_at < edge.valid_time().end()
            && edge.transaction_time() <= self.transaction_at
    }

    pub const fn valid_at(&self) -> i64 {
        self.valid_at
    }

    pub const fn transaction_at(&self) -> TransactionTime {
        self.transaction_at
    }

    pub const fn limit(&self) -> u32 {
        self.limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangesRead {
    after: Option<ChangeCursor>,
    through_index: u64,
    limit: u32,
}

impl ChangesRead {
    pub fn new(
        after: Option<ChangeCursor>,
        through_index: u64,
        limit: u32,
    ) -> Result<Self, StorageError> {
        if after.is_some_and(|cursor| cursor.raft_index() > through_index) || limit == 0 {
            return Err(StorageError::InvalidMutation(
                "change bounds must be ordered and nonempty".into(),
            ));
        }
        Ok(Self {
            after,
            through_index,
            limit,
        })
    }

    pub const fn includes(&self, cursor: ChangeCursor) -> bool {
        (match self.after {
            Some(after) => {
                after.raft_index < cursor.raft_index
                    || (after.raft_index == cursor.raft_index
                        && after.mutation_ordinal < cursor.mutation_ordinal)
            }
            None => true,
        }) && cursor.raft_index <= self.through_index
    }

    pub const fn limit(&self) -> u32 {
        self.limit
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ChangeCursor {
    raft_index: u64,
    mutation_ordinal: u64,
}

impl ChangeCursor {
    pub const fn new(raft_index: u64, mutation_ordinal: u64) -> Self {
        Self {
            raft_index,
            mutation_ordinal,
        }
    }

    pub const fn raft_index(self) -> u64 {
        self.raft_index
    }

    pub const fn mutation_ordinal(self) -> u64 {
        self.mutation_ordinal
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeRecord {
    cursor: ChangeCursor,
    mutation: LogicalMutation,
}

impl ChangeRecord {
    pub const fn new(cursor: ChangeCursor, mutation: LogicalMutation) -> Self {
        Self { cursor, mutation }
    }

    pub const fn cursor(&self) -> ChangeCursor {
        self.cursor
    }

    pub const fn raft_index(&self) -> u64 {
        self.cursor.raft_index()
    }

    pub const fn mutation_ordinal(&self) -> u64 {
        self.cursor.mutation_ordinal()
    }

    pub const fn mutation(&self) -> &LogicalMutation {
        &self.mutation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangePage {
    rows: Vec<ChangeRecord>,
    next_after: Option<ChangeCursor>,
}

impl ChangePage {
    pub const fn new(rows: Vec<ChangeRecord>, next_after: Option<ChangeCursor>) -> Self {
        Self { rows, next_after }
    }

    pub fn rows(&self) -> &[ChangeRecord] {
        &self.rows
    }

    pub const fn next_after(&self) -> Option<ChangeCursor> {
        self.next_after
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexScan {
    valid_at: i64,
    transaction_at: TransactionTime,
    after: Option<VertexId>,
    limit: u32,
}

impl VertexScan {
    pub fn new(
        valid_at: i64,
        transaction_at: TransactionTime,
        after: Option<VertexId>,
        limit: u32,
    ) -> Result<Self, StorageError> {
        if limit == 0 {
            return Err(StorageError::InvalidMutation(
                "scan limit must be nonzero".into(),
            ));
        }
        Ok(Self {
            valid_at,
            transaction_at,
            after,
            limit,
        })
    }

    pub const fn valid_at(&self) -> i64 {
        self.valid_at
    }

    pub const fn transaction_at(&self) -> TransactionTime {
        self.transaction_at
    }

    pub const fn after(&self) -> Option<VertexId> {
        self.after
    }

    pub const fn limit(&self) -> u32 {
        self.limit
    }
}

/// A point-in-time logical edge scan.
///
/// Results contain at most one edge per [`EdgeId`]: the visible version with
/// the greatest `(transaction_time, version)`, ordered by `EdgeId`. The cursor
/// is exclusive and names the last logical edge identity returned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeScan {
    valid_at: i64,
    transaction_at: TransactionTime,
    after: Option<EdgeId>,
    limit: u32,
}

impl EdgeScan {
    pub fn new(
        valid_at: i64,
        transaction_at: TransactionTime,
        after: Option<EdgeId>,
        limit: u32,
    ) -> Result<Self, StorageError> {
        if limit == 0 {
            return Err(StorageError::InvalidMutation(
                "scan limit must be nonzero".into(),
            ));
        }
        Ok(Self {
            valid_at,
            transaction_at,
            after,
            limit,
        })
    }

    pub fn includes(&self, edge: &EdgeVersion) -> bool {
        self.after.is_none_or(|after| edge.id() > after)
            && edge.valid_time().start() <= self.valid_at
            && self.valid_at < edge.valid_time().end()
            && edge.transaction_time() <= self.transaction_at
    }

    pub const fn valid_at(&self) -> i64 {
        self.valid_at
    }

    pub const fn transaction_at(&self) -> TransactionTime {
        self.transaction_at
    }

    pub const fn after(&self) -> Option<EdgeId> {
        self.after
    }

    pub const fn limit(&self) -> u32 {
        self.limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanPage<T, C> {
    rows: Vec<T>,
    next_after: Option<C>,
}

impl<T, C> ScanPage<T, C> {
    pub const fn new(rows: Vec<T>, next_after: Option<C>) -> Self {
        Self { rows, next_after }
    }

    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    pub const fn next_after(&self) -> Option<C>
    where
        C: Copy,
    {
        self.next_after
    }
}

pub trait ReplicaStateStore: Send + Sync {
    fn binding(&self) -> &ReplicaBinding;
    fn applied_index(&self) -> StoreFuture<'_, u64>;
    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt>;
    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>>;
}

pub trait TemporalReadView: Send + Sync {
    fn fence(&self) -> &ReadFence;
    fn get_vertex(&self, request: VertexRead) -> StoreFuture<'_, Option<VertexVersion>>;
    fn get_edge(&self, request: EdgeRead) -> StoreFuture<'_, Option<EdgeVersion>>;
    fn vertex_history(&self, request: VertexHistoryRead) -> StoreFuture<'_, Vec<VertexVersion>>;
    fn edge_history(&self, request: EdgeHistoryRead) -> StoreFuture<'_, Vec<EdgeVersion>>;
    fn expand(&self, request: AdjacencyRead) -> StoreFuture<'_, Vec<EdgeVersion>>;
    fn changes(&self, request: ChangesRead) -> StoreFuture<'_, ChangePage>;
    fn scan_vertices(
        &self,
        request: VertexScan,
    ) -> StoreFuture<'_, ScanPage<VertexVersion, VertexId>>;
    /// Returns the latest visible version per edge identity, ordered by edge ID.
    fn scan_edges(&self, request: EdgeScan) -> StoreFuture<'_, ScanPage<EdgeVersion, EdgeId>>;
}
