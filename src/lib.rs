//! A PUT/DELETE recovery log for transactional collection subjects.
#![forbid(unsafe_code)]

mod public;
mod storage;
mod sync;

pub use storage::{ChainFile, ChainFileType, MutationRecord};

/// Native declaration path for the implemented Chain variant.
pub const SYNC_CHAIN: pathlink::PathLabel = pathlink::path_label(&["state", "chain", "sync"]);

/// Caller-configured pending work preserving the protocol transaction identity.
pub type TxnTaskQueue<T> = txn_lock::queue::task::TaskQueue<tc_ir::TxnId, T>;

pub use sync::SyncChain;

#[cfg(test)]
mod tests;
