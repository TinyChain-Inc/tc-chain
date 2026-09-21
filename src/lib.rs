//! A PUT/DELETE recovery log for transactional collection subjects.
#![forbid(unsafe_code)]

mod public;
mod storage;
mod sync;

pub use storage::{ChainFile, MutationRecord};

/// Caller-configured pending work preserving the protocol transaction identity.
pub type TxnTaskQueue<T> = txn_lock::queue::task::TaskQueue<tc_ir::TxnId, T>;

pub use sync::SyncChain;

#[cfg(test)]
mod tests;
