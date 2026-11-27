//! A PUT/DELETE recovery log for transactional collection subjects.
#![forbid(unsafe_code)]

mod public;
mod storage;
mod sync;

pub use storage::{CapturedCollection, ChainFile, ChainFileType, MutationRecord, StoredValue};

/// Native declaration path for the implemented Chain variant.
pub const SYNC_CHAIN: pathlink::PathLabel = pathlink::path_label(&["state", "chain", "sync"]);

pub use sync::SyncChain;

#[cfg(test)]
mod tests;
