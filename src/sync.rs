use std::sync::Arc;

use freqfs::DirLock;
use pathlink::PathBuf;
use safecast::TryCastInto;
use tc_collection::{Collection, CollectionState, Restore, StorageContext};
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Public, Scalar, Sha256Hash, Transact, TxnId};
use txn_lock::queue::task::TaskQueue;

use crate::storage::{self, ChainFile, ChainFileType, MutationRecord, Store};

fn pending(capacity: usize) -> TCResult<TaskQueue<TxnId, MutationRecord>> {
    if capacity == 0 {
        return Err(TCError::bad_request(
            "Chain mutation capacity must be positive",
        ));
    }

    Ok(TaskQueue::new(capacity))
}

pub(crate) struct Inner<Txn: StorageContext, FE> {
    pub(crate) subject: Collection<Txn>,
    store: Store<Txn, FE>,
    queue: TaskQueue<TxnId, MutationRecord>,
    // Immutable admission configuration, computed where the file composition is
    // known. Native routing need not know how the host persists that composition.
    record_capacity: usize,
}

/// A durable request WAL around a native persistent collection.
/// Application mutations enter through this owner; returned collection views
/// are read-only. Callers relinquish mutable aliases when handing over the subject.
pub struct SyncChain<Txn: StorageContext, FE = ChainFile> {
    pub(crate) inner: Arc<Inner<Txn, FE>>,
}

impl<Txn: StorageContext, FE> Clone for SyncChain<Txn, FE> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Txn: StorageContext + 'static, FE: ChainFileType> SyncChain<Txn, FE> {
    async fn new(
        subject: Collection<Txn>,
        store: Store<Txn, FE>,
        queue: TaskQueue<TxnId, MutationRecord>,
    ) -> Self {
        let record_capacity = store
            .root
            .read()
            .await
            .max_file_size()
            .saturating_sub(FE::from(ChainFile::default()).get_size())
            // Immutable replay snapshots share the Vec allocation with the cache.
            // Its container lives in the Arc, outside the file's inline envelope.
            .saturating_sub(std::mem::size_of::<Vec<MutationRecord>>());

        Self {
            inner: Arc::new(Inner {
                subject,
                store,
                queue,
                record_capacity,
            }),
        }
    }

    /// Publish an empty WAL around an unpublished canonical subject.
    /// The caller durably establishes the delegated WAL and capture roots.
    /// The caller transfers exclusive mutation and lifecycle ownership. Retained
    /// mutable aliases must not be used, and storage must not be reopened independently.
    pub async fn create(
        subject: Collection<Txn>,
        root: DirLock<FE>,
        values: DirLock<Txn::File>,
        capacity: usize,
    ) -> TCResult<Self> {
        let queue = pending(capacity)?;
        if !subject.is_persistent() {
            return Err(TCError::bad_request(
                "SyncChain requires a persistent collection owner",
            ));
        }
        if !root.read().await.is_empty() || !values.read().await.is_empty() {
            return Err(TCError::bad_request("creation requires empty WAL storage"));
        }

        let mut operation = queue.operation().await?;
        operation.arm();
        let committed = root
            .write()
            .await
            .create_file::<FE>(storage::COMMITTED.into(), FE::from(ChainFile::default()))
            .await?;

        committed.sync_all().await?;
        operation.complete();
        Ok(Self::new(subject, Store { root, values }, queue).await)
    }

    /// Check the frontier before invoking the caller's strict native subject loader.
    /// The returned owner must remain unpublished until recovery succeeds.
    pub async fn open<L, Loaded>(
        subject: L,
        root: DirLock<FE>,
        values: DirLock<Txn::File>,
        capacity: usize,
    ) -> TCResult<Self>
    where
        L: FnOnce() -> Loaded,
        Loaded: std::future::Future<Output = TCResult<Collection<Txn>>>,
    {
        let queue = pending(capacity)?;
        let store = Store { root, values };
        store.frontier().await?;

        let subject = subject().await?;
        if !subject.is_persistent() {
            return Err(TCError::bad_request(
                "SyncChain requires a persistent collection owner",
            ));
        }

        Ok(Self::new(subject, store, queue).await)
    }

    /// Replay an opened, unpublished owner after the caller has assembled its runtime.
    /// Readiness and transaction expiry must remain stopped until this succeeds.
    pub async fn recover<S, F, Fut>(&self, mut transaction: F) -> TCResult<()>
    where
        S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
        F: FnMut(TxnId) -> Fut + Send,
        Fut: std::future::Future<Output = TCResult<Txn>> + Send,
    {
        self.inner.queue.validate_fresh()?;
        let finalized = self.inner.store.frontier().await?;
        let mut operation = self.inner.queue.operation().await?;
        operation.arm();
        if let Some(frontier) = finalized {
            self.inner.subject.finalize(&frontier).await?;
            operation.finalize(frontier)?;
        }

        let transactions = self.inner.store.transactions().await?;
        for (&id, file) in &transactions {
            if finalized.is_some_and(|frontier| id <= frontier) {
                continue;
            }

            // Release the WAL guard before native storage or capability acquisition.
            // Replay independently retains the immutable payload after releasing
            // its WAL guard. Clone only the selected record when invocation consumes it.
            let records = { file.read::<ChainFile>().await?.records(id)?.clone() };
            self.inner
                .subject
                .recover(id, || async {
                    let txn = transaction(id).await?;
                    if txn.id() != id {
                        return Err(TCError::bad_request(
                            "recovery transaction identity mismatch",
                        ));
                    }

                    for record in records.iter().cloned() {
                        match record {
                            MutationRecord::Restore(value) => {
                                let snapshot =
                                    self.inner.store.resolve_collection(id, &value).await?;

                                self.inner.subject.restore(&txn, &snapshot).await?;
                            }
                            MutationRecord::Put(path, key, value) => {
                                self.inner
                                    .subject
                                    .put(
                                        &txn,
                                        &path,
                                        key,
                                        self.inner.store.resolve::<S>(id, *value).await?,
                                    )
                                    .await?
                            }
                            MutationRecord::Delete(path, key) => {
                                <Collection<Txn> as Public<S>>::delete(
                                    &self.inner.subject,
                                    &txn,
                                    &path,
                                    key,
                                )
                                .await?
                            }
                        }
                    }

                    Ok(())
                })
                .await?;

            operation.commit(&id)?;
        }

        self.inner.store.reclaim(finalized, &transactions).await?;
        operation.complete();
        Ok(())
    }

    /// Open, validate, and recover using caller-supplied original-ID capabilities.
    pub async fn load<S, L, Loaded, F, Fut>(
        subject: L,
        root: DirLock<FE>,
        values: DirLock<Txn::File>,
        capacity: usize,
        transaction: F,
    ) -> TCResult<Self>
    where
        S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
        L: FnOnce() -> Loaded,
        Loaded: std::future::Future<Output = TCResult<Collection<Txn>>>,
        F: FnMut(TxnId) -> Fut + Send,
        Fut: std::future::Future<Output = TCResult<Txn>> + Send,
    {
        let chain = Self::open(subject, root, values, capacity).await?;
        chain.recover::<S, _, _>(transaction).await?;
        Ok(chain)
    }
}

impl<Txn: StorageContext + 'static, FE: Send + Sync> SyncChain<Txn, FE> {
    /// Record and stage a native replacement using a caller-supplied snapshot.
    pub async fn restore_from(&self, txn: &Txn, snapshot: &Collection<Txn>) -> TCResult<()> {
        let mut task = self.inner.queue.start(txn.id()).await?;
        let (reference, captured) = self
            .inner
            .store
            .capture_collection(txn, snapshot.clone())
            .await?;

        task.record_bounded(
            MutationRecord::Restore(reference),
            self.inner.record_capacity,
        )?;

        self.inner.subject.restore(txn, &captured).await?;
        task.complete().map_err(Into::into)
    }

    /// Hash the transaction-visible subject, including its class, schema, and contents.
    pub async fn hash(&self, txn: &Txn) -> TCResult<Sha256Hash> {
        self.readable(txn)?;
        self.inner.subject.hash(txn.id()).await
    }

    pub(crate) fn readable(&self, txn: &Txn) -> TCResult<()> {
        self.inner.queue.readable(txn.id()).map_err(Into::into)
    }

    pub(crate) async fn put_recorded<S, F, Fut>(
        &self,
        txn: &Txn,
        path: PathBuf,
        key: Scalar,
        value: S,
        selected: F,
    ) -> TCResult<()>
    where
        S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
        F: FnOnce(S) -> Fut + Send,
        Fut: std::future::Future<Output = TCResult<()>> + Send,
    {
        let mut task = self.inner.queue.start(txn.id()).await?;

        let (value, accepted) = self.inner.store.capture(txn, value).await?;
        task.record_bounded(
            MutationRecord::Put(path, key, Box::new(value)),
            self.inner.record_capacity,
        )?;

        // Apply the same native values retained for replay, including the exact
        // materialization of a lazy floating-point Tensor input.
        selected(accepted).await?;
        task.complete().map_err(Into::into)
    }

    pub(crate) async fn delete_recorded<F, Fut>(
        &self,
        txn: &Txn,
        path: PathBuf,
        key: Scalar,
        selected: F,
    ) -> TCResult<()>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = TCResult<()>> + Send,
    {
        let mut task = self.inner.queue.start(txn.id()).await?;
        task.record_bounded(
            MutationRecord::Delete(path, key),
            self.inner.record_capacity,
        )?;

        selected().await?;
        task.complete().map_err(Into::into)
    }
}

impl<Txn: StorageContext + 'static, FE: Send + Sync> IntoView for SyncChain<Txn, FE> {
    type Txn = Txn;

    type View = tc_collection::CollectionView;

    async fn into_view(self, txn: Txn) -> TCResult<Self::View> {
        self.readable(&txn)?;
        self.inner.subject.clone().into_view(txn).await
    }
}

impl<Txn: StorageContext + 'static, FE: ChainFileType> Transact for SyncChain<Txn, FE> {
    async fn commit(&self, id: TxnId) -> TCResult<()> {
        let mut operation = self.inner.queue.operation().await?;
        let Some(records) = operation.commit(&id)? else {
            return Ok(());
        };

        if !records.is_empty() {
            self.inner.store.sync_captures(&records).await?;
            self.inner.store.publish_transaction(id, records).await?;
        }

        self.inner.subject.commit(id).await?;
        operation.complete();
        Ok(())
    }

    async fn rollback(&self, id: &TxnId) -> TCResult<()> {
        let mut operation = self.inner.queue.operation().await?;
        if !operation.check_rollback(id)? {
            return Ok(());
        }

        operation.arm();
        self.inner.subject.rollback(id).await?;
        self.inner.store.remove_captures(*id).await?;
        operation.rollback(id)?;
        operation.complete();
        Ok(())
    }

    async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        let mut operation = self.inner.queue.operation().await?;
        operation.check_finalize(cutoff)?;
        let finalized = self.inner.store.frontier().await?;
        if finalized.is_some_and(|prior| *cutoff <= prior) {
            return Ok(());
        }

        let transactions = self.inner.store.transactions_through(*cutoff).await?;
        operation.arm();
        self.inner.subject.finalize(cutoff).await?;
        self.inner.store.publish_frontier(Some(*cutoff)).await?;

        self.inner
            .store
            .clean_through(*cutoff, &transactions)
            .await?;

        operation.finalize(*cutoff)?;
        operation.complete();
        Ok(())
    }
}

impl<Txn: StorageContext + std::fmt::Debug, FE> std::fmt::Debug for SyncChain<Txn, FE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SyncChain")
            .field(&self.inner.subject)
            .finish()
    }
}

#[cfg(test)]
#[path = "../tests/unit/sync.rs"]
mod tests;
