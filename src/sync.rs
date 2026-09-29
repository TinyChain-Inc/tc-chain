use std::sync::Arc;

use freqfs::DirLock;
use pathlink::PathBuf;
use safecast::TryCastInto;
use tc_collection::{Collection, CollectionState, StorageContext};
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Public, Scalar, Sha256Hash, Transact, TxnId};

use crate::TxnTaskQueue;
use crate::storage::{self, ChainFile, ChainFileType, MutationRecord, Store};

pub(crate) struct Inner<Txn: StorageContext, FE> {
    pub(crate) subject: Collection<Txn>,
    store: Store<Txn, FE>,
    queue: TxnTaskQueue<MutationRecord>,
}

/// A v1-style request WAL around a native persistent collection.
/// Escaped native collection handles remain outside this log's coverage.
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
    fn new(
        subject: Collection<Txn>,
        store: Store<Txn, FE>,
        queue: TxnTaskQueue<MutationRecord>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                subject,
                store,
                queue,
            }),
        }
    }

    /// Publish an empty WAL around an unpublished canonical subject.
    pub async fn create(
        subject: Collection<Txn>,
        root: DirLock<FE>,
        values: DirLock<Txn::File>,
        queue: TxnTaskQueue<MutationRecord>,
    ) -> TCResult<Self> {
        if !subject.is_persistent() {
            return Err(TCError::bad_request(
                "SyncChain requires a persistent collection owner",
            ));
        }
        if !root.read().await.is_empty() || !values.read().await.is_empty() {
            return Err(TCError::bad_request("creation requires empty WAL storage"));
        }

        queue.validate_fresh()?;
        let mut operation = queue.operation().await?;
        operation.arm();
        subject.sync_all().await?;
        let committed = storage::create(&root).await?;
        operation.complete();
        Ok(Self::new(subject, Store { committed, values }, queue))
    }

    /// Check the WAL before invoking the caller's strict native subject loader.
    /// Replay retained requests using original identities and fresh workspaces.
    /// Unfinished materialization requires authoritative resynchronization;
    /// no native loader, replay capability, or cleanup is invoked in that case.
    pub async fn open<L, Loaded>(
        subject: L,
        root: DirLock<FE>,
        values: DirLock<Txn::File>,
        queue: TxnTaskQueue<MutationRecord>,
    ) -> TCResult<Self>
    where
        L: FnOnce() -> Loaded,
        Loaded: std::future::Future<Output = TCResult<Collection<Txn>>>,
    {
        queue.validate_fresh()?;
        let file = storage::load(&root).await?;
        let committed = file.read::<ChainFile>().await?.clone();
        if committed.materializing.is_some() {
            return Err(TCError::bad_request(
                "recovery required: unfinished canonical materialization; authoritative resynchronization is required",
            ));
        }
        if committed.block.previous_hash != tc_ir::Sha256Hash::default() {
            return Err(TCError::bad_request("invalid SyncChain predecessor"));
        }
        if committed.block.mutations.iter().any(|(id, records)| {
            committed.finalized.is_some_and(|frontier| *id <= frontier) || records.is_empty()
        }) {
            return Err(TCError::bad_request("conflicting or empty WAL transaction"));
        }
        let subject = subject().await?;
        if !subject.is_persistent() {
            return Err(TCError::bad_request(
                "SyncChain requires a persistent collection owner",
            ));
        }
        let chain = Self::new(
            subject,
            Store {
                committed: file,
                values,
            },
            queue,
        );
        // Validate every record and stored collection before invoking any mutation handler.
        for (id, records) in &committed.block.mutations {
            for value in records.iter().filter_map(MutationRecord::value) {
                if storage::reference(value)?.is_some() {
                    chain
                        .inner
                        .store
                        .resolve_collection(*id, value.clone())
                        .await?;
                }
            }
        }

        Ok(chain)
    }

    /// Replay an opened, unpublished owner after the caller has assembled its runtime.
    /// Readiness and transaction expiry must remain stopped until this succeeds.
    pub async fn recover<S, F, Fut>(&self, mut transaction: F) -> TCResult<()>
    where
        S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
        F: FnMut(TxnId) -> Fut,
        Fut: std::future::Future<Output = TCResult<Txn>>,
    {
        self.inner.queue.validate_fresh()?;
        let committed = self
            .inner
            .store
            .committed
            .read::<ChainFile>()
            .await?
            .clone();
        let mut operation = self.inner.queue.operation().await?;
        operation.arm();
        if let Some(frontier) = committed.finalized {
            self.inner.subject.finalize(&frontier).await?;
            operation.finalize(frontier)?;
        }

        for (id, records) in committed.block.mutations {
            let txn = transaction(id).await?;
            if txn.id() != id {
                return Err(TCError::bad_request(
                    "recovery transaction identity mismatch",
                ));
            }

            for record in records {
                match record {
                    MutationRecord::Restore(value) => {
                        let snapshot = self.inner.store.resolve_collection(id, value).await?;
                        self.inner.subject.restore_from(&txn, &snapshot).await?;
                    }
                    MutationRecord::Put(path, key, value) => {
                        self.inner
                            .subject
                            .put(
                                &txn,
                                &path,
                                key,
                                self.inner.store.resolve::<S>(id, value).await?,
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
            self.inner.subject.commit(id).await?;
            operation.commit(&id)?;
        }

        self.inner.store.reclaim().await?;
        operation.complete();
        Ok(())
    }

    /// Open, validate, and recover using caller-supplied original-ID capabilities.
    pub async fn load<S, L, Loaded, F, Fut>(
        subject: L,
        root: DirLock<FE>,
        values: DirLock<Txn::File>,
        queue: TxnTaskQueue<MutationRecord>,
        transaction: F,
    ) -> TCResult<Self>
    where
        S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
        L: FnOnce() -> Loaded,
        Loaded: std::future::Future<Output = TCResult<Collection<Txn>>>,
        F: FnMut(TxnId) -> Fut,
        Fut: std::future::Future<Output = TCResult<Txn>>,
    {
        let chain = Self::open(subject, root.clone(), values, queue).await?;
        chain.recover::<S, _, _>(transaction).await?;
        root.sync_deleted().await?;
        Ok(chain)
    }
}

impl<Txn: StorageContext + 'static, FE: Send + Sync> SyncChain<Txn, FE> {
    /// Record and stage a native replacement using a caller-supplied snapshot.
    pub async fn restore_from(&self, txn: &Txn, snapshot: &Collection<Txn>) -> TCResult<()> {
        let mut task = self.inner.queue.start(txn.id()).await?;
        let reference = self
            .inner
            .store
            .capture_collection(txn, snapshot.clone())
            .await?;
        let captured = self
            .inner
            .store
            .resolve_collection(txn.id(), reference.clone())
            .await?;
        task.record(MutationRecord::Restore(reference))?;
        self.inner.subject.restore_from(txn, &captured).await?;
        task.complete().map_err(Into::into)
    }

    /// Admit a caller-owned transaction without allocating an identity.
    pub fn register(&self, txn: Txn) -> TCResult<()> {
        self.inner.queue.register(txn.id()).map_err(Into::into)
    }

    /// Hash the transaction-visible subject, including its class, schema, and contents.
    pub async fn hash(&self, txn: &Txn) -> TCResult<Sha256Hash> {
        self.readable(txn)?;
        self.inner.subject.hash(txn.id()).await
    }

    pub(crate) fn readable(&self, txn: &Txn) -> TCResult<()> {
        self.inner.queue.readable(txn.id()).map_err(Into::into)
    }

    pub(crate) async fn mutate<S, F, Fut>(
        &self,
        txn: &Txn,
        path: PathBuf,
        key: Scalar,
        value: Option<S>,
        selected: F,
    ) -> TCResult<()>
    where
        S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = TCResult<()>> + Send,
    {
        let mut task = self.inner.queue.start(txn.id()).await?;

        let record = match value {
            Some(value) => {
                MutationRecord::Put(path, key, self.inner.store.capture(txn, value).await?)
            }
            None => MutationRecord::Delete(path, key),
        };

        task.record(record)?;
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
            let mut committed = self
                .inner
                .store
                .committed
                .read::<ChainFile>()
                .await?
                .clone();
            self.inner.store.sync(&records).await?;
            if committed.block.mutations.insert(id, records).is_some() {
                return Err(TCError::conflict("transaction already published"));
            }

            storage::publish(&self.inner.store.committed, committed).await?;
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
        operation.rollback(id)?;
        operation.complete();
        Ok(())
    }

    async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        let mut operation = self.inner.queue.operation().await?;
        operation.check_finalize(cutoff)?;
        let mut committed = {
            let committed = self.inner.store.committed.read::<ChainFile>().await?;
            if committed.finalized.is_some_and(|prior| *cutoff <= prior) {
                return Ok(());
            }
            committed.clone()
        };

        let materialize = committed.block.mutations.range(..=*cutoff).next().is_some();
        operation.arm();
        if materialize {
            committed.materializing = Some(*cutoff);
            storage::publish(&self.inner.store.committed, committed.clone()).await?;
        }
        self.inner.subject.finalize(cutoff).await?;
        if materialize {
            self.inner.subject.sync_all().await?;
        }
        committed.finalized = Some(*cutoff);
        committed.materializing = None;
        committed.block.mutations.retain(|id, _| id > cutoff);
        storage::publish(&self.inner.store.committed, committed).await?;

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
