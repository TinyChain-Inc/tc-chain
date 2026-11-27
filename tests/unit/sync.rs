use super::*;

impl<Txn: StorageContext + 'static, FE: ChainFileType> SyncChain<Txn, FE> {
    // A restart prefix after durable WAL publication, before native commit.
    pub(crate) async fn publish_before_subject_commit(&self, id: TxnId) {
        let mut operation = self.inner.queue.operation().await.unwrap();
        let records = operation.commit(&id).unwrap().unwrap();
        self.inner.store.sync_captures(&records).await.unwrap();
        self.inner
            .store
            .publish_transaction(id, records)
            .await
            .unwrap();

        operation.complete();
    }
}
