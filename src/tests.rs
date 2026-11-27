use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use freqfs::{Cache, DirLock};
use tc_collection::btree::{BTreeColumnSchema, StorageConfig};
use tc_collection::collection::CollectionSchema;
use tc_collection::table::{Column, PersistentTable, TableSchema};
use tc_collection::{Collection, CopyFrom, PersistentFile, StorageContext};
use tc_error::TCResult;
use tc_ir::{NetworkTime, Public, Scalar, Transact, Transaction, TxnId};
use tc_state::State;
use tc_value::{NumberType, Value, ValueType};

use crate::{ChainFile, SyncChain};

#[path = "../tests/unit/transaction_history.rs"]
mod stack_tests;

fn run<F: std::future::Future<Output = ()> + Send + 'static>(make: fn() -> F) {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        // Scenario futures retain several native operations. Keep one owning
        // allocation rather than copying the whole scenario through block_on.
        .block_on(Box::pin(make()));
}

#[derive(Clone)]
struct Txn {
    id: TxnId,
    root: DirLock<PersistentFile>,
    path: Vec<String>,
    next: Arc<AtomicUsize>,
}

impl Transaction for Txn {
    fn id(&self) -> TxnId {
        self.id
    }
}

impl StorageContext for Txn {
    type File = PersistentFile;

    async fn context(&self) -> TCResult<DirLock<PersistentFile>> {
        let mut dir = self.root.clone();
        for name in &self.path {
            let next = dir.write().await.get_or_create_dir(name.clone())?;
            dir = next;
        }
        Ok(dir)
    }

    fn subcontext(&self, name: impl Into<String>) -> Self {
        let mut txn = self.clone();
        txn.path.push(name.into());
        txn
    }

    fn subcontext_unique(&self) -> Self {
        self.subcontext(format!(
            "allocation-{}",
            self.next.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn materialized_tensor_bytes(&self) -> usize {
        1024 * 1024
    }
}

fn id(n: u16) -> TxnId {
    TxnId::from_parts(NetworkTime::from_nanos(100), n).with_trace([n as u8; 32])
}

async fn log_file(root: &DirLock<ChainFile>) -> freqfs::FileLock<ChainFile> {
    root.read()
        .await
        .get_file(crate::storage::COMMITTED)
        .cloned()
        .unwrap()
}

// Aggregate snapshots are test assertions, never a production history index.
struct History {
    finalized: Option<TxnId>,
    block: HistoryRecords,
}

#[derive(Default)]
struct HistoryRecords {
    mutations: std::collections::BTreeMap<TxnId, Vec<crate::MutationRecord>>,
}

fn store(root: &DirLock<ChainFile>, values: DirLock<PersistentFile>) -> crate::storage::Store<Txn> {
    crate::storage::Store {
        root: root.clone(),
        values,
    }
}

async fn read_log(root: &DirLock<ChainFile>) -> TCResult<History> {
    let finalized = log_file(root).await.read::<ChainFile>().await?.frontier()?;
    let mut block = HistoryRecords::default();
    let names: Vec<_> = root
        .read()
        .await
        .names()
        .filter(|n| n.as_str() != crate::storage::COMMITTED)
        .cloned()
        .collect();

    for name in names {
        let file = root.read().await.get_file(&name).cloned().unwrap();
        let file = file.read::<ChainFile>().await?;
        let ChainFile::Transaction { id, records } = &*file else {
            panic!("transaction file")
        };

        block.mutations.insert(*id, records.as_ref().clone());
    }

    Ok(History { finalized, block })
}

async fn write_records(
    root: &DirLock<ChainFile>,
    id: TxnId,
    records: Vec<crate::MutationRecord>,
) -> TCResult<()> {
    let file = root
        .write()
        .await
        .create_file(
            crate::storage::transaction_name(id),
            ChainFile::Transaction {
                id,
                records: records.into(),
            },
        )
        .await?;

    file.sync_all().await?;
    Ok(())
}

async fn read_records(
    root: &DirLock<ChainFile>,
    id: TxnId,
) -> TCResult<Vec<crate::MutationRecord>> {
    let file = root
        .read()
        .await
        .get_file(&crate::storage::transaction_name(id))
        .cloned()
        .unwrap();

    let records = file.read::<ChainFile>().await?.records(id)?.to_vec();
    Ok(records)
}

struct Fixture {
    _temp: tempfile::TempDir,
    path: std::path::PathBuf,
    txn: Txn,
    log: DirLock<ChainFile>,
    values: DirLock<PersistentFile>,
    table: AtomicBool,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().to_owned();
        std::fs::create_dir(path.join("log")).unwrap();
        std::fs::create_dir(path.join("work")).unwrap();
        std::fs::create_dir(path.join("values")).unwrap();
        std::fs::create_dir(path.join("canonical")).unwrap();
        let cache = Cache::<PersistentFile>::new(32 * 1024 * 1024, None, 0, Duration::from_secs(3));
        let root = cache.load(path.join("work")).unwrap();
        let log = Self::open_log(&path);
        let values = Self::open_values(&path);
        Self {
            _temp: temp,
            path,
            txn: Txn {
                id: id(1),
                root,
                path: vec![],
                next: Arc::new(AtomicUsize::new(0)),
            },
            log,
            values,
            table: AtomicBool::new(false),
        }
    }

    fn open_log(path: &std::path::Path) -> DirLock<ChainFile> {
        Cache::<ChainFile>::new(32 * 1024 * 1024, None, 0, Duration::from_secs(3))
            .load(path.join("log"))
            .unwrap()
    }

    fn open_values(path: &std::path::Path) -> DirLock<PersistentFile> {
        Cache::<PersistentFile>::new(32 * 1024 * 1024, None, 0, Duration::from_secs(3))
            .load(path.join("values"))
            .unwrap()
    }

    fn txn(&self, n: u16) -> Txn {
        let mut txn = self.txn.clone();
        txn.id = id(n);
        txn
    }

    async fn working(&self) -> DirLock<PersistentFile> {
        self.txn.subcontext_unique().context().await.unwrap()
    }

    async fn source(&self, table: bool) -> Collection<Txn> {
        Self::subject(self.working().await, table, false)
            .await
            .unwrap()
    }

    async fn subject(
        dir: DirLock<PersistentFile>,
        table: bool,
        load: bool,
    ) -> TCResult<Collection<Txn>> {
        // Lifecycle fixtures accept native and JSON-replayed numbers alike.
        let dtype = ValueType::Number(NumberType::Number);
        let schema = if table {
            CollectionSchema::Table(
                TableSchema::new(
                    vec![Column {
                        name: "key".parse().unwrap(),
                        dtype: dtype.clone(),
                    }],
                    vec![Column {
                        name: "value".parse().unwrap(),
                        dtype,
                    }],
                    vec![],
                    StorageConfig::default(),
                )
                .unwrap(),
            )
        } else {
            CollectionSchema::BTree(
                vec![BTreeColumnSchema {
                    name: "key".into(),
                    dtype,
                    max_size: None,
                }]
                .try_into()
                .unwrap(),
            )
        };

        if load {
            Collection::load(dir, schema).await
        } else {
            Collection::create(dir, schema).await
        }
    }

    async fn canonical(&self, load: bool) -> TCResult<Collection<Txn>> {
        let dir = Cache::<PersistentFile>::new(32 * 1024 * 1024, None, 0, Duration::from_secs(3))
            .load(self.path.join("canonical"))?;

        Self::subject(dir, self.table.load(Ordering::Relaxed), load).await
    }

    async fn chain(&self, table: bool) -> SyncChain<Txn> {
        self.table.store(table, Ordering::Relaxed);
        SyncChain::create(
            self.canonical(false).await.unwrap(),
            self.log.clone(),
            self.values.clone(),
            64,
        )
        .await
        .unwrap()
    }

    async fn reopen(&self) -> TCResult<SyncChain<Txn>> {
        SyncChain::load::<State<_>, _, _, _, _>(
            || self.canonical(true),
            Self::open_log(&self.path),
            Self::open_values(&self.path),
            64,
            |id| {
                let mut txn = self.txn.clone();
                txn.id = id;
                async move { Ok(txn) }
            },
        )
        .await
    }
}

async fn insert(chain: &SyncChain<Txn>, txn: &Txn, table: bool, n: u64) -> TCResult<()> {
    let key = if table {
        Value::Tuple(vec![Value::from(n)])
    } else {
        Value::None
    };

    chain
        .put(
            txn,
            &["insert".parse().unwrap()],
            key.into(),
            State::from(Value::Tuple(vec![Value::from(n)])),
        )
        .await
}

#[tokio::test]
#[ignore = "cutoff preparation measurements; run in release mode with --ignored --nocapture"]
async fn cutoff_preparation_benchmark() {
    for retained in [16_u16, 256, 4096] {
        let f = Fixture::new();
        for n in 0..retained {
            let id = id(n + 2);
            f.log
                .write()
                .await
                .create_file(
                    crate::storage::transaction_name(id),
                    ChainFile::Transaction {
                        id,
                        records: vec![crate::MutationRecord::Delete(
                            Default::default(),
                            Default::default(),
                        )]
                        .into(),
                    },
                )
                .await
                .unwrap();
        }

        let store = store(&f.log, f.values.clone());
        let start = std::time::Instant::now();
        for _ in 0..16 {
            assert_eq!(
                store.transactions().await.unwrap().len(),
                usize::from(retained)
            );
        }

        println!(
            "PERSISTENCE_CUTOFF,{retained},full_index_16,ns,{}",
            start.elapsed().as_nanos()
        );

        let start = std::time::Instant::now();
        for _ in 0..16 {
            assert_eq!(
                store.transactions_through(id(2)).await.unwrap(),
                vec![id(2)]
            );
        }

        println!(
            "PERSISTENCE_CUTOFF,{retained},covered_16,ns,{}",
            start.elapsed().as_nanos()
        );
    }
}

async fn count(chain: &SyncChain<Txn>, txn: &Txn) -> u64 {
    let state =
        Public::<State<Txn>>::get(chain, txn, &["count".parse().unwrap()], Value::None.into())
            .await
            .unwrap();

    let State::Scalar(scalar) = state else {
        panic!("invalid count")
    };

    let Scalar::Value(value) = scalar else {
        panic!("invalid count")
    };

    let Value::Number(number) = value else {
        panic!("invalid count")
    };

    number.to_string().parse().unwrap()
}

#[test]
fn delegated_queue_admits_before_capture_and_recovery_ignores_pending_capacity() {
    run(|| async {
        let f = Fixture::new();
        let chain = SyncChain::create(
            f.canonical(false).await.unwrap(),
            f.log.clone(),
            f.values.clone(),
            2,
        )
        .await
        .unwrap();

        insert(&chain, &f.txn(2), false, 1).await.unwrap();
        insert(&chain, &f.txn(2), false, 2).await.unwrap();
        let error = chain
            .put(
                &f.txn(2),
                &["insert".parse().unwrap()],
                Value::None.into(),
                State::from(f.source(false).await),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("capacity exhausted"));
        assert!(
            f.values.read().await.is_empty(),
            "admission precedes capture"
        );

        assert_eq!(count(&chain, &f.txn(2)).await, 2);
        chain.commit(id(2)).await.unwrap();
        drop(chain);

        let reopened = SyncChain::load::<State<_>, _, _, _, _>(
            || f.canonical(true),
            Fixture::open_log(&f.path),
            Fixture::open_values(&f.path),
            1,
            |original| {
                assert_eq!(original, id(2));
                let txn = f.txn(2);
                async move { Ok(txn) }
            },
        )
        .await
        .unwrap();

        assert_eq!(count(&reopened, &f.txn(3)).await, 2);
        reopened.commit(id(2)).await.unwrap();
        insert(&reopened, &f.txn(3), false, 3).await.unwrap();
        reopened.rollback(&id(3)).await.unwrap();
        insert(&reopened, &f.txn(4), false, 4).await.unwrap();
        reopened.commit(id(4)).await.unwrap();
        reopened.finalize(&id(4)).await.unwrap();
        drop(reopened);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(5)).await, 3);
    });
}

#[test]
fn constructors_reject_zero_capacity_and_distinguish_creation_from_loading() {
    run(|| async {
        let f = Fixture::new();
        assert!(f.reopen().await.is_err());
        let subject = f.canonical(false).await.unwrap();
        assert!(
            SyncChain::create(subject.clone(), f.log.clone(), f.values.clone(), 0)
                .await
                .is_err()
        );

        assert!(f.log.read().await.is_empty());
        f.log
            .write()
            .await
            .create_file(crate::storage::COMMITTED.into(), ChainFile::default())
            .await
            .unwrap()
            .sync_all()
            .await
            .unwrap();

        assert!(
            SyncChain::create(subject, f.log.clone(), f.values.clone(), 64)
                .await
                .is_err()
        );

        assert!(
            SyncChain::load::<State<_>, _, _, _, _>(
                || f.canonical(true),
                f.log.clone(),
                f.values.clone(),
                0,
                |_| async { panic!("zero capacity must be rejected") }
            )
            .await
            .is_err()
        );
    });
}

#[test]
fn rollback_failure_and_strict_insert_are_not_replayed() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(true).await;
        insert(&chain, &f.txn(2), true, 1).await.unwrap();
        assert!(insert(&chain, &f.txn(2), true, 1).await.is_err());
        assert!(chain.commit(id(2)).await.is_err());
        chain.rollback(&id(2)).await.unwrap();
        chain.rollback(&id(2)).await.unwrap();
        insert(&chain, &f.txn(3), true, 2).await.unwrap();
        chain.commit(id(3)).await.unwrap();
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 1);
    });
}

#[test]
fn missing_and_corrupt_committed_blocks_fail_closed() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(false).await;
        insert(&chain, &f.txn(2), false, 1).await.unwrap();
        chain.commit(id(2)).await.unwrap();
        drop(chain);
        let path = f.path.join("log/committed.chain_block");
        let original = std::fs::read(&path).unwrap();
        let mut changed = original.clone();
        changed[0] ^= 1;
        for bytes in [
            changed,
            b"broken".to_vec(),
            original[..original.len() - 1].to_vec(),
        ] {
            // External corruption is inspected through a fresh cache.
            std::fs::write(&path, bytes).unwrap();
            assert!(
                SyncChain::load::<State<_>, _, _, _, _>(
                    || f.canonical(true),
                    Fixture::open_log(&f.path),
                    Fixture::open_values(&f.path),
                    64,
                    |_| async { panic!("validate the entire WAL before requesting capabilities") }
                )
                .await
                .err()
                .unwrap()
                .requires_recovery()
            );
        }

        std::fs::remove_file(&path).unwrap();
        assert!(f.reopen().await.is_err());
        std::fs::write(f.path.join("log/index.json"), original).unwrap();
        assert!(
            f.reopen().await.is_err(),
            "old layouts are never recreated or migrated"
        );
    });
}

#[test]
fn recovery_rejects_replacement_transaction_ids() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(false).await;
        insert(&chain, &f.txn(2), false, 1).await.unwrap();
        write_records(
            &f.log,
            id(2),
            vec![crate::MutationRecord::Put(
                "/insert".parse().unwrap(),
                Value::None.into(),
                Box::new(Scalar::from(Value::Tuple(vec![Value::from(1_u64)])).into()),
            )],
        )
        .await
        .unwrap();

        drop(chain);
        let result = SyncChain::load::<State<_>, _, _, _, _>(
            || f.canonical(true),
            Fixture::open_log(&f.path),
            Fixture::open_values(&f.path),
            64,
            |_| async { Ok(f.txn(9)) },
        )
        .await;

        assert!(result.is_err());
    });
}

#[test]
fn finalization_waits_serialize_newer_mutations_and_commits() {
    run(|| async {
        for table in [false, true] {
            for pending_commit in [false, true] {
                let f = Fixture::new();
                let chain = f.chain(table).await;
                insert(&chain, &f.txn(2), table, 1).await.unwrap();
                chain.commit(id(2)).await.unwrap();
                let txn = f.txn(3);
                if pending_commit {
                    insert(&chain, &txn, table, 2).await.unwrap();
                }

                let file = log_file(&f.log).await;
                let guard = file.write::<ChainFile>().await.unwrap();
                let cutoff = id(2);
                let mut finalization = Box::pin(chain.finalize(&cutoff));
                assert!(futures::poll!(&mut finalization).is_pending());
                assert_eq!(count(&chain, &txn).await, 1 + u64::from(pending_commit));
                let mut request = Box::pin(async {
                    if !pending_commit {
                        insert(&chain, &txn, table, 2).await?;
                    }
                    chain.commit(txn.id()).await
                });

                assert!(futures::poll!(&mut request).is_pending());
                drop(guard);
                finalization.await.unwrap();
                request.await.unwrap();
                assert_eq!(count(&chain, &f.txn(4)).await, 2);
                assert!(
                    read_log(&f.log)
                        .await
                        .unwrap()
                        .block
                        .mutations
                        .contains_key(&id(3))
                );
            }
        }
    });
}

#[test]
fn cancelled_commit_requires_reopening() {
    run(|| async {
        for table in [false, true] {
            let f = Fixture::new();
            let chain = f.chain(table).await;
            insert(&chain, &f.txn(2), table, 1).await.unwrap();
            chain.commit(id(2)).await.unwrap();
            insert(&chain, &f.txn(3), table, 2).await.unwrap();
            let publication = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();

            let guard = f.log.write().await;
            let mut commit = Box::pin(chain.commit(id(3)));
            assert!(futures::poll!(&mut commit).is_pending());
            assert!(futures::poll!(Box::pin(chain.rollback(&id(3)))).is_pending());
            drop(commit);
            drop(guard);

            assert!(chain.hash(&f.txn(4)).await.is_err());
            assert!(chain.commit(id(3)).await.is_err());
            assert!(chain.rollback(&id(3)).await.is_err());
            assert!(chain.finalize(&id(3)).await.is_err());
            assert!(
                Public::<State<Txn>>::get(
                    &chain,
                    &f.txn(4),
                    &["count".parse().unwrap()],
                    Value::None.into()
                )
                .await
                .is_err()
            );

            assert_eq!(
                std::fs::read(f.path.join("log/committed.chain_block")).unwrap(),
                publication
            );

            drop(chain);
            assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 1);
        }
    });
}

#[test]
fn stalled_handlers_do_not_exclude_earlier_decisions() {
    run(|| async {
        for table in [false, true] {
            for complete in [false, true] {
                let f = Fixture::new();
                let chain = f.chain(table).await;
                insert(&chain, &f.txn(2), table, 1).await.unwrap();
                let txn = f.txn(3);
                // Source I/O retains the task without retaining lifecycle exclusion.
                let source_dir = f.working().await;
                let source = Fixture::subject(source_dir.clone(), table, false)
                    .await
                    .unwrap();

                let base = source_dir.read().await.get_dir("base").cloned().unwrap();
                let native = if table {
                    base.read().await.get_dir("primary").cloned().unwrap()
                } else {
                    base
                };

                let workspace = native.write().await;
                let mut mutation = Box::pin(chain.restore_from(&txn, &source));
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), &mut mutation)
                        .await
                        .is_err(),
                    "the selected handler must wait, not encounter Chain-wide exclusion"
                );

                tokio::time::timeout(Duration::from_secs(1), chain.commit(id(2)))
                    .await
                    .unwrap()
                    .unwrap();

                tokio::time::timeout(Duration::from_secs(1), chain.finalize(&id(2)))
                    .await
                    .unwrap()
                    .unwrap();

                if complete {
                    drop(workspace);
                    tokio::time::timeout(Duration::from_secs(1), mutation)
                        .await
                        .unwrap()
                        .unwrap();

                    chain.commit(id(3)).await.unwrap();
                } else {
                    drop(mutation);
                    drop(workspace);
                    assert!(chain.commit(id(3)).await.is_err());
                    chain.rollback(&id(3)).await.unwrap();
                }

                let expected = if complete { 0 } else { 1 };
                assert_eq!(count(&chain, &f.txn(4)).await, expected);
                drop(chain);
                assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, expected);
            }
        }
    });
}

#[test]
fn same_transaction_keeps_replay_order_and_cancellation_preserves_failure() {
    run(|| async {
        {
            let f = Fixture::new();
            let chain = f.chain(false).await;
            let txn = f.txn(2);
            let value = State::from(f.source(false).await);
            let guard = f.values.write().await;

            let path = ["insert".parse().unwrap()];
            let mut first = Box::pin(chain.put(&txn, &path, Value::None.into(), value));
            assert!(futures::poll!(&mut first).is_pending());
            assert!(insert(&chain, &txn, false, 2).await.is_err());
            assert!(chain.commit(txn.id()).await.is_err());
            assert!(chain.rollback(&txn.id()).await.is_err());
            assert!(chain.finalize(&txn.id()).await.is_err());
            drop(first);
            drop(guard);

            assert!(chain.commit(txn.id()).await.is_err());
            assert!(
                Public::<State<Txn>>::get(
                    &chain,
                    &txn,
                    &["count".parse().unwrap()],
                    Value::None.into()
                )
                .await
                .is_err()
            );

            chain.rollback(&txn.id()).await.unwrap();
            assert_eq!(count(&chain, &f.txn(3)).await, 0);
            drop(chain);
            assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 0);
        }
    });
}

#[test]
fn reads_wait_for_pending_commit_without_blocking_lifecycle() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(false).await;
        insert(&chain, &f.txn(2), false, 1).await.unwrap();
        let txn = f.txn(3);
        let mut read = Box::pin(count(&chain, &txn));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut read)
                .await
                .is_err()
        );

        chain.commit(id(2)).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), read)
                .await
                .unwrap(),
            1
        );
    });
}

#[test]
fn cutoff_preserves_future_commits_and_discards_pending() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(true).await;
        insert(&chain, &f.txn(2), true, 1).await.unwrap();
        chain.commit(id(2)).await.unwrap();
        insert(&chain, &f.txn(3), true, 3).await.unwrap();
        insert(&chain, &f.txn(4), true, 2).await.unwrap();
        chain.commit(id(4)).await.unwrap();
        chain.finalize(&id(3)).await.unwrap();
        assert!(
            Public::<State<Txn>>::get(
                &chain,
                &f.txn(3),
                &["count".parse().unwrap()],
                Value::None.into()
            )
            .await
            .is_err()
        );

        assert_eq!(count(&chain, &f.txn(5)).await, 2);
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(5)).await, 2);
    });
}

#[test]
fn scalar_writes_are_batched_and_read_only_commit_does_not_write_wal() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(true).await;
        let initial = f.log.read().await.len();
        for key in 0..32 {
            insert(&chain, &f.txn(2), true, key).await.unwrap();
        }
        assert_eq!(
            f.log.read().await.len(),
            initial,
            "scalar capture must not allocate WAL files"
        );

        chain.commit(id(2)).await.unwrap();
        let committed = read_log(&f.log).await.unwrap();
        assert_eq!(committed.block.mutations[&id(2)].len(), 32);
        assert!(
            f.log
                .read()
                .await
                .get_file(crate::storage::COMMITTED)
                .is_some()
        );

        assert_eq!(f.log.read().await.len(), initial + 1);
        assert_eq!(count(&chain.clone(), &f.txn(3)).await, 32);
        let publication = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();
        chain.commit(id(3)).await.unwrap();
        assert_eq!(
            std::fs::read(f.path.join("log/committed.chain_block")).unwrap(),
            publication
        );

        assert_eq!(f.log.read().await.len(), initial + 1);
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 32);
    });
}

#[test]
fn subject_hash_tracks_transactional_visibility_and_recovery() {
    run(|| async {
        for table in [false, true] {
            let f = Fixture::new();
            let chain = f.chain(table).await;
            let empty = chain.hash(&f.txn(1)).await.unwrap();
            insert(&chain, &f.txn(2), table, 1).await.unwrap();
            let pending = chain.hash(&f.txn(2)).await.unwrap();
            assert_ne!(pending, empty);
            assert_eq!(chain.hash(&f.txn(1)).await.unwrap(), empty);
            assert_eq!(pending, chain.inner.subject.hash(id(2)).await.unwrap());

            chain.commit(id(2)).await.unwrap();
            assert_eq!(chain.hash(&f.txn(3)).await.unwrap(), pending);
            insert(&chain, &f.txn(3), table, 2).await.unwrap();
            assert_ne!(chain.hash(&f.txn(3)).await.unwrap(), pending);
            chain.rollback(&id(3)).await.unwrap();
            assert!(chain.hash(&f.txn(3)).await.is_err());
            assert_eq!(chain.hash(&f.txn(4)).await.unwrap(), pending);
            drop(chain);

            let chain = f.reopen().await.unwrap();
            assert_eq!(chain.hash(&f.txn(4)).await.unwrap(), pending);
            chain.finalize(&id(2)).await.unwrap();
            assert!(chain.hash(&f.txn(2)).await.is_err());
            assert_eq!(chain.hash(&f.txn(4)).await.unwrap(), pending);
            drop(chain);
            assert_eq!(
                f.reopen().await.unwrap().hash(&f.txn(4)).await.unwrap(),
                pending
            );
        }
    });
}

#[test]
fn tied_native_keys_replay_and_finalize_without_changing_identity() {
    run(|| async {
        for table in [false, true] {
            let f = Fixture::new();
            let chain = f.chain(table).await;
            let keys: Vec<Value> = ["4+3i", "3+4i"]
                .into_iter()
                .map(|number| Value::Number(number.parse().unwrap()))
                .collect();

            for (index, key) in keys.iter().enumerate() {
                let txn = f.txn(index as u16 + 2);
                chain
                    .put(
                        &txn,
                        &["insert".parse().unwrap()],
                        if table {
                            Value::Tuple(vec![key.clone()]).into()
                        } else {
                            Value::None.into()
                        },
                        State::from(Value::Tuple(vec![if table {
                            Value::from(0u64)
                        } else {
                            key.clone()
                        }])),
                    )
                    .await
                    .unwrap();

                chain.commit(txn.id()).await.unwrap();
            }

            assert_eq!(count(&chain, &f.txn(4)).await, 2);
            let hash = chain.hash(&f.txn(4)).await.unwrap();
            drop(chain);
            let chain = f.reopen().await.unwrap();
            assert_eq!(count(&chain, &f.txn(4)).await, 2);
            assert_eq!(chain.hash(&f.txn(4)).await.unwrap(), hash);
            chain.finalize(&id(3)).await.unwrap();
            assert_eq!(chain.hash(&f.txn(4)).await.unwrap(), hash);
            drop(chain);
            assert_eq!(
                f.reopen().await.unwrap().hash(&f.txn(4)).await.unwrap(),
                hash
            );
        }
    });
}

#[test]
fn observations_do_not_register_and_unseen_read_only_decisions_are_deterministic() {
    run(|| async {
        let f = Fixture::new();
        let chain = SyncChain::create(
            f.canonical(false).await.unwrap(),
            f.log.clone(),
            f.values.clone(),
            64,
        )
        .await
        .unwrap();

        for n in 2..20 {
            assert_eq!(count(&chain, &f.txn(n)).await, 0);
            chain.hash(&f.txn(n)).await.unwrap();
        }
        let view = tc_ir::IntoView::into_view(chain.clone(), f.txn(20))
            .await
            .unwrap();

        drop(view);
        let path = f.path.join("log/committed.chain_block");
        let before = std::fs::read(&path).unwrap();
        chain.commit(id(21)).await.unwrap();
        chain.commit(id(21)).await.unwrap();
        assert!(chain.rollback(&id(21)).await.is_err());
        chain.rollback(&id(22)).await.unwrap();
        chain.rollback(&id(22)).await.unwrap();
        assert!(chain.commit(id(22)).await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(chain.hash(&f.txn(22)).await.is_err());
        chain.finalize(&id(22)).await.unwrap();
        assert_eq!(read_log(&f.log).await.unwrap().finalized, Some(id(22)));
        assert!(chain.hash(&f.txn(21)).await.is_err());
        insert(&chain, &f.txn(23), false, 1).await.unwrap();
        chain.commit(id(23)).await.unwrap();
    });
}

#[test]
fn ordered_put_delete_batch_replays_exactly() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(true).await;
        let txn = f.txn(2);
        insert(&chain, &txn, true, 1).await.unwrap();
        Public::<State<Txn>>::delete(
            &chain,
            &txn,
            &[],
            Value::Tuple(vec![Value::from(1_u64)]).into(),
        )
        .await
        .unwrap();

        insert(&chain, &txn, true, 1).await.unwrap();
        chain.publish_before_subject_commit(id(2)).await;
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 1);
    });
}

#[test]
fn older_disjoint_wal_replays_after_newer_native_commit() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(true).await;
        insert(&chain, &f.txn(2), true, 1).await.unwrap();
        insert(&chain, &f.txn(3), true, 2).await.unwrap();
        chain.commit(id(3)).await.unwrap();
        chain.publish_before_subject_commit(id(2)).await;
        drop(chain);

        let recovered = f.reopen().await.unwrap();
        assert_eq!(count(&recovered, &f.txn(4)).await, 2);
    });
}

#[test]
fn published_transaction_is_not_overwritten() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(false).await;
        // Simulate publication without a live decision receipt.
        write_records(
            &f.log,
            id(2),
            vec![crate::MutationRecord::Delete(
                "/".parse().unwrap(),
                tc_ir::Scalar::default(),
            )],
        )
        .await
        .unwrap();

        let path = f.path.join("log/committed.chain_block");
        let original = std::fs::read(&path).unwrap();
        insert(&chain, &f.txn(2), false, 1).await.unwrap();
        assert!(
            chain
                .commit(id(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("already exists")
        );

        assert_eq!(std::fs::read(path).unwrap(), original);
    });
}

#[test]
#[ignore = "filesystem timing benchmark; run explicitly with --ignored --nocapture"]
fn wal_batch_benchmark() {
    run(|| async {
        fn retained(path: &std::path::Path) -> (u64, u64) {
            let mut total = (0, 0);
            for entry in std::fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                let size = if entry.file_type().unwrap().is_dir() {
                    retained(&entry.path())
                } else {
                    (1, entry.metadata().unwrap().len())
                };

                total.0 += size.0;
                total.1 += size.1;
            }

            total
        }

        for (batch, finalize) in [(true, false), (false, false), (false, true)] {
            let f = Fixture::new();
            let chain = f.chain(true).await;
            let start = std::time::Instant::now();
            eprintln!("WAL measurement root: {}", f.path.display());
            for n in 0..32_u16 {
                let txn = f.txn(if batch { 2 } else { n + 2 });
                insert(&chain, &txn, true, n as u64).await.unwrap();
                if !batch {
                    chain.commit(txn.id()).await.unwrap();
                    if finalize {
                        chain.finalize(&txn.id()).await.unwrap();
                        // Duplicate decisions must add no publication or durability calls.
                        chain.finalize(&txn.id()).await.unwrap();
                    }
                }
            }
            if batch {
                chain.commit(id(2)).await.unwrap();
            }
            eprintln!(
                "32 scalar writes: batched={batch}, finalize={finalize}, elapsed={:?}, WAL files={}, retained WAL bytes={}; measure cumulative writes/barriers with strace",
                start.elapsed(),
                f.log.read().await.len(),
                retained(&f.path.join("log")).1
            );

            let read = std::time::Instant::now();
            assert_eq!(count(&chain, &f.txn(34)).await, 32);
            eprintln!(
                "visible scan={:?}, retained Collection (files, bytes)={:?}",
                read.elapsed(),
                retained(&f.path.join("canonical"))
            );
        }
    });
}

#[test]
fn native_finalization_recovers_before_chain_frontier_publication() {
    run(|| async {
        for table in [false, true] {
            for stage in 0..3 {
                let f = Fixture::new();
                let chain = f.chain(table).await;
                insert(&chain, &f.txn(2), table, 1).await.unwrap();
                chain.commit(id(2)).await.unwrap();
                insert(&chain, &f.txn(3), table, 2).await.unwrap();
                chain.commit(id(3)).await.unwrap();
                if stage > 0 {
                    chain
                        .inner
                        .subject
                        .finalize(&id(if stage == 1 { 2 } else { 3 }))
                        .await
                        .unwrap();
                }
                drop(chain);
                let recovered = SyncChain::<Txn>::load::<State<_>, _, _, _, _>(
                    || f.canonical(true),
                    Fixture::open_log(&f.path),
                    Fixture::open_values(&f.path),
                    64,
                    |_| async { panic!("durable native versions must not be replayed") },
                )
                .await
                .unwrap();

                assert_eq!(count(&recovered, &f.txn(4)).await, 2);
                recovered.finalize(&id(3)).await.unwrap();
                assert!(
                    read_log(&Fixture::open_log(&f.path))
                        .await
                        .unwrap()
                        .block
                        .mutations
                        .is_empty()
                );
            }
        }
    });
}

#[test]
fn native_commits_survive_workspace_removal_and_reopening() {
    run(|| async {
        for table in [false, true] {
            let f = Fixture::new();
            let chain = f.chain(table).await;
            assert!(
                tc_ir::Route::<State<Txn>>::route(&chain, &["restore".parse().unwrap()]).is_none()
            );

            insert(&chain, &f.txn(2), table, 7).await.unwrap();
            assert_eq!(count(&chain, &f.txn(2)).await, 1);
            assert_eq!(count(&chain, &f.txn(1)).await, 0);
            chain.commit(id(2)).await.unwrap();
            chain.commit(id(2)).await.unwrap();
            assert_eq!(count(&chain, &f.txn(3)).await, 1);
            assert!(chain.rollback(&id(2)).await.is_err());
            assert_eq!(read_log(&f.log).await.unwrap().block.mutations.len(), 1);
            let before = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();
            drop(chain);
            std::fs::remove_dir_all(f.path.join("work")).unwrap();
            for attempt in 0..2 {
                let workspace = f.path.join(format!("replay-{attempt}"));
                std::fs::create_dir(&workspace).unwrap();
                let root =
                    Cache::<PersistentFile>::new(32 * 1024 * 1024, None, 0, Duration::from_secs(3))
                        .load(workspace)
                        .unwrap();

                let calls = AtomicUsize::new(0);
                let chain = SyncChain::load::<State<_>, _, _, _, _>(
                    || f.canonical(true),
                    Fixture::open_log(&f.path),
                    Fixture::open_values(&f.path),
                    64,
                    |original| {
                        assert_eq!(original, id(2));
                        calls.fetch_add(1, Ordering::Relaxed);
                        let txn = Txn {
                            id: original,
                            root: root.clone(),
                            path: vec![],
                            next: Arc::new(AtomicUsize::new(0)),
                        };

                        async move { Ok(txn) }
                    },
                )
                .await
                .unwrap();

                assert_eq!(calls.load(Ordering::Relaxed), 0);
                chain.commit(id(2)).await.unwrap();
                assert_eq!(count(&chain, &f.txn(3)).await, 1);
                assert_eq!(
                    std::fs::read(f.path.join("log/committed.chain_block")).unwrap(),
                    before
                );

                if attempt == 1 {
                    chain.finalize(&id(2)).await.unwrap();
                    chain.finalize(&id(2)).await.unwrap();
                }
            }

            let chain = f.reopen().await.unwrap();
            assert_eq!(count(&chain, &f.txn(3)).await, 1);
            let log = read_log(&Fixture::open_log(&f.path)).await.unwrap();
            assert_eq!(log.finalized, Some(id(2)));
            assert!(log.block.mutations.is_empty());
        }
    });
}

#[test]
fn cutoff_only_and_duplicate_finalization_do_not_touch_native_storage() {
    run(|| async {
        let f = Fixture::new();
        let dir = Cache::<PersistentFile>::new(32 * 1024 * 1024, None, 0, Duration::from_secs(3))
            .load(f.path.join("canonical"))
            .unwrap();

        let subject = Fixture::subject(dir.clone(), false, false).await.unwrap();
        let chain = SyncChain::create(subject, f.log.clone(), f.values.clone(), 64)
            .await
            .unwrap();

        let held = dir.write().await;
        tokio::time::timeout(Duration::from_secs(1), chain.finalize(&id(2)))
            .await
            .unwrap()
            .unwrap();

        drop(held);

        insert(&chain, &f.txn(3), false, 7).await.unwrap();
        chain.commit(id(3)).await.unwrap();
        let held = dir.write().await;
        let before = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();
        let file = log_file(&f.log).await;
        let log_guard = file.read::<ChainFile>().await.unwrap();
        assert_eq!(log_guard.frontier().unwrap(), Some(id(2)));
        assert_eq!(read_records(&f.log, id(3)).await.unwrap().len(), 1);
        tokio::time::timeout(Duration::from_secs(1), chain.finalize(&id(2)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            std::fs::read(f.path.join("log/committed.chain_block")).unwrap(),
            before
        );

        drop(log_guard);
        drop(held);
    });
}

#[test]
fn storage_pressure_rejects_before_mutation_and_preserves_rollback() {
    run(|| async {
        let mut f = Fixture::new();
        f.log = Cache::<ChainFile>::new(8192, None, 0, Duration::from_secs(3))
            .load(f.path.join("log"))
            .unwrap();

        let chain = SyncChain::create(
            f.canonical(false).await.unwrap(),
            f.log.clone(),
            f.values.clone(),
            128,
        )
        .await
        .unwrap();

        let mut admitted = 0;
        while admitted < 128 {
            if insert(&chain, &f.txn(2), false, admitted).await.is_err() {
                break;
            }

            admitted += 1;
        }

        assert!(admitted > 0 && admitted < 128);
        // Inspect through the native owner: the queue correctly rejects further
        // use of a failed transaction until its caller rolls it back.
        let Collection::BTree(subject) = &chain.inner.subject else {
            unreachable!()
        };

        assert_eq!(subject.count(id(2)).await.unwrap(), admitted);
        assert!(chain.commit(id(2)).await.is_err());
        chain.rollback(&id(2)).await.unwrap();
        assert!(read_log(&f.log).await.unwrap().block.mutations.is_empty());
        assert_eq!(
            f.log.read().await.len(),
            1,
            "storage rejection preserves the committed file"
        );

        assert_eq!(count(&chain, &f.txn(3)).await, 0);
        insert(&chain, &f.txn(3), false, 999).await.unwrap();
        chain.commit(id(3)).await.unwrap();
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 1);
    });
}

#[test]
fn oversized_record_payloads_do_not_invoke_selected_mutations() {
    run(|| async {
        let mut f = Fixture::new();
        f.log = Cache::<ChainFile>::new(8192, None, 0, Duration::from_secs(3))
            .load(f.path.join("log"))
            .unwrap();

        let chain = f.chain(false).await;
        let called = AtomicBool::new(false);
        assert!(
            chain
                .put_recorded(
                    &f.txn(2),
                    pathlink::PathBuf::new(),
                    Value::None.into(),
                    State::<Txn>::from(Value::from("v".repeat(65536))),
                    |_| async {
                        called.store(true, Ordering::Relaxed);
                        Ok(())
                    },
                )
                .await
                .is_err()
        );

        assert!(!called.load(Ordering::Relaxed));
        chain.rollback(&id(2)).await.unwrap();
        assert!(
            chain
                .delete_recorded(
                    &f.txn(3),
                    pathlink::PathBuf::new(),
                    Value::from("k".repeat(65536)).into(),
                    || async {
                        called.store(true, Ordering::Relaxed);
                        Ok(())
                    },
                )
                .await
                .is_err()
        );

        assert!(!called.load(Ordering::Relaxed));
        chain.rollback(&id(3)).await.unwrap();
        assert_eq!(count(&chain, &f.txn(4)).await, 0);
        insert(&chain, &f.txn(4), false, 1).await.unwrap();
        chain.commit(id(4)).await.unwrap();
    });
}

#[test]
fn collection_views_are_not_persistent_subjects() {
    run(|| async {
        let f = Fixture::new();
        let Collection::BTree(mut view) = f.source(false).await else {
            unreachable!()
        };

        *view = view.slice(
            (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            true,
        );

        assert!(
            SyncChain::create(
                Collection::BTree(view.clone()),
                f.log.clone(),
                f.values.clone(),
                64
            )
            .await
            .is_err()
        );

        let chain = f.chain(false).await;
        drop(chain);
        assert!(
            SyncChain::load::<State<_>, _, _, _, _>(
                || async { Ok(Collection::BTree(view)) },
                Fixture::open_log(&f.path),
                Fixture::open_values(&f.path),
                64,
                |_| async { Ok(f.txn(2)) }
            )
            .await
            .is_err()
        );
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_collection_writes_are_recorded_and_views_remain_read_only() {
    for table in [false, true] {
        let f = Fixture::new();
        let chain = Box::pin(f.chain(table)).await;
        // Each phase owns its test temporaries; boxing avoids embedding their
        // native collection futures in the ordinary Tokio test's stack frame.
        Box::pin(check_selected_collection_views(&f, &chain, table)).await;
        let expected = Box::pin(write_selected_collection_history(&f, &chain, table)).await;
        drop(chain);
        Box::pin(check_unmaterialized_collection(&f, table)).await;
        let replayed = Box::pin(replay_selected_collection_history(&f)).await;
        Box::pin(finalize_selected_collection_history(&f, replayed, expected)).await;
    }
}

async fn check_selected_collection_views(f: &Fixture, chain: &SyncChain<Txn>, table: bool) {
    for n in [1, 2] {
        insert(chain, &f.txn(2), table, n).await.unwrap();
    }

    chain.commit(id(2)).await.unwrap();
    let records = read_records(&f.log, id(2)).await.unwrap();
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| matches!(record,
        crate::MutationRecord::Put(path, _, _) if path[..] == ["insert"]
    )));

    {
        let full: State<Txn> = chain.get(&f.txn(3), &[], Scalar::default()).await.unwrap();
        let State::Collection(full) = full else {
            panic!("expected a collection view");
        };

        let selected = if table {
            Scalar::Value(Value::Tuple(vec![Value::Tuple(vec![
                Value::from("key"),
                Value::from(2_u64),
            ])]))
        } else {
            Scalar::Map(
                [("start".parse().unwrap(), Scalar::from(2_u64))]
                    .into_iter()
                    .collect(),
            )
        };

        let selected: State<Txn> = chain.get(&f.txn(3), &[], selected).await.unwrap();
        let State::Collection(selected) = selected else {
            panic!("expected a selected collection view");
        };

        for view in [&full, &selected] {
            assert!(!view.is_persistent());
            assert!(view.as_btree().is_none());
            if let Some(table) = view.as_table() {
                assert!(!matches!(table, tc_collection::table::Table::File(_)));
            }

            assert!(view.commit(id(3)).await.is_err());
            assert!(view.rollback(&id(3)).await.is_err());
            assert!(view.finalize(&id(3)).await.is_err());
            assert!(
                view.put(
                    &f.txn(3),
                    &["insert".parse().unwrap()],
                    if table {
                        Value::Tuple(vec![7_u64.into()])
                    } else {
                        Value::None
                    }
                    .into(),
                    State::from(Value::Tuple(vec![7_u64.into()])),
                )
                .await
                .is_err()
            );

            assert!(
                Public::<State<Txn>>::delete(
                    view,
                    &f.txn(3),
                    &[],
                    Value::Tuple(vec![2_u64.into()]).into(),
                )
                .await
                .is_err()
            );
        }

        assert_eq!(count(chain, &f.txn(3)).await, 2);
        let copy = Box::pin(Collection::copy_from(&f.txn(3), f.working().await, &full))
            .await
            .unwrap();

        assert!(copy.is_persistent());
        copy.put(
            &f.txn(3),
            &["insert".parse().unwrap()],
            if table {
                Value::Tuple(vec![7_u64.into()])
            } else {
                Value::None
            }
            .into(),
            State::from(Value::Tuple(vec![7_u64.into()])),
        )
        .await
        .unwrap();

        assert_ne!(
            copy.hash(id(3)).await.unwrap(),
            full.hash(id(3)).await.unwrap()
        );

        assert_eq!(count(chain, &f.txn(3)).await, 2);
    }
}

async fn write_selected_collection_history(
    f: &Fixture,
    chain: &SyncChain<Txn>,
    table: bool,
) -> tc_ir::Sha256Hash {
    insert(chain, &f.txn(3), table, 3).await.unwrap();
    let selected = Value::Tuple(vec![Value::Tuple(vec![
        Value::from("key"),
        Value::from(2_u64),
    ])]);

    if table {
        chain
            .put(
                &f.txn(3),
                &[],
                selected.clone().into(),
                State::from(Scalar::Map(
                    [("value".parse().unwrap(), Scalar::from(22_u64))]
                        .into_iter()
                        .collect(),
                )),
            )
            .await
            .unwrap();
    }

    Public::<State<Txn>>::delete(
        chain,
        &f.txn(3),
        &[],
        if table {
            Value::Tuple(vec![Value::Tuple(vec![Value::from("key"), 1_u64.into()])])
        } else {
            Value::Tuple(vec![1_u64.into()])
        }
        .into(),
    )
    .await
    .unwrap();

    chain.commit(id(3)).await.unwrap();
    assert_eq!(count(chain, &f.txn(3)).await, 2);
    let expected = chain.hash(&f.txn(3)).await.unwrap();
    let records = read_records(&f.log, id(3)).await.unwrap();
    assert_eq!(records.len(), if table { 3 } else { 2 });
    if table {
        assert!(
            matches!(&records[1], crate::MutationRecord::Put(path, key, _)
            if path.is_empty() && *key == Scalar::from(selected))
        );
    }

    assert!(
        matches!(records.last().unwrap(), crate::MutationRecord::Delete(path, _)
        if path.is_empty())
    );

    insert(chain, &f.txn(4), table, 99).await.unwrap();
    Public::<State<Txn>>::delete(
        chain,
        &f.txn(4),
        &[],
        Value::Tuple(vec![3_u64.into()]).into(),
    )
    .await
    .unwrap();

    chain.rollback(&id(4)).await.unwrap();
    assert!(chain.commit(id(4)).await.is_err());
    assert_eq!(chain.hash(&f.txn(5)).await.unwrap(), expected);
    assert_eq!(
        read_log(&f.log)
            .await
            .unwrap()
            .block
            .mutations
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![id(2), id(3)],
    );

    expected
}

async fn check_unmaterialized_collection(f: &Fixture, table: bool) {
    let canonical = Box::pin(f.canonical(true)).await.unwrap();
    let untouched = Box::pin(f.source(table)).await;
    assert_eq!(
        canonical.hash(id(1)).await.unwrap(),
        untouched.hash(id(1)).await.unwrap()
    );
}

async fn replay_selected_collection_history(f: &Fixture) -> SyncChain<Txn> {
    let mut replayed_ids = Vec::new();
    let replayed = Box::pin(SyncChain::load::<State<_>, _, _, _, _>(
        || f.canonical(true),
        Fixture::open_log(&f.path),
        Fixture::open_values(&f.path),
        64,
        |original| {
            replayed_ids.push(original);
            let mut txn = f.txn(2).subcontext_unique();
            txn.id = original;
            async move { Ok(txn) }
        },
    ))
    .await
    .unwrap();

    assert!(replayed_ids.is_empty());
    replayed
}

async fn finalize_selected_collection_history(
    f: &Fixture,
    replayed: SyncChain<Txn>,
    expected: tc_ir::Sha256Hash,
) {
    assert_eq!(replayed.hash(&f.txn(5)).await.unwrap(), expected);
    assert_eq!(count(&replayed, &f.txn(5)).await, 2);
    Box::pin(replayed.finalize(&id(3))).await.unwrap();
    assert!(
        read_log(&Fixture::open_log(&f.path))
            .await
            .unwrap()
            .block
            .mutations
            .is_empty()
    );

    drop(replayed);
    let reopened = Box::pin(f.reopen()).await.unwrap();
    assert_eq!(reopened.hash(&f.txn(5)).await.unwrap(), expected);
    assert_eq!(count(&reopened, &f.txn(5)).await, 2);
}

#[test]
fn native_restoration_is_transactional_and_recoverable() {
    run(|| async {
        for table in [false, true] {
            let f = Fixture::new();
            let chain = Box::pin(f.chain(table)).await;
            // Separate scenario phases so debug temporaries do not accumulate in
            // one poll frame. Native calls still run on ordinary worker stacks.
            let snapshot = Box::pin(stage_restoration(&f, &chain, table)).await;
            Box::pin(check_restoration_errors(&f, &chain, table)).await;
            chain.restore_from(&f.txn(7), &snapshot).await.unwrap();
            chain.commit(id(7)).await.unwrap();
            drop(chain);
            Box::pin(check_restoration_recovery(&f)).await;
        }
    });
}

async fn stage_restoration(f: &Fixture, chain: &SyncChain<Txn>, table: bool) -> Collection<Txn> {
    insert(chain, &f.txn(2), table, 1).await.unwrap();
    chain.commit(id(2)).await.unwrap();

    let snapshot = f.source(table).await;
    for key in [8_u64, 9] {
        snapshot
            .put(
                &f.txn(3),
                &["insert".parse().unwrap()],
                if table {
                    Value::Tuple(vec![Value::from(key)])
                } else {
                    Value::None
                }
                .into(),
                State::from(Value::Tuple(vec![Value::from(key)])),
            )
            .await
            .unwrap();
    }

    snapshot.commit(id(3)).await.unwrap();

    chain.restore_from(&f.txn(4), &snapshot).await.unwrap();
    assert_eq!(count(chain, &f.txn(4)).await, 2);
    assert_eq!(count(chain, &f.txn(3)).await, 1);

    chain.rollback(&id(4)).await.unwrap();
    assert_eq!(count(chain, &f.txn(5)).await, 1);
    snapshot
}

async fn check_restoration_errors(f: &Fixture, chain: &SyncChain<Txn>, table: bool) {
    assert!(
        chain
            .restore_from(&f.txn(5), &f.source(!table).await)
            .await
            .is_err()
    );

    chain.rollback(&id(5)).await.unwrap();

    let mismatched = if table {
        let dtype = ValueType::Number(NumberType::Number);
        let schema = TableSchema::new(
            vec![Column {
                name: "other_key".parse().unwrap(),
                dtype: dtype.clone(),
            }],
            vec![Column {
                name: "value".parse().unwrap(),
                dtype,
            }],
            vec![],
            StorageConfig::default(),
        )
        .unwrap();

        PersistentTable::<Txn>::try_new(f.working().await, schema)
            .await
            .unwrap()
            .into()
    } else {
        Collection::<Txn>::create(
            f.working().await,
            tc_collection::collection::CollectionSchema::BTree(
                vec![BTreeColumnSchema {
                    name: "other_key".into(),
                    dtype: ValueType::Number(NumberType::Number),
                    max_size: None,
                }]
                .try_into()
                .unwrap(),
            ),
        )
        .await
        .unwrap()
    };

    assert!(chain.restore_from(&f.txn(6), &mismatched).await.is_err());
    chain.rollback(&id(6)).await.unwrap();
}

async fn check_restoration_recovery(f: &Fixture) {
    let chain = f.reopen().await.unwrap();
    assert_eq!(count(&chain, &f.txn(8)).await, 2);

    chain.finalize(&id(7)).await.unwrap();
    drop(chain);
    assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(8)).await, 2);
}

#[test]
fn tensor_values_replay_with_original_transaction_ids_and_materialize_at_cutoff() {
    run(|| async {
        use tc_collection::collection::CollectionSchema;
        use tc_collection::tensor::{AxisRange, Tensor, TensorSchema};

        for (sparse, floating) in [(false, false), (true, false), (false, true), (true, true)] {
            let f = Fixture::new();
            let template = if floating {
                Tensor::dense_f64(&f.txn(1), vec![4], vec![0.0; 4])
                    .await
                    .unwrap()
            } else {
                Tensor::dense_u64(&f.txn(1), vec![4], vec![0; 4])
                    .await
                    .unwrap()
            };

            let schema = CollectionSchema::Tensor(
                TensorSchema::new(template.number_type(), vec![4].into(), sparse.then_some(0))
                    .unwrap(),
            );

            let native = || {
                Cache::<PersistentFile>::new(1 << 20, None, 0, Duration::from_secs(3))
                    .load(f.path.join("canonical"))
                    .unwrap()
            };

            let subject = Collection::<Txn>::create(native(), schema.clone())
                .await
                .unwrap();

            let chain = SyncChain::create(subject, f.log.clone(), f.values.clone(), 64)
                .await
                .unwrap();

            let coordinate = || Value::Tuple(vec![2u64.into()]);
            chain
                .put(
                    &f.txn(2),
                    &[],
                    coordinate().into(),
                    State::from(Value::from(7u64)),
                )
                .await
                .unwrap();

            chain.commit(id(2)).await.unwrap();
            let view: State<Txn> = chain.get(&f.txn(3), &[], Value::None.into()).await.unwrap();
            let State::Collection(Collection::Tensor(tensor)) = view else {
                panic!("native Tensor view")
            };

            for view in [
                tensor.clone(),
                tensor
                    .clone()
                    .slice(vec![AxisRange::In(0, 4, 1)].into())
                    .unwrap(),
                tensor
                    .clone()
                    .slice(vec![AxisRange::In(1, 3, 1)].into())
                    .unwrap(),
                tensor.clone().transpose(None).unwrap(),
            ] {
                assert!(!view.is_persistent());
                assert!(
                    view.write_value(&f.txn(3), &[0], 99_u64.into())
                        .await
                        .is_err()
                );

                assert!(view.fill(&f.txn(3), 99_u64.into()).await.is_err());
                assert!(view.commit(id(3)).await.is_err());
                assert!(view.rollback(&id(3)).await.is_err());
                assert!(view.finalize(&id(3)).await.is_err());
            }

            let copy =
                tc_collection::tensor::Tensor::copy_from(&f.txn(3), f.working().await, &tensor)
                    .await
                    .unwrap();

            assert!(copy.is_persistent());
            copy.write_value(&f.txn(3), &[2], 99_u64.into())
                .await
                .unwrap();

            assert_eq!(copy.read_value(id(3), &[2]).await.unwrap(), 99_u64.into());
            assert_eq!(tensor.read_value(id(3), &[2]).await.unwrap(), 7_u64.into());
            drop(copy);
            let factor = if floating { 0.1f64.into() } else { 2u64.into() };
            let expected = if floating {
                (7.0 * 0.1f64).into()
            } else {
                14u64.into()
            };
            let accepted = tensor.mul_scalar(id(3), factor).await.unwrap();

            chain
                .put(
                    &f.txn(3),
                    &[],
                    Value::None.into(),
                    State::from(Collection::Tensor(accepted)),
                )
                .await
                .unwrap();

            let selected = Value::Tuple(vec![Value::Tuple(vec![0_u64.into(), 2_u64.into()])]);
            chain
                .put(
                    &f.txn(3),
                    &[],
                    selected.clone().into(),
                    State::from(Value::from(9_u64)),
                )
                .await
                .unwrap();

            Public::<State<Txn>>::delete(
                &chain,
                &f.txn(3),
                &[],
                Value::Tuple(vec![0_u64.into()]).into(),
            )
            .await
            .unwrap();

            chain.publish_before_subject_commit(id(3)).await;
            let expected_hash = chain.hash(&f.txn(3)).await.unwrap();
            let records = read_records(&f.log, id(2)).await.unwrap();
            assert_eq!(records.len(), 1);
            assert!(
                matches!(&records[0], crate::MutationRecord::Put(path, key, value)
                if path.is_empty() && *key == Scalar::from(coordinate()) && matches!(value.as_ref(), crate::StoredValue::Scalar(value) if *value == Scalar::from(7_u64)))
            );

            let records = read_records(&f.log, id(3)).await.unwrap();
            assert_eq!(records.len(), 3);
            assert!(
                matches!(&records[0], crate::MutationRecord::Put(path, key, _)
                if path.is_empty() && *key == Scalar::default())
            );

            assert!(
                matches!(&records[1], crate::MutationRecord::Put(path, key, value)
                if path.is_empty() && *key == Scalar::from(selected.clone()) && matches!(value.as_ref(), crate::StoredValue::Scalar(value) if *value == Scalar::from(9_u64)))
            );

            assert!(
                matches!(&records[2], crate::MutationRecord::Delete(path, key)
                if path.is_empty() && *key == Scalar::from(Value::Tuple(vec![0_u64.into()])))
            );

            assert_eq!(
                read_log(&f.log)
                    .await
                    .unwrap()
                    .block
                    .mutations
                    .keys()
                    .copied()
                    .collect::<Vec<_>>(),
                vec![id(2), id(3)],
            );

            drop(tensor);
            drop(chain);
            let canonical = Collection::<Txn>::load(native(), schema.clone())
                .await
                .unwrap();

            let before: State<Txn> = canonical
                .get(&f.txn(1), &[], coordinate().into())
                .await
                .unwrap();

            assert!(
                matches!(&before, State::Scalar(scalar) if matches!(&scalar, Scalar::Value(value) if matches!(&value, Value::Number(n) if *n == 0u64.into())))
            );

            drop(canonical);
            let mut replayed_ids = Vec::new();
            let replayed = SyncChain::load::<State<_>, _, _, _, _>(
                || Collection::<Txn>::load(native(), schema.clone()),
                Fixture::open_log(&f.path),
                Fixture::open_values(&f.path),
                64,
                |original| {
                    assert!(original == id(2) || original == id(3));
                    replayed_ids.push(original);
                    let mut txn = f.txn(2).subcontext_unique();
                    txn.id = original;
                    async move { Ok(txn) }
                },
            )
            .await
            .unwrap();

            assert_eq!(replayed_ids, vec![id(3)]);
            assert_eq!(replayed.hash(&f.txn(5)).await.unwrap(), expected_hash);
            for (coord, expected) in [(0_u64, 0_u64), (1, 9)] {
                let value: State<Txn> = replayed
                    .get(&f.txn(5), &[], Value::Tuple(vec![coord.into()]).into())
                    .await
                    .unwrap();

                assert!(
                    matches!(&value, State::Scalar(Scalar::Value(Value::Number(number)))
                    if *number == expected.into())
                );
            }

            let value: State<Txn> = replayed
                .get(&f.txn(3), &[], coordinate().into())
                .await
                .unwrap();

            assert!(
                matches!(&value, State::Scalar(scalar) if matches!(&scalar, Scalar::Value(value) if matches!(&value, Value::Number(n) if *n == expected)))
            );

            replayed.finalize(&id(3)).await.unwrap();
            let canonical = Collection::<Txn>::load(native(), schema.clone())
                .await
                .unwrap();

            let value: State<Txn> = canonical
                .get(&f.txn(5), &[], coordinate().into())
                .await
                .unwrap();

            assert!(
                matches!(&value, State::Scalar(scalar) if matches!(&scalar, Scalar::Value(value) if matches!(&value, Value::Number(n) if *n == expected)))
            );

            drop(canonical);
            drop(replayed);
            let reopened = SyncChain::load::<State<_>, _, _, _, _>(
                || Collection::<Txn>::load(native(), schema.clone()),
                Fixture::open_log(&f.path),
                Fixture::open_values(&f.path),
                64,
                |original| async move {
                    Err(tc_error::TCError::bad_request(format!(
                        "unexpected replay after finalization: {original}",
                    )))
                },
            )
            .await
            .unwrap();

            assert_eq!(reopened.hash(&f.txn(5)).await.unwrap(), expected_hash);
        }
    });
}
