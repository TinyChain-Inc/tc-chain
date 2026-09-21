use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use freqfs::{Cache, DirLock};
use tc_collection::{
    Collection, PersistentFile, StorageContext,
    btree::{BTree, BTreeColumnSchema, BTreeSchema, StorageConfig},
    collection::BTreeView,
    table::{Column, PersistentTable, TableSchema},
};
use tc_error::TCResult;
use tc_ir::{NetworkTime, Public, Transact, Transaction, TxnId};
use tc_state::State;
use tc_value::Value;

use crate::{ChainFile, SyncChain, TxnTaskQueue};

fn run<F: std::future::Future<Output = ()> + Send + 'static>(make: fn() -> F) {
    use futures::FutureExt;
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap();
            let (send, receive) = std::sync::mpsc::sync_channel(0);
            runtime.spawn(async move {
                let result = std::panic::AssertUnwindSafe(make()).catch_unwind().await;
                send.send(result).unwrap();
            });
            if let Err(panic) = receive.recv().unwrap() {
                std::panic::resume_unwind(panic);
            }
        })
        .unwrap()
        .join()
        .unwrap();
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
    if let Ok(file) = crate::storage::load(root).await {
        file
    } else {
        crate::storage::create(root).await.unwrap()
    }
}

async fn read_log(root: &DirLock<ChainFile>) -> TCResult<ChainFile> {
    Ok(crate::storage::load(root).await?.read().await?.clone())
}

async fn write_log(root: &DirLock<ChainFile>, value: ChainFile) {
    let file = log_file(root).await;
    crate::storage::publish(&file, value).await.unwrap();
}

async fn write_records(
    root: &DirLock<ChainFile>,
    id: TxnId,
    records: Vec<crate::MutationRecord>,
) -> TCResult<()> {
    let file = log_file(root).await;
    let mut value = file.read().await?.clone();
    value.block.mutations.insert(id, records);
    crate::storage::publish(&file, value).await
}

async fn read_records(
    root: &DirLock<ChainFile>,
    id: TxnId,
) -> TCResult<Vec<crate::MutationRecord>> {
    Ok(read_log(root).await?.block.mutations.remove(&id).unwrap())
}

async fn retain_records(root: &DirLock<ChainFile>, ids: &[TxnId]) {
    let mut value = read_log(root).await.unwrap();
    value.block.mutations.retain(|id, _| ids.contains(id));
    write_log(root, value).await;
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
        Self::subject(self.working().await, table, false).unwrap()
    }

    fn subject(dir: DirLock<PersistentFile>, table: bool, load: bool) -> TCResult<Collection<Txn>> {
        let dtype = Value::from(0_u64).class();
        if table {
            let schema = TableSchema::new(
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
            .unwrap();
            Ok(if load {
                PersistentTable::load(dir, schema)?
            } else {
                PersistentTable::try_new(dir, schema)?
            }
            .into())
        } else {
            let schema = BTreeSchema::new(StorageConfig::default(), 1, Some(vec![dtype.clone()]));
            let btree = if load {
                BTree::load(dir, schema)?
            } else {
                BTree::try_with_schema(dir, schema)?
            };
            Ok(Collection::BTree(Box::new(BTreeView::new(
                vec![BTreeColumnSchema {
                    name: "key".into(),
                    dtype,
                    max_size: None,
                }],
                btree,
            ))))
        }
    }

    fn canonical(&self, load: bool) -> TCResult<Collection<Txn>> {
        let dir = Cache::<PersistentFile>::new(32 * 1024 * 1024, None, 0, Duration::from_secs(3))
            .load(self.path.join("canonical"))?;
        Self::subject(dir, self.table.load(Ordering::Relaxed), load)
    }

    async fn chain(&self, table: bool) -> SyncChain<Txn> {
        self.table.store(table, Ordering::Relaxed);
        SyncChain::create(
            self.canonical(false).unwrap(),
            self.log.clone(),
            self.values.clone(),
            TxnTaskQueue::new(64),
        )
        .await
        .unwrap()
    }

    async fn reopen(&self) -> TCResult<SyncChain<Txn>> {
        SyncChain::load(
            self.canonical(true)?,
            Self::open_log(&self.path),
            Self::open_values(&self.path),
            TxnTaskQueue::new(64),
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

async fn count(chain: &SyncChain<Txn>, txn: &Txn) -> u64 {
    let state = chain
        .get(txn, &["count".parse().unwrap()], Value::None.into())
        .await
        .unwrap();
    let State::Scalar(tc_ir::Scalar::Value(Value::Number(number))) = state else {
        panic!("invalid count")
    };
    number.to_string().parse().unwrap()
}

#[test]
fn delegated_queue_admits_before_capture_and_recovery_ignores_pending_capacity() {
    run(|| async {
        let f = Fixture::new();
        let chain = SyncChain::create(
            f.canonical(false).unwrap(),
            f.log.clone(),
            f.values.clone(),
            TxnTaskQueue::new(2),
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
                f.source(false).await.into(),
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

        let queue = TxnTaskQueue::new(1);
        let reopened = SyncChain::load(
            f.canonical(true).unwrap(),
            Fixture::open_log(&f.path),
            Fixture::open_values(&f.path),
            queue,
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
fn create_and_load_reject_used_queues_before_publication_or_replay() {
    run(|| async {
        for finalized in [false, true] {
            let f = Fixture::new();
            let subject = f.canonical(false).unwrap();
            let queue = TxnTaskQueue::new(1);
            if finalized {
                queue.operation().unwrap().finalize(id(1)).unwrap();
            } else {
                queue.register(id(1)).unwrap();
            }
            assert!(
                SyncChain::create(
                    subject.clone(),
                    f.log.clone(),
                    f.values.clone(),
                    queue.clone()
                )
                .await
                .is_err()
            );
            assert!(f.log.read().await.is_empty());
            subject.sync_all().await.unwrap();
            write_log(&f.log, ChainFile::default()).await;
            assert!(
                SyncChain::load(
                    f.canonical(true).unwrap(),
                    f.log.clone(),
                    f.values.clone(),
                    queue,
                    |_| async { panic!("used queue must be rejected") }
                )
                .await
                .is_err()
            );
        }
    });
}

#[test]
fn commit_restart_finalize_restart_for_btree_and_table() {
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
            assert_eq!(read_records(&f.log, id(2)).await.unwrap().len(), 1);
            drop(chain);
            let chain = f.reopen().await.unwrap();
            assert_eq!(count(&chain, &f.txn(3)).await, 1);
            chain.finalize(&id(2)).await.unwrap();
            chain.finalize(&id(2)).await.unwrap();
            let committed = read_log(&Fixture::open_log(&f.path)).await.unwrap();
            assert_eq!(committed.frontier, Some(id(2)));
            assert!(committed.block.mutations.is_empty());
            drop(chain);
            let chain = f.reopen().await.unwrap();
            assert_eq!(count(&chain, &f.txn(4)).await, 1);
        }
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
                SyncChain::load(
                    f.canonical(true).unwrap(),
                    Fixture::open_log(&f.path),
                    Fixture::open_values(&f.path),
                    TxnTaskQueue::new(64),
                    |_| async { panic!("validate the entire WAL before requesting capabilities") }
                )
                .await
                .is_err()
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
        chain.commit(id(2)).await.unwrap();
        drop(chain);
        let result = SyncChain::load(
            f.canonical(true).unwrap(),
            Fixture::open_log(&f.path),
            Fixture::open_values(&f.path),
            TxnTaskQueue::new(64),
            |_| async { Ok(f.txn(9)) },
        )
        .await;
        assert!(result.is_err());
    });
}

#[test]
fn durable_canonical_insert_with_retained_wal_fails_closed() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(true).await;
        insert(&chain, &f.txn(2), true, 1).await.unwrap();
        chain.commit(id(2)).await.unwrap();
        let publication = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();

        // Construct the recovery state explicitly: canonical effects are durable,
        // but the WAL still contains the strict insert. This is not a crash simulation.
        chain.inner.subject.finalize(&id(2)).await.unwrap();
        chain.inner.subject.sync_all().await.unwrap();
        drop(chain);

        assert!(f.reopen().await.is_err());
        assert_eq!(
            std::fs::read(f.path.join("log/committed.chain_block")).unwrap(),
            publication
        );
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

            let file = log_file(&f.log).await;
            let guard = file.write().await.unwrap();
            let mut commit = Box::pin(chain.commit(id(3)));
            assert!(futures::poll!(&mut commit).is_pending());
            assert!(chain.rollback(&id(3)).await.is_err());
            drop(commit);
            drop(guard);

            assert!(chain.register(f.txn(4)).is_err());
            assert!(chain.commit(id(3)).await.is_err());
            assert!(chain.rollback(&id(3)).await.is_err());
            assert!(chain.finalize(&id(3)).await.is_err());
            assert!(
                chain
                    .get(&f.txn(4), &["count".parse().unwrap()], Value::None.into())
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
fn stalled_handlers_do_not_exclude_earlier_commit() {
    run(|| async {
        for table in [false, true] {
            let f = Fixture::new();
            let chain = f.chain(table).await;
            insert(&chain, &f.txn(2), table, 1).await.unwrap();
            let txn = f.txn(3);
            // Stall the selected handler in its delegated workspace allocation.
            // The WAL has already recorded the scalar request at this point.
            let workspace = txn.root.write().await;
            let mut mutation = Box::pin(insert(&chain, &txn, table, 2));
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
            drop(workspace);
            tokio::time::timeout(Duration::from_secs(1), mutation)
                .await
                .unwrap()
                .unwrap();
            chain.commit(id(3)).await.unwrap();
            drop(chain);
            assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 2);
        }
    });
}

#[test]
fn same_transaction_keeps_replay_order_and_cancellation_preserves_failure() {
    run(|| async {
        for capture in [false, true] {
            let f = Fixture::new();
            let chain = f.chain(false).await;
            let txn = f.txn(2);
            let value = if capture {
                State::from(f.source(false).await)
            } else {
                State::from(Value::Tuple(vec![Value::from(1_u64)]))
            };
            // Block capture before recording, or workspace allocation in the selected handler.
            let guard = if capture {
                f.values.write().await
            } else {
                txn.root.write().await
            };
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
                chain
                    .get(&txn, &["count".parse().unwrap()], Value::None.into())
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
fn unfinished_later_requests_do_not_exclude_earlier_decisions() {
    run(|| async {
        for table in [false, true] {
            let f = Fixture::new();
            let chain = f.chain(table).await;
            insert(&chain, &f.txn(2), table, 1).await.unwrap();
            let txn = f.txn(3);
            let workspace = txn.root.write().await;
            let mut later = Box::pin(insert(&chain, &txn, table, 2));
            assert!(futures::poll!(&mut later).is_pending());
            chain.commit(id(2)).await.unwrap();
            chain.finalize(&id(2)).await.unwrap();
            drop(later);
            drop(workspace);
            chain.rollback(&id(3)).await.unwrap();
            assert_eq!(count(&chain, &f.txn(4)).await, 1);
            drop(chain);
            assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 1);
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
        insert(&chain, &f.txn(4), true, 2).await.unwrap();
        chain.commit(id(4)).await.unwrap();
        insert(&chain, &f.txn(3), true, 3).await.unwrap();
        chain.finalize(&id(3)).await.unwrap();
        assert!(
            chain
                .get(&f.txn(3), &["count".parse().unwrap()], Value::None.into())
                .await
                .is_err()
        );
        assert_eq!(count(&chain, &f.txn(5)).await, 2);
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(5)).await, 2);
    });
}

#[test]
fn creation_and_loading_are_distinct_and_frontier_conflicts_fail_closed() {
    run(|| async {
        let f = Fixture::new();
        assert!(f.reopen().await.is_err());
        let chain = f.chain(false).await;
        assert!(
            SyncChain::create(
                f.source(false).await,
                f.log.clone(),
                f.values.clone(),
                TxnTaskQueue::new(64)
            )
            .await
            .is_err()
        );
        insert(&chain, &f.txn(2), false, 1).await.unwrap();
        chain.commit(id(2)).await.unwrap();
        let mut committed = read_log(&f.log).await.unwrap();
        committed.frontier = Some(id(2));
        write_log(&f.log, committed).await;
        drop(chain);
        assert!(f.reopen().await.is_err());
    });
}

#[test]
fn scalar_writes_are_batched_and_read_only_commit_does_not_write_wal() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(true).await;
        let owners = Arc::strong_count(&f.txn.next);
        chain.register(f.txn(2)).unwrap();
        assert_eq!(
            Arc::strong_count(&f.txn.next),
            owners,
            "registration retains no capability"
        );
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
        assert_eq!(committed.block.mutations.len(), 1);
        assert!(
            f.log
                .read()
                .await
                .get_file(crate::storage::COMMITTED)
                .is_some()
        );
        assert_eq!(f.log.read().await.len(), initial);
        assert_eq!(count(&chain.clone(), &f.txn(3)).await, 32);
        let publication = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();
        chain.commit(id(3)).await.unwrap();
        assert_eq!(
            std::fs::read(f.path.join("log/committed.chain_block")).unwrap(),
            publication
        );
        assert_eq!(f.log.read().await.len(), initial);
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 32);
    });
}

#[test]
fn observations_do_not_register_and_unseen_read_only_decisions_are_deterministic() {
    run(|| async {
        let f = Fixture::new();
        let queue = TxnTaskQueue::new(64);
        let chain = SyncChain::create(
            f.canonical(false).unwrap(),
            f.log.clone(),
            f.values.clone(),
            queue.clone(),
        )
        .await
        .unwrap();
        for n in 2..20 {
            assert_eq!(count(&chain, &f.txn(n)).await, 0);
        }
        let view = tc_ir::IntoView::into_view(chain.clone(), f.txn(20))
            .await
            .unwrap();
        drop(view);
        queue.validate_fresh().unwrap();
        let path = f.path.join("log/committed.chain_block");
        let before = std::fs::read(&path).unwrap();
        chain.commit(id(21)).await.unwrap();
        chain.commit(id(21)).await.unwrap();
        assert!(chain.rollback(&id(21)).await.is_err());
        chain.rollback(&id(22)).await.unwrap();
        chain.rollback(&id(22)).await.unwrap();
        assert!(chain.commit(id(22)).await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(queue.readable(id(22)).is_err());
        chain.finalize(&id(22)).await.unwrap();
        assert!(queue.readable(id(21)).is_err());
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
        chain
            .delete(&txn, &[], Value::Tuple(vec![Value::from(1_u64)]).into())
            .await
            .unwrap();
        insert(&chain, &txn, true, 1).await.unwrap();
        chain.commit(id(2)).await.unwrap();
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 1);
    });
}

#[test]
fn collection_identity_includes_semantic_schema_and_contents() {
    run(|| async {
        use safecast::TryCastFrom;
        use tc_collection::collection::CollectionSchema;

        let f = Fixture::new();
        let txn = f.txn(2);
        let btree = f.source(false).await;
        let table = f.source(true).await;
        assert_ne!(
            btree.hash(txn.id()).await.unwrap(),
            table.hash(txn.id()).await.unwrap()
        );

        for source in [&btree, &table] {
            let schema: (pathlink::PathBuf, Value) = source.schema().unwrap().into();
            let decoded = CollectionSchema::opt_cast_from(schema.clone()).unwrap();
            assert_eq!(<(pathlink::PathBuf, Value)>::from(decoded), schema);
            let copy = source.copy_into(&txn, f.working().await).await.unwrap();
            assert_eq!(
                source.hash(txn.id()).await.unwrap(),
                copy.hash(txn.id()).await.unwrap()
            );
        }

        let original = btree.hash(txn.id()).await.unwrap();
        for property in 0..3 {
            let mut changed = btree.clone();
            let Collection::BTree(view) = &mut changed else {
                unreachable!()
            };
            match property {
                0 => view.schema[0].name = "renamed".into(),
                1 => view.schema[0].max_size = Some(64_u64.into()),
                _ => view.schema[0].dtype = Value::from("text").class(),
            }
            assert_ne!(changed.hash(txn.id()).await.unwrap(), original);
        }
        btree
            .put(
                &txn,
                &["insert".parse().unwrap()],
                Value::None.into(),
                State::from(Value::Tuple(vec![Value::from(5_u64)])),
            )
            .await
            .unwrap();
        assert_ne!(btree.hash(txn.id()).await.unwrap(), original);

        let mut hashes = Vec::new();
        for (name, indices, storage) in [
            ("key", vec![], StorageConfig::default()),
            (
                "key",
                vec![],
                StorageConfig {
                    block_size: 8192,
                    order: 32,
                },
            ),
            ("renamed", vec![], StorageConfig::default()),
            (
                "key",
                vec![("by_value".into(), vec!["value".parse().unwrap()])],
                StorageConfig::default(),
            ),
        ] {
            let schema = TableSchema::new(
                vec![Column {
                    name: name.parse().unwrap(),
                    dtype: Value::from(0_u64).class(),
                }],
                vec![Column {
                    name: "value".parse().unwrap(),
                    dtype: Value::from(0_u64).class(),
                }],
                indices,
                storage,
            )
            .unwrap();
            let source = Collection::from(
                PersistentTable::<Txn>::try_new(f.working().await, schema).unwrap(),
            );
            let copy = source.copy_into(&txn, f.working().await).await.unwrap();
            let hash = source.hash(txn.id()).await.unwrap();
            assert_eq!(hash, copy.hash(txn.id()).await.unwrap());
            let native: (pathlink::PathBuf, Value) = source.schema().unwrap().into();
            assert_eq!(
                <(pathlink::PathBuf, Value)>::from(
                    CollectionSchema::opt_cast_from(native.clone()).unwrap()
                ),
                native
            );
            hashes.push(hash);
        }
        assert_eq!(hashes[0], hashes[1]);
        assert_ne!(hashes[0], hashes[2]);
        assert_ne!(hashes[0], hashes[3]);

        // Hashes are ordered, while native copying materializes canonical order.
        // Never publish a reordered view under an identity its copy cannot reproduce.
        btree
            .put(
                &txn,
                &["insert".parse().unwrap()],
                Value::None.into(),
                State::from(Value::Tuple(vec![Value::from(6_u64)])),
            )
            .await
            .unwrap();
        let Collection::BTree(mut view) = btree else {
            unreachable!()
        };
        view.reverse = true;
        let store = crate::storage::Store {
            committed: log_file(&f.log).await,
            values: f.values.clone(),
        };
        assert!(
            store
                .capture(&txn, Collection::BTree(view).into())
                .await
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
        write_log(&f.log, ChainFile::default()).await;
        store.reclaim().await.unwrap();
        assert!(f.values.read().await.is_empty());
    });
}

#[test]
fn native_references_are_shared_and_corrupt_captures_are_not_replaced() {
    run(|| async {
        for table in [false, true] {
            let f = Fixture::new();
            let store = crate::storage::Store {
                committed: log_file(&f.log).await,
                values: f.values.clone(),
            };
            let source = f.source(table).await;
            let first = store
                .capture(&f.txn(2), source.clone().into())
                .await
                .unwrap();
            let second = store
                .capture(&f.txn(3), f.source(table).await.into())
                .await
                .unwrap();
            assert_eq!(first, second);
            assert_eq!(f.values.read().await.len(), 1);
            let name = crate::storage::reference(&first)
                .unwrap()
                .unwrap()
                .0
                .to_string();
            let records = vec![
                crate::storage::MutationRecord::Put(
                    pathlink::PathBuf::new(),
                    tc_ir::Scalar::default(),
                    first.clone()
                );
                2
            ];
            store.sync(&records).await.unwrap();
            for n in [2, 3] {
                write_records(&f.log, id(n), records.clone()).await.unwrap();
            }
            retain_records(&f.log, &[id(3)]).await;
            store.reclaim().await.unwrap();
            assert_eq!(f.values.read().await.len(), 1);
            assert_eq!(read_records(&f.log, id(3)).await.unwrap().len(), 2);
            let State::Collection(captured) = store.resolve(id(3), second).await.unwrap() else {
                panic!("expected captured collection");
            };
            captured
                .put(
                    &f.txn(4),
                    &["insert".parse().unwrap()],
                    if table {
                        Value::Tuple(vec![Value::from(1_u64)])
                    } else {
                        Value::None
                    }
                    .into(),
                    State::from(Value::Tuple(vec![Value::from(1_u64)])),
                )
                .await
                .unwrap();
            captured.commit(id(4)).await.unwrap();
            captured.finalize(&id(4)).await.unwrap();
            captured.sync_all().await.unwrap();
            assert!(
                store
                    .capture(&f.txn(5), source.clone().into())
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("checksum mismatch")
            );
            assert!(f.values.read().await.get_dir(&name).is_some());

            write_log(&f.log, ChainFile::default()).await;
            store.reclaim().await.unwrap();
            assert!(f.values.read().await.is_empty());

            // An interrupted capture may leave an empty directory. Never fill it in on reuse.
            let incomplete = f.values.write().await.create_dir(name.clone()).unwrap();
            assert!(store.capture(&f.txn(4), source.into()).await.is_err());
            assert!(incomplete.read().await.is_empty());
            assert!(f.values.read().await.get_dir(&name).is_some());
            write_log(&f.log, ChainFile::default()).await;
            store.reclaim().await.unwrap();
            assert!(f.values.read().await.is_empty());
        }
    });
}

#[test]
fn unsupported_collection_references_fail_before_replay() {
    run(|| async {
        use tc_ir::{IdRef, OpRef, Scalar, Subject, TCRef};
        use tc_value::class::NativeClass;

        let f = Fixture::new();
        let _chain = f.chain(false).await;
        let store = crate::storage::Store {
            committed: log_file(&f.log).await,
            values: f.values.clone(),
        };
        let value = store
            .capture(&f.txn(2), f.source(false).await.into())
            .await
            .unwrap();
        let (name, path, schema) = crate::storage::reference(&value).unwrap().unwrap();
        let get = |name: &str, path, schema| {
            Scalar::from(TCRef::Op(OpRef::Get((
                Subject::Ref(IdRef::new(name.parse().unwrap()), path),
                schema,
            ))))
        };
        let invalid = [
            Scalar::from(TCRef::Id(IdRef::new("literal".parse().unwrap()))),
            Scalar::from(TCRef::Op(OpRef::Get((
                Subject::Link("/state/collection/btree".parse().unwrap()),
                schema.clone(),
            )))),
            get("short", path.clone(), schema.clone()),
            get(&"g".repeat(64), path.clone(), schema.clone()),
            get(
                name.as_str(),
                tc_collection::TensorType.path(),
                schema.clone(),
            ),
            get(name.as_str(), path.clone(), Scalar::default()),
            get(&"0".repeat(64), path.clone(), schema.clone()),
        ];
        for value in invalid {
            assert!(
                store
                    .capture(&f.txn(2), value.clone().into())
                    .await
                    .is_err()
            );
            assert!(store.resolve(id(2), value.clone()).await.is_err());
        }
        let records = vec![
            crate::storage::MutationRecord::Put(
                "/insert".parse().unwrap(),
                Scalar::default(),
                value.clone()
            );
            2
        ];
        store.sync(&records).await.unwrap();
        write_records(&f.log, id(2), records).await.unwrap();
        write_records(
            &f.log,
            id(3),
            vec![crate::storage::MutationRecord::Put(
                "/insert".parse().unwrap(),
                Scalar::default(),
                value.clone(),
            )],
        )
        .await
        .unwrap();
        retain_records(&f.log, &[id(2), id(3)]).await;
        let recovered = SyncChain::load(
            f.canonical(true).unwrap(),
            Fixture::open_log(&f.path),
            Fixture::open_values(&f.path),
            TxnTaskQueue::new(64),
            |original| async move {
                assert_eq!(original, id(2));
                Err(tc_error::TCError::internal("shared references validated"))
            },
        )
        .await;
        assert!(
            recovered
                .err()
                .unwrap()
                .to_string()
                .contains("shared references validated")
        );

        // Replace the later transaction with a malformed reference. Validation must finish
        // across all transactions before any capability is requested.
        write_records(
            &f.log,
            id(3),
            vec![crate::storage::MutationRecord::Put(
                "/insert".parse().unwrap(),
                Scalar::default(),
                get(name.as_str(), path.clone(), Scalar::default()),
            )],
        )
        .await
        .unwrap();
        retain_records(&f.log, &[id(2), id(3)]).await;
        assert!(
            SyncChain::load(
                f.canonical(true).unwrap(),
                Fixture::open_log(&f.path),
                Fixture::open_values(&f.path),
                TxnTaskQueue::new(64),
                |_| async {
                    panic!("all references must be validated before requesting capabilities")
                }
            )
            .await
            .is_err()
        );
        assert_eq!(f.log.read().await.len(), 1);
    });
}

#[test]
fn native_collection_values_are_copied_and_verified() {
    run(|| async {
        for table in [false, true] {
            let f = Fixture::new();
            let source = f.source(table).await;
            let txn = f.txn(2);
            source
                .put(
                    &txn,
                    &["insert".parse().unwrap()],
                    if table {
                        Value::Tuple(vec![Value::from(9_u64)])
                    } else {
                        Value::None
                    }
                    .into(),
                    State::from(Value::Tuple(vec![Value::from(9_u64)])),
                )
                .await
                .unwrap();
            let store = crate::storage::Store {
                committed: log_file(&f.log).await,
                values: f.values.clone(),
            };
            let value = store
                .capture(&txn, State::from(source.clone()))
                .await
                .unwrap();
            assert_eq!(
                store.capture(&txn, source.clone().into()).await.unwrap(),
                value
            );
            assert_eq!(f.values.read().await.len(), 1);

            source.rollback(&txn.id()).await.unwrap();
            let records = vec![crate::storage::MutationRecord::Put(
                pathlink::PathBuf::new(),
                tc_ir::Scalar::default(),
                value.clone(),
            )];
            store.sync(&records).await.unwrap();
            write_records(&f.log, txn.id(), records).await.unwrap();
            let mut records = read_records(&Fixture::open_log(&f.path), txn.id())
                .await
                .unwrap();
            let crate::storage::MutationRecord::Put(_, _, value) = records.remove(0) else {
                panic!("expected PUT");
            };
            let reopened = crate::storage::Store {
                committed: log_file(&Fixture::open_log(&f.path)).await,
                values: Fixture::open_values(&f.path),
            };
            let State::Collection(copy) = reopened.resolve(txn.id(), value.clone()).await.unwrap()
            else {
                panic!("expected collection");
            };
            let count: State<Txn> = copy
                .get(&txn, &["count".parse().unwrap()], Value::None.into())
                .await
                .unwrap();
            assert!(
                matches!(count, State::Scalar(tc_ir::Scalar::Value(value)) if value == Value::from(1_u64))
            );
            copy.put(
                &f.txn(3),
                &["insert".parse().unwrap()],
                if table {
                    Value::Tuple(vec![Value::from(10_u64)])
                } else {
                    Value::None
                }
                .into(),
                State::from(Value::Tuple(vec![Value::from(10_u64)])),
            )
            .await
            .unwrap();
            copy.commit(id(3)).await.unwrap();
            copy.finalize(&id(3)).await.unwrap();
            copy.sync_all().await.unwrap();
            let (orphan, dir) = reopened.values.write().await.create_dir_unique().unwrap();
            f.source(table).await.copy_into(&txn, dir).await.unwrap();
            reopened.values.sync().await.unwrap();
            let publication = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();
            assert!(
                reopened.resolve(txn.id(), value.clone()).await.is_err(),
                "changed native rows must fail validation"
            );
            retain_records(&f.log, &[txn.id()]).await;
            assert!(
                SyncChain::load(
                    f.canonical(false).unwrap(),
                    Fixture::open_log(&f.path),
                    Fixture::open_values(&f.path),
                    TxnTaskQueue::new(64),
                    |_| async {
                        panic!("validate native values before requesting replay capabilities")
                    }
                )
                .await
                .is_err()
            );
            assert!(f.path.join("values").join(orphan.to_string()).is_dir());
            assert_eq!(
                std::fs::read(f.path.join("log/committed.chain_block")).unwrap(),
                publication
            );
            reopened
                .values
                .write()
                .await
                .delete(
                    crate::storage::reference(&value)
                        .unwrap()
                        .unwrap()
                        .0
                        .as_str(),
                )
                .await;
            reopened.values.sync_deleted().await.unwrap();
            assert!(reopened.resolve(txn.id(), value).await.is_err());
        }
    });
}

#[test]
fn failed_collection_arguments_are_reclaimed_on_load() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(false).await;
        assert!(
            chain
                .put(
                    &f.txn(3),
                    &["insert".parse().unwrap()],
                    Value::None.into(),
                    f.source(false).await.into()
                )
                .await
                .is_err()
        );
        let publication = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();
        let txn = f.txn(2);
        let path = ["insert".parse().unwrap()];
        let source = State::from(f.source(false).await);
        assert!(
            chain
                .put(&txn, &path, Value::None.into(), source)
                .await
                .is_err()
        );
        assert_eq!(f.values.read().await.len(), 1);
        assert!(chain.commit(txn.id()).await.is_err());
        chain.rollback(&id(4)).await.unwrap();
        chain.rollback(&txn.id()).await.unwrap();
        assert_eq!(
            std::fs::read(f.path.join("log/committed.chain_block")).unwrap(),
            publication
        );
        assert_eq!(f.values.read().await.len(), 1);
        chain.finalize(&id(3)).await.unwrap();
        assert_eq!(f.values.read().await.len(), 1);
        f.values.sync().await.unwrap();
        drop(chain);
        let reopened = f.reopen().await.unwrap();
        assert_eq!(count(&reopened, &f.txn(5)).await, 0);
        assert!(Fixture::open_values(&f.path).read().await.is_empty());
    });
}

#[test]
fn cleanup_failure_fails_load_without_changing_the_wal() {
    run(|| async {
        let f = Fixture::new();
        let chain = f.chain(false).await;
        assert!(
            chain
                .put(
                    &f.txn(2),
                    &["insert".parse().unwrap()],
                    Value::None.into(),
                    f.source(false).await.into(),
                )
                .await
                .is_err()
        );
        f.values.sync_all().await.unwrap();
        let name = f.values.read().await.names().next().unwrap().clone();
        let path = f.path.join("values").join(name);
        // Deliberately invalidate the delegated handle to inject a directory deletion error.
        std::fs::remove_dir_all(&path).unwrap();
        std::fs::write(&path, b"obstruction").unwrap();
        chain.rollback(&id(2)).await.unwrap();
        chain.finalize(&id(2)).await.unwrap();
        let publication = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();
        drop(chain);
        let failure = SyncChain::load(
            f.canonical(true).unwrap(),
            Fixture::open_log(&f.path),
            f.values.clone(),
            TxnTaskQueue::new(64),
            |_| async { panic!("no retained transactions") },
        )
        .await;
        assert!(failure.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"obstruction");
        assert_eq!(
            std::fs::read(f.path.join("log/committed.chain_block")).unwrap(),
            publication
        );
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        f.reopen().await.unwrap();
        assert!(!path.exists());
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
                .contains("already published")
        );
        assert_eq!(std::fs::read(path).unwrap(), original);
        assert!(chain.register(f.txn(3)).is_err());
    });
}

#[test]
fn collection_reclamation_preserves_the_committed_file_and_published_captures() {
    run(|| async {
        let f = Fixture::new();
        let _chain = f.chain(false).await;
        for name in [id(2).to_string(), crate::storage::COMMITTED.into()] {
            f.values.write().await.create_dir(name).unwrap();
        }
        let store = crate::storage::Store {
            committed: log_file(&f.log).await,
            values: f.values.clone(),
        };
        let value = store
            .capture(&f.txn(4), f.source(false).await.into())
            .await
            .unwrap();
        let name = crate::storage::reference(&value)
            .unwrap()
            .unwrap()
            .0
            .to_string();
        write_records(
            &f.log,
            id(4),
            vec![crate::MutationRecord::Put(
                "/".parse().unwrap(),
                tc_ir::Scalar::default(),
                value,
            )],
        )
        .await
        .unwrap();
        store.reclaim().await.unwrap();
        assert_eq!(f.values.read().await.len(), 1);
        assert!(f.values.read().await.get_dir(&name).is_some());
        assert_eq!(f.log.read().await.len(), 1);
        assert!(
            f.log
                .read()
                .await
                .get_file(crate::storage::COMMITTED)
                .is_some()
        );
    });
}

#[test]
fn native_record_paths_and_committed_structure_are_validated() {
    run(|| async {
        use crate::storage::{ChainBlock, MutationRecord};
        for path in ["/", "/insert", "/nested/delete"] {
            let f = Fixture::new();
            let records = vec![
                MutationRecord::Put(
                    path.parse().unwrap(),
                    tc_ir::Scalar::default(),
                    tc_ir::Scalar::default(),
                ),
                MutationRecord::Delete(path.parse().unwrap(), tc_ir::Scalar::default()),
            ];
            write_records(&f.log, id(2), records).await.unwrap();
            let bytes = std::fs::read(f.path.join("log/committed.chain_block")).unwrap();
            assert!(
                std::str::from_utf8(&bytes[32..])
                    .unwrap()
                    .contains(&format!("\"{path}\""))
            );
            let records = read_records(&Fixture::open_log(&f.path), id(2))
                .await
                .unwrap();
            let expected: pathlink::PathBuf = path.parse().unwrap();
            assert!(matches!(&records[0], MutationRecord::Put(actual, ..) if actual == &expected));
            assert!(
                matches!(&records[1], MutationRecord::Delete(actual, ..) if actual == &expected)
            );
        }
        for path in ["/trailing/", "/double//slash"] {
            let encoded = destream_json::encode((
                path.to_string(),
                tc_ir::Scalar::default(),
                None::<tc_ir::Scalar>,
            ))
            .unwrap();
            let decoded: Result<MutationRecord, _> = destream_json::try_decode((), encoded).await;
            assert!(decoded.is_err());
        }
        for json in ["[]", "[null]", "[null,[],null]", "[7,null,[]]", "[null,{}]"] {
            let stream = futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from(json))]);
            let decoded: Result<(Option<TxnId>, ChainBlock), _> =
                destream_json::try_decode((), stream).await;
            assert!(decoded.is_err(), "{json}");
        }
    });
}

#[test]
fn mutation_record_arity_is_unambiguous() {
    run(|| async {
        use crate::storage::MutationRecord;
        for (json, valid, put) in [
            (r#"["/",null]"#, true, false),
            (r#"["/nested",null,null]"#, true, true),
            ("[]", false, false),
            (r#"["/"]"#, false, false),
            (r#"["/",null,null,null]"#, false, false),
        ] {
            let stream = futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from(json))]);
            let decoded: Result<MutationRecord, _> = destream_json::try_decode((), stream).await;
            assert_eq!(decoded.is_ok(), valid, "{json}");
            if let Ok(record) = decoded {
                assert_eq!(matches!(record, MutationRecord::Put(..)), put);
            }
        }
    });
}

#[test]
fn shared_blocks_preserve_order_and_reject_duplicate_ids() {
    run(|| async {
        use crate::storage::{ChainBlock, MutationRecord};
        use futures::TryStreamExt;
        for size in [0, 31, 32, 33] {
            let block = (
                bytes::Bytes::from(vec![7; size]),
                std::collections::BTreeMap::from([
                    (
                        id(10),
                        vec![MutationRecord::Delete(
                            "/delete".parse().unwrap(),
                            tc_ir::Scalar::default(),
                        )],
                    ),
                    (
                        id(2),
                        vec![
                            MutationRecord::Put(
                                "/insert".parse().unwrap(),
                                tc_ir::Scalar::default(),
                                tc_ir::Scalar::default(),
                            ),
                            MutationRecord::Delete("/".parse().unwrap(), tc_ir::Scalar::default()),
                        ],
                    ),
                ]),
            );
            let decoded: Result<ChainBlock, _> =
                destream_json::try_decode((), destream_json::encode(block).unwrap()).await;
            assert_eq!(decoded.is_ok(), size == 32);
            if let Ok(block) = decoded {
                assert_eq!(block.previous_hash, tc_ir::Sha256Hash::from([7; 32]));
                assert_eq!(
                    block.mutations.keys().copied().collect::<Vec<_>>(),
                    vec![id(2), id(10)]
                );
                assert!(matches!(
                    block.mutations[&id(2)][0],
                    MutationRecord::Put(..)
                ));
                assert!(matches!(
                    block.mutations[&id(2)][1],
                    MutationRecord::Delete(..)
                ));
            }
        }

        let duplicate = destream::en::MapStream::from(futures::stream::iter([
            (format!("0{}", id(2)), Vec::<MutationRecord>::new()),
            (id(2).to_string(), Vec::<MutationRecord>::new()),
        ]));
        let encoded = destream_json::encode((bytes::Bytes::from_static(&[0; 32]), duplicate))
            .unwrap()
            .map_err(std::io::Error::other);
        let decoded: Result<ChainBlock, _> = destream_json::try_decode((), encoded).await;
        assert!(
            decoded
                .err()
                .unwrap()
                .to_string()
                .contains("duplicate block transaction ID")
        );
    });
}

#[test]
fn borrowed_history_encoding_retains_references_without_cloning_the_tree() {
    run(|| async {
        use crate::storage::{ChainBlock, MutationRecord};
        use futures::TryStreamExt;
        let bytes: Arc<[u8]> = vec![42; 200_000].into();
        let block = ChainBlock {
            previous_hash: tc_ir::Sha256Hash::default(),
            mutations: std::collections::BTreeMap::from([(
                id(2),
                vec![
                    MutationRecord::Put(
                        "/insert".parse().unwrap(),
                        tc_ir::Scalar::default(),
                        Value::Bytes(bytes.clone()).into(),
                    ),
                    MutationRecord::Put(
                        "/".parse().unwrap(),
                        tc_ir::Scalar::default(),
                        tc_ir::Scalar::default(),
                    ),
                    MutationRecord::Delete("/".parse().unwrap(), tc_ir::Scalar::default()),
                ],
            )]),
        };
        let stream = destream_json::encode(&block).unwrap();
        assert_eq!(
            Arc::strong_count(&bytes),
            2,
            "constructing the stream must not clone the block"
        );
        let borrowed = stream
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
            .unwrap();
        assert_eq!(Arc::strong_count(&bytes), 2);
        let owned = destream_json::encode(block)
            .unwrap()
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
            .unwrap();
        assert_eq!(borrowed, owned);
        assert_eq!(Arc::strong_count(&bytes), 1);
    });
}

#[test]
fn sync_rejects_blocks_outside_its_publication_contract() {
    run(|| async {
        for case in 0..3 {
            let f = Fixture::new();
            let chain = f.chain(false).await;
            let mut committed = ChainFile::default();
            committed.block.mutations.insert(
                id(2),
                vec![crate::MutationRecord::Delete(
                    "/".parse().unwrap(),
                    tc_ir::Scalar::default(),
                )],
            );
            match case {
                0 => committed.block.previous_hash = [1; 32].into(),
                1 => committed.frontier = Some(id(2)),
                2 => committed.block.mutations.get_mut(&id(2)).unwrap().clear(),
                _ => unreachable!(),
            }
            write_log(&f.log, committed).await;
            drop(chain);
            let path = f.path.join("log/committed.chain_block");
            let original = std::fs::read(&path).unwrap();
            assert!(
                SyncChain::load(
                    f.canonical(true).unwrap(),
                    Fixture::open_log(&f.path),
                    Fixture::open_values(&f.path),
                    TxnTaskQueue::new(64),
                    |_| async { panic!("invalid block must fail before replay") }
                )
                .await
                .is_err()
            );
            assert_eq!(std::fs::read(path).unwrap(), original);
        }
    });
}

#[test]
fn large_payload_and_history_round_trip_without_fixed_quotas() {
    run(|| async {
        let f = Fixture::new();
        let value = "x".repeat(200_000);
        let mut committed = ChainFile::default();
        committed.block.mutations = (0..16384)
            .map(|n| {
                (
                    id(n),
                    vec![crate::MutationRecord::Delete(
                        pathlink::PathBuf::new(),
                        tc_ir::Scalar::default(),
                    )],
                )
            })
            .collect();
        committed.block.mutations.insert(
            id(2),
            vec![crate::MutationRecord::Put(
                pathlink::PathBuf::new(),
                tc_ir::Scalar::default(),
                Value::from(value.clone()).into(),
            )],
        );
        write_log(&f.log, committed).await;
        assert!(
            std::fs::metadata(f.path.join("log/committed.chain_block"))
                .unwrap()
                .len()
                > 1024 * 1024
        );
        let mut recovered = read_log(&Fixture::open_log(&f.path)).await.unwrap();
        assert_eq!(recovered.block.mutations.len(), 16384);
        let mut records = recovered.block.mutations.remove(&id(2)).unwrap();
        let crate::storage::MutationRecord::Put(_, _, tc_ir::Scalar::Value(Value::String(actual))) =
            records.remove(0)
        else {
            panic!("expected string");
        };
        assert_eq!(actual, value);
    });
}

#[test]
#[ignore = "filesystem timing benchmark; run explicitly with --ignored --nocapture"]
fn wal_batch_benchmark() {
    run(|| async {
        for (batch, finalize) in [(true, false), (false, false), (false, true)] {
            let f = Fixture::new();
            let chain = f.chain(true).await;
            let start = std::time::Instant::now();
            let path = f.path.join("log/committed.chain_block");
            let mut published_bytes = std::fs::metadata(&path).unwrap().len();
            for n in 0..32_u16 {
                let txn = f.txn(if batch { 2 } else { n + 2 });
                insert(&chain, &txn, true, n as u64).await.unwrap();
                if !batch {
                    chain.commit(txn.id()).await.unwrap();
                    published_bytes += std::fs::metadata(&path).unwrap().len();
                    if finalize {
                        chain.finalize(&txn.id()).await.unwrap();
                        published_bytes += std::fs::metadata(&path).unwrap().len();
                    }
                }
            }
            if batch {
                chain.commit(id(2)).await.unwrap();
                published_bytes += std::fs::metadata(&path).unwrap().len();
            }
            eprintln!(
                "32 scalar writes: batched={batch}, finalize={finalize}, elapsed={:?}, WAL files={}, retained bytes={}, published bytes={published_bytes}",
                start.elapsed(),
                f.log.read().await.len(),
                std::fs::metadata(path).unwrap().len()
            );
        }
    });
}

#[test]
fn cumulative_history_saturates_and_finalization_recovers_capacity() {
    run(|| async {
        let f = Fixture::new();
        let small_log = || {
            Cache::<ChainFile>::new(1024, None, 0, Duration::from_secs(3))
                .load(f.path.join("log"))
                .unwrap()
        };
        let chain = SyncChain::create(
            f.canonical(false).unwrap(),
            small_log(),
            f.values.clone(),
            TxnTaskQueue::new(64),
        )
        .await
        .unwrap();
        let mut failed = None;
        for n in 2..64 {
            insert(&chain, &f.txn(n), false, n as u64).await.unwrap();
            if chain.commit(id(n)).await.is_err() {
                failed = Some(n);
                break;
            }
        }
        let failed = failed.expect("retained history must reach the cache bound");
        assert!(failed > 2, "individual transactions fit");
        let before = read_log(&Fixture::open_log(&f.path)).await.unwrap();
        assert_eq!(before.block.mutations.len(), (failed - 2) as usize);
        assert!(chain.register(f.txn(100)).is_err());
        drop(chain);

        let chain = SyncChain::load(
            f.canonical(true).unwrap(),
            small_log(),
            Fixture::open_values(&f.path),
            TxnTaskQueue::new(64),
            |original| {
                let mut txn = f.txn(1);
                txn.id = original;
                async move { Ok(txn) }
            },
        )
        .await
        .unwrap();
        chain.finalize(&id(failed - 1)).await.unwrap();
        insert(&chain, &f.txn(failed), false, 100).await.unwrap();
        chain.commit(id(failed)).await.unwrap();
        drop(chain);
        assert_eq!(
            count(&f.reopen().await.unwrap(), &f.txn(100)).await,
            (failed - 1) as u64
        );
    });
}

#[test]
fn storage_pressure_prevents_commit_without_discarding_evidence() {
    run(|| async {
        let mut f = Fixture::new();
        f.log = Cache::<ChainFile>::new(1024, None, 0, Duration::from_secs(3))
            .load(f.path.join("log"))
            .unwrap();
        let chain = SyncChain::create(
            f.canonical(false).unwrap(),
            f.log.clone(),
            f.values.clone(),
            TxnTaskQueue::new(128),
        )
        .await
        .unwrap();
        for key in 0..128 {
            insert(&chain, &f.txn(2), false, key).await.unwrap();
        }
        assert!(chain.commit(id(2)).await.is_err());
        assert!(chain.commit(id(2)).await.is_err());
        assert!(read_log(&f.log).await.unwrap().block.mutations.is_empty());
        assert_eq!(
            f.log.read().await.len(),
            1,
            "storage rejection preserves the committed file"
        );
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(3)).await, 0);
    });
}

#[test]
fn collection_views_are_not_persistent_subjects() {
    run(|| async {
        let f = Fixture::new();
        let Collection::BTree(mut view) = f.source(false).await else {
            unreachable!()
        };
        view.reverse = true;
        assert!(
            SyncChain::create(
                Collection::BTree(view.clone()),
                f.log.clone(),
                f.values.clone(),
                TxnTaskQueue::new(64)
            )
            .await
            .is_err()
        );
        let chain = f.chain(false).await;
        drop(chain);
        assert!(
            SyncChain::load(
                Collection::BTree(view),
                Fixture::open_log(&f.path),
                Fixture::open_values(&f.path),
                TxnTaskQueue::new(64),
                |_| async { Ok(f.txn(2)) }
            )
            .await
            .is_err()
        );
    });
}
