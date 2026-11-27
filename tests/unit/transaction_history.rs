//! Ordinary-stack coverage for capture, persistence, replay, and cleanup.

use super::*;

#[tokio::test]
async fn cutoff_prepares_only_covered_identities_without_loading_later_payloads() {
    let f = Fixture::new();
    let chain = f.chain(false).await;
    for version in 2..18 {
        insert(&chain, &f.txn(version), false, u64::from(version))
            .await
            .unwrap();

        chain.commit(id(version)).await.unwrap();
    }

    let later = f
        .log
        .read()
        .await
        .get_file(&crate::storage::transaction_name(id(17)))
        .unwrap()
        .clone();

    let _guard = later.write::<ChainFile>().await.unwrap();
    let store = store(&f.log, f.values.clone());
    assert_eq!(store.transactions_through(id(1)).await.unwrap(), vec![]);
    assert_eq!(
        store.transactions_through(id(2)).await.unwrap(),
        vec![id(2)]
    );

    tokio::time::timeout(Duration::from_secs(1), chain.finalize(&id(2)))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(f.log.read().await.len(), 16);
}

#[tokio::test]
async fn capture_waits_without_blocking_the_commit_it_needs() {
    for table in [false, true] {
        let f = Fixture::new();
        let chain = f.chain(table).await;
        insert(&chain, &f.txn(2), table, 1).await.unwrap();
        let txn = f.txn(3);
        let view = Public::<State<Txn>>::get(&chain, &txn, &[], Scalar::default())
            .await
            .unwrap();

        let State::Collection(source) = view else {
            panic!("collection view")
        };

        let mut capture = Box::pin(chain.restore_from(&txn, &source));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut capture)
                .await
                .is_err()
        );

        assert!(chain.commit(id(3)).await.is_err());
        assert!(chain.finalize(&id(3)).await.is_err());
        tokio::time::timeout(Duration::from_secs(1), chain.commit(id(2)))
            .await
            .unwrap()
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), capture)
            .await
            .unwrap()
            .unwrap();

        chain.commit(id(3)).await.unwrap();
        chain.finalize(&id(3)).await.unwrap();
        assert!(f.values.read().await.is_empty());
        drop(chain);
        assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 1);
    }
}

#[tokio::test]
async fn captures_are_independent_and_cleanup_is_live() {
    for table in [false, true] {
        let f = Fixture::new();
        let chain = f.chain(table).await;
        let source = f.source(table).await;
        chain.restore_from(&f.txn(2), &source).await.unwrap();
        chain.commit(id(2)).await.unwrap();
        chain.restore_from(&f.txn(3), &source).await.unwrap();
        assert_eq!(f.values.read().await.len(), 2);
        chain.rollback(&id(3)).await.unwrap();
        assert_eq!(f.values.read().await.len(), 1);
        chain.restore_from(&f.txn(4), &source).await.unwrap();
        assert_eq!(f.values.read().await.len(), 2);
        chain.finalize(&id(3)).await.unwrap();
        assert_eq!(f.values.read().await.len(), 1);
        assert!(f.values.read().await.get_dir(&id(4).to_string()).is_some());
        chain.commit(id(4)).await.unwrap();
        chain.finalize(&id(4)).await.unwrap();
        assert!(f.values.read().await.is_empty());
        assert_eq!(f.log.read().await.len(), 1);
    }
}

#[tokio::test]
async fn capture_materializes_directly_into_transaction_storage() {
    let f = Fixture::new();
    let source = f.source(false).await;
    let workspace_entries = f.txn.root.read().await.len();
    let store = store(&f.log, f.values.clone());
    let (record, accepted) = store.capture_collection(&f.txn(2), source).await.unwrap();
    assert_eq!(f.txn.root.read().await.len(), workspace_entries);
    let transaction = f
        .values
        .read()
        .await
        .get_dir(&id(2).to_string())
        .cloned()
        .unwrap();

    assert_eq!(transaction.read().await.len(), 1);
    assert_eq!(
        accepted.hash(id(2)).await.unwrap(),
        store
            .resolve_collection(id(2), &record)
            .await
            .unwrap()
            .hash(id(2))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn resources_in_one_transaction_have_independent_capture_storage() {
    let first = Fixture::new();
    let second = Fixture::new();
    let a = first.chain(false).await;
    let b = second.chain(false).await;
    let snapshot = first.source(false).await;
    snapshot
        .put(
            &first.txn(1),
            &["insert".parse().unwrap()],
            Scalar::default(),
            State::from(Value::Tuple(vec![9_u64.into()])),
        )
        .await
        .unwrap();

    snapshot.commit(id(1)).await.unwrap();
    insert(&a, &first.txn(3), false, 2).await.unwrap();
    a.restore_from(&first.txn(3), &snapshot).await.unwrap();
    b.restore_from(&second.txn(3), &second.source(false).await)
        .await
        .unwrap();

    a.commit(id(3)).await.unwrap();
    b.commit(id(3)).await.unwrap();
    assert_eq!(read_records(&first.log, id(3)).await.unwrap().len(), 2);
    assert_eq!(read_records(&second.log, id(3)).await.unwrap().len(), 1);
    drop(a);
    drop(b);
    assert_eq!(
        count(&first.reopen().await.unwrap(), &first.txn(4)).await,
        1
    );

    assert_eq!(
        count(&second.reopen().await.unwrap(), &second.txn(4)).await,
        0
    );
}

#[tokio::test]
async fn committing_a_new_transaction_does_not_access_older_payloads() {
    let f = Fixture::new();
    let chain = f.chain(false).await;
    insert(&chain, &f.txn(2), false, 1).await.unwrap();
    chain.commit(id(2)).await.unwrap();
    let older = f
        .log
        .read()
        .await
        .get_file(&crate::storage::transaction_name(id(2)))
        .cloned()
        .unwrap();

    let guard = older.write::<ChainFile>().await.unwrap();
    insert(&chain, &f.txn(3), false, 2).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), chain.commit(id(3)))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(guard.records(id(2)).unwrap().len(), 1);
    drop(guard);
    drop(chain);
    assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(4)).await, 2);
}

#[tokio::test]
async fn small_cache_admits_independent_history_files() {
    let f = Fixture::new();
    let root = Cache::<ChainFile>::new(8192, None, 0, Duration::from_secs(3))
        .load(f.path.join("log"))
        .unwrap();

    let chain = SyncChain::create(
        f.canonical(false).await.unwrap(),
        root.clone(),
        f.values.clone(),
        64,
    )
    .await
    .unwrap();

    for n in 2..64 {
        insert(&chain, &f.txn(n), false, u64::from(n))
            .await
            .unwrap();

        chain.commit(id(n)).await.unwrap();
    }

    assert_eq!(root.read().await.len(), 63);
    chain.finalize(&id(63)).await.unwrap();
    assert_eq!(root.read().await.len(), 1);
    drop(chain);
    assert_eq!(count(&f.reopen().await.unwrap(), &f.txn(64)).await, 62);
}

#[tokio::test]
async fn finalized_residue_is_deleted_without_loading_its_payload() {
    let f = Fixture::new();
    let chain = f.chain(false).await;
    insert(&chain, &f.txn(2), false, 1).await.unwrap();
    chain.commit(id(2)).await.unwrap();
    chain.finalize(&id(2)).await.unwrap();
    drop(chain);
    // Simulate a crash after durable frontier publication but before deletion.
    let obsolete = f
        .path
        .join("log")
        .join(crate::storage::transaction_name(id(2)));

    std::fs::write(&obsolete, b"obsolete incomplete payload").unwrap();
    let orphan = f.path.join("values").join(id(2).to_string());
    std::fs::create_dir(&orphan).unwrap();
    let reopened = f.reopen().await.unwrap();
    assert_eq!(count(&reopened, &f.txn(3)).await, 1);
    assert!(!obsolete.exists());
    assert!(!orphan.exists());
}

#[tokio::test]
async fn missing_corrupt_and_misbound_captures_fail_recovery() {
    for fault in 0..3 {
        let f = Fixture::new();
        let chain = f.chain(false).await;
        let source = f.source(false).await;
        let store = store(&f.log, f.values.clone());
        let (record, captured) = store
            .capture_collection(&f.txn(if fault == 2 { 3 } else { 2 }), source)
            .await
            .unwrap();

        if fault == 1 {
            captured
                .put(
                    &f.txn(2),
                    &["insert".parse().unwrap()],
                    Scalar::default(),
                    State::from(Value::Tuple(vec![1_u64.into()])),
                )
                .await
                .unwrap();

            captured.commit(id(2)).await.unwrap();
            captured.finalize(&id(2)).await.unwrap();
        }

        let records = vec![crate::MutationRecord::Restore(record)];
        store.sync_captures(&records).await.unwrap();

        if fault == 0 {
            store.remove_captures(id(2)).await.unwrap();
        }

        store.publish_transaction(id(2), records).await.unwrap();

        drop(chain);
        let result = SyncChain::<Txn>::load::<State<_>, _, _, _, _>(
            || f.canonical(true),
            Fixture::open_log(&f.path),
            Fixture::open_values(&f.path),
            64,
            |original| {
                assert_eq!(original, id(2));
                let txn = f.txn(2);
                async move { Ok(txn) }
            },
        )
        .await;

        assert!(result.is_err());
    }
}

#[tokio::test]
async fn interrupted_capture_is_reclaimed_after_successful_recovery() {
    let f = Fixture::new();
    let chain = f.chain(false).await;
    let txn = f.txn(2);
    let source_dir = f.working().await;
    let source = Fixture::subject(source_dir.clone(), false, false)
        .await
        .unwrap();

    let base = source_dir.read().await.get_dir("base").cloned().unwrap();
    let guard = base.write().await;
    let mut capture = Box::pin(chain.restore_from(&txn, &source));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut capture)
            .await
            .is_err()
    );

    drop(capture);
    drop(guard);
    assert!(chain.commit(id(2)).await.is_err());
    assert!(!f.values.read().await.is_empty());
    drop(chain);
    let chain = f.reopen().await.unwrap();
    assert!(!f.path.join("values").join(id(2).to_string()).exists());
    assert_eq!(count(&chain, &f.txn(3)).await, 0);
}

#[tokio::test]
async fn late_recovery_failure_preserves_canonical_storage_and_all_evidence() {
    for fault in 0..3 {
        let corrupt_capture = fault == 1;
        let f = Fixture::new();
        let chain = f.chain(false).await;
        insert(&chain, &f.txn(2), false, 1).await.unwrap();
        chain.commit(id(2)).await.unwrap();
        let initial_hash = chain.hash(&f.txn(2)).await.unwrap();
        chain
            .restore_from(&f.txn(3), &f.source(false).await)
            .await
            .unwrap();

        chain.publish_before_subject_commit(id(3)).await;
        drop(chain);

        let orphan = f
            .values
            .write()
            .await
            .get_or_create_dir(id(9).to_string())
            .unwrap();

        orphan.sync_all().await.unwrap();
        let late_file = f
            .path
            .join("log")
            .join(crate::storage::transaction_name(id(3)));

        let original = std::fs::read(&late_file).unwrap();
        let capture_path = if corrupt_capture {
            let transaction = f
                .values
                .read()
                .await
                .get_dir(&id(3).to_string())
                .cloned()
                .unwrap();

            let name = transaction.read().await.names().next().unwrap().to_string();
            let path = f.path.join("values").join(id(3).to_string()).join(name);
            let hidden = f.path.join("held-capture");
            std::fs::rename(&path, &hidden).unwrap();
            Some((path, hidden))
        } else if fault == 0 {
            let mut corrupt = original.clone();
            corrupt[0] ^= 1;
            std::fs::write(&late_file, corrupt).unwrap();
            None
        } else {
            None
        };

        let evidence = std::fs::read(&late_file).unwrap();
        let opened = SyncChain::open(
            || f.canonical(true),
            Fixture::open_log(&f.path),
            Fixture::open_values(&f.path),
            64,
        )
        .await
        .unwrap();

        let replayed = std::sync::Mutex::new(Vec::new());
        let replay = Txn {
            root: f.working().await,
            path: vec![],
            ..f.txn(2)
        };

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            opened.recover::<State<_>, _, _>(|original| {
                replayed.lock().unwrap().push(original);
                let mut txn = replay.clone();
                txn.id = original;
                async move {
                    if fault == 2 && original == id(3) {
                        futures::future::pending::<()>().await;
                    }

                    Ok(txn)
                }
            }),
        )
        .await;

        if fault == 2 {
            assert!(result.is_err(), "cancel the pending recovery capability");
        } else {
            assert!(result.unwrap().is_err());
        }

        assert_eq!(
            *replayed.lock().unwrap(),
            if fault != 0 { vec![id(3)] } else { vec![] }
        );

        assert!(opened.hash(&f.txn(4)).await.is_err());
        assert_eq!(
            f.canonical(true).await.unwrap().hash(id(4)).await.unwrap(),
            initial_hash
        );

        assert_eq!(std::fs::read(&late_file).unwrap(), evidence);
        assert!(
            f.path
                .join("log")
                .join(crate::storage::transaction_name(id(2)))
                .exists()
        );

        assert!(f.path.join("values").join(id(9).to_string()).exists());
        drop(opened);

        if let Some((path, hidden)) = capture_path {
            std::fs::rename(hidden, path).unwrap();
        } else {
            std::fs::write(&late_file, original).unwrap();
        }

        let replay = Txn {
            root: f.working().await,
            path: vec![],
            ..f.txn(2)
        };

        let recovered = SyncChain::load::<State<_>, _, _, _, _>(
            || f.canonical(true),
            Fixture::open_log(&f.path),
            Fixture::open_values(&f.path),
            64,
            |original| {
                let mut txn = replay.clone();
                txn.id = original;
                async move { Ok(txn) }
            },
        )
        .await
        .unwrap();

        assert_eq!(count(&recovered, &f.txn(4)).await, 0);
        assert!(!f.path.join("values").join(id(9).to_string()).exists());
    }
}

#[test]
fn tensor_capture_replay_and_cutoff() {
    super::tensor_values_replay_with_original_transaction_ids_and_materialize_at_cutoff();
}

#[test]
fn native_restoration_roundtrip() {
    super::native_restoration_is_transactional_and_recoverable();
}

#[tokio::test]
async fn native_recovery_preserves_full_transaction_order() {
    let f = Fixture::new();
    let chain = f.chain(true).await;
    insert(&chain, &f.txn(2), true, 1).await.unwrap();
    chain.commit(id(2)).await.unwrap();
    chain
        .put(
            &f.txn(10),
            &[],
            Value::Tuple(vec![1_u64.into()]).into(),
            State::from(Value::Tuple(vec![99_u64.into()])),
        )
        .await
        .unwrap();

    chain.commit(id(10)).await.unwrap();
    let expected = chain.hash(&f.txn(11)).await.unwrap();
    drop(chain);
    let mut replayed = Vec::new();
    let chain = SyncChain::load::<State<_>, _, _, _, _>(
        || f.canonical(true),
        Fixture::open_log(&f.path),
        Fixture::open_values(&f.path),
        64,
        |original| {
            replayed.push(original);
            let mut txn = f.txn(2);
            txn.id = original;
            async move { Ok(txn) }
        },
    )
    .await
    .unwrap();

    assert!(replayed.is_empty());
    assert_eq!(chain.hash(&f.txn(11)).await.unwrap(), expected);
}

#[tokio::test]
async fn interrupted_cleanup_requires_reopening_and_preserves_the_frontier() {
    for finalize in [false, true] {
        let f = Fixture::new();
        let chain = f.chain(false).await;
        chain
            .restore_from(&f.txn(2), &f.source(false).await)
            .await
            .unwrap();

        if finalize {
            chain.commit(id(2)).await.unwrap();
        }

        let guard = f.values.write().await;
        let cutoff = id(2);
        let mut cleanup = Box::pin(async {
            if finalize {
                chain.finalize(&cutoff).await
            } else {
                chain.rollback(&cutoff).await
            }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut cleanup)
                .await
                .is_err()
        );

        assert_eq!(
            log_file(&f.log)
                .await
                .read::<ChainFile>()
                .await
                .unwrap()
                .frontier()
                .unwrap(),
            finalize.then_some(cutoff)
        );

        drop(cleanup);
        drop(guard);
        assert!(chain.hash(&f.txn(3)).await.is_err());
        drop(chain);
        let chain = f.reopen().await.unwrap();
        assert_eq!(count(&chain, &f.txn(3)).await, 0);
        assert!(!f.path.join("values").join(cutoff.to_string()).exists());
    }
}

#[tokio::test]
async fn conflicting_transaction_file_is_not_overwritten() {
    let f = Fixture::new();
    let chain = f.chain(false).await;
    write_records(
        &f.log,
        id(2),
        vec![crate::MutationRecord::Delete(
            "/".parse().unwrap(),
            Scalar::default(),
        )],
    )
    .await
    .unwrap();

    let path = f
        .path
        .join("log")
        .join(crate::storage::transaction_name(id(2)));

    let before = std::fs::read(&path).unwrap();
    insert(&chain, &f.txn(2), false, 1).await.unwrap();
    assert!(chain.commit(id(2)).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(chain.hash(&f.txn(3)).await.is_err());
}
