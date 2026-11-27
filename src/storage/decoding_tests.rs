use std::io;
use std::mem::size_of;
use std::sync::Arc;

use freqfs::{FileLoad, FileSave};
use futures::TryStreamExt;
use get_size::GetSize;
use pathlink::PathBuf;
use tc_ir::{Scalar, TxnId};
use tc_value::Value;

use super::{ChainFile, MutationRecord, StoredValue, transaction_name};

#[test]
#[ignore = "replay clone measurements; run in release mode with --ignored --nocapture"]
fn replay_record_clone_benchmark() {
    for payload_bytes in [64, 4096] {
        for count in [1, 128, 4096] {
            let records: Vec<_> = (0..count)
                .map(|_| {
                    MutationRecord::Put(
                        "/insert".parse().unwrap(),
                        Scalar::default(),
                        Box::new(Scalar::from(Value::String("x".repeat(payload_bytes))).into()),
                    )
                })
                .collect();

            let mut samples = Vec::new();
            for _ in 0..7 {
                let start = std::time::Instant::now();
                let replay = std::hint::black_box(records.as_slice()).to_vec();
                samples.push(start.elapsed().as_nanos());
                assert_eq!(replay.len(), count);
                assert_eq!(replay.get_size(), records.get_size());
                std::hint::black_box(replay);
            }

            samples.sort_unstable();
            println!(
                "PERSISTENCE_REPLAY,{count},{payload_bytes},native_bytes,{}",
                records.get_size()
            );

            println!(
                "PERSISTENCE_REPLAY,{count},{payload_bytes},clone_ns,{}",
                samples[3]
            );

            let id = TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 1);
            let file = ChainFile::Transaction {
                id,
                records: records.into(),
            };

            let mut snapshots = Vec::new();
            let mut invocations = Vec::new();
            for _ in 0..7 {
                let start = std::time::Instant::now();
                let replay = std::hint::black_box(&file).records(id).unwrap().clone();
                snapshots.push(start.elapsed().as_nanos());
                let start = std::time::Instant::now();
                for record in replay.iter().cloned() {
                    std::hint::black_box(record);
                }

                invocations.push(start.elapsed().as_nanos());
                assert!(Arc::ptr_eq(&replay, file.records(id).unwrap()));
            }

            snapshots.sort_unstable();
            invocations.sort_unstable();
            let maximum_record = file
                .records(id)
                .unwrap()
                .iter()
                .map(GetSize::get_size)
                .max()
                .unwrap();

            println!(
                "PERSISTENCE_REPLAY,{count},{payload_bytes},snapshot_ns,{}",
                snapshots[3]
            );

            println!(
                "PERSISTENCE_REPLAY,{count},{payload_bytes},invocations_ns,{}",
                invocations[3]
            );

            println!(
                "PERSISTENCE_REPLAY,{count},{payload_bytes},max_record_bytes,{maximum_record}"
            );
        }
    }
}

#[tokio::test]
async fn replay_snapshot_shares_payload_without_retaining_file_guard() {
    let directory = tempfile::tempdir().unwrap();
    let root =
        freqfs::Cache::<ChainFile>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3))
            .load(directory.path().to_path_buf())
            .unwrap();

    let id = TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 1);
    let records = Arc::new(vec![MutationRecord::Delete(
        PathBuf::new(),
        Scalar::default(),
    )]);

    let file = root
        .write()
        .await
        .create_file(
            transaction_name(id),
            ChainFile::Transaction {
                id,
                records: records.clone(),
            },
        )
        .await
        .unwrap();

    let snapshot = {
        file.read::<ChainFile>()
            .await
            .unwrap()
            .records(id)
            .unwrap()
            .clone()
    };

    assert!(Arc::ptr_eq(&records, &snapshot));
    {
        let _writer = file.try_write::<ChainFile>().unwrap();
        assert_eq!(snapshot.len(), 1);
    }

    drop(file);
    drop(root);
    assert_eq!(snapshot.len(), 1);
}

#[tokio::test]
#[ignore = "retained record measurements; run in release mode with --ignored --nocapture"]
async fn pending_record_retention_benchmark() {
    use txn_lock::queue::task::TaskQueue;

    for transactions in [1_u16, 16, 128] {
        let queues: Vec<_> = (0..2).map(|_| TaskQueue::new(128)).collect();
        for queue in &queues {
            for transaction in 0..transactions {
                let id = TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), transaction);
                for _ in 0..128 {
                    let mut task = queue.start(id).await.unwrap();
                    task.record_bounded(
                        MutationRecord::Put(
                            PathBuf::new(),
                            Scalar::default(),
                            Box::new(Scalar::from(Value::String("x".repeat(4096))).into()),
                        ),
                        32 * 1024 * 1024,
                    )
                    .unwrap();

                    task.complete().unwrap();
                }
            }
        }

        // Move, rather than clone, the admitted payloads out for native size
        // observation. Retain them together to preserve the fixture's peak.
        let mut retained = Vec::new();
        for queue in &queues {
            let mut operation = queue.operation().await.unwrap();
            for transaction in 0..transactions {
                let id = TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), transaction);
                retained.push(operation.commit(&id).unwrap().unwrap());
            }

            operation.complete();
        }

        let bytes: usize = retained.iter().map(GetSize::get_size).sum();
        println!("PERSISTENCE_PENDING,2,{transactions},128,4096,retained_bytes,{bytes}");
        std::hint::black_box(retained);
    }
}

#[tokio::test]
async fn admitted_batches_reopen_with_the_same_native_budget() {
    use txn_lock::queue::task::TaskQueue;

    for key in [
        Scalar::Tuple(vec![Value::from(1_u64).into()]),
        Scalar::Value(Value::Tuple(vec![
            Value::from("value"),
            Value::Link("/state/scalar/op/post".parse().unwrap()),
        ])),
    ] {
        let id = TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 1);
        let record = MutationRecord::Delete(PathBuf::new(), key.clone());
        let limit = size_of::<ChainFile>()
            + size_of::<Vec<MutationRecord>>()
            + 3 * size_of::<MutationRecord>()
            + record.get_heap_size();

        let queue = TaskQueue::new(16);
        let mut task = queue.start(id).await.unwrap();
        task.record_bounded(
            record,
            limit - size_of::<ChainFile>() - size_of::<Vec<MutationRecord>>(),
        )
        .unwrap();

        task.complete().unwrap();
        let mut operation = queue.operation().await.unwrap();
        let records = operation.commit(&id).unwrap().unwrap();
        operation.complete();
        let value = ChainFile::Transaction {
            id,
            records: records.into(),
        };

        assert!(value.get_size() <= limit);

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(transaction_name(id));
        // Establish the fixture before publishing its cache root.
        let mut file = tokio::fs::File::create(&path).await.unwrap();
        value.save(&mut file).await.unwrap();
        drop(file);
        let root =
            freqfs::Cache::<ChainFile>::new(limit, None, 0, std::time::Duration::from_secs(3))
                .load(directory.path().to_path_buf())
                .unwrap();

        let file = root
            .read()
            .await
            .get_file(&transaction_name(id))
            .unwrap()
            .clone();

        let decoded = file.read::<ChainFile>().await.unwrap();
        assert!(decoded.get_size() <= value.get_size());
        assert!(
            matches!(&decoded.records(id).unwrap()[0], MutationRecord::Delete(_, actual) if actual == &key)
        );
    }
}

#[tokio::test]
async fn decoding_preserves_containers_paths_and_payloads() {
    use tc_ir::{After, IdRef, Map, OpDef, OpRef, Subject, TCRef};

    let id = TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 1);
    let tuple = Scalar::Tuple(vec![
        Value::from("quoted \\\" / [ { ".repeat(1000)).into(),
        Value::Bytes(vec![7; 5000].into()).into(),
        Scalar::Tuple(vec![Scalar::default(); 17]),
    ]);

    let reference = Scalar::from(TCRef::After(Box::new(After::new(
        Scalar::from(TCRef::Op(OpRef::Get((
            Subject::Ref(
                IdRef::new("source".parse().unwrap()),
                "/a/b".parse().unwrap(),
            ),
            tuple.clone(),
        )))),
        tuple.clone(),
    ))));

    let map = Scalar::Map(Map::from_iter([
        ("tuple".parse().unwrap(), tuple),
        ("reference".parse().unwrap(), reference),
        (
            "operation".parse().unwrap(),
            Scalar::Op(OpDef::Post(
                (0..17)
                    .map(|n| (format!("binding{n}").parse().unwrap(), Scalar::default()))
                    .collect(),
            )),
        ),
    ]));

    for payload in [Scalar::default(), map] {
        for count in [1, 2, 3, 4, 5, 17] {
            let value = ChainFile::Transaction {
                id,
                records: (0..count)
                    .map(|_| {
                        MutationRecord::Put(
                            "/a/b/c/d/e/f/g/h/i/j/k".parse().unwrap(),
                            Scalar::default(),
                            Box::new(payload.clone().into()),
                        )
                    })
                    .collect::<Vec<_>>()
                    .into(),
            };

            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join(transaction_name(id));
            let mut file = tokio::fs::File::create(&path).await.unwrap();
            value.save(&mut file).await.unwrap();
            drop(file);
            let file = tokio::fs::File::open(&path).await.unwrap();
            let metadata = file.metadata().await.unwrap();
            let decoded = ChainFile::load(&path, file, metadata).await.unwrap();
            assert!(decoded.get_size() <= value.get_size());
            let records = decoded.records(id).unwrap();
            assert_eq!(records.len(), count);
            for record in records.iter() {
                assert!(matches!(record, MutationRecord::Put(path, key, value)
                    if path == &"/a/b/c/d/e/f/g/h/i/j/k".parse::<PathBuf>().unwrap() && *key == Scalar::default() && matches!(value.as_ref(), StoredValue::Scalar(value) if value == &payload)));
            }
        }
    }
}

#[tokio::test]
async fn invalid_and_previous_formats_fail_decoding() {
    for input in [
        "",
        "[",
        "[null,null,[",
        "[null,null,{}]",
        "[\"frontier\",null,null]",
        "[\"frontier\",null,null,0]",
        "[\"frontier\",null,null]garbage",
        "[\"unknown\",null,null]",
    ] {
        let result = destream_json::try_decode::<_, _, ChainFile>(
            (),
            futures::stream::iter([Ok::<_, io::Error>(bytes::Bytes::copy_from_slice(
                input.as_bytes(),
            ))]),
        )
        .await;

        assert!(result.is_err(), "{input:?}");
    }

    for (input, valid) in [
        (r#"["delete","/",null]"#, true),
        (r#"["put","/nested",null,["scalar",null]]"#, true),
        (r#"["put","/",null,null]"#, false),
        (r#"["delete","/trailing/",null]"#, false),
        (r#"["delete","/",null,null]"#, false),
        (r#"["/",null]"#, false),
        (
            r#"["restore",[0,"/state/collection/btree",[],"invalid"]]"#,
            false,
        ),
    ] {
        let result = destream_json::try_decode::<_, _, MutationRecord>(
            (),
            futures::stream::iter([Ok::<_, io::Error>(bytes::Bytes::copy_from_slice(
                input.as_bytes(),
            ))]),
        )
        .await;

        assert_eq!(result.is_ok(), valid, "{input:?}");
    }
}

#[tokio::test]
async fn borrowed_history_encoding_does_not_clone_payloads() {
    let bytes: Arc<[u8]> = vec![42; 200_000].into();
    let record = MutationRecord::Put(
        "/insert".parse().unwrap(),
        Scalar::default(),
        Box::new(Scalar::from(Value::Bytes(bytes.clone())).into()),
    );

    let stream = destream_json::encode(&record).unwrap();
    assert_eq!(Arc::strong_count(&bytes), 2);
    let borrowed = stream
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .unwrap();

    assert_eq!(Arc::strong_count(&bytes), 2);
    let owned = destream_json::encode(record)
        .unwrap()
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .unwrap();

    assert_eq!(borrowed, owned);
    assert_eq!(Arc::strong_count(&bytes), 1);
}

#[test]
fn retained_size_counts_record_and_scalar_capacity() {
    let mut payload = Vec::with_capacity(24);
    payload.push(Scalar::default());
    let payload_size = payload.capacity() * size_of::<Scalar>();
    let mut records = Vec::with_capacity(16);
    records.push(MutationRecord::Put(
        PathBuf::default(),
        Scalar::default(),
        Box::new(Scalar::Tuple(payload).into()),
    ));

    let retained =
        records.capacity() * size_of::<MutationRecord>() + size_of::<StoredValue>() + payload_size;

    let value = ChainFile::Transaction {
        id: TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 1),
        records: records.into(),
    };

    assert_eq!(
        value.get_size(),
        size_of::<ChainFile>() + size_of::<Vec<MutationRecord>>() + retained
    );
}
