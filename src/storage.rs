//! Typed transaction history and native captures. Encoding stays at the file boundary.

use std::collections::BTreeMap;
use std::io;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::Path;
use std::sync::Arc;

use destream::{IntoStream, de, en};
use freqfs::{Dir, DirLock, FileLoad, FileLock, FileSave};
use futures::TryStreamExt;
use pathlink::PathBuf;
use safecast::{AsType, TryCastInto};
use sha2::{Digest, Sha256};
use tc_collection::collection::CollectionSchema;
use tc_collection::{Collection, CollectionState, CopyFrom, StorageContext};
use tc_error::{TCError, TCResult};
use tc_ir::{Id, Scalar, Sha256Hash, TxnId};
use tc_value::Value;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

#[cfg(test)]
mod decoding_tests;

pub(crate) const COMMITTED: &str = "committed.chain_block";

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

pub(crate) fn transaction_name(id: TxnId) -> String {
    format!("{id}.chain_block")
}

fn transaction_id(name: &str) -> TCResult<TxnId> {
    let id: TxnId = name
        .strip_suffix(".chain_block")
        .ok_or_else(|| TCError::bad_request("invalid Chain transaction filename"))?
        .parse()
        .map_err(TCError::bad_request)?;

    if transaction_name(id) != name {
        return Err(TCError::bad_request(
            "noncanonical Chain transaction filename",
        ));
    }

    Ok(id)
}

/// Description of an immutable, transaction-owned native collection capture.
#[derive(Clone)]
pub struct CapturedCollection {
    id: Id,
    class: PathBuf,
    schema: Value,
    hash: Sha256Hash,
}

impl CapturedCollection {
    fn schema(&self) -> TCResult<CollectionSchema> {
        CollectionSchema::try_from((self.class.clone(), self.schema.clone())).map_err(|error| {
            TCError::bad_request("invalid captured collection schema").consume(error)
        })
    }
}

impl<'en> en::ToStream<'en> for CapturedCollection {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        (
            &self.id,
            &self.class,
            &self.schema,
            bytes::Bytes::copy_from_slice(&self.hash),
        )
            .into_stream(encoder)
    }
}

impl<'en> en::IntoStream<'en> for CapturedCollection {
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        (
            self.id,
            self.class,
            self.schema,
            bytes::Bytes::copy_from_slice(&self.hash),
        )
            .into_stream(encoder)
    }
}

impl de::FromStream for CapturedCollection {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        let (id, class, schema, hash) =
            <(Id, PathBuf, Value, bytes::Bytes)>::from_stream((), decoder).await?;

        let hash = <[u8; 32]>::try_from(hash.as_ref()).map_err(de::Error::custom)?;
        let value = Self {
            id,
            class,
            schema,
            hash: hash.into(),
        };

        value.schema().map_err(de::Error::custom)?;
        Ok(value)
    }
}

/// Native scalar data or a reference to a transaction-owned capture.
#[derive(Clone)]
pub enum StoredValue {
    Scalar(Scalar),
    Collection(CapturedCollection),
}

impl From<Scalar> for StoredValue {
    fn from(value: Scalar) -> Self {
        Self::Scalar(value)
    }
}

impl<'en> en::ToStream<'en> for StoredValue {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Scalar(value) => ("scalar", value).into_stream(encoder),
            Self::Collection(value) => ("collection", value).into_stream(encoder),
        }
    }
}

impl<'en> en::IntoStream<'en> for StoredValue {
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Scalar(value) => ("scalar", value).into_stream(encoder),
            Self::Collection(value) => ("collection", value).into_stream(encoder),
        }
    }
}

impl de::FromStream for StoredValue {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor for Visitor {
            type Value = StoredValue;

            fn expecting() -> &'static str {
                "a tagged stored value"
            }

            async fn visit_seq<A: de::SeqAccess>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let tag: String = required(&mut seq).await?;
                let value = match tag.as_str() {
                    "scalar" => StoredValue::Scalar(required(&mut seq).await?),
                    "collection" => StoredValue::Collection(required(&mut seq).await?),
                    _ => return Err(de::Error::custom("invalid stored value tag")),
                };

                end(&mut seq).await?;
                Ok(value)
            }
        }

        decoder.decode_seq(Visitor).await
    }
}

async fn required<T: de::FromStream<Context = ()>, A: de::SeqAccess>(
    seq: &mut A,
) -> Result<T, A::Error> {
    seq.next_element(())
        .await?
        .ok_or_else(|| de::Error::custom("missing Chain record field"))
}

async fn end<A: de::SeqAccess>(seq: &mut A) -> Result<(), A::Error> {
    if seq.next_element::<de::IgnoredAny>(()).await?.is_some() {
        return Err(de::Error::custom("unexpected Chain record field"));
    }

    Ok(())
}

/// A persisted request, never a second invocation interface.
#[derive(Clone)]
pub enum MutationRecord {
    Restore(CapturedCollection),
    // Keep PUT payloads out of every retained record's inline allocation.
    Put(PathBuf, Scalar, Box<StoredValue>),
    Delete(PathBuf, Scalar),
}

impl<'en> en::ToStream<'en> for MutationRecord {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Restore(value) => ("restore", value).into_stream(encoder),
            Self::Put(path, key, value) => ("put", path, key, value).into_stream(encoder),
            Self::Delete(path, key) => ("delete", path, key).into_stream(encoder),
        }
    }
}

impl<'en> en::IntoStream<'en> for MutationRecord {
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Restore(value) => ("restore", value).into_stream(encoder),
            Self::Put(path, key, value) => ("put", path, key, value).into_stream(encoder),
            Self::Delete(path, key) => ("delete", path, key).into_stream(encoder),
        }
    }
}

impl de::FromStream for MutationRecord {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor for Visitor {
            type Value = MutationRecord;

            fn expecting() -> &'static str {
                "a tagged Chain mutation"
            }

            async fn visit_seq<A: de::SeqAccess>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let tag: String = required(&mut seq).await?;
                let record = match tag.as_str() {
                    "restore" => MutationRecord::Restore(required(&mut seq).await?),
                    "put" => MutationRecord::Put(
                        required(&mut seq).await?,
                        required(&mut seq).await?,
                        Box::new(required(&mut seq).await?),
                    ),
                    "delete" => {
                        MutationRecord::Delete(required(&mut seq).await?, required(&mut seq).await?)
                    }
                    _ => return Err(de::Error::custom("invalid Chain mutation tag")),
                };

                end(&mut seq).await?;
                Ok(record)
            }
        }

        decoder.decode_seq(Visitor).await
    }
}

/// The frontier is replaced atomically; transaction files are immutable.
#[derive(Clone)]
pub enum ChainFile {
    Frontier {
        finalized: Option<TxnId>,
    },
    Transaction {
        id: TxnId,
        records: Arc<Vec<MutationRecord>>,
    },
}

impl Default for ChainFile {
    fn default() -> Self {
        Self::Frontier { finalized: None }
    }
}

impl ChainFile {
    pub(crate) fn frontier(&self) -> TCResult<Option<TxnId>> {
        match self {
            Self::Frontier { finalized } => Ok(*finalized),
            _ => Err(TCError::bad_request("expected Chain frontier file")),
        }
    }

    pub(crate) fn records(&self, expected: TxnId) -> TCResult<&Arc<Vec<MutationRecord>>> {
        match self {
            Self::Transaction { id, records } if *id == expected && !records.is_empty() => {
                Ok(records)
            }
            _ => Err(TCError::bad_request("invalid Chain transaction file")),
        }
    }
}

impl<'en> en::ToStream<'en> for ChainFile {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Frontier { finalized } => ("frontier", finalized).into_stream(encoder),
            Self::Transaction { id, records } => ("transaction", id, records).into_stream(encoder),
        }
    }
}

impl de::FromStream for ChainFile {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor for Visitor {
            type Value = ChainFile;

            fn expecting() -> &'static str {
                "a tagged Chain file"
            }

            async fn visit_seq<A: de::SeqAccess>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let tag: String = required(&mut seq).await?;
                let value = match tag.as_str() {
                    "frontier" => ChainFile::Frontier {
                        finalized: required(&mut seq).await?,
                    },
                    "transaction" => ChainFile::Transaction {
                        id: required(&mut seq).await?,
                        records: Arc::new(required(&mut seq).await?),
                    },
                    _ => return Err(de::Error::custom("unsupported Chain file format")),
                };

                end(&mut seq).await?;
                match &value {
                    ChainFile::Frontier { .. } => {
                        value.frontier().map_err(de::Error::custom)?;
                    }
                    ChainFile::Transaction { id, .. } => {
                        value.records(*id).map_err(de::Error::custom)?;
                    }
                }

                Ok(value)
            }
        }

        decoder.decode_seq(Visitor).await
    }
}

impl get_size::GetSize for CapturedCollection {
    fn get_heap_size(&self) -> usize {
        self.id.get_heap_size() + self.class.get_heap_size() + self.schema.get_heap_size()
    }
}

impl get_size::GetSize for StoredValue {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Scalar(value) => value.get_heap_size(),
            Self::Collection(value) => value.get_heap_size(),
        }
    }
}

impl get_size::GetSize for MutationRecord {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Restore(value) => value.get_heap_size(),
            Self::Put(path, key, value) => {
                path.get_heap_size() + key.get_heap_size() + value.get_heap_size()
            }
            Self::Delete(path, key) => path.get_heap_size() + key.get_heap_size(),
        }
    }
}

impl get_size::GetSize for ChainFile {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Frontier { .. } => 0,
            Self::Transaction { records, .. } => records.get_heap_size(),
        }
    }
}

impl AsType<ChainFile> for ChainFile {
    fn as_type(&self) -> Option<&Self> {
        Some(self)
    }

    fn as_type_mut(&mut self) -> Option<&mut Self> {
        Some(self)
    }

    fn into_type(self) -> Option<Self> {
        Some(self)
    }
}

/// A delegated file composition containing Chain's semantic history.
/// Conversion retains the native ChainFile with a fixed composition envelope;
/// GetSize accounts that envelope plus the contained ChainFile's heap footprint.
pub trait ChainFileType:
    Clone + FileLoad + FileSave + AsType<ChainFile> + From<ChainFile> + Send + Sync + 'static
{
}

impl<T> ChainFileType for T where
    T: Clone + FileLoad + FileSave + AsType<ChainFile> + From<ChainFile> + Send + Sync + 'static
{
}

impl FileLoad for ChainFile {
    async fn load(
        path: &Path,
        mut file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> io::Result<Self> {
        let mut expected = [0; 32];
        file.read_exact(&mut expected).await?;
        let mut hash = Sha256::new();
        let value: Self = {
            let stream = ReaderStream::new(file).inspect_ok(|bytes| hash.update(bytes));
            futures::pin_mut!(stream);
            let value = destream_json::try_decode((), &mut stream)
                .await
                .map_err(invalid)?;

            while stream.try_next().await?.is_some() {}
            value
        };

        if hash.finalize().as_slice() != expected {
            return Err(invalid("WAL file checksum mismatch"));
        }

        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("invalid Chain filename"))?;

        if name == COMMITTED {
            value.frontier().map_err(invalid)?;
        } else {
            value
                .records(transaction_id(name).map_err(invalid)?)
                .map_err(invalid)?;
        }

        Ok(value)
    }
}

impl FileSave for ChainFile {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        let mut hash = Sha256::new();
        let mut stream = destream_json::encode(self).map_err(invalid)?;
        let mut size = 32_u64;
        {
            let mut writer = tokio::io::BufWriter::new(&mut *file);
            writer.write_all(&[0; 32]).await?;
            while let Some(bytes) = stream.try_next().await.map_err(invalid)? {
                size = size
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid("WAL file size overflow"))?;
                hash.update(&bytes);
                writer.write_all(&bytes).await?;
            }
            writer.flush().await?;
        }
        file.seek(io::SeekFrom::Start(0)).await?;
        file.write_all(&hash.finalize()).await?;
        Ok(size)
    }
}

pub(crate) struct Store<Txn: StorageContext, FE = ChainFile> {
    pub root: DirLock<FE>,
    pub values: DirLock<Txn::File>,
}

impl<Txn: StorageContext + 'static, FE: Send + Sync> Store<Txn, FE> {
    pub async fn capture<S>(&self, txn: &Txn, value: S) -> TCResult<(StoredValue, S)>
    where
        S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
    {
        if value.can_cast_into() {
            let (stored, accepted) = self
                .capture_collection(txn, value.opt_cast_into().expect("collection value"))
                .await?;

            Ok((StoredValue::Collection(stored), S::from(accepted)))
        } else {
            let scalar = value.into_scalar()?;
            let accepted = S::from(scalar.clone());
            Ok((StoredValue::Scalar(scalar), accepted))
        }
    }

    pub async fn capture_collection(
        &self,
        txn: &Txn,
        source: Collection<Txn>,
    ) -> TCResult<(CapturedCollection, Collection<Txn>)> {
        let transaction = self
            .values
            .write()
            .await
            .get_or_create_dir(txn.id().to_string())?;

        let (id, dir) = transaction.write().await.create_dir_unique()?;
        let accepted = Collection::copy_from(txn, dir, &source).await?;
        let hash = accepted.hash_native_capture(txn.id()).await?;
        let (class, schema) = accepted.schema()?.into();
        Ok((
            CapturedCollection {
                id: id.to_string().parse().expect("UUID is a valid capture Id"),
                class,
                schema,
                hash,
            },
            accepted,
        ))
    }

    pub async fn resolve<S: From<Scalar> + From<Collection<Txn>>>(
        &self,
        id: TxnId,
        value: StoredValue,
    ) -> TCResult<S> {
        match value {
            StoredValue::Scalar(value) => Ok(S::from(value)),
            StoredValue::Collection(value) => {
                self.resolve_collection(id, &value).await.map(S::from)
            }
        }
    }

    pub async fn resolve_collection(
        &self,
        id: TxnId,
        value: &CapturedCollection,
    ) -> TCResult<Collection<Txn>> {
        let transaction = self
            .values
            .read()
            .await
            .get_dir(&id.to_string())
            .cloned()
            .ok_or_else(|| TCError::bad_request("missing capture transaction"))?;

        let dir = transaction
            .read()
            .await
            .get_dir(value.id.as_str())
            .cloned()
            .ok_or_else(|| TCError::bad_request("missing stored collection"))?;

        let collection = Collection::<Txn>::load(dir, value.schema()?).await?;
        if collection.hash_native_capture(id).await? != value.hash {
            return Err(TCError::bad_request("stored collection checksum mismatch"));
        }

        Ok(collection)
    }

    pub async fn sync_captures(&self, records: &[MutationRecord]) -> TCResult<()> {
        // CopyFrom completed each capture's data and immediate parent barriers.
        // Publish the enclosing transaction directory only when captures exist.
        if records.iter().any(|record| match record {
            MutationRecord::Restore(_) => true,
            MutationRecord::Put(_, _, value) => {
                matches!(value.as_ref(), StoredValue::Collection(_))
            }
            MutationRecord::Delete(_, _) => false,
        }) {
            self.values.sync_deleted().await?;
        }

        Ok(())
    }

    pub async fn remove_captures(&self, id: TxnId) -> TCResult<()> {
        self.values.write().await.delete(&id.to_string()).await;
        self.values.sync_deleted().await?;
        Ok(())
    }
}

impl<Txn: StorageContext + 'static, FE: ChainFileType> Store<Txn, FE> {
    async fn frontier_file(&self) -> TCResult<FileLock<FE>> {
        self.root
            .read()
            .await
            .get_file(COMMITTED)
            .cloned()
            .ok_or_else(|| TCError::bad_request("missing Chain frontier file"))
    }

    pub async fn frontier(&self) -> TCResult<Option<TxnId>> {
        self.frontier_file()
            .await?
            .read::<ChainFile>()
            .await?
            .frontier()
    }

    // This index contains delegated handles, never history payloads. The caller's
    // finite admitted transaction set bounds live files; expiry advances the cutoff.
    pub async fn transactions(&self) -> TCResult<BTreeMap<TxnId, FileLock<FE>>> {
        let root = self.root.read().await;
        transaction_files(&root)
            .map(|entry| entry.map(|(id, file)| (id, file.clone())))
            .collect()
    }

    /// Finalization needs only covered identities, not a sorted index of later files.
    pub async fn transactions_through(&self, cutoff: TxnId) -> TCResult<Vec<TxnId>> {
        let root = self.root.read().await;
        transaction_files(&root)
            .filter_map(|entry| match entry {
                Ok((id, _)) if id <= cutoff => Some(Ok(id)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    pub async fn publish_transaction(
        &self,
        id: TxnId,
        records: Vec<MutationRecord>,
    ) -> TCResult<()> {
        let file = self
            .root
            .write()
            .await
            .create_file::<FE>(
                transaction_name(id),
                FE::from(ChainFile::Transaction {
                    id,
                    records: Arc::new(records),
                }),
            )
            .await?;

        file.sync_all().await?;
        Ok(())
    }

    pub async fn publish_frontier(&self, finalized: Option<TxnId>) -> TCResult<()> {
        self.frontier_file()
            .await?
            .replace_all(FE::from(ChainFile::Frontier { finalized }))
            .await?;

        Ok(())
    }

    async fn remove_history(&self, transactions: impl Iterator<Item = TxnId>) -> TCResult<()> {
        for id in transactions {
            self.root.write().await.delete(&transaction_name(id)).await;
        }

        self.root.sync_deleted().await?;
        Ok(())
    }

    pub async fn clean_through(&self, cutoff: TxnId, transactions: &[TxnId]) -> TCResult<()> {
        self.remove_history(transactions.iter().copied()).await?;
        self.clean_captures(|id| id <= cutoff).await
    }

    async fn clean_captures(&self, remove: impl Fn(TxnId) -> bool) -> TCResult<()> {
        // Walk a finite delegated file set one directory at a time, without a
        // second reference graph or retaining native collection guards.
        let mut after = None;
        loop {
            let next = {
                let values = self.values.read().await;
                let start = after.as_deref().map_or(Unbounded, Excluded);
                values
                    .range((start, Unbounded))
                    .next()
                    .map(|(name, _)| name.clone())
            };

            let Some(name) = next else {
                break;
            };

            let id: TxnId = name.parse().map_err(TCError::bad_request)?;
            if id.to_string() != name {
                return Err(TCError::bad_request("invalid capture transaction name"));
            }

            if remove(id) {
                self.values.write().await.delete(&name).await;
            }

            after = Some(name);
        }

        self.values.sync_deleted().await?;
        Ok(())
    }

    pub async fn reclaim(
        &self,
        frontier: Option<TxnId>,
        transactions: &BTreeMap<TxnId, FileLock<FE>>,
    ) -> TCResult<()> {
        if let Some(frontier) = frontier {
            self.remove_history(transactions.range(..=frontier).map(|(&id, _)| id))
                .await?;
        }

        self.clean_captures(|id| {
            frontier.is_some_and(|frontier| id <= frontier) || !transactions.contains_key(&id)
        })
        .await
    }
}

fn transaction_files<FE: Send + Sync>(
    root: &Dir<FE>,
) -> impl Iterator<Item = TCResult<(TxnId, &FileLock<FE>)>> {
    root.iter()
        .filter(|(name, _)| name.as_str() != COMMITTED)
        .map(|(name, entry)| {
            let id = transaction_id(name)?;
            let file = entry
                .as_file()
                .ok_or_else(|| TCError::bad_request("expected Chain transaction file"))?;

            Ok((id, file))
        })
}
