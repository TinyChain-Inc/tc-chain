//! Typed WAL files. Encoding and checksums stay at the filesystem boundary.

use std::{collections::BTreeMap, io, path::Path};

use destream::{IntoStream, de, en};
use freqfs::{DirLock, FileLoad, FileLock, FileSave};
use futures::{StreamExt, TryStreamExt};
use pathlink::PathBuf;
use safecast::{AsType, TryCastFrom, TryCastInto};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use tc_collection::{Collection, CollectionState, StorageContext, collection::CollectionSchema};
use tc_error::{TCError, TCResult};
use tc_ir::{Id, IdRef, OpRef, Scalar, Sha256Hash, Subject, TCRef, TxnId};
use tc_value::Value;

pub(crate) const COMMITTED: &str = "committed.chain_block";

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

/// Only scoped GET references to hash-addressed native collections are stored references.
pub(crate) fn reference(value: &Scalar) -> TCResult<Option<(&Id, &PathBuf, &Scalar)>> {
    let Scalar::Ref(reference) = value else {
        return Ok(None);
    };
    match reference.as_ref() {
        TCRef::Op(OpRef::Get((Subject::Ref(id, path), schema)))
            if id.as_str().len() == 64
                && id
                    .as_str()
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) =>
        {
            Ok(Some((id.id(), path, schema)))
        }
        _ => Err(TCError::bad_request("invalid stored collection reference")),
    }
}

async fn identity<Txn: StorageContext>(collection: &Collection<Txn>, id: TxnId) -> TCResult<Id> {
    let hash = collection.hash(id).await?;
    Ok(format!("{hash:x}")
        .parse()
        .expect("hexadecimal collection identity"))
}

/// A persisted request, never a second invocation interface.
#[derive(Clone)]
pub enum MutationRecord {
    Restore(Scalar),
    Put(PathBuf, Scalar, Scalar),
    Delete(PathBuf, Scalar),
}

impl MutationRecord {
    pub fn value(&self) -> Option<&Scalar> {
        match self {
            Self::Put(_, _, value) | Self::Restore(value) => Some(value),
            Self::Delete(..) => None,
        }
    }
}

impl<'en> en::IntoStream<'en> for MutationRecord {
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Restore(value) => (value,).into_stream(encoder),
            Self::Put(path, key, value) => (path, key, value).into_stream(encoder),
            Self::Delete(path, key) => (path, key).into_stream(encoder),
        }
    }
}

impl<'en> en::ToStream<'en> for MutationRecord {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Restore(value) => (value,).into_stream(encoder),
            Self::Put(path, key, value) => (path, key, value).into_stream(encoder),
            Self::Delete(path, key) => (path, key).into_stream(encoder),
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
                "a restoration (collection,), DELETE (path, key), or PUT (path, key, value)"
            }

            async fn visit_seq<A: de::SeqAccess>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let first: Scalar = seq
                    .next_element(())
                    .await?
                    .ok_or_else(|| de::Error::invalid_length(0, Self::expecting()))?;
                let Some(key) = seq.next_element(()).await? else {
                    if reference(&first).map_err(de::Error::custom)?.is_none() {
                        return Err(de::Error::custom(
                            "restoration requires a stored collection reference",
                        ));
                    }
                    return Ok(MutationRecord::Restore(first));
                };
                let path = match first {
                    Scalar::Value(Value::String(path)) => {
                        path.as_str().parse().map_err(de::Error::custom)?
                    }
                    _ => return Err(de::Error::custom("invalid mutation path")),
                };

                let record = match seq.next_element(()).await? {
                    Some(value) => MutationRecord::Put(path, key, value),
                    None => return Ok(MutationRecord::Delete(path, key)),
                };
                if seq.next_element::<de::IgnoredAny>(()).await?.is_some() {
                    return Err(de::Error::invalid_length(4, Self::expecting()));
                }
                Ok(record)
            }
        }

        decoder.decode_seq(Visitor).await
    }
}

/// Shared semantic history; file checksums are separate from predecessor linkage.
#[derive(Clone)]
pub(crate) struct ChainBlock {
    pub previous_hash: Sha256Hash,
    pub mutations: BTreeMap<TxnId, Vec<MutationRecord>>,
}

impl<'en> en::IntoStream<'en> for ChainBlock {
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        (
            bytes::Bytes::copy_from_slice(&self.previous_hash),
            en::MapStream::from(futures::stream::iter(self.mutations)),
        )
            .into_stream(encoder)
    }
}

impl<'en> en::ToStream<'en> for ChainBlock {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        (
            bytes::Bytes::copy_from_slice(&self.previous_hash),
            en::MapStream::from(futures::stream::iter(&self.mutations)),
        )
            .into_stream(encoder)
    }
}

impl de::FromStream for ChainBlock {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct Mutations(BTreeMap<TxnId, Vec<MutationRecord>>);

        impl de::FromStream for Mutations {
            type Context = ();

            async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
                struct Visitor;

                impl de::Visitor for Visitor {
                    type Value = Mutations;

                    fn expecting() -> &'static str {
                        "mutations keyed by original transaction ID"
                    }

                    async fn visit_map<A: de::MapAccess>(
                        self,
                        mut map: A,
                    ) -> Result<Self::Value, A::Error> {
                        let mut mutations = BTreeMap::new();
                        while let Some(id) = map.next_key::<TxnId>(()).await? {
                            if mutations.insert(id, map.next_value(()).await?).is_some() {
                                return Err(de::Error::custom("duplicate block transaction ID"));
                            }
                        }
                        Ok(Mutations(mutations))
                    }
                }

                decoder.decode_map(Visitor).await
            }
        }

        let (previous_hash, Mutations(mutations)) =
            <(bytes::Bytes, Mutations)>::from_stream((), decoder).await?;
        let previous_hash = <[u8; 32]>::try_from(previous_hash.as_ref())
            .map_err(|_| de::Error::custom("invalid predecessor hash length"))?;

        Ok(Self {
            previous_hash: previous_hash.into(),
            mutations,
        })
    }
}

pub(crate) struct Store<Txn: StorageContext, FE = ChainFile> {
    pub committed: FileLock<FE>,
    pub values: DirLock<Txn::File>,
}

impl<Txn: StorageContext + 'static, FE> Store<Txn, FE> {
    pub async fn capture<S>(&self, txn: &Txn, value: S) -> TCResult<Scalar>
    where
        S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
    {
        let value = if value.can_cast_into() {
            self.capture_collection(txn, value.opt_cast_into().expect("collection value"))
                .await?
        } else {
            value.into_scalar()?
        };

        // Verify reused storage and that a new native copy preserves the identity.
        if matches!(&value, Scalar::Ref(_)) {
            self.resolve_collection(txn.id(), value.clone()).await?;
        }
        Ok(value)
    }

    pub async fn capture_collection(&self, txn: &Txn, source: Collection<Txn>) -> TCResult<Scalar> {
        let name = identity(&source, txn.id()).await?;
        let (path, schema): (PathBuf, Value) = source.schema()?.into();
        let value = Scalar::from(TCRef::Op(OpRef::Get((
            Subject::Ref(IdRef::new(name.clone()), path),
            schema.into(),
        ))));

        let target = {
            let mut values = self.values.write().await;
            if values.contains(name.as_str()) {
                None
            } else {
                Some(values.create_dir(name.to_string())?)
            }
        };
        if let Some(dir) = target {
            source.copy_into(txn, dir).await?;
        }
        Ok(value)
    }

    pub async fn resolve<S>(&self, id: TxnId, value: Scalar) -> TCResult<S>
    where
        S: From<Scalar> + From<Collection<Txn>>,
    {
        if reference(&value)?.is_some() {
            self.resolve_collection(id, value).await.map(S::from)
        } else {
            Ok(S::from(value))
        }
    }

    pub async fn resolve_collection(&self, id: TxnId, value: Scalar) -> TCResult<Collection<Txn>> {
        let (name, path, schema) = reference(&value)?
            .ok_or_else(|| TCError::bad_request("expected stored collection reference"))?;
        let schema = Value::try_cast_from(schema.clone(), |_| {
            TCError::bad_request("invalid collection schema")
        })?;
        let schema = CollectionSchema::try_cast_from((path.clone(), schema), |_| {
            TCError::bad_request("invalid collection schema")
        })?;
        let dir = self
            .values
            .read()
            .await
            .get_dir(name.as_str())
            .cloned()
            .ok_or_else(|| TCError::bad_request("missing stored collection"))?;
        let collection = Collection::<Txn>::load(dir, schema).await?;
        if identity(&collection, id).await? != *name {
            return Err(TCError::bad_request("stored collection checksum mismatch"));
        }
        Ok(collection)
    }

    pub async fn sync(&self, records: &[MutationRecord]) -> TCResult<()> {
        for name in references(records.iter())? {
            let dir = self
                .values
                .read()
                .await
                .get_dir(&name)
                .cloned()
                .ok_or_else(|| TCError::bad_request("missing stored collection"))?;
            dir.sync_all().await?;
        }
        Ok(())
    }
}

impl<Txn: StorageContext + 'static, FE: ChainFileType> Store<Txn, FE> {
    /// Reclaim orphans after recovery, before exposing the loaded Chain to requests.
    pub(super) async fn reclaim(&self) -> TCResult<()> {
        let retained = {
            let committed = self.committed.read::<ChainFile>().await?;
            references(committed.block.mutations.values().flatten())?
        };
        let names = self
            .values
            .read()
            .await
            .names()
            .filter(|name| !retained.contains(*name))
            .cloned()
            .collect::<Vec<_>>();

        for name in &names {
            self.values.write().await.delete(name).await;
        }

        if !names.is_empty() {
            self.values.sync_deleted().await?;
        }
        Ok(())
    }
}

/// A delegated file composition containing Chain's semantic WAL record.
pub trait ChainFileType:
    Clone + FileLoad + FileSave + AsType<ChainFile> + From<ChainFile> + Send + Sync + 'static
{
}

impl<T> ChainFileType for T where
    T: Clone + FileLoad + FileSave + AsType<ChainFile> + From<ChainFile> + Send + Sync + 'static
{
}

impl get_size::GetSize for MutationRecord {
    fn get_size(&self) -> usize {
        match self {
            Self::Restore(value) => value.get_size(),
            Self::Put(path, key, value) => path.get_size() + key.get_size() + value.get_size(),
            Self::Delete(path, key) => path.get_size() + key.get_size(),
        }
    }
}

/// Filesystem encoding and checksum boundary for the committed semantic block.
#[derive(Clone)]
pub struct ChainFile {
    pub(crate) finalized: Option<TxnId>,
    pub(crate) materializing: Option<TxnId>,
    pub(crate) block: ChainBlock,
}

impl get_size::GetSize for ChainFile {
    fn get_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .block
                .mutations
                .values()
                .map(|records| {
                    records
                        .iter()
                        .map(get_size::GetSize::get_size)
                        .sum::<usize>()
                })
                .sum::<usize>()
    }
}

impl Default for ChainFile {
    fn default() -> Self {
        Self {
            finalized: None,
            materializing: None,
            block: ChainBlock {
                previous_hash: Sha256Hash::default(),
                mutations: BTreeMap::new(),
            },
        }
    }
}

impl<'en> en::ToStream<'en> for ChainFile {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        (self.finalized, self.materializing, &self.block).into_stream(encoder)
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

impl FileLoad for ChainFile {
    async fn load(_: &Path, mut file: tokio::fs::File, _: std::fs::Metadata) -> io::Result<Self> {
        let mut expected = [0; 32];
        file.read_exact(&mut expected).await?;

        let mut hash = Sha256::new();
        let value = {
            let stream = ReaderStream::new(file).inspect_ok(|bytes| hash.update(bytes));
            futures::pin_mut!(stream);
            let (finalized, materializing, block) = destream_json::try_decode((), &mut stream)
                .await
                .map_err(invalid)?;
            let value = Self {
                finalized,
                materializing,
                block,
            };
            while stream.try_next().await?.is_some() {}
            value
        };

        if hash.finalize().as_slice() != expected {
            return Err(invalid("WAL file checksum mismatch"));
        }

        Ok(value)
    }
}

impl FileSave for ChainFile {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        let mut hash = Sha256::new();
        let mut stream = self.encoded()?;
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

impl ChainFile {
    fn encoded(&self) -> io::Result<futures::stream::BoxStream<'_, io::Result<bytes::Bytes>>> {
        Ok(destream_json::encode(self)
            .map_err(invalid)?
            .map_err(invalid)
            .boxed())
    }

    // Measure at the persistence boundary before cache and disk admission.
    async fn size(&self) -> TCResult<usize> {
        let mut stream = self.encoded()?;
        let mut size = 32_usize;
        while let Some(bytes) = stream.try_next().await.map_err(TCError::internal)? {
            size = size
                .checked_add(bytes.len())
                .ok_or_else(|| TCError::bad_request("WAL file size overflow"))?;
        }
        Ok(size)
    }
}

pub(crate) async fn load<FE: Send + Sync>(root: &DirLock<FE>) -> TCResult<FileLock<FE>> {
    root.read()
        .await
        .get_file(COMMITTED)
        .cloned()
        .ok_or_else(|| TCError::bad_request("missing committed SyncChain block"))
}

pub(crate) async fn create<FE: ChainFileType>(root: &DirLock<FE>) -> TCResult<FileLock<FE>> {
    let value = ChainFile::default();
    let size = value.size().await?;
    let file = root
        .write()
        .await
        .create_file(COMMITTED.into(), value, size)
        .await?;
    file.sync_all().await?;
    Ok(file)
}

pub(crate) async fn publish<FE: ChainFileType>(
    file: &FileLock<FE>,
    value: ChainFile,
) -> TCResult<()> {
    let size = value.size().await?;
    file.replace_all(FE::from(value), size).await?;
    Ok(())
}

fn references<'a>(
    records: impl Iterator<Item = &'a MutationRecord>,
) -> TCResult<std::collections::BTreeSet<String>> {
    records
        .filter_map(MutationRecord::value)
        .filter_map(|value| reference(value).transpose())
        .map(|reference| reference.map(|(name, _, _)| name.to_string()))
        .collect()
}
