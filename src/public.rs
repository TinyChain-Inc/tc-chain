use pathlink::{PathBuf, PathSegment};
use safecast::TryCastInto;
use tc_collection::{Collection, CollectionState, StorageContext};
use tc_ir::{Handler, Route};

use crate::SyncChain;

struct ChainHandler<'a, Txn: StorageContext, FE, S: CollectionState<Txn = Txn>> {
    chain: &'a SyncChain<Txn, FE>,
    path: PathBuf,
    leaf: Box<dyn Handler<'a, S> + 'a>,
}

impl<Txn, FE, S> Route<S> for SyncChain<Txn, FE>
where
    Txn: StorageContext + 'static,
    FE: Send + Sync,
    S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
{
    fn route<'a>(&'a self, path: &[PathSegment]) -> Option<Box<dyn Handler<'a, S> + 'a>> {
        Some(Box::new(ChainHandler {
            chain: self,
            path: PathBuf::from_slice(path),
            leaf: self.inner.subject.route(path)?,
        }))
    }
}

impl<'a, Txn, FE, S> Handler<'a, S> for ChainHandler<'a, Txn, FE, S>
where
    Txn: StorageContext + 'static,
    FE: Send + Sync,
    S: CollectionState<Txn = Txn> + TryCastInto<Collection<Txn>>,
{
    fn get<'txn>(self: Box<Self>) -> Option<tc_ir::GetHandler<'a, 'txn, S>>
    where
        'txn: 'a,
    {
        let Self { chain, leaf, .. } = *self;
        let get = leaf.get()?;

        Some(Box::new(move |txn, key| {
            Box::pin(async move {
                chain.readable(txn)?;
                get(txn, key).await
            })
        }))
    }

    fn post<'txn>(self: Box<Self>) -> Option<tc_ir::PostHandler<'a, 'txn, S>>
    where
        'txn: 'a,
    {
        let Self { chain, leaf, .. } = *self;
        let post = leaf.post()?;

        Some(Box::new(move |txn, params| {
            Box::pin(async move {
                chain.readable(txn)?;
                post(txn, params).await
            })
        }))
    }

    fn put<'txn>(self: Box<Self>) -> Option<tc_ir::PutHandler<'a, 'txn, S>>
    where
        'txn: 'a,
    {
        let Self { chain, path, leaf } = *self;
        let put = leaf.put()?;

        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                chain
                    .mutate(txn, path, key.clone(), Some(value.clone()), || {
                        put(txn, key, value)
                    })
                    .await
            })
        }))
    }

    fn delete<'txn>(self: Box<Self>) -> Option<tc_ir::DeleteHandler<'a, 'txn, S>>
    where
        'txn: 'a,
    {
        let Self { chain, path, leaf } = *self;
        let delete = leaf.delete()?;

        Some(Box::new(move |txn, key| {
            Box::pin(async move {
                chain
                    .mutate::<S, _, _>(txn, path, key.clone(), None, || delete(txn, key))
                    .await
            })
        }))
    }
}
