use pathlink::{PathBuf, PathSegment};
use tc_collection::StorageContext;
use tc_ir::{Handler, Route};
use tc_state::State;

use crate::SyncChain;

struct ChainHandler<'a, Txn: StorageContext> {
    chain: &'a SyncChain<Txn>,
    path: PathBuf,
    leaf: Box<dyn Handler<'a, State<Txn>> + 'a>,
}

impl<Txn: StorageContext + 'static> Route<State<Txn>> for SyncChain<Txn> {
    fn route<'a>(&'a self, path: &[PathSegment]) -> Option<Box<dyn Handler<'a, State<Txn>> + 'a>> {
        Some(Box::new(ChainHandler {
            chain: self,
            path: PathBuf::from_slice(path),
            leaf: self.inner.subject.route(path)?,
        }))
    }
}

impl<'a, Txn: StorageContext + 'static> Handler<'a, State<Txn>> for ChainHandler<'a, Txn> {
    fn get<'txn>(self: Box<Self>) -> Option<tc_ir::GetHandler<'a, 'txn, State<Txn>>>
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

    fn post<'txn>(self: Box<Self>) -> Option<tc_ir::PostHandler<'a, 'txn, State<Txn>>>
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

    fn put<'txn>(self: Box<Self>) -> Option<tc_ir::PutHandler<'a, 'txn, State<Txn>>>
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

    fn delete<'txn>(self: Box<Self>) -> Option<tc_ir::DeleteHandler<'a, 'txn, State<Txn>>>
    where
        'txn: 'a,
    {
        let Self { chain, path, leaf } = *self;
        let delete = leaf.delete()?;

        Some(Box::new(move |txn, key| {
            Box::pin(async move {
                chain
                    .mutate(txn, path, key.clone(), None, || delete(txn, key))
                    .await
            })
        }))
    }
}
