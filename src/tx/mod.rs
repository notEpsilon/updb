#![deny(unsafe_code)]

use std::sync::Arc;

use arc_swap::ArcSwap;
use imbl::OrdMap;
use tokio::sync::OwnedMutexGuard;

use crate::error::Error;

pub struct Tx<K, V> {
    pub(crate) closed: bool,
    pub(crate) writable: bool,
    pub(crate) imm_data: OrdMap<K, V>,
    pub(crate) lst_data: Arc<ArcSwap<OrdMap<K, V>>>,
    pub(crate) writ_mux: Option<OwnedMutexGuard<()>>,
}

impl<K, V> Tx<K, V>
where
    K: Ord + Clone,
    V: Eq + Clone,
{
    pub(crate) fn new(
        data_ptr: Arc<ArcSwap<OrdMap<K, V>>>,
        writable: bool,
        guard: Option<OwnedMutexGuard<()>>,
    ) -> Tx<K, V> {
        Tx {
            writable,
            closed: false,
            imm_data: (*(*data_ptr.load())).clone(),
            lst_data: data_ptr.clone(),
            writ_mux: guard,
        }
    }

    pub fn closed(&self) -> bool {
        self.closed
    }

    pub fn rollback(&mut self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::TxClosed);
        }

        self.closed = true;

        if let Some(lock) = &self.writ_mux.take() {
            drop(lock);
        }

        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::TxClosed);
        }

        if !self.writable {
            return Err(Error::TxNotWritable);
        }

        self.closed = true;

        self.lst_data.store(Arc::new(self.imm_data.clone()));

        if let Some(lock) = &self.writ_mux.take() {
            drop(lock);
        }

        Ok(())
    }

    pub fn exists(&self, key: K) -> Result<bool, Error> {
        if self.closed {
            return Err(Error::TxClosed);
        }

        Ok(self.imm_data.contains_key(&key))
    }

    pub fn get(&self, key: K) -> Result<Option<V>, Error> {
        if self.closed {
            return Err(Error::TxClosed);
        }

        Ok(self.imm_data.get(&key).cloned())
    }

    pub fn set(&mut self, key: K, value: V) -> Result<(), Error> {
        if self.closed {
            return Err(Error::TxClosed);
        }

        if !self.writable {
            return Err(Error::TxNotWritable);
        }

        self.imm_data.insert(key, value);

        Ok(())
    }

    pub fn del(&mut self, key: K) -> Result<(), Error> {
        if self.closed {
            return Err(Error::TxClosed);
        }

        if !self.writable {
            return Err(Error::TxNotWritable);
        }

        self.imm_data.remove(&key);

        Ok(())
    }
}
