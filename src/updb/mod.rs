#![deny(unsafe_code)]

use std::sync::Arc;

use arc_swap::ArcSwap;
use imbl::OrdMap;
use tokio::sync::Mutex;

use crate::{error::Error, tx::Tx};

pub struct Db<K, V> {
    pub(crate) lock: Arc<Mutex<()>>,
    pub(crate) data: Arc<ArcSwap<OrdMap<K, V>>>,
}

pub fn new<K, V>() -> Db<K, V> {
    Db {
        lock: Arc::new(Mutex::new(())),
        data: Arc::new(ArcSwap::new(Arc::new(OrdMap::new()))),
    }
}

impl<K, V> Db<K, V>
where
    K: Ord + Clone,
    V: Eq + Clone,
{
    pub async fn begin(&self, write: bool) -> Result<Tx<K, V>, Error> {
        match write {
            true => Ok(Tx::new(
                self.data.clone(),
                write,
                Some(self.lock.clone().lock_owned().await),
            )),
            false => Ok(Tx::new(self.data.clone(), write, None)),
        }
    }

    pub fn size(&self) -> usize {
        self.data.load().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn can_create_db() {
        let db: Db<String, String> = new();
        assert_eq!(0, db.data.load().len());
    }

    #[tokio::test]
    async fn can_create_read_transactions() {
        let db: Db<String, String> = new();
        let tx = db.begin(false).await;
        assert!(tx.is_ok());
    }

    #[tokio::test]
    async fn can_create_writable_transactions() {
        let db: Db<String, String> = new();
        let tx = db.begin(true).await;
        assert!(tx.is_ok());
    }

    #[tokio::test]
    async fn readable_transactions_not_writable() {
        let db: Db<&str, &str> = new();

        let tx = db.begin(false).await;
        assert!(tx.is_ok());
        let mut tx = tx.unwrap();

        let res = tx.set("name", "ibrahim");
        assert!(res.is_err());

        let res = tx.del("name");
        assert!(res.is_err());

        let res = tx.commit();
        assert!(res.is_err());

        let res = tx.rollback();
        assert!(res.is_ok());
    }
}
