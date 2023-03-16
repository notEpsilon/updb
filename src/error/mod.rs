#![deny(unsafe_code)]

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unexpected error occurred")]
    Generic,

    #[error("Transaction is closed")]
    TxClosed,

    #[error("Transaction is not writable")]
    TxNotWritable,
}
