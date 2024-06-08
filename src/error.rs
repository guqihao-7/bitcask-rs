use thiserror::Error;

#[derive(Error, Debug)]
pub enum E {
    #[error("failed to read from data file")]
    Failed2ReadFromDataFile,

    #[error("failed to write to data file")]
    Failed2Write2DataFile,

    #[error("failed to sync data file")]
    Failed2SyncDataFile,

    #[error("failed to open data file")]
    Failed2OpenDataFile,

    #[error("key is empty")]
    EmptyKey,

    #[error("mem index update failed")]
    Failed2UpdateMemIndex,

    #[error("data is corrupted")]
    DataCorrupted,
}

pub type R<T> = Result<T, E>;