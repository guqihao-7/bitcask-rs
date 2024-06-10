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

    #[error("key is empty and it's illegal")]
    EmptyKey,

    #[error("mem index update failed")]
    Failed2UpdateMemIndex,

    #[error("data is corrupted")]
    DataCorrupted,

    #[error("value is empty and it's illegal")]
    EmptyValue,

    #[error("cannot open or create data file")]
    CanNotOpenOrCreateDateFile,
}

pub type R<T> = Result<T, E>;