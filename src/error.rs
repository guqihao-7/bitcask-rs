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

    #[error("cannot write old file, it's immutable")]
    CanNotWriteOldFile,

    #[error("key not exist so the value is nil")]
    Nil,

    #[error("key not exist")]
    KeyNotExist,

    #[error("dir path is empty")]
    DirPathIsEmpty,

    #[error("failed to create data dir")]
    Failed2CreateDataDir,

    #[error("could not open data dir")]
    CouldNotOpenDataDir,

    #[error("could not read database datafile dir")]
    Failed2ReadDBDir,
}

pub type R<T> = Result<T, E>;
