use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::windows::fs::FileExt;
use std::sync::Arc;

use log::error;
use parking_lot::RwLock;

use crate::error::E::{Failed2OpenDataFile, Failed2ReadFromDataFile, Failed2SyncDataFile, Failed2Write2DataFile};
use crate::error::R;
use crate::fio::IOManager;

/// 文件 IO
pub struct FileIO {
    /// file descriptor
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file: File) -> Self {
        Self {
            fd: Arc::new(RwLock::new(file)),
        }
    }

    fn from(file_path: &str) -> R<Self> {
        return match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_path)
        {
            Ok(file) => {
                Ok(FileIO { fd: Arc::new(RwLock::new(file)) })
            }
            Err(e) => {
                error!("failed to open data file: {}", e);
                Err(Failed2OpenDataFile)
            }
        };
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> R<usize> {
        let read_guard = self.fd.read();
        return match read_guard.seek_read(buf, offset) {
            Ok(n) => Ok(n),
            Err(e) => {
                error!("read from data file err: {}", e);
                Err(Failed2ReadFromDataFile)
            }
        };
    }

    fn write(&self, buf: &[u8]) -> R<usize> {
        let mut write_guard = self.fd.write();
        return match write_guard.write(buf) {
            Ok(n) => { Ok(n) }
            Err(e) => {
                error!("read from data file err: {}", e);
                Err(Failed2Write2DataFile)
            }
        };
    }

    fn sync(&self) -> R<()> {
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            error!("failed to sync data file {}", e);
            return Err(Failed2SyncDataFile);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_io_write() {
        let fio_res = FileIO::from("./tmp.data");
        assert!(fio_res.is_ok());

        let fio = fio_res.ok().unwrap();
        let result = fio.write("hello".as_bytes());
        assert!(result.is_ok());
        assert_eq!(result.ok().unwrap(), 5);
    }

    #[test]
    fn test_file_io_read() {
        let fio_res = FileIO::from("./tmp.data");
        assert!(fio_res.is_ok());

        let fio = fio_res.ok().unwrap();
        let buf: &mut [u8] = &mut vec![0; 1];
        let result = fio.read(buf, 3);
        println!("{:?}", buf);
        assert_eq!(result.unwrap(), 1);
    }
}