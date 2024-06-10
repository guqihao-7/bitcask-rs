use std::fs::{ OpenOptions};
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use log::error;

use parking_lot::RwLock;
use crate::error::E::{CanNotOpenOrCreateDateFile, Failed2ReadFromDataFile};

use crate::error::R;
use crate::fio::file_io::FileIO;
use crate::fio::IOManager;

const FILE_SUFFIX: &str = ".bck";
const WINDOWS_FILE_SPLITTER: &str = "\\";
const UNIX_FILE_SPLITTER: &str = "/";

pub struct DataFile<'a> {
    file: &'a File,
    next_write_begin_pos: Arc<RwLock<usize>>,
    io_manager: Box<dyn IOManager>,
}

impl DataFile {
    pub fn new(dir_path: String, file_id: u32) -> R<Self> {
        let full_path = Self::get_file_full_path(dir_path, file_id.to_string());
        match Self::get_file(true, true, full_path) {
            Ok(file) => {
                let nwbp = Arc::new(RwLock::new(0));
                let io_manager = Box::new(FileIO::new(file)) as Box<dyn IOManager>;
                Ok(Self {
                    file_id,
                    next_write_begin_pos: nwbp,
                    io_manager,
                })
            }
            Err(e) => {
                error!("read from data file err: {}", e);
                Err(CanNotOpenOrCreateDateFile)
            }
        }
    }

    fn get_file(readable: bool, writeable: bool, full_path: PathBuf) -> Result<File, Error> {
        match OpenOptions::new()
            .read(readable)
            .write(writeable)
            .open(&full_path) {
            Ok(file) => {
                Ok(file)
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    fn get_file_full_path(dir_path: String, file_id: String) -> PathBuf {
        Path::new(&dir_path)
            .join(WINDOWS_FILE_SPLITTER)
            .join(file_id)
            .join(FILE_SUFFIX)
    }

    pub fn write(&self, buf: Vec<u8>) -> R<usize> {
        let read_guard = self.next_write_begin_pos.read();
        Self::get_file(true, true, )
        Ok(buf.len())
    }

    pub fn next_write_begin_pos(&self) -> usize {
        let read_guard = self.next_write_begin_pos.read();
        *read_guard
    }

    pub fn sync(&self) -> R<()> {
        self.io_manager.sync()
    }

    pub fn file_id(&self) -> u32 {
        self.file_id
    }

    pub fn read_with_given_pos(&self, pos: usize, buf: &mut Vec<u8>) -> R<Vec<u8>> {
        unimplemented!()
    }
}