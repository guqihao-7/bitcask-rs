use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::R;
use crate::fio::IOManager;

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    next_write_begin_pos: Arc<RwLock<usize>>,
    io_manager: Box<dyn IOManager>,
}

impl DataFile {
    pub fn new(dir_path: String, file_id: u32) -> R<DataFile> {
        unimplemented!()
    }

    pub fn write(&self, buf: Vec<u8>) -> R<usize> {
        unimplemented!()
    }

    pub fn next_write_begin_pos(&self) -> usize {
        let read_guard = self.next_write_begin_pos.read();
        *read_guard
    }

    pub fn sync(&self) -> R<()> {
        self.io_manager.sync()
    }

    pub fn file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn read_with_given_pos(&self, pos: usize, buf: &mut Vec<u8>) -> R<Vec<u8>> {
        unimplemented!()
    }
}