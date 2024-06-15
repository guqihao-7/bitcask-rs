use std::fs::OpenOptions;
use std::io::{Error};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fs::File;
use std::os::windows::fs::FileExt;
use log::error;
use parking_lot::RwLock;
use crate::error::E::{CanNotOpenOrCreateDateFile, CanNotWriteOldFile};

use crate::error::R;
use crate::fio::file_io::FileIO;
use crate::fio::IOManager;

const FILE_SUFFIX: &str = ".bck";
const UNIX_FILE_SPLITTER: &str = "/";

/// older file 和 active file 的抽象
/// 即 DataFile 既可以表示 older file，也可以表示 active file
pub struct DataFile {
    /// 文件的全路径
    file_full_path: String,

    /// 下次开始写的位置
    /// 读线程不仅可以读 old file，也可以读 active file，所以这里要用 RWLock
    next_write_begin_pos: Arc<RwLock<usize>>,

    /// 操作 disk 的抽象接口, DataFile 使用该接口操作 disk
    io_manager: Box<dyn IOManager>,

    /// 文件类型
    file_type: DataFileType,
}

pub enum DataFileType {
    /// old file
    OLD,

    /// active file
    ACTIVE,
}

impl DataFile {
    pub fn new(dir_path: String, file_id: u32) -> R<Self> {
        let full_path = Self::get_file_full_path(dir_path, file_id.to_string());
        match Self::get_file(true, true, &full_path) {
            Ok(file) => {
                let nwbp = Arc::new(RwLock::new(0));
                let file_type = DataFileType::ACTIVE;
                let io_manager = Box::new(FileIO::new(file)) as Box<dyn IOManager>;
                Ok(Self {
                    file_full_path: full_path.display().to_string(),
                    next_write_begin_pos: nwbp,
                    io_manager,
                    file_type,
                })
            }
            Err(e) => {
                println!("{:?}", e);
                error!("read from data file err: {}", e);
                Err(CanNotOpenOrCreateDateFile {})
            }
        }
    }

    /// 不存在则以读写模式创建然后返回，已存在以读写模式直接返回
    fn get_file(readable: bool, appendable: bool, full_path: &PathBuf)
                -> Result<File, Error> {
        let mut open_options = OpenOptions::new();
        open_options.read(readable);
        open_options.append(appendable);
        open_options.create(true); // Sets the option to create a new file, or open it if it already exists
        match open_options.open(full_path) {
            Ok(file) => {
                Ok(file)
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    fn get_file_full_path(dir_path: String, file_id: String) -> PathBuf {
        let full_path = dir_path + UNIX_FILE_SPLITTER + &file_id + FILE_SUFFIX;
        let path_buf = PathBuf::from(full_path);
        path_buf
    }

    pub fn append(&self, buf: Vec<u8>) -> R<usize> {
        match self.file_type {
            DataFileType::OLD => {
                Err(CanNotWriteOldFile)
            }
            DataFileType::ACTIVE => {
                let mut write_begin_pos =
                    self.next_write_begin_pos.write();
                let write_end_pos = *write_begin_pos + buf.len();
                self.io_manager.append(&buf)?;
                *write_begin_pos = write_end_pos;
                Ok(write_end_pos)
            }
        }
    }

    pub fn next_write_begin_pos(&self) -> usize {
        let read_guard = self.next_write_begin_pos.read();
        *read_guard
    }

    pub fn sync(&self) -> R<()> {
        self.io_manager.sync()
    }

    pub fn file_id(&self) -> u32 {
        let file_name_with_suffix = Path::new(&self.file_full_path)
            .file_name()
            .unwrap();
        let file_name_with_suffix = file_name_with_suffix.to_str().unwrap();
        let file_name = &file_name_with_suffix[0..file_name_with_suffix.len() - FILE_SUFFIX.len()];
        u32::from_str_radix(file_name, 10).unwrap()
    }

    pub fn read_with_given_pos(&self, pos: usize, buf: &mut Vec<u8>) -> R<usize> {
        let file = File::open(&self.file_full_path).unwrap();
        Ok(file.seek_read(buf, pos as u64).unwrap())
    }
}

