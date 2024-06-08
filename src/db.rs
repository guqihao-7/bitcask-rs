use std::collections::HashMap;
use std::fs::File;
use std::ops::Index;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use crc::{Crc, CRC_32_ISO_HDLC};
use parking_lot::RwLock;
use crate::data::datafile::DataFile;
use crate::data::entry::{Entry, EntryType};
use crate::data::meta_data::MetaData;
use crate::error::E::{DataCorrupted, EmptyKey, Failed2UpdateMemIndex};
use crate::error::R;
use crate::index::Indexer;
use crate::options::Options;

pub struct Engine {
    options: Arc<Options>,
    mem_index: Box<dyn Indexer>,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
}

impl Engine {
    /// 存储 kv, k不能为空
    pub fn put(&self, key: String, value: Vec<u8>) -> R<()> {
        if key.is_empty() {
            return Err(EmptyKey);
        }

        let crc = Crc::<u32>::new(&CRC_32_ISO_HDLC);
        let crc = crc.checksum(&value[..]);
        let tstamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis()
            as u64;
        let mut entry = Entry::new(crc, tstamp, key.len(), value.len(), key, value);
        let _ = self.append_entry_to_active_file(&mut entry);
        Ok(())
    }

    pub fn append_entry_to_active_file(&self, entry: &mut Entry) -> R<MetaData> {
        let dir_path = self.options.dir_path.clone();
        let entry_type = entry.encode();
        let data: Vec<u8>;
        match entry_type {
            EntryType::Normal(t) => {
                data = t;
            }
            EntryType::Tombstone => {
                
            }
        }
        let entry_sz = data.len();

        // 1. 获取 active file
        let mut active_file = self.active_file.write();

        // 2. 如果超过阈值，关闭 active file，创建 new file
        let next_write_pos = active_file.next_write_begin_pos();
        if next_write_pos + entry_sz > self.options.file_threshold {
            // 2.1 sync 当前的 active file，将 page cache 刷盘
            active_file.sync()?;

            // 2.2 active file 加入到 older files 中
            let curr_active_file_id = active_file.file_id();
            let mut write_guard = self.older_files.write();
            write_guard.insert(curr_active_file_id, DataFile::new(dir_path.clone(), curr_active_file_id).unwrap());

            // 2.3 创建 new file 作为 active file
            let new_file = DataFile::new(dir_path.clone(), curr_active_file_id + 1)?;
            *active_file = new_file;
        }

        // 3. 写入 disk
        active_file.write(data)?;

        if self.options.syn_after_each_write {
            active_file.sync()?;
        }

        // 4. 更新内存 index
        let meta_data = MetaData::new(active_file.file_id(), entry.value_sz(),
                                      active_file.next_write_begin_pos(), entry.tstamp());
        if !self.mem_index.put((*entry.k()).parse().unwrap(), meta_data) {
            return Err(Failed2UpdateMemIndex);
        }
        Ok(meta_data)
    }

    pub fn read(&self, key: String) -> R<Vec<u8>> {
        if key.is_empty() {
            return Err(EmptyKey);
        }

        // 1. 读 index
        let meta_data = self.mem_index.as_ref()
            .get(&key).unwrap();
        let active_file_read_guard = self.active_file.read();

        // 2. 读 data
        let mut buf = vec![0; meta_data.value_sz];
        let data = if active_file_read_guard.file_id() == meta_data.file_id {
            let tmp = active_file_read_guard.read_with_given_pos(meta_data.value_pos, &mut buf).unwrap();
            drop(active_file_read_guard);
            tmp
        } else {
            drop(active_file_read_guard);
            let older_file_read_guard = self.older_files.read();
            let target_old_file = older_file_read_guard.get(&meta_data.file_id).unwrap();
            let tmp = target_old_file.read_with_given_pos(meta_data.value_pos, &mut buf).unwrap();
            drop(older_file_read_guard);
            tmp
        };

        // 3. 校验 crc 并解析
        let entry = Entry::decode(data);
        let disk_checksum = entry.crc();
        let calculated_checksum = Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(entry.v());
        if disk_checksum != calculated_checksum {
            // crc 校验不一致
            return Err(DataCorrupted);
        }
        return Ok((*entry.v()).to_owned());
    }

    /// 在 active file 写入一个 tomb。删除 keydir 对应的索引
    pub fn delete(&self, key: String) -> R<Vec<u8>> {
        unimplemented!()
    }
}