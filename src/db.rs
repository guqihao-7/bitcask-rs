use std::collections::HashMap;
use std::sync::Arc;

use crc::{Crc, CRC_32_ISO_HDLC};
use parking_lot::RwLock;

use crate::data::datafile::DataFile;
use crate::data::entry::Entry;
use crate::data::meta_data::MetaData;
use crate::error::E::{DataCorrupted, EmptyKey, EmptyValue, Failed2UpdateMemIndex, KeyNotExist, Nil};
use crate::error::R;
use crate::index::Indexer;
use crate::options::Options;

pub struct Engine {
    options: Arc<Options>,
    mem_index: Arc<RwLock<Box<dyn Indexer>>>,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
}

impl Engine {
    pub fn new(options: Arc<Options>,
               mem_index: Arc<RwLock<Box<dyn Indexer>>>,
               active_file: Arc<RwLock<DataFile>>,
               older_files: Arc<RwLock<HashMap<u32, DataFile>>>, ) -> Self {
        Self {
            options,
            mem_index,
            active_file,
            older_files,
        }
    }
}

impl Engine {
    /// 存储 kv, k不能为空, v 也不能为空
    pub fn put(&self, key: String, value: Vec<u8>) -> R<()> {
        if key.is_empty() {
            return Err(EmptyKey);
        }

        if value.len() <= 0 {
            return Err(EmptyValue);
        }

        let mut entry = Entry::new(key, value).unwrap();
        let _ = self.append_entry_to_active_file(&mut entry);
        Ok(())
    }

    pub fn read(&self, key: String) -> R<Vec<u8>> {
        if key.is_empty() {
            return Err(EmptyKey);
        }

        // 1. 读 index
        let mem_index_read_guard = self.mem_index.read();
        let meta_data = mem_index_read_guard.as_ref()
            .get(&key);

        if meta_data.is_none() {
            return Err(Nil);
        }

        let meta_data = meta_data.unwrap();
        drop(mem_index_read_guard);

        // 2. 读 file 中的 data
        let active_file_read_guard = self.active_file.read();
        let mut buf = vec![0; meta_data.entry_sz];
        let data: Vec<u8> = if active_file_read_guard.file_id() == meta_data.file_id {
            let _ = active_file_read_guard.read_with_given_pos(meta_data.entry_start_pos, &mut buf).unwrap();
            drop(active_file_read_guard);
            buf
        } else {
            drop(active_file_read_guard);
            let older_file_read_guard = self.older_files.read();
            let target_old_file = older_file_read_guard.get(&meta_data.file_id).unwrap();
            let _ = target_old_file.read_with_given_pos(meta_data.entry_start_pos, &mut buf).unwrap();
            drop(older_file_read_guard);
            buf
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
    /// tombstone 就是 value_sz 是 0，value 是 len 为 0 的 vec
    pub fn delete(&self, key: String) -> R<Vec<u8>> {
        // 先判断 key 是否存在
        let read_guard = self.mem_index.read();
        if read_guard.get(&key).is_none() {
            return Err(KeyNotExist);
        }
        drop(read_guard);

        let mut tombstone = Entry::get_tombstone_with_given_key(key.clone()).unwrap();
        let res = self.read(key.clone()).unwrap();
        let _ = self.append_entry_to_active_file(&mut tombstone);
        let mem_index_write_guard = self.mem_index.write();
        mem_index_write_guard.delete(&key);
        Ok(res)
    }

    /// 将 key 的值更新为 new_value, 返回 old value
    pub fn update(&self, key: String, new_value: Vec<u8>) -> R<Vec<u8>> {
        let old_val = self.delete(key.clone());
        let _ = self.put(key, new_value);
        old_val
    }

    fn append_entry_to_active_file(&self, entry: &mut Entry) -> R<MetaData> {
        let dir_path = self.options.dir_path.clone();
        let data: Vec<u8> = entry.encode();
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

        let write_begin_pos = active_file.next_write_begin_pos();

        // 3. 写入 disk, 这里会修改 next_write_pos, 所以需要先保存下来
        active_file.append(data)?;

        if self.options.syn_after_each_write {
            active_file.sync()?;
        }

        // 4. 更新内存 index
        let meta_data = MetaData::new(active_file.file_id(),
                                      entry.get_self_size(),
                                      write_begin_pos, entry.tstamp());
        let mem_index_write_guard = self.mem_index.write();
        if !mem_index_write_guard.put((*entry.k()).parse().unwrap(), meta_data) {
            return Err(Failed2UpdateMemIndex);
        }
        Ok(meta_data)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;
    use crate::index::keydir::KeyDir;
    use super::*;

    #[test]
    fn test_put_and_read() {
        let engine = get_engine();
        engine.put("hello".to_string(), "world".to_string().into_bytes()).unwrap();
        let vec = engine.read("hello".to_string()).unwrap();
        println!("{:?}", String::from_utf8(vec).unwrap());
    }

    #[test]
    fn test_delete() {
        let engine = get_engine();
        engine.put("hello".to_string(), "world".to_string().into_bytes()).unwrap();
        let vec = engine.read("hello".to_string()).unwrap();
        println!("{:?}", String::from_utf8(vec).unwrap());
        let _vec = engine.delete("hello".to_string()).unwrap();
        let vec = match engine.read("hello".to_string()) {
            Ok(data) => { data }
            Err(e) => {
                println!("{}", e);
                vec![]
            }
        };
        println!("{:?}", String::from_utf8(vec).unwrap());
    }

    #[test]
    fn test_update() {
        let engine = get_engine();
        engine.put("hello".to_string(), "1".to_string().into_bytes()).unwrap();
        println!("{:?}", String::from_utf8(engine.read("hello".to_string()).unwrap()));
        let _ = engine.update("hello".to_string(), "wow".to_string().into_bytes()).unwrap();
        println!("{:?}", String::from_utf8(engine.read("hello".to_string()).unwrap()));
    }

    #[test]
    fn test_create_file() {
        let open_options = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open("./test_data/1.bck".to_string());
        let mut file = open_options.unwrap();
        let result = file.write("hello".as_ref());
        println!("{}", result.unwrap());
    }

    pub fn get_engine() -> Engine {
        let dir_path = "./test_data".to_string();
        let options = Arc::new(Options {
            dir_path: dir_path.clone(),
            file_threshold: 5000,
            syn_after_each_write: false,
        });

        let mem_index: Arc<RwLock<Box<dyn Indexer>>> =
            Arc::new(RwLock::new(Box::new(KeyDir::new()) as Box<dyn Indexer>));

        let active_file = Arc::new(RwLock::new(DataFile::new(dir_path.clone(), 1).unwrap()));

        let older_files = Arc::new(RwLock::new(HashMap::<u32, DataFile>::new()));

        let engine = Engine::new(options, mem_index, active_file, older_files);
        engine
    }
}

