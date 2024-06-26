use std::collections::HashMap;
use std::fs::{self, create_dir_all};
use std::path::Path;
use std::sync::Arc;

use crate::data::datafile;
use crate::data::datafile::{DataFile, DataFileType, DATA_FILE_SUFFIX};
use crate::data::entry::Entry;
use crate::data::meta_data::MetaData;
use crate::error::E::{
    CouldNotOpenDataDir, DataCorrupted, DirPathIsEmpty, EmptyKey, EmptyValue, Failed2CreateDataDir,
    Failed2ReadDBDir, Failed2UpdateMemIndex, KeyNotExist, Nil,
};
use crate::error::{E, R};
use crate::index::keydir::KeyDir;
use crate::index::Indexer;
use crate::options::Options;
use crc::{Crc, CRC_32_ISO_HDLC};
use log::{error, warn};
use parking_lot::RwLock;

pub struct Engine {
    options: Arc<Options>,
    mem_index: Arc<RwLock<Box<dyn Indexer>>>,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
}

impl Engine {
    pub fn new(
        options: Arc<Options>,
        mem_index: Arc<RwLock<Box<dyn Indexer>>>,
        active_file: Arc<RwLock<DataFile>>,
        older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    ) -> Self {
        Self {
            options,
            mem_index,
            active_file,
            older_files,
        }
    }

    pub fn open(mut opts: Options) -> R<Self> {
        if let Some(e) = check_options(&mut opts) {
            return Err(e);
        }

        // 1. 校验 Options
        let opts: Options = opts.clone();
        let dir_path = opts.dir_path;
        let path = Path::new(dir_path.as_str());
        match path.try_exists() {
            Ok(exist) => {
                if !exist {
                    if let Err(e) = create_dir_all(path) {
                        warn!("failed to create data dir, err is {}", e);
                        return Err(Failed2CreateDataDir);
                    }
                }
            }
            Err(e) => {
                error!("could not open data dir, {}", e);
                return Err(CouldNotOpenDataDir);
            }
        }

        // 2. 读取所有的 Files 构建 DataFile(OlderFiles and active file)
        let mut older_files: HashMap<u32, DataFile> = HashMap::new();
        let data_files = load_data_files(dir_path)?;
        if data_files.len() > 1 {
            for i in 0..=data_files.len() - 2 {
                older_files.insert(data_files[i].file_id(), data_files[i]);
            }
        }

        let active_file = data_files[data_files.len() - 1];

        // 3. 构建内存索引，当前默认内存是 hash 表
        let mut mem_index: Box<dyn Indexer> = Box::new(KeyDir::new()) as Box<dyn Indexer>;
        // 已经是从小到大排好序的
        for data_file in data_files {
            let entry_with_metadatas = data_file.get_all_entries_with_metadata()?;
            for entry_with_metadata in entry_with_metadatas {
                let entry = entry_with_metadata.entry;
                if entry.is_tombstone() {
                    mem_index.delete(&entry.k().to_string());
                } else {
                    let meta_data = entry_with_metadata.meta_data;
                    mem_index.put(entry.k(), meta_data);
                }
            }
        }

        // 4. 构建 Engine
        let options = Arc::new(opts);
        let mem_index = Arc::new(RwLock::new(mem_index));
        let active_file = Arc::new(RwLock::new(active_file));
        let older_files = Arc::new(RwLock::new(older_files));
        let engine = Engine::new(options, mem_index, active_file, older_files);
        Ok(engine)
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
        let meta_data = mem_index_read_guard.as_ref().get(&key);

        if meta_data.is_none() {
            return Err(Nil);
        }

        let meta_data = meta_data.unwrap();
        drop(mem_index_read_guard);

        // 2. 读 file 中的 data
        let active_file_read_guard = self.active_file.read();
        let mut buf = vec![0; meta_data.entry_sz];
        let data: Vec<u8> = if active_file_read_guard.file_id() == meta_data.file_id {
            let _ = active_file_read_guard
                .read_with_given_pos(meta_data.entry_start_pos, &mut buf)
                .unwrap();
            drop(active_file_read_guard);
            buf
        } else {
            drop(active_file_read_guard);
            let older_file_read_guard = self.older_files.read();
            let target_old_file = older_file_read_guard.get(&meta_data.file_id).unwrap();
            let _ = target_old_file
                .read_with_given_pos(meta_data.entry_start_pos, &mut buf)
                .unwrap();
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
            write_guard.insert(
                curr_active_file_id,
                DataFile::new(dir_path.clone(), curr_active_file_id).unwrap(),
            );

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
        let meta_data = MetaData::new(
            active_file.file_id(),
            entry.get_self_size(),
            write_begin_pos,
            entry.tstamp(),
        );
        let mem_index_write_guard = self.mem_index.write();
        if !mem_index_write_guard.put((*entry.k()).parse().unwrap(), meta_data) {
            return Err(Failed2UpdateMemIndex);
        }
        Ok(meta_data)
    }
}

fn load_data_files(dir_path: String) -> R<Vec<DataFile>> {
    let res = fs::read_dir(Path::new(dir_path.as_str()));
    if res.is_err() {
        return Failed2ReadDBDir;
    }

    let file_ids: Vec<u32> = Vec::new();
    let mut data_files: Vec<DataFile> = Vec::new();
    for file in res.unwrap() {
        if let Ok(entry) = file {
            // 判断是否是数据文件
            if !entry
                .file_name()
                .into_string()
                .unwrap()
                .ends_with(DATA_FILE_SUFFIX)
            {
                continue;
            }
            let datafile = DataFile::create_from_full_path(entry.path(), DataFileType::OLD)?;
            data_files.push(datafile);
        }
    }

    let mut max_id = u32::MIN;

    // 从小到大排序, 找到最大 id 将类型更新为 active
    data_files.sort_by(|a, b| a.file_id().cmp(&b.file_id()));
    data_files[data_files.len() - 1].set_filetype(DataFileType::ACTIVE);
    data_files
}

fn check_options(opts: &mut Options) -> Option<E> {
    let dir_path = opts.dir_path.clone();
    if dir_path == None || dir_path.len() == 0 {
        return Some(DirPathIsEmpty);
    }

    let threshold = opts.file_threshold;
    if threshold == 0 {
        opts.file_threshold = 200 * 1024;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::keydir::KeyDir;
    use std::fs::OpenOptions;
    use std::io::Write;

    #[test]
    fn test_put_and_read() {
        let engine = get_engine();
        engine
            .put("hello".to_string(), "world".to_string().into_bytes())
            .unwrap();
        let vec = engine.read("hello".to_string()).unwrap();
        println!("{:?}", String::from_utf8(vec).unwrap());
    }

    #[test]
    fn test_delete() {
        let engine = get_engine();
        engine
            .put("hello".to_string(), "world".to_string().into_bytes())
            .unwrap();
        let vec = engine.read("hello".to_string()).unwrap();
        println!("{:?}", String::from_utf8(vec).unwrap());
        let _vec = engine.delete("hello".to_string()).unwrap();
        let vec = engine.read("hello".to_string()).unwrap_or_else(|e| {
            println!("{}", e);
            vec![]
        });
        println!("{:?}", String::from_utf8(vec).unwrap());
    }

    #[test]
    fn test_put_and_read_chinese() {
        let engine = get_engine();
        // 测试中文，将 你好-世界 改为 你好-中国
        engine
            .put("你好".to_string(), "世界".to_string().into_bytes())
            .unwrap();
        println!(
            "{:?}",
            String::from_utf8(engine.read("你好".to_string()).unwrap())
        );
        let _ = engine
            .update("你好".to_string(), "中国".to_string().into_bytes())
            .unwrap();
        println!(
            "{:?}",
            String::from_utf8(engine.read("你好".to_string()).unwrap())
        );
    }

    #[test]
    fn update() {
        let engine = get_engine();
        engine
            .put("hello".to_string(), "1".to_string().into_bytes())
            .unwrap();
        println!(
            "{:?}",
            String::from_utf8(engine.read("hello".to_string()).unwrap())
        );
        let _ = engine
            .update("hello".to_string(), "wow".to_string().into_bytes())
            .unwrap();
        println!(
            "{:?}",
            String::from_utf8(engine.read("hello".to_string()).unwrap())
        );
    }

    #[test]
    fn test_multiple_put_and_read() {
        let engine = get_engine();
        engine
            .put("hello1".to_string(), "1".to_string().into_bytes())
            .unwrap();
        engine
            .put("hello2".to_string(), "2".to_string().into_bytes())
            .unwrap();
        engine
            .put("hello3".to_string(), "三".to_string().into_bytes())
            .unwrap();
        engine
            .put("hello4".to_string(), "四".to_string().into_bytes())
            .unwrap();
        engine
            .put("hello5".to_string(), "five".to_string().into_bytes())
            .unwrap();

        let r1 = engine.read("hello1".to_string()).unwrap();
        println!("{:?}", String::from_utf8(r1));

        let r2 = engine.read("hello2".to_string()).unwrap();
        println!("{:?}", String::from_utf8(r2));

        let r3 = engine.read("hello3".to_string()).unwrap();
        println!("{:?}", String::from_utf8(r3));

        let r4 = engine.read("hello4".to_string()).unwrap();
        println!("{:?}", String::from_utf8(r4));

        let r5 = engine.read("hello5".to_string()).unwrap();
        println!("{:?}", String::from_utf8(r5));
    }

    #[test]
    fn test_multiple_delete_and_update() {
        let engine = get_engine();
        engine
            .put("hello1".to_string(), "1".to_string().into_bytes())
            .unwrap();
        engine
            .put("hello2".to_string(), "2".to_string().into_bytes())
            .unwrap();
        engine
            .put("hello3".to_string(), "三".to_string().into_bytes())
            .unwrap();
        engine
            .put("hello4".to_string(), "四".to_string().into_bytes())
            .unwrap();
        engine
            .put("hello5".to_string(), "five".to_string().into_bytes())
            .unwrap();

        let _ = engine.delete("hello1".to_string());
        let _ = engine.delete("hello3".to_string());
        let _ = engine.update("hello5".to_string(), "five-five".to_string().into_bytes());

        let r1 = engine.read("hello1".to_string()).unwrap_err();
        println!("{}", r1.to_string());

        let r2 = engine.read("hello2".to_string()).unwrap();
        println!("{:?}", String::from_utf8(r2));

        let r3 = engine.read("hello3".to_string()).unwrap_err();
        println!("{:?}", r3.to_string());

        let r4 = engine.read("hello4".to_string()).unwrap();
        println!("{:?}", String::from_utf8(r4));

        let r5 = engine.read("hello5".to_string()).unwrap();
        println!("{:?}", String::from_utf8(r5));

        let _ = engine.update("hello4".to_string(), "④".to_string().into_bytes());
        let r4 = engine.read("hello4".to_string()).unwrap();
        println!("{:?}", String::from_utf8(r4));
    }

    #[test]
    pub fn test_delete_and_put() {
        let engine = get_engine();
        engine
            .put("hello".to_string(), "世界".to_string().into_bytes())
            .unwrap();
        let old = engine.delete("hello".to_string()).unwrap();
        println!("{:?}", String::from_utf8(old));
        engine
            .put("hello".to_string(), "wow".to_string().into_bytes())
            .unwrap();
        let vec = engine.read("hello".to_string()).unwrap();
        println!("{:?}", String::from_utf8(vec));
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
