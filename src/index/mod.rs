pub mod btree;
pub mod keydir;

use core::panic;

use crate::{data::meta_data::MetaData, options::IndexType};

// 内存中索引接口
pub trait Indexer: Send + Sync {
    /// 内存索引新增一个 metadata，对于 hashtable 而言，k 就是用户存储的 k，value 在 disk 文件上的位置被封装成了 MetaData
    fn put(&self, key: String, meta_data: MetaData) -> bool;

    /// 根据 key 取出 metadata，metadata 根据不同的内存索引含义不同，hashtable（keydir）就是在文件中的位置的封装
    fn get(&self, key: &String) -> Option<MetaData>;

    /// 根据 key 删除 metadata
    fn delete(&self, key: &String) -> bool;
}

pub fn new_indexer(index_type: IndexType) -> Box<dyn Indexer> {
    match index_type {
        IndexType::BTree => Box::new(btree::BTree::new()),
        IndexType::Hash => Box::new(keydir::KeyDir::new()),
        IndexType::SkipList => todo!(),
        _ => panic!("index type not supported now!"),
    }
}
