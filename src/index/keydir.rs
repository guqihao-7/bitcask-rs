use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::data::meta_data::MetaData;
use crate::index::Indexer;

pub struct KeyDir {
    hash_table: Arc<RwLock<HashMap<String, MetaData>>>,
}

impl KeyDir {
    pub fn new() -> Self {
        Self {
            hash_table: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Indexer for KeyDir {
    fn put(&self, key: String, meta_data: MetaData) -> bool {
        let mut write_guard = self.hash_table.write();
        write_guard.insert(key, meta_data);
        true
    }

    fn get(&self, key: &String) -> Option<MetaData> {
        let read_guard = self.hash_table.read();
        read_guard.get(key).copied()
    }

    fn delete(&self, key: &String) -> bool {
        let mut write_guard = self.hash_table.write();
        let remove_res = write_guard.remove(key);
        remove_res.is_some()
    }
}

#[cfg(test)]
mod tests {
    use crate::index::keydir::KeyDir;

    use super::*;

    #[test]
    fn test_btree_put() {
        let keydir = KeyDir::new();
        let fake_meta_data = MetaData::new(0, 1, 2, 3);
        let x = keydir.put("hello".to_string(), fake_meta_data);
        assert_eq!(x, true);
    }

    #[test]
    fn test_btree_get() {
        let keydir = KeyDir::new();
        let fake_meta_data = MetaData::new(0, 1, 2, 3);
        let k = "hello".to_string();
        let x = keydir.put(k, fake_meta_data);
        assert_eq!(x, true);
        let k = "hello".to_string();
        let get_res = keydir.get(&k);
        assert_eq!(get_res.unwrap(), fake_meta_data);
    }

    #[test]
    fn test_btree_delete() {
        let keydir = KeyDir::new();
        let fake_meta_data = MetaData::new(0, 1, 2, 3);
        let k = "hello".to_string();
        let x = keydir.put(k, fake_meta_data);
        assert_eq!(x, true);
        let k = "hello".to_string();
        let get_res = keydir.get(&k);
        assert_eq!(get_res.unwrap(), fake_meta_data);
        let removed_data = keydir.delete(&k);
        assert_eq!(removed_data, true);
        let get_res = keydir.get(&k);
        assert_eq!(get_res, None);
    }
}