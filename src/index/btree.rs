use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::data::meta_data::MetaData;
use crate::index::Indexer;

/// 主要封装了标准库的 BTreeMap
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<String, MetaData>>>,
}

impl BTree {
    fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: String, meta_data: MetaData) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, meta_data);
        true
    }

    fn get(&self, key: &String) -> Option<MetaData> {
        let read_guard = self.tree.read();
        read_guard.get(key).copied()
    }

    fn delete(&self, key: &String) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(key);
        remove_res.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let tree = BTree::new();
        let fake_meta_data = MetaData::new(0, 1, 2, 3);
        let x = tree.put("hello".to_string(), fake_meta_data);
        assert_eq!(x, true);
    }

    #[test]
    fn test_btree_get() {
        let tree = BTree::new();
        let fake_meta_data = MetaData::new(0, 1, 2, 3);
        let k = "hello".to_string();
        let x = tree.put(k, fake_meta_data);
        assert_eq!(x, true);
        let k = "hello".to_string();
        let get_res = tree.get(&k);
        assert_eq!(get_res.unwrap(), fake_meta_data);
    }

    #[test]
    fn test_btree_delete() {
        let tree = BTree::new();
        let fake_meta_data = MetaData::new(0, 1, 2, 3);
        let k = "hello".to_string();
        let x = tree.put(k, fake_meta_data);
        assert_eq!(x, true);
        let k = "hello".to_string();
        let get_res = tree.get(&k);
        assert_eq!(get_res.unwrap(), fake_meta_data);
        let removed_data = tree.delete(&k);
        assert_eq!(removed_data, true);
        let get_res = tree.get(&k);
        assert_eq!(get_res, None);
    }
}