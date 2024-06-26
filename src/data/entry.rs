use crate::error::E::{EmptyKey, EmptyValue};
use crate::error::R;
use crc::{Crc, CRC_32_ISO_HDLC};
use std::fmt::Display;
use std::mem;
use std::time::{SystemTime, UNIX_EPOCH};

/// disk 上的表示形式 crc-tstamp-ksz-valuesz-k-v
#[derive(Debug)]
pub struct Entry {
    crc: u32,
    tstamp: u64,
    ksz: usize,
    value_sz: usize,
    k: String,

    /// 字节数组
    v: Vec<u8>,
}

/// 正常数据 entry 和 tombstone 区分点在于
/// 1. tombstone 的 value_sz 是 0，
/// 2. v 的 len 是 0，也就是没有值
impl Entry {
    pub fn calculate_crc_by_vec(v: &Vec<u8>) -> u32 {
        Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(&v[..])
    }

    fn get_tstamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    }

    pub fn get_tombstone_with_given_key(k: String) -> R<Self> {
        if k.len() <= 0 {
            return Err(EmptyKey);
        }
        let v = Vec::with_capacity(0);
        Ok(Self::get_entry(k, v))
    }

    pub fn new(k: String, v: Vec<u8>) -> R<Self> {
        if k.len() <= 0 {
            return Err(EmptyKey);
        }

        if v.len() <= 0 {
            return Err(EmptyValue);
        }
        Ok(Self::get_entry(k, v))
    }

    fn get_entry(k: String, v: Vec<u8>) -> Self {
        let crc = Self::calculate_crc_by_vec(&v);
        let tstamp = Self::get_tstamp();
        let value_sz = v.len();
        let ksz = k.len();
        Self {
            crc,
            tstamp,
            ksz,
            value_sz,
            k,
            v,
        }
    }

    /// 将整个 entry 解析成 Vec<u8>
    pub fn encode(&self) -> Vec<u8> {
        let mut ans: Vec<u8> = Vec::new();

        // native endian
        ans.extend(&self.crc.to_ne_bytes());
        ans.extend(&self.tstamp.to_ne_bytes());
        ans.extend(&(self.ksz).to_ne_bytes());
        ans.extend(&(self.value_sz).to_ne_bytes());

        // 字符串转成字节
        ans.extend(self.k.as_bytes());
        ans.extend(&self.v[..]);
        ans
    }

    /// 根据 Vec<u8> 解析出 entry
    pub fn decode(entry: Vec<u8>) -> Self {
        let usize_bytes = mem::size_of::<usize>();
        let mut idx = 0;

        // crc 32bit=4Byte
        let crc_bytes = entry[idx..=idx + 3].to_vec();
        idx += 4;
        let crc = u32::from_ne_bytes(crc_bytes.try_into().unwrap());

        // tstamp u64bit=8Byte
        let tstamp_bytes = entry[idx..=idx + 7].to_vec();
        idx += 8;
        let tstamp = u64::from_ne_bytes(tstamp_bytes.try_into().unwrap());

        // ksz usize=mem::size_of::<usize>()
        let ksz_bytes = entry[idx..=idx + usize_bytes - 1].to_vec();
        idx += usize_bytes;
        let ksz = usize::from_ne_bytes(ksz_bytes.try_into().unwrap());

        // value_sz usize=mem::size_of::<usize>()
        let value_sz_bytes = entry[idx..=idx + usize_bytes - 1].to_vec();
        idx += usize_bytes;
        let value_sz = usize::from_ne_bytes(value_sz_bytes.try_into().unwrap());

        // k String -> 字节转字符串
        let k_bytes = entry[idx..=idx + ksz - 1].to_vec();
        idx += ksz;
        let k = String::from_utf8(k_bytes).unwrap();

        // v Vec<u8>
        let v = if value_sz == 0 {
            // tombstone
            Vec::<u8>::new()
        } else {
            entry[idx..=idx + value_sz - 1].to_vec()
        };

        Self {
            crc,
            tstamp,
            ksz,
            value_sz,
            k,
            v,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value_sz == 0 && self.v.len() == 0
    }

    pub fn crc(&self) -> u32 {
        self.crc
    }
    pub fn tstamp(&self) -> u64 {
        self.tstamp
    }
    pub fn ksz(&self) -> usize {
        self.ksz
    }
    pub fn value_sz(&self) -> usize {
        self.value_sz
    }
    pub fn k(&self) -> &str {
        &self.k
    }
    pub fn v(&self) -> &Vec<u8> {
        &self.v
    }

    pub fn get_self_size(&self) -> usize {
        let mut size = 0;
        size += mem::size_of::<u32>(); // crc
        size += mem::size_of::<u64>(); // tstamp
        size += mem::size_of::<usize>(); // ksz
        size += mem::size_of::<usize>(); // value_sz
        size += self.ksz; // k
        size += self.value_sz; // v
        size
    }
}

impl Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = format!(
            "crc: {:?}, tstamp: {:?}, ksz: {:?} bytes, value_sz: {:?} bytes, k: {:?}, v: {:?}, total size is {}",
            self.crc, self.tstamp, self.ksz, self.value_sz, self.k, self.v, self.get_self_size()
        );
        write!(f, "{}", str)
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.crc == other.crc
            && self.tstamp == other.tstamp
            && self.ksz == other.ksz
            && self.value_sz == other.value_sz
            && self.k == other.k
            && self.v == other.v
    }
}

impl Eq for Entry {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry() {
        let k = "key".to_string();
        let v = vec![1, 2, 3];
        let entry = Entry::new(k.clone(), v.clone()).unwrap();
        assert_eq!(entry.k, k);
        assert_eq!(entry.v, v);
        assert_eq!(entry.ksz, 3);
        assert_eq!(entry.value_sz, 3);
        assert_eq!(entry.crc, Entry::calculate_crc_by_vec(&v));
        assert_eq!(entry.tstamp, Entry::get_tstamp());

        let tombstone = Entry::get_tombstone_with_given_key(k.clone()).unwrap();
        assert_eq!(tombstone.k, k);
        assert_eq!(tombstone.v, vec![]);
        assert_eq!(tombstone.ksz, 3);
        assert_eq!(tombstone.value_sz, 0);
        assert_eq!(tombstone.crc, Entry::calculate_crc_by_vec(&vec![]));
        assert_eq!(tombstone.tstamp, Entry::get_tstamp());
    }

    #[test]
    fn test_encode_decode() {
        let k = "key".to_string();
        let v = vec![1, 2, 3];
        let entry = Entry::new(k.clone(), v.clone()).unwrap();
        let encoded = entry.encode();
        let decoded = Entry::decode(encoded);
        assert_eq!(decoded, entry);
    }

    #[test]
    fn test_is_tombstone() {
        let k = "key".to_string();
        let v = vec![1, 2, 3];
        let entry = Entry::new(k.clone(), v.clone()).unwrap();
        assert_eq!(entry.is_tombstone(), false);

        let tombstone = Entry::get_tombstone_with_given_key(k.clone()).unwrap();
        assert_eq!(tombstone.is_tombstone(), true);
    }

    #[test]
    fn test_get_self_size() {
        let k = "key".to_string();
        let v = vec![1, 2, 3];
        let entry = Entry::new(k.clone(), v.clone()).unwrap();
        assert_eq!(entry.get_self_size(), 32);

        let tombstone = Entry::get_tombstone_with_given_key(k.clone()).unwrap();
        assert_eq!(tombstone.get_self_size(), 24);
    }

    #[test]
    fn test_to_string() {
        let k = "key".to_string();
        let v = vec![1, 2, 3];
        let entry = Entry::new(k.clone(), v.clone()).unwrap();
        assert_eq!(
            entry.to_string(),
            format!(
                "crc: {:?} tstamp: {:?} ksz: {:?} value_sz: {:?} k: {:?} v: {:?}",
                entry.crc, entry.tstamp, entry.ksz, entry.value_sz, entry.k, entry.v
            )
        )
    }
}
