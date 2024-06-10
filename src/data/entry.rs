use std::time::{SystemTime, UNIX_EPOCH};
use crc::{Crc, CRC_32_ISO_HDLC};
use crate::error::E::{EmptyKey, EmptyValue};
use crate::error::R;

/// disk 上的表示形式 crc-tstamp-ksz-valuesz-k-v
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
    fn calculate_crc_by_vec(v: &Vec<u8>) -> u32 {
        Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(&v[..])
    }

    fn get_tstamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis()
            as u64
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
        unimplemented!()
    }

    /// 根据 Vec<u8> 解析出 entry
    pub fn decode(entry: Vec<u8>) -> Self {
        unimplemented!()
    }

    pub fn is_tombstone(&self) -> bool {
        unimplemented!()
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
}