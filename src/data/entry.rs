/// disk 上的表示形式 crc-tstamp-ksz-valuesz-k-v
pub struct Entry {
    /// 墓碑，也是 commonFields
    tombstone: Tomb,

    /// 字节数组
    v: Vec<u8>,
}

pub struct Tomb {
    crc: u32,
    tstamp: u64,
    ksz: usize,
    value_sz: usize,
    k: String,
}

impl Tomb {
    pub(crate) fn new(crc: u32, tstamp: u64, ksz: usize,
                      value_sz: usize, k: String, ) -> Self {
        Self {
            crc,
            tstamp,
            ksz,
            value_sz,
            k,
        }
    }
}

impl Entry {
    pub(crate) fn new(crc: u32, tstamp: u64, ksz: usize,
                      value_sz: usize, k: String, v: Vec<u8>) -> Self {
        let tomb = Tomb::new(crc, tstamp, ksz, value_sz, k);
        Self {
            tombstone: tomb,
            v,
        }
    }

    /// 将整个 entry 解析成 Vec<u8>
    pub(crate) fn encode(&self) -> EntryType {
        unimplemented!()
    }

    /// 根据 Vec<u8> 解析出 entry
    pub(crate) fn decode(entry: Vec<u8>) -> Self {
        unimplemented!()
    }

    pub fn crc(&self) -> u32 {
        self.tombstone.crc
    }
    pub fn tstamp(&self) -> u64 {
        self.tombstone.tstamp
    }
    pub fn ksz(&self) -> usize {
        self.tombstone.ksz
    }
    pub fn value_sz(&self) -> usize {
        self.tombstone.value_sz
    }
    pub fn k(&self) -> &str {
        &self.tombstone.k
    }
    pub fn v(&self) -> &Vec<u8> {
        &self.v
    }
}

pub enum EntryType {
    /// 整个 entry 被当作字节数组
    Normal(Vec<u8>),
    Tombstone,
}