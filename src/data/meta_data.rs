// 内存索引的 value, 格式 fileid-valuesz-valuepos-tstamp，保存了 kv 在磁盘位置的基本信息
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MetaData {
    pub file_id: u32,

    // entry 的大小
    pub entry_sz: usize,

    // entry 在磁盘中开始的位置
    pub entry_start_pos: usize,
    pub tstamp: u64,
}

impl MetaData {
    pub fn new(file_id: u32, value_sz: usize, value_pos: usize, tstamp: u64) -> Self {
        Self {
            file_id,
            entry_sz: value_sz,
            entry_start_pos: value_pos,
            tstamp,
        }
    }

    pub fn file_id(&self) -> u32 {
        self.file_id
    }
    pub fn value_sz(&self) -> usize {
        self.entry_sz
    }
    pub fn value_pos(&self) -> usize {
        self.entry_start_pos
    }
    pub fn tstamp(&self) -> u64 {
        self.tstamp
    }
}