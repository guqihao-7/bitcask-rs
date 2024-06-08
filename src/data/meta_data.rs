// 内存索引的 value, 格式 fileid-valuesz-valuepos-tstamp，保存了 kv 在磁盘位置的基本信息
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MetaData {
    pub(crate) file_id: u32,
    pub(crate) value_sz: usize,
    pub(crate) value_pos: usize,
    pub(crate) tstamp: u64,
}

impl MetaData {
    pub(crate) fn new(file_id: u32, value_sz: usize, value_pos: usize, tstamp: u64) -> Self {
        Self {
            file_id,
            value_sz,
            value_pos,
            tstamp,
        }
    }

    pub fn file_id(&self) -> u32 {
        self.file_id
    }
    pub fn value_sz(&self) -> usize {
        self.value_sz
    }
    pub fn value_pos(&self) -> usize {
        self.value_pos
    }
    pub fn tstamp(&self) -> u64 {
        self.tstamp
    }
}