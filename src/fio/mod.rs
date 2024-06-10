pub mod file_io;

use crate::error::R;

/// IO 层接口
pub trait IOManager: Send + Sync {
    /// 从文件的给定位置开始读取数据，返回读取到的字节数
    fn read(&self, buf: &mut [u8], offset: u64) -> R<usize>;

    /// 写入字节数组到文件中，返回写入的字节数
    fn write(&self, buf: &[u8]) -> R<usize>;

    /// 持久化数据
    fn sync(&self) -> R<()>;
}