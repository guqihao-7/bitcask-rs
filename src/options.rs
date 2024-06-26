#[derive(Debug, Clone)]
pub struct Options {
    /// 数据库目录路径
    pub dir_path: String,

    /// 文件大小上限, 字节为单位, 默认 200MB
    pub file_threshold: usize,

    /// 每次写后是否 sync
    pub syn_after_each_write: bool,
}
