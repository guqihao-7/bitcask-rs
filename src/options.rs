pub struct Options {
    /// 数据库目录
    pub dir_path: String,

    /// 文件大小上限
    pub file_threshold: usize,

    /// 写后 sync
    pub syn_after_each_write: bool,
}