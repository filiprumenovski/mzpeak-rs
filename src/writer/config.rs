//! Compression and buffer settings shared by conversion entry points.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Zstd(i32),
    Snappy,
    Uncompressed,
}
impl Default for CompressionType {
    fn default() -> Self {
        Self::Zstd(3)
    }
}
#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub compression: CompressionType,
    pub row_group_size: usize,
    pub data_page_size: usize,
    pub write_statistics: bool,
    pub dictionary_page_size_limit: usize,
    pub use_byte_stream_split: bool,
}
impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            compression: CompressionType::default(),
            row_group_size: 100000,
            data_page_size: 1024 * 1024,
            write_statistics: true,
            dictionary_page_size_limit: 1024 * 1024,
            use_byte_stream_split: true,
        }
    }
}
