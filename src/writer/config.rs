use std::collections::HashMap;

use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::KeyValue;
use parquet::schema::types::ColumnPath;

use crate::schema::columns;

/// Compression options for mzPeak files
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// ZSTD compression (recommended, best compression ratio)
    Zstd(i32),
    /// Snappy compression (faster, slightly larger files)
    Snappy,
    /// No compression (fastest write, largest files)
    Uncompressed,
}

impl Default for CompressionType {
    fn default() -> Self {
        // ZSTD level 3 is a good balance of speed and compression
        // For maximum compression, use Zstd(9) or higher
        Self::Zstd(3)
    }
}

impl CompressionType {
    /// Maximum compression (slower write, smallest files)
    pub fn max_compression() -> Self {
        Self::Zstd(22)
    }

    /// Balanced compression (recommended default)
    pub fn balanced() -> Self {
        Self::Zstd(3)
    }

    /// Fast compression (faster write, larger files)
    pub fn fast() -> Self {
        Self::Snappy
    }
}

/// Configuration for the mzPeak writer
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Compression type to use
    pub compression: CompressionType,

    /// Target row group size (number of rows per group)
    /// Smaller = better random access, larger = better compression
    pub row_group_size: usize,

    /// Data page size in bytes
    pub data_page_size: usize,

    /// Whether to write statistics for columns
    pub write_statistics: bool,

    /// Dictionary encoding threshold (0.0 to disable)
    pub dictionary_page_size_limit: usize,

    /// Maximum peaks per file before rotating (None = no rotation)
    pub max_peaks_per_file: Option<usize>,

    /// Enable BYTE_STREAM_SPLIT encoding for floating-point columns.
    /// This encoding significantly improves compression for scientific data
    /// (mz, intensity, ion_mobility) by grouping bytes with similar values together.
    /// Default: true
    pub use_byte_stream_split: bool,

    /// Buffer capacity for async writer pipeline (number of batches).
    /// Higher values reduce backpressure but use more memory.
    /// Default: 8
    pub async_buffer_capacity: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            // ZSTD level 9 for better compression (was level 3)
            // This is a good balance for archival storage
            // Use Zstd(3) or Snappy for faster writing if needed
            compression: CompressionType::Zstd(9),
            // 100k peaks per row group is a good balance
            row_group_size: 100_000,
            // 1MB data pages
            data_page_size: 1024 * 1024,
            write_statistics: true,
            // 1MB dictionary page limit
            dictionary_page_size_limit: 1024 * 1024,
            // Default to 50M peaks per file for sharding
            max_peaks_per_file: Some(50_000_000),
            // BYTE_STREAM_SPLIT improves compression for floating-point scientific data
            use_byte_stream_split: true,
            // Buffer 8 batches for async writer pipeline
            async_buffer_capacity: 8,
        }
    }
}

impl WriterConfig {
    /// Configuration optimized for maximum compression (slower write)
    pub fn max_compression() -> Self {
        Self {
            compression: CompressionType::Zstd(22),
            row_group_size: 500_000, // Larger row groups = better compression
            data_page_size: 2 * 1024 * 1024, // 2MB pages
            write_statistics: true,
            dictionary_page_size_limit: 2 * 1024 * 1024,
            max_peaks_per_file: Some(100_000_000),
            use_byte_stream_split: true,
            async_buffer_capacity: 8,
        }
    }

    /// Configuration optimized for fast writing (larger files)
    pub fn fast_write() -> Self {
        Self {
            compression: CompressionType::Snappy,
            row_group_size: 50_000,
            data_page_size: 512 * 1024,
            write_statistics: true,
            dictionary_page_size_limit: 512 * 1024,
            max_peaks_per_file: Some(50_000_000),
            use_byte_stream_split: true,
            async_buffer_capacity: 16, // Larger buffer for fast writes
        }
    }

    /// Balanced configuration (default)
    pub fn balanced() -> Self {
        Self::default()
    }

    /// Create writer properties from this configuration
    pub(super) fn to_writer_properties(
        &self,
        metadata: &HashMap<String, String>,
    ) -> WriterProperties {
        let compression = match self.compression {
            CompressionType::Zstd(level) => {
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default()))
            }
            CompressionType::Snappy => Compression::SNAPPY,
            CompressionType::Uncompressed => Compression::UNCOMPRESSED,
        };

        let statistics = if self.write_statistics {
            EnabledStatistics::Chunk
        } else {
            EnabledStatistics::None
        };

        let mut builder = WriterProperties::builder()
            .set_compression(compression)
            .set_data_page_size_limit(self.data_page_size)
            .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
            .set_statistics_enabled(statistics)
            .set_max_row_group_size(self.row_group_size);

        // Enable dictionary encoding for columns that benefit from it (repeated metadata)
        // These columns have the same value for all peaks in a spectrum, so dictionary
        // encoding + RLE will achieve excellent compression.
        // Note: Parquet automatically uses RLE for dictionary-encoded data.
        let dict_columns = [
            columns::SPECTRUM_ID,
            columns::SCAN_NUMBER,
            columns::MS_LEVEL,
            columns::RETENTION_TIME,
            columns::POLARITY,
            columns::PRECURSOR_MZ,
            columns::PRECURSOR_CHARGE,
            columns::PRECURSOR_INTENSITY,
            columns::ISOLATION_WINDOW_LOWER,
            columns::ISOLATION_WINDOW_UPPER,
            columns::COLLISION_ENERGY,
            columns::TOTAL_ION_CURRENT,
            columns::BASE_PEAK_MZ,
            columns::BASE_PEAK_INTENSITY,
            columns::INJECTION_TIME,
            // MSI columns also benefit from dictionary encoding (same value per spectrum)
            columns::PIXEL_X,
            columns::PIXEL_Y,
            columns::PIXEL_Z,
        ];

        for col in dict_columns {
            builder = builder.set_column_dictionary_enabled(
                ColumnPath::new(vec![col.to_string()]),
                true,
            );
        }

        // m/z, intensity, and ion_mobility columns: disable dictionary (high cardinality data)
        let float_columns = [columns::MZ, columns::INTENSITY, columns::ION_MOBILITY];
        for col in float_columns {
            builder = builder.set_column_dictionary_enabled(
                ColumnPath::new(vec![col.to_string()]),
                false,
            );
        }

        // Apply BYTE_STREAM_SPLIT encoding for floating-point scientific data columns.
        // This encoding groups bytes with similar values together (exponents, mantissas),
        // significantly improving compression ratios for correlated floating-point data.
        if self.use_byte_stream_split {
            for col in float_columns {
                builder = builder.set_column_encoding(
                    ColumnPath::new(vec![col.to_string()]),
                    Encoding::BYTE_STREAM_SPLIT,
                );
            }
        }

        // Add key-value metadata
        let kv_metadata: Vec<KeyValue> = metadata
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.clone(),
                value: Some(v.clone()),
            })
            .collect();

        builder = builder.set_key_value_metadata(Some(kv_metadata));

        builder.build()
    }

    /// Create writer properties for v2.0 peaks table (reduced columns).
    ///
    /// The v2.0 peaks table has only 3-4 columns: spectrum_id, mz, intensity,
    /// and optionally ion_mobility. All columns are high-cardinality, so we
    /// disable dictionary encoding and use specialized encodings:
    /// - DELTA_BINARY_PACKED for spectrum_id (monotonic integers)
    /// - BYTE_STREAM_SPLIT for mz, intensity, ion_mobility (floating-point)
    pub(super) fn to_peaks_v2_writer_properties(
        &self,
        metadata: &HashMap<String, String>,
    ) -> WriterProperties {
        let compression = match self.compression {
            CompressionType::Zstd(level) => {
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default()))
            }
            CompressionType::Snappy => Compression::SNAPPY,
            CompressionType::Uncompressed => Compression::UNCOMPRESSED,
        };

        let statistics = if self.write_statistics {
            EnabledStatistics::Chunk
        } else {
            EnabledStatistics::None
        };

        let mut builder = WriterProperties::builder()
            .set_compression(compression)
            .set_data_page_size_limit(self.data_page_size)
            .set_statistics_enabled(statistics)
            .set_max_row_group_size(self.row_group_size);

        // Disable dictionary encoding for all columns (high-cardinality data)
        // spectrum_id: unique per peak group, mz/intensity/ion_mobility: unique per peak
        builder = builder.set_dictionary_enabled(false);

        // Use DELTA_BINARY_PACKED for spectrum_id column
        // This encoding is optimal for monotonically increasing integers
        builder = builder.set_column_encoding(
            ColumnPath::new(vec!["spectrum_id".to_string()]),
            Encoding::DELTA_BINARY_PACKED,
        );

        // Use BYTE_STREAM_SPLIT for floating-point columns
        // This encoding groups bytes with similar values (exponents, mantissas),
        // significantly improving compression for scientific data
        if self.use_byte_stream_split {
            let float_columns = ["mz", "intensity", "ion_mobility"];
            for col in float_columns {
                builder = builder.set_column_encoding(
                    ColumnPath::new(vec![col.to_string()]),
                    Encoding::BYTE_STREAM_SPLIT,
                );
            }
        }

        // Add key-value metadata
        let kv_metadata: Vec<KeyValue> = metadata
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.clone(),
                value: Some(v.clone()),
            })
            .collect();

        builder = builder.set_key_value_metadata(Some(kv_metadata));

        builder.build()
    }

    /// Create writer properties for v2.0 spectra table (one row per spectrum).
    ///
    /// The spectra table contains metadata columns with one row per spectrum.
    /// Most columns have unique values per spectrum, so we use:
    /// - DELTA_BINARY_PACKED for spectrum_id (monotonic integers)
    /// - BYTE_STREAM_SPLIT for float columns (retention_time, precursor_mz, etc.)
    /// - Dictionary encoding only for low-cardinality columns (ms_level, polarity)
    pub(super) fn to_spectra_writer_properties(
        &self,
        metadata: &HashMap<String, String>,
    ) -> WriterProperties {
        let compression = match self.compression {
            CompressionType::Zstd(level) => {
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default()))
            }
            CompressionType::Snappy => Compression::SNAPPY,
            CompressionType::Uncompressed => Compression::UNCOMPRESSED,
        };

        let statistics = if self.write_statistics {
            EnabledStatistics::Chunk
        } else {
            EnabledStatistics::None
        };

        let mut builder = WriterProperties::builder()
            .set_compression(compression)
            .set_data_page_size_limit(self.data_page_size)
            .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
            .set_statistics_enabled(statistics)
            .set_max_row_group_size(self.row_group_size);

        // Disable dictionary encoding by default (most columns are unique per spectrum)
        builder = builder.set_dictionary_enabled(false);

        // Use DELTA_BINARY_PACKED for spectrum_id (monotonic integers)
        builder = builder.set_column_encoding(
            ColumnPath::new(vec!["spectrum_id".to_string()]),
            Encoding::DELTA_BINARY_PACKED,
        );

        // Use DELTA_BINARY_PACKED for scan_number (also monotonic integers)
        builder = builder.set_column_encoding(
            ColumnPath::new(vec!["scan_number".to_string()]),
            Encoding::DELTA_BINARY_PACKED,
        );

        // Enable dictionary encoding only for low-cardinality columns
        let dict_columns = ["ms_level", "polarity"];
        for col in dict_columns {
            builder = builder.set_column_dictionary_enabled(
                ColumnPath::new(vec![col.to_string()]),
                true,
            );
        }

        // Use BYTE_STREAM_SPLIT for floating-point columns
        if self.use_byte_stream_split {
            let float_columns = [
                "retention_time",
                "precursor_mz",
                "precursor_intensity",
                "isolation_window_lower",
                "isolation_window_upper",
                "collision_energy",
                "total_ion_current",
                "base_peak_mz",
                "base_peak_intensity",
                "injection_time",
            ];
            for col in float_columns {
                builder = builder.set_column_encoding(
                    ColumnPath::new(vec![col.to_string()]),
                    Encoding::BYTE_STREAM_SPLIT,
                );
            }
        }

        // Add key-value metadata
        let kv_metadata: Vec<KeyValue> = metadata
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.clone(),
                value: Some(v.clone()),
            })
            .collect();

        builder = builder.set_key_value_metadata(Some(kv_metadata));

        builder.build()
    }
}
