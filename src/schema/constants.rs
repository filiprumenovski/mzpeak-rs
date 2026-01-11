/// HUPO-PSI MS CV namespace prefix
pub const MS_CV_PREFIX: &str = "MS";

/// mzPeak format version - follows semantic versioning
pub const MZPEAK_FORMAT_VERSION: &str = "1.0.0";

/// File extension for mzPeak files (legacy single-file format)
pub const MZPEAK_EXTENSION: &str = ".mzpeak.parquet";

/// MIME type for mzPeak container files (public for use in validator and dataset modules)
pub const MZPEAK_MIMETYPE: &str = "application/vnd.mzpeak";

/// Metadata key for format version in Parquet footer
pub const KEY_FORMAT_VERSION: &str = "mzpeak:format_version";

/// Metadata key for SDRF metadata in Parquet footer
pub const KEY_SDRF_METADATA: &str = "mzpeak:sdrf_metadata";

/// Metadata key for instrument configuration in Parquet footer
pub const KEY_INSTRUMENT_CONFIG: &str = "mzpeak:instrument_config";

/// Metadata key for LC configuration in Parquet footer
pub const KEY_LC_CONFIG: &str = "mzpeak:lc_config";

/// Metadata key for run-level technical parameters in Parquet footer
pub const KEY_RUN_PARAMETERS: &str = "mzpeak:run_parameters";

/// Metadata key for source file information
pub const KEY_SOURCE_FILE: &str = "mzpeak:source_file";

/// Metadata key for conversion timestamp
pub const KEY_CONVERSION_TIMESTAMP: &str = "mzpeak:conversion_timestamp";

/// Metadata key for converter software info
pub const KEY_CONVERTER_INFO: &str = "mzpeak:converter_info";

/// Metadata key for data processing history
pub const KEY_PROCESSING_HISTORY: &str = "mzpeak:processing_history";

/// Metadata key for checksum of original raw file
pub const KEY_RAW_FILE_CHECKSUM: &str = "mzpeak:raw_file_checksum";
