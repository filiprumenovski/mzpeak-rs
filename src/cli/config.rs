//! TOML configuration file support for power users.
//!
//! Instead of passing many CLI flags, users can specify settings in a config file:
//!
//! ```toml
//! # mzpeak.toml
//! [conversion]
//! compression_level = 15
//! row_group_size = 200000
//! batch_size = 2000
//! parallel = true
//! legacy = false
//! ```

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

/// Root configuration structure for mzpeak.toml files.
#[derive(Debug, Default, Deserialize)]
pub struct Config {
    /// Conversion-specific settings.
    #[serde(default)]
    pub conversion: ConversionConfig,
}

/// Configuration for the convert command.
#[derive(Debug, Default, Deserialize)]
pub struct ConversionConfig {
    /// ZSTD compression level (1-22).
    pub compression_level: Option<i32>,

    /// Number of peaks per Parquet row group.
    pub row_group_size: Option<usize>,

    /// Number of spectra to process per batch.
    pub batch_size: Option<usize>,

    /// Enable parallel decoding (requires parallel-decode feature).
    pub parallel: Option<bool>,

    /// Use legacy single-file .mzpeak.parquet format.
    pub legacy: Option<bool>,
}

impl Config {
    /// Load configuration from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        Self::from_str(&content)
    }

    /// Parse configuration from a TOML string.
    pub fn from_str(content: &str) -> Result<Self> {
        toml::from_str(content).context("Failed to parse TOML configuration")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let toml = r#"
            [conversion]
            compression_level = 15
            row_group_size = 200000
            batch_size = 2000
            parallel = true
            legacy = false
        "#;

        let config = Config::from_str(toml).unwrap();
        assert_eq!(config.conversion.compression_level, Some(15));
        assert_eq!(config.conversion.row_group_size, Some(200_000));
        assert_eq!(config.conversion.batch_size, Some(2_000));
        assert_eq!(config.conversion.parallel, Some(true));
        assert_eq!(config.conversion.legacy, Some(false));
    }

    #[test]
    fn test_partial_config() {
        let toml = r#"
            [conversion]
            compression_level = 10
        "#;

        let config = Config::from_str(toml).unwrap();
        assert_eq!(config.conversion.compression_level, Some(10));
        assert_eq!(config.conversion.row_group_size, None);
    }

    #[test]
    fn test_empty_config() {
        let config = Config::from_str("").unwrap();
        assert_eq!(config.conversion.compression_level, None);
    }
}
