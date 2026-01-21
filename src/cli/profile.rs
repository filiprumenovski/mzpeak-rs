//! Conversion profiles for common use cases.
//!
//! Profiles provide sensible defaults for compression and performance tuning,
//! hiding low-level Parquet settings from end users.

use std::fmt;
use std::str::FromStr;

/// Conversion profiles for common use cases.
///
/// Each profile pre-configures compression level, row group size, and batch size
/// to optimize for different scenarios.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Profile {
    /// Prioritize speed over compression.
    ///
    /// - Compression: ZSTD level 1
    /// - Row group size: 50,000 peaks
    /// - Batch size: 500 spectra
    Fast,

    /// Balance between speed and compression (default).
    ///
    /// - Compression: ZSTD level 3
    /// - Row group size: 100,000 peaks
    /// - Batch size: 1,000 spectra
    #[default]
    Balanced,

    /// Maximum compression, slower conversion.
    ///
    /// - Compression: ZSTD level 15
    /// - Row group size: 200,000 peaks
    /// - Batch size: 2,000 spectra
    MaxCompression,
}

impl Profile {
    /// Returns the ZSTD compression level for this profile.
    pub fn compression_level(&self) -> i32 {
        match self {
            Profile::Fast => 1,
            Profile::Balanced => 3,
            Profile::MaxCompression => 15,
        }
    }

    /// Returns the row group size (number of peaks per row group) for this profile.
    pub fn row_group_size(&self) -> usize {
        match self {
            Profile::Fast => 50_000,
            Profile::Balanced => 100_000,
            Profile::MaxCompression => 200_000,
        }
    }

    /// Returns the batch size (number of spectra per batch) for this profile.
    pub fn batch_size(&self) -> usize {
        match self {
            Profile::Fast => 500,
            Profile::Balanced => 1_000,
            Profile::MaxCompression => 2_000,
        }
    }

    /// Returns all available profile names.
    pub fn variants() -> &'static [&'static str] {
        &["fast", "balanced", "max-compression"]
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Profile::Fast => write!(f, "fast"),
            Profile::Balanced => write!(f, "balanced"),
            Profile::MaxCompression => write!(f, "max-compression"),
        }
    }
}

impl FromStr for Profile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fast" => Ok(Profile::Fast),
            "balanced" | "default" => Ok(Profile::Balanced),
            "max-compression" | "maxcompression" | "max" => Ok(Profile::MaxCompression),
            _ => Err(format!(
                "Unknown profile '{}'. Valid options: {}",
                s,
                Profile::variants().join(", ")
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_defaults() {
        let balanced = Profile::default();
        assert_eq!(balanced, Profile::Balanced);
        assert_eq!(balanced.compression_level(), 3);
        assert_eq!(balanced.row_group_size(), 100_000);
        assert_eq!(balanced.batch_size(), 1_000);
    }

    #[test]
    fn test_profile_from_str() {
        assert_eq!(Profile::from_str("fast").unwrap(), Profile::Fast);
        assert_eq!(Profile::from_str("BALANCED").unwrap(), Profile::Balanced);
        assert_eq!(
            Profile::from_str("max-compression").unwrap(),
            Profile::MaxCompression
        );
        assert!(Profile::from_str("invalid").is_err());
    }
}
