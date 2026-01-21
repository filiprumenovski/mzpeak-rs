/// Column names as constants for type safety
/// Unique spectrum identifier
pub const SPECTRUM_ID: &str = "spectrum_id";
/// Native scan number from the instrument
pub const SCAN_NUMBER: &str = "scan_number";
/// MS level (1 for MS1, 2 for MS/MS, etc.)
pub const MS_LEVEL: &str = "ms_level";
/// Retention time in seconds
pub const RETENTION_TIME: &str = "retention_time";
/// Polarity (1 for positive, -1 for negative)
pub const POLARITY: &str = "polarity";
/// Mass-to-charge ratio (MS:1000040)
pub const MZ: &str = "mz";
/// Peak intensity (MS:1000042)
pub const INTENSITY: &str = "intensity";
/// Ion mobility drift time in milliseconds (MS:1002476)
pub const ION_MOBILITY: &str = "ion_mobility";
/// Precursor m/z for MS2+ spectra
pub const PRECURSOR_MZ: &str = "precursor_mz";
/// Precursor charge state
pub const PRECURSOR_CHARGE: &str = "precursor_charge";
/// Precursor intensity
pub const PRECURSOR_INTENSITY: &str = "precursor_intensity";
/// Lower isolation window offset
pub const ISOLATION_WINDOW_LOWER: &str = "isolation_window_lower";
/// Upper isolation window offset
pub const ISOLATION_WINDOW_UPPER: &str = "isolation_window_upper";
/// Collision energy in eV
pub const COLLISION_ENERGY: &str = "collision_energy";
/// Total ion current
pub const TOTAL_ION_CURRENT: &str = "total_ion_current";
/// Base peak m/z
pub const BASE_PEAK_MZ: &str = "base_peak_mz";
/// Base peak intensity
pub const BASE_PEAK_INTENSITY: &str = "base_peak_intensity";
/// Ion injection time in milliseconds
pub const INJECTION_TIME: &str = "injection_time";

// MSI (Mass Spectrometry Imaging) spatial columns
/// X coordinate position for imaging data (pixels)
pub const PIXEL_X: &str = "pixel_x";
/// Y coordinate position for imaging data (pixels)
pub const PIXEL_Y: &str = "pixel_y";
/// Z coordinate position for 3D imaging data (pixels, optional)
pub const PIXEL_Z: &str = "pixel_z";

// =============================================================================
// v2.0 Schema Column Constants
// =============================================================================
// The v2.0 peaks schema is simplified to just 4 columns (3 for 3D datasets):
// - spectrum_id (UInt32) - DELTA_BINARY_PACKED encoding
// - mz (Float64) - BYTE_STREAM_SPLIT encoding
// - intensity (Float32) - BYTE_STREAM_SPLIT encoding
// - ion_mobility (Float64, optional) - BYTE_STREAM_SPLIT encoding

/// Spectrum ID for v2.0 schema (UInt32 type, uses DELTA_BINARY_PACKED encoding)
pub const SPECTRUM_ID_V2: &str = "spectrum_id";
