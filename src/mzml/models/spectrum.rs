use std::collections::HashMap;

use crate::mzml::cv_params::CvParam;

/// Represents a single spectrum from an mzML file
#[derive(Debug, Clone, Default)]
pub struct MzMLSpectrum {
    /// Spectrum index (0-based)
    pub index: i64,

    /// Native spectrum ID from the file
    pub id: String,

    /// Default array length (number of peaks)
    pub default_array_length: usize,

    /// MS level (1 for MS1, 2 for MS2, etc.)
    pub ms_level: i16,

    /// Whether this is a centroid (true) or profile (false) spectrum
    pub centroided: bool,

    /// Polarity: 1 for positive, -1 for negative, 0 for unknown
    pub polarity: i8,

    /// Retention time in seconds
    pub retention_time: Option<f64>,

    /// Total ion current
    pub total_ion_current: Option<f64>,

    /// Base peak m/z
    pub base_peak_mz: Option<f64>,

    /// Base peak intensity
    pub base_peak_intensity: Option<f64>,

    /// Lowest observed m/z
    pub lowest_mz: Option<f64>,

    /// Highest observed m/z
    pub highest_mz: Option<f64>,

    /// Scan window lower limit
    pub scan_window_lower: Option<f64>,

    /// Scan window upper limit
    pub scan_window_upper: Option<f64>,

    /// MSI X coordinate (pixel)
    pub pixel_x: Option<i32>,

    /// MSI Y coordinate (pixel)
    pub pixel_y: Option<i32>,

    /// MSI Z coordinate (pixel, optional for 3D)
    pub pixel_z: Option<i32>,

    /// Ion injection time in milliseconds
    pub ion_injection_time: Option<f64>,

    /// Filter string (vendor-specific)
    pub filter_string: Option<String>,

    /// Preset scan configuration
    pub preset_scan_configuration: Option<i32>,

    /// Precursor information (for MS2+ spectra)
    pub precursors: Vec<Precursor>,

    /// m/z array (decoded)
    pub mz_array: Vec<f64>,

    /// Intensity array (decoded)
    pub intensity_array: Vec<f64>,

    /// Ion mobility array (decoded, optional)
    pub ion_mobility_array: Vec<f64>,

    /// Whether m/z was stored as 64-bit in source
    pub mz_precision_64bit: bool,

    /// Whether intensity was stored as 64-bit in source
    pub intensity_precision_64bit: bool,

    /// All CV parameters for this spectrum
    pub cv_params: Vec<CvParam>,

    /// User parameters
    pub user_params: HashMap<String, String>,
}

impl MzMLSpectrum {
    /// Get the scan number from the native ID
    pub fn scan_number(&self) -> Option<i64> {
        // Common formats:
        // "scan=12345"
        // "controllerType=0 controllerNumber=1 scan=12345"
        // "S12345"
        if let Some(pos) = self.id.find("scan=") {
            let start = pos + 5;
            let end = self.id[start..]
                .find(|c: char| !c.is_ascii_digit())
                .map(|i| start + i)
                .unwrap_or(self.id.len());
            self.id[start..end].parse().ok()
        } else if self.id.starts_with('S') {
            self.id[1..].parse().ok()
        } else {
            // Fall back to index + 1
            Some(self.index + 1)
        }
    }

    /// Get the number of peaks
    pub fn peak_count(&self) -> usize {
        self.mz_array.len()
    }
}

/// Precursor ion information for MS2+ spectra
#[derive(Debug, Clone, Default)]
pub struct Precursor {
    /// Reference to the precursor spectrum ID
    pub spectrum_ref: Option<String>,

    /// Isolation window target m/z
    pub isolation_window_target: Option<f64>,

    /// Isolation window lower offset
    pub isolation_window_lower: Option<f64>,

    /// Isolation window upper offset
    pub isolation_window_upper: Option<f64>,

    /// Selected ion m/z
    pub selected_ion_mz: Option<f64>,

    /// Selected ion intensity
    pub selected_ion_intensity: Option<f64>,

    /// Selected ion charge state
    pub selected_ion_charge: Option<i16>,

    /// Activation method (CID, HCD, ETD, etc.)
    pub activation_method: Option<String>,

    /// Collision energy
    pub collision_energy: Option<f64>,

    /// CV parameters for this precursor
    pub cv_params: Vec<CvParam>,
}
