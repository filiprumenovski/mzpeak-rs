//! Lightweight raw TDF frame wrapper with deferred binary payloads for streaming decode.

use std::sync::Arc;

use timsrust::{AcquisitionType, Frame, MaldiInfo, MSLevel, QuadrupoleSettings};

/// Raw TDF frame with deferred binary payloads and minimal metadata needed for decode.
#[derive(Debug)]
pub struct RawTdfFrame {
    /// Frame index (native ID from vendor file).
    pub frame_index: usize,
    /// MS level for this frame.
    pub ms_level: MSLevel,
    /// Acquisition type (DDA/DIA/MALDI/etc.).
    pub acquisition: AcquisitionType,
    /// Retention time in seconds (derived via converter).
    pub rt_seconds: f64,
    /// Intensity correction factor applied to raw intensities.
    pub intensity_correction_factor: f64,
    /// Optional window group for DIA/PASEF.
    pub window_group: Option<u8>,
    /// Quadrupole settings for DIA/PASEF frames.
    pub quadrupole_settings: Option<Arc<QuadrupoleSettings>>,
    /// Offsets delimiting scans within the frame.
    pub scan_offsets: Vec<usize>,
    /// TOF indices per peak (delta-encoded across scans).
    pub tof_indices: Vec<u32>,
    /// Raw intensities per peak.
    pub intensities: Vec<u32>,
    /// Optional MALDI imaging metadata for the frame.
    pub maldi_info: Option<MaldiInfo>,
}

impl RawTdfFrame {
    /// Build a raw frame wrapper by moving data out of the timsrust frame.
    pub fn from_frame(frame: Frame, rt_seconds_hint: f64) -> Self {
        let Frame {
            scan_offsets,
            tof_indices,
            intensities,
            index,
            rt_in_seconds,
            acquisition_type,
            ms_level,
            quadrupole_settings,
            intensity_correction_factor,
            window_group,
            maldi_info,
        } = frame;

        let window_group = if window_group > 0 { Some(window_group) } else { None };
        // Only keep quadrupole settings when they are populated (DIA/PASEF frames).
        let quadrupole_settings = if window_group.is_some() {
            Some(quadrupole_settings)
        } else {
            None
        };

        let rt_seconds = if rt_seconds_hint.is_finite() {
            rt_seconds_hint
        } else {
            rt_in_seconds
        };

        Self {
            frame_index: index,
            ms_level,
            acquisition: acquisition_type,
            rt_seconds,
            intensity_correction_factor,
            window_group,
            quadrupole_settings,
            scan_offsets,
            tof_indices,
            intensities,
            maldi_info,
        }
    }

    /// Number of peaks in this frame.
    pub fn peak_count(&self) -> usize {
        self.intensities.len()
    }

    /// Number of scans in this frame (derived from scan offsets).
    pub fn scan_count(&self) -> usize {
        self.scan_offsets.len().saturating_sub(1)
    }
}
