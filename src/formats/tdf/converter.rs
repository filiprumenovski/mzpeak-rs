//! Conversion from Bruker TDF format to mzpeak thin-waist contract.

use std::collections::HashMap;
use std::path::Path;

#[cfg(feature = "parallel-decode")]
use rayon::prelude::*;
use timsrust::converters::{ConvertableDomain, Scan2ImConverter, Tof2MzConverter};
use timsrust::readers::PrecursorReader;
use timsrust::{MSLevel, Precursor};

use crate::dataset::{DatasetWriterV2Config, MzPeakDatasetWriterV2};
use crate::ingest::{IngestSpectrum, IngestSpectrumConverter};
use crate::metadata::{MzPeakMetadata, SourceFileInfo, VendorHints};
use crate::readers::{RawTdfFrame, TdfStreamer};
use crate::schema::manifest::Modality;
use crate::writer::{
    OptionalColumnBuf, PeakArrays, PeaksWriterV2Config, SpectraWriterConfig, SpectrumArrays,
    SpectrumV2, WriterConfig,
};

use super::error::TdfError;

/// Configuration for TDF to SpectrumArrays conversion.
pub struct TdfConversionConfig {
    /// Whether to include extended metadata (e.g., TIC/base peak prepopulation).
    pub include_extended_metadata: bool,
    /// Batch size for streaming + parallel decode.
    pub batch_size: usize,
}

impl Default for TdfConversionConfig {
    fn default() -> Self {
        Self {
            include_extended_metadata: true,
            batch_size: 256,
        }
    }
}

/// Statistics from TDF conversion.
#[derive(Default)]
pub struct TdfConversionStats {
    /// Number of spectra converted.
    pub spectra_read: usize,
    /// Total peak count processed.
    pub peaks_total: usize,
    /// Count of MS1 spectra.
    pub ms1_count: usize,
    /// Count of MS2 spectra.
    pub ms2_count: usize,
    /// Number of frames with MALDI imaging metadata.
    pub imaging_frames: usize,
}

/// Shared decode context for TDF batches.
struct DecoderContext {
    tof_to_mz: Tof2MzConverter,
    scan_to_im: Scan2ImConverter,
    include_extended_metadata: bool,
    precursors_by_frame: HashMap<usize, Vec<Precursor>>,
}

/// Raw frame plus assigned spectrum ID for ordering enforcement.
struct IndexedRawFrame {
    spectrum_id: i64,
    frame: RawTdfFrame,
}

/// Converter from Bruker TDF format to mzpeak SpectrumArrays.
pub struct TdfConverter {
    config: TdfConversionConfig,
}

impl TdfConverter {
    /// Create a new TDF converter with default configuration.
    pub fn new() -> Self {
        Self::with_config(TdfConversionConfig::default())
    }

    /// Create a new TDF converter with custom configuration.
    pub fn with_config(config: TdfConversionConfig) -> Self {
        Self { config }
    }

    /// Convert a Bruker TDF dataset to SpectrumArrays.
    pub fn try_convert<P: AsRef<Path>>(path: P) -> Result<Vec<SpectrumArrays>, TdfError> {
        let converter = Self::new();
        converter.convert(path)
    }

    /// Convert a Bruker TDF dataset to SpectrumArrays.
    pub fn convert<P: AsRef<Path>>(&self, path: P) -> Result<Vec<SpectrumArrays>, TdfError> {
        let path = path.as_ref();

        // Validate path
        if !path.exists() {
            return Err(TdfError::InvalidPath(format!(
                "Path does not exist: {}",
                path.display()
            )));
        }

        if !path.is_dir() {
            return Err(TdfError::InvalidPath(format!(
                "Not a directory: {}",
                path.display()
            )));
        }

        let mut streamer = TdfStreamer::new(path, self.config.batch_size)?;
        let (tof_to_mz, scan_to_im, _rt_conv) = streamer.converters();

        // Build precursor lookup (best-effort; absence is tolerated)
        let precursors_by_frame = PrecursorReader::new(path)
            .ok()
            .map(|reader| build_precursor_map(&reader))
            .unwrap_or_default();

        let ctx = DecoderContext {
            tof_to_mz: *tof_to_mz,
            scan_to_im: *scan_to_im,
            include_extended_metadata: self.config.include_extended_metadata,
            precursors_by_frame,
        };

        let mut ingest_converter = IngestSpectrumConverter::new();
        let mut next_spectrum_id: i64 = 0;
        let mut spectra: Vec<SpectrumArrays> = Vec::new();

        while let Some(raw_batch) = streamer.next_batch()? {
            let mut indexed: Vec<IndexedRawFrame> = Vec::with_capacity(raw_batch.len());
            for frame in raw_batch.into_iter() {
                indexed.push(IndexedRawFrame {
                    spectrum_id: next_spectrum_id,
                    frame,
                });
                next_spectrum_id += 1;
            }

            // Parallel decode if available
            #[cfg(feature = "parallel-decode")]
            let decoded: Vec<IngestSpectrum> = indexed
                .into_par_iter()
                .map(|raw| decode_raw_frame(raw, &ctx))
                .collect::<Result<_, _>>()?;

            #[cfg(not(feature = "parallel-decode"))]
            let decoded: Vec<IngestSpectrum> = indexed
                .into_iter()
                .map(|raw| decode_raw_frame(raw, &ctx))
                .collect::<Result<_, _>>()?;

            for ingest in decoded {
                let spec = ingest_converter
                    .convert(ingest)
                    .map_err(|e| TdfError::PeakConversionError(format!("{e}")))?;
                spectra.push(spec);
            }
        }

        Ok(spectra)
    }

    /// Convert a Bruker TDF dataset directly to an mzPeak v2.0 container.
    pub fn convert_to_v2_container<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: Q,
        writer_config: WriterConfig,
    ) -> Result<TdfConversionStats, TdfError> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();

        // Validate path
        if !input_path.exists() {
            return Err(TdfError::InvalidPath(format!(
                "Path does not exist: {}",
                input_path.display()
            )));
        }

        if !input_path.is_dir() {
            return Err(TdfError::InvalidPath(format!(
                "Not a directory: {}",
                input_path.display()
            )));
        }

        let mut streamer = TdfStreamer::new(input_path, self.config.batch_size)?;
        let (tof_to_mz, scan_to_im, _rt_conv) = streamer.converters();

        let precursors_by_frame = PrecursorReader::new(input_path)
            .ok()
            .map(|reader| build_precursor_map(&reader))
            .unwrap_or_default();

        let ctx = DecoderContext {
            tof_to_mz: *tof_to_mz,
            scan_to_im: *scan_to_im,
            include_extended_metadata: self.config.include_extended_metadata,
            precursors_by_frame,
        };

        let modality = Modality::from_flags(true, streamer.is_maldi());
        let metadata = build_metadata(input_path);
        let vendor_hints = metadata.vendor_hints.clone();

        let dataset_config = DatasetWriterV2Config {
            spectra_config: SpectraWriterConfig {
                compression: writer_config.compression,
                ..Default::default()
            },
            peaks_config: PeaksWriterV2Config {
                compression: writer_config.compression,
                row_group_size: writer_config.row_group_size,
                ..Default::default()
            },
        };

        let mut writer =
            MzPeakDatasetWriterV2::with_config(output_path, modality, vendor_hints, dataset_config)
                .map_err(|e| TdfError::ReadError(format!("Failed to create writer: {e}")))?;
        writer.set_metadata(metadata);

        let mut stats = TdfConversionStats::default();
        let mut ingest_converter = IngestSpectrumConverter::new();
        let mut next_spectrum_id: i64 = 0;

        while let Some(raw_batch) = streamer.next_batch()? {
            let mut indexed: Vec<IndexedRawFrame> = Vec::with_capacity(raw_batch.len());
            for frame in raw_batch.into_iter() {
                indexed.push(IndexedRawFrame {
                    spectrum_id: next_spectrum_id,
                    frame,
                });
                next_spectrum_id += 1;
            }

            #[cfg(feature = "parallel-decode")]
            let decoded: Vec<IngestSpectrum> = indexed
                .into_par_iter()
                .map(|raw| decode_raw_frame(raw, &ctx))
                .collect::<Result<_, _>>()?;

            #[cfg(not(feature = "parallel-decode"))]
            let decoded: Vec<IngestSpectrum> = indexed
                .into_iter()
                .map(|raw| decode_raw_frame(raw, &ctx))
                .collect::<Result<_, _>>()?;

            for ingest in decoded {
                let spectrum = ingest_converter
                    .convert(ingest)
                    .map_err(|e| TdfError::PeakConversionError(format!("{e}")))?;
                let spectrum_v2 = SpectrumV2::try_from_spectrum_arrays(spectrum)
                    .map_err(|e| TdfError::PeakConversionError(format!("{e}")))?;

                if spectrum_v2.peaks.ion_mobility.is_none() {
                    return Err(TdfError::PeakConversionError(
                        "ion mobility missing for TDF spectrum".to_string(),
                    ));
                }

                if modality.has_imaging() {
                    if spectrum_v2.metadata.pixel_x.is_none()
                        || spectrum_v2.metadata.pixel_y.is_none()
                    {
                        return Err(TdfError::PeakConversionError(
                            "pixel coordinates missing for MALDI imaging dataset".to_string(),
                        ));
                    }
                } else if spectrum_v2.metadata.pixel_x.is_some()
                    || spectrum_v2.metadata.pixel_y.is_some()
                    || spectrum_v2.metadata.pixel_z.is_some()
                {
                    return Err(TdfError::PeakConversionError(
                        "imaging coordinates present for non-imaging dataset".to_string(),
                    ));
                }

                writer
                    .write_spectrum(&spectrum_v2)
                    .map_err(|e| TdfError::ReadError(format!("Failed to write spectrum: {e}")))?;

                stats.spectra_read += 1;
                stats.peaks_total += spectrum_v2.peaks.len();
                match spectrum_v2.metadata.ms_level {
                    1 => stats.ms1_count += 1,
                    2 => stats.ms2_count += 1,
                    _ => {}
                }
                if spectrum_v2.metadata.pixel_x.is_some()
                    && spectrum_v2.metadata.pixel_y.is_some()
                {
                    stats.imaging_frames += 1;
                }
            }
        }

        writer
            .close()
            .map_err(|e| TdfError::ReadError(format!("Failed to finalize dataset: {e}")))?;

        Ok(stats)
    }
}

impl Default for TdfConverter {
    fn default() -> Self {
        Self::new()
    }
}

fn decode_raw_frame(raw: IndexedRawFrame, ctx: &DecoderContext) -> Result<IngestSpectrum, TdfError> {
    let IndexedRawFrame { spectrum_id, frame } = raw;

    let peak_count = frame.peak_count();
    if peak_count == 0 {
        return Err(TdfError::PeakConversionError(
            "Frame has no peaks".to_string(),
        ));
    }

    if frame.tof_indices.len() != peak_count {
        return Err(TdfError::PeakConversionError(format!(
            "TOF count ({}) != intensity count ({peak_count})",
            frame.tof_indices.len()
        )));
    }

    let mut mz_values: Vec<f64> = Vec::with_capacity(peak_count);
    let mut intensities: Vec<f32> = Vec::with_capacity(peak_count);
    let mut ion_mobility: Vec<f64> = vec![0.0; peak_count];

    // Convert TOF -> m/z and intensity correction
    for (&tof_idx, &intensity) in frame.tof_indices.iter().zip(frame.intensities.iter()) {
        mz_values.push(ctx.tof_to_mz.convert(tof_idx));
        intensities.push((intensity as f64 * frame.intensity_correction_factor) as f32);
    }

    // Expand scan -> ion mobility across peaks using scan offsets, with bounds checks
    let scan_count = frame.scan_count();
    for scan_idx in 0..scan_count {
        let start = frame.scan_offsets[scan_idx];
        let end = frame.scan_offsets[scan_idx + 1];

        if end > peak_count || start > end {
            return Err(TdfError::MobilityConversionError(format!(
                "Scan offsets out of bounds: start={}, end={}, peaks={peak_count}",
                start, end
            )));
        }

        let im_val = ctx.scan_to_im.convert(scan_idx as u32);
        ion_mobility[start..end].fill(im_val);
    }

    let ion_mobility = OptionalColumnBuf::AllPresent(ion_mobility);

    let ms_level: i16 = match frame.ms_level {
        MSLevel::MS1 => 1,
        MSLevel::MS2 => 2,
        MSLevel::Unknown => 0,
    };

    // MALDI spatial metadata
    let (pixel_x, pixel_y) = frame
        .maldi_info
        .as_ref()
        .map(|m| (Some(m.pixel_x as i32), Some(m.pixel_y as i32)))
        .unwrap_or((None, None));

    // Precursor / isolation information (best-effort)
    let mut precursor_mz = None;
    let mut precursor_charge = None;
    let mut precursor_intensity = None;
    let mut isolation_window_lower = None;
    let mut isolation_window_upper = None;
    let mut collision_energy = None;

    if ms_level >= 2 {
        if let Some(precursors) = ctx.precursors_by_frame.get(&frame.frame_index) {
            if let Some(prec) = precursors.first() {
                precursor_mz = Some(prec.mz);
                precursor_charge = prec.charge.map(|c| c as i16);
                precursor_intensity = prec.intensity.map(|i| i as f32);
            }
        }

        if let Some(qs) = frame.quadrupole_settings.as_ref() {
            if let Some(center) = qs.isolation_mz.first() {
                let width = qs.isolation_width.first().copied().unwrap_or_default();
                precursor_mz.get_or_insert(*center);
                isolation_window_lower = Some((width / 2.0) as f32);
                isolation_window_upper = Some((width / 2.0) as f32);
            }
            if let Some(ce) = qs.collision_energy.first() {
                collision_energy = Some(*ce as f32);
            }
        }
    }

    // Optional precomputed TIC/BPC
    let (total_ion_current, base_peak_mz, base_peak_intensity) = if ctx.include_extended_metadata {
        if let Some((max_idx, max_intensity)) = intensities
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        {
            (
                Some(intensities.iter().map(|v| *v as f64).sum()),
                mz_values.get(max_idx).copied(),
                Some(*max_intensity),
            )
        } else {
            (None, None, None)
        }
    } else {
        (None, None, None)
    };

    let peaks = PeakArrays {
        mz: mz_values,
        intensity: intensities,
        ion_mobility,
    };

    Ok(IngestSpectrum {
        spectrum_id,
        scan_number: frame.frame_index as i64,
        ms_level,
        retention_time: frame.rt_seconds as f32,
        polarity: 0,
        precursor_mz,
        precursor_charge,
        precursor_intensity,
        isolation_window_lower,
        isolation_window_upper,
        collision_energy,
        total_ion_current,
        base_peak_mz,
        base_peak_intensity,
        injection_time: None,
        pixel_x,
        pixel_y,
        pixel_z: None,
        peaks,
    })
}

fn build_precursor_map(reader: &PrecursorReader) -> HashMap<usize, Vec<Precursor>> {
    let mut map: HashMap<usize, Vec<Precursor>> = HashMap::new();
    for idx in 0..reader.len() {
        if let Some(prec) = reader.get(idx) {
            map.entry(prec.frame_index).or_default().push(prec);
        }
    }
    map
}

fn build_metadata(path: &Path) -> MzPeakMetadata {
    let mut metadata = MzPeakMetadata::new();

    let mut source = SourceFileInfo::new(
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown"),
    );
    source.path = path.to_str().map(|s| s.to_string());
    source.format = Some("Bruker TDF".to_string());
    metadata.source_file = Some(source);

    let vendor_hints = VendorHints::new("Bruker Daltonics")
        .with_format("bruker_tdf")
        .with_conversion_path(vec!["bruker_tdf".to_string(), "mzpeak".to_string()]);
    metadata.vendor_hints = Some(vendor_hints);

    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use timsrust::converters::{Scan2ImConverter, Tof2MzConverter};
    use timsrust::{AcquisitionType, MaldiInfo, QuadrupoleSettings};

    fn dummy_ctx(include_extended_metadata: bool) -> DecoderContext {
        DecoderContext {
            tof_to_mz: Tof2MzConverter::from_boundaries(100.0, 200.0, 1),
            scan_to_im: Scan2ImConverter::from_boundaries(0.7, 1.1, 1),
            include_extended_metadata,
            precursors_by_frame: HashMap::new(),
        }
    }

    fn raw_frame_basic() -> RawTdfFrame {
        RawTdfFrame {
            frame_index: 1,
            ms_level: MSLevel::MS1,
            acquisition: AcquisitionType::Unknown,
            rt_seconds: 12.3,
            intensity_correction_factor: 2.0,
            window_group: None,
            quadrupole_settings: None,
            scan_offsets: vec![0, 2],
            tof_indices: vec![0, 1],
            intensities: vec![100, 200],
            maldi_info: None,
        }
    }

    #[test]
    fn decode_basic_ms1_with_mobility_and_stats() {
        let ctx = dummy_ctx(true);
        let raw = IndexedRawFrame {
            spectrum_id: 5,
            frame: raw_frame_basic(),
        };

        let ingest = decode_raw_frame(raw, &ctx).expect("decode should succeed");
        assert_eq!(ingest.spectrum_id, 5);
        assert_eq!(ingest.ms_level, 1);
        assert_eq!(ingest.scan_number, 1);
        assert_eq!(ingest.retention_time, 12.3_f32);
        assert_eq!(ingest.peaks.mz.len(), 2);
        assert_eq!(ingest.peaks.intensity, vec![200.0, 400.0]);

        // Ion mobility present for each peak
        match &ingest.peaks.ion_mobility {
            OptionalColumnBuf::AllPresent(im) => {
                assert_eq!(im.len(), 2);
            }
            _ => panic!("expected ion mobility values"),
        }

        // Stats present when include_extended_metadata=true
        assert!(ingest.total_ion_current.is_some());
        assert!(ingest.base_peak_intensity.is_some());
    }

    #[test]
    fn decode_ms2_with_precursor_and_isolation() {
        let mut ctx = dummy_ctx(false);
        ctx.precursors_by_frame.insert(
            2,
            vec![Precursor {
                mz: 555.5,
                rt: 0.0,
                im: 0.0,
                charge: Some(3),
                intensity: Some(1234.0),
                index: 1,
                frame_index: 2,
            }],
        );

        let mut quad = QuadrupoleSettings::default();
        quad.isolation_mz.push(600.0);
        quad.isolation_width.push(1.0);
        quad.collision_energy.push(27.5);

        let raw = IndexedRawFrame {
            spectrum_id: 9,
            frame: RawTdfFrame {
                ms_level: MSLevel::MS2,
                acquisition: AcquisitionType::DDAPASEF,
                frame_index: 2,
                rt_seconds: 30.0,
                intensity_correction_factor: 1.0,
                window_group: Some(1),
                quadrupole_settings: Some(Arc::new(quad)),
                scan_offsets: vec![0, 1],
                tof_indices: vec![0],
                intensities: vec![100],
                maldi_info: None,
            },
        };

        let ingest = decode_raw_frame(raw, &ctx).expect("decode should succeed");
        assert_eq!(ingest.ms_level, 2);
        assert_eq!(ingest.precursor_mz, Some(555.5));
        assert_eq!(ingest.precursor_charge, Some(3));
        assert_eq!(ingest.precursor_intensity, Some(1234.0_f32));
        assert_eq!(ingest.isolation_window_lower, Some(0.5));
        assert_eq!(ingest.isolation_window_upper, Some(0.5));
        assert_eq!(ingest.collision_energy, Some(27.5_f32));
    }

    #[test]
    fn decode_maldi_pixels_mapped() {
        let ctx = dummy_ctx(false);
        let raw = IndexedRawFrame {
            spectrum_id: 1,
            frame: RawTdfFrame {
                ms_level: MSLevel::MS1,
                acquisition: AcquisitionType::Unknown,
                frame_index: 0,
                rt_seconds: 1.0,
                intensity_correction_factor: 1.0,
                window_group: None,
                quadrupole_settings: None,
                scan_offsets: vec![0, 1],
                tof_indices: vec![0],
                intensities: vec![10],
                maldi_info: Some(MaldiInfo {
                    spot_name: "A1".to_string(),
                    pixel_x: 5,
                    pixel_y: 7,
                    position_x_um: Some(10.0),
                    position_y_um: Some(20.0),
                    laser_power: None,
                    laser_rep_rate: None,
                    laser_shots: None,
                }),
            },
        };

        let ingest = decode_raw_frame(raw, &ctx).expect("decode should succeed");
        assert_eq!(ingest.pixel_x, Some(5));
        assert_eq!(ingest.pixel_y, Some(7));
    }
}
