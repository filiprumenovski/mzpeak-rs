//! Parallel TDF to Parquet converter with sharded output.
//!
//! This module provides a high-performance parallel conversion pipeline that:
//! - Partitions the input dataset across N workers
//! - Each worker writes to its own Parquet shard file
//! - Achieves near-linear scaling with CPU cores
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │ TdfStreamer │────▶│ partition() │────▶│  Worker 0   │───▶ shard_0.parquet
//! │  (shared)   │     │             │     │  Worker 1   │───▶ shard_1.parquet
//! │             │     │             │     │  Worker N   │───▶ shard_n.parquet
//! └─────────────┘     └─────────────┘     └─────────────┘
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rayon::prelude::*;
use timsrust::converters::{ConvertableDomain, Scan2ImConverter, Tof2MzConverter};
use timsrust::readers::{FrameReader, PrecursorReader};
use timsrust::{MSLevel, Precursor};

use crate::ingest::{IngestSpectrum, IngestSpectrumConverter};
use crate::metadata::MzPeakMetadata;
use crate::readers::{FramePartition, RawTdfFrame, TdfStreamer};
use crate::writer::{MzPeakWriter, OptionalColumnBuf, PeakArrays, WriterConfig};

use super::error::TdfError;

/// Configuration for parallel TDF conversion.
#[derive(Clone)]
pub struct ParallelConversionConfig {
    /// Number of worker threads (defaults to available parallelism)
    pub num_workers: usize,
    /// Whether to include extended metadata (TIC, base peak, etc.)
    pub include_extended_metadata: bool,
    /// Parquet writer configuration
    pub writer_config: WriterConfig,
    /// Whether to merge shards into a single file after conversion
    pub merge_shards: bool,
}

impl Default for ParallelConversionConfig {
    fn default() -> Self {
        Self {
            num_workers: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4),
            include_extended_metadata: true,
            writer_config: WriterConfig::default(),
            merge_shards: false,
        }
    }
}

/// Statistics from parallel conversion.
#[derive(Debug, Default)]
pub struct ParallelConversionStats {
    /// Total spectra written across all shards
    pub total_spectra: usize,
    /// Total peaks written across all shards
    pub total_peaks: usize,
    /// Per-shard statistics
    pub shard_stats: Vec<ShardStats>,
    /// Wall-clock time for conversion
    pub elapsed_seconds: f64,
}

/// Statistics for a single shard.
#[derive(Debug, Clone)]
pub struct ShardStats {
    /// Shard identifier (0-indexed)
    pub shard_id: usize,
    /// Number of spectra written to this shard
    pub spectra_written: usize,
    /// Number of peaks written to this shard
    pub peaks_written: usize,
    /// Path to the shard file
    pub path: PathBuf,
}

/// Shared context for parallel decode workers.
struct SharedDecodeContext {
    tof_to_mz: Tof2MzConverter,
    scan_to_im: Scan2ImConverter,
    include_extended_metadata: bool,
    precursors_by_frame: HashMap<usize, Vec<Precursor>>,
    rt_converter: Arc<timsrust::converters::Frame2RtConverter>,
}

/// Parallel TDF to Parquet converter.
pub struct ParallelTdfConverter {
    config: ParallelConversionConfig,
}

impl ParallelTdfConverter {
    /// Create a new parallel converter with default configuration.
    pub fn new() -> Self {
        Self::with_config(ParallelConversionConfig::default())
    }

    /// Create a new parallel converter with custom configuration.
    pub fn with_config(config: ParallelConversionConfig) -> Self {
        Self { config }
    }

    /// Convert a TDF dataset to sharded Parquet files.
    ///
    /// # Arguments
    /// * `input_path` - Path to the `.d` directory
    /// * `output_dir` - Directory where shard files will be written
    ///
    /// # Returns
    /// Conversion statistics including per-shard details
    pub fn convert<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_dir: Q,
    ) -> Result<ParallelConversionStats, TdfError> {
        let input_path = input_path.as_ref();
        let output_dir = output_dir.as_ref();
        let start_time = std::time::Instant::now();

        // Validate input
        if !input_path.exists() {
            return Err(TdfError::InvalidPath(format!(
                "Path does not exist: {}",
                input_path.display()
            )));
        }

        // Create output directory
        fs::create_dir_all(output_dir).map_err(|e| {
            TdfError::InvalidPath(format!(
                "Failed to create output directory {}: {}",
                output_dir.display(),
                e
            ))
        })?;

        // Initialize streamer and get converters
        let streamer = TdfStreamer::new(input_path, 256)?;
        let (tof_to_mz, scan_to_im, _rt_conv) = streamer.converters();

        // Build precursor lookup
        let precursors_by_frame = PrecursorReader::new(input_path)
            .ok()
            .map(|reader| build_precursor_map(&reader))
            .unwrap_or_default();

        // Create shared context (Arc for thread safety)
        let ctx = Arc::new(SharedDecodeContext {
            tof_to_mz: *tof_to_mz,
            scan_to_im: *scan_to_im,
            include_extended_metadata: self.config.include_extended_metadata,
            precursors_by_frame,
            rt_converter: streamer.rt_converter(),
        });

        // Partition the dataset
        let partitions = streamer.partition(self.config.num_workers);
        let num_partitions = partitions.len();

        println!(
            "🚀 Starting parallel conversion with {} workers ({} frames)",
            num_partitions,
            streamer.len()
        );

        // Calculate spectrum ID offsets for each partition
        let mut spectrum_offsets: Vec<i64> = Vec::with_capacity(partitions.len());
        let mut offset: i64 = 0;
        for partition in &partitions {
            spectrum_offsets.push(offset);
            offset += (partition.range.end - partition.range.start) as i64;
        }

        // Store input path for per-worker FrameReader creation
        let input_path_arc = Arc::new(input_path.to_path_buf());
        let metadata = MzPeakMetadata::default();
        let writer_config = self.config.writer_config.clone();

        // Process partitions in parallel - each worker gets its own FrameReader
        let shard_results: Vec<Result<ShardStats, TdfError>> = partitions
            .into_par_iter()
            .enumerate()
            .map(|(shard_id, partition)| {
                let shard_path = output_dir.join(format!("shard_{:04}.parquet", shard_id));
                let spectrum_id_offset = spectrum_offsets[shard_id];

                process_shard(
                    shard_id,
                    &input_path_arc,  // Pass path instead of shared streamer
                    &ctx,
                    partition,
                    spectrum_id_offset,
                    &shard_path,
                    &metadata,
                    &writer_config,
                )
            })
            .collect();

        // Aggregate results
        let mut stats = ParallelConversionStats::default();
        for result in shard_results {
            let shard_stat = result?;
            stats.total_spectra += shard_stat.spectra_written;
            stats.total_peaks += shard_stat.peaks_written;
            stats.shard_stats.push(shard_stat);
        }

        stats.elapsed_seconds = start_time.elapsed().as_secs_f64();

        // Optionally merge shards
        if self.config.merge_shards {
            let merged_path = output_dir.join("merged.mzpeak");
            merge_shards(&stats.shard_stats, &merged_path)?;
            println!("📦 Merged shards into: {}", merged_path.display());
        }

        Ok(stats)
    }
}

impl Default for ParallelTdfConverter {
    fn default() -> Self {
        Self::new()
    }
}

/// Process a single shard: read frames, decode, write to Parquet.
/// Each worker creates its own FrameReader to avoid lock contention.
fn process_shard(
    shard_id: usize,
    input_path: &Arc<PathBuf>,  // Path for creating per-worker FrameReader
    ctx: &Arc<SharedDecodeContext>,
    partition: FramePartition,
    spectrum_id_offset: i64,
    output_path: &Path,
    metadata: &MzPeakMetadata,
    writer_config: &WriterConfig,
) -> Result<ShardStats, TdfError> {
    // Create per-worker FrameReader - no lock contention!
    let frame_reader = FrameReader::new(input_path.as_ref())
        .map_err(|e| TdfError::ReadError(format!("Failed to create FrameReader: {e}")))?;

    let range_start = partition.range.start;
    let range_end = partition.range.end;

    // Create shard writer
    let mut writer = MzPeakWriter::new_file(output_path, metadata, writer_config.clone())
        .map_err(|e| TdfError::ReadError(format!("Failed to create shard writer: {e}")))?;

    // Create converter for this shard
    let mut ingest_converter = IngestSpectrumConverter::new();
    let mut local_spectrum_id: i64 = spectrum_id_offset;
    let target_peaks_per_batch = writer_config.row_group_size.max(1);
    let mut batch: Vec<crate::writer::SpectrumArrays> = Vec::with_capacity(256);
    let mut batch_peaks: usize = 0;

    // Process frames sequentially within this shard
    for frame_idx in range_start..range_end {
        match frame_reader.get(frame_idx) {
            Ok(frame) => {
                // Derive RT using shared converter
                let rt_seconds = if frame.index < frame_reader.len() {
                    ctx.rt_converter.convert(frame.index as u32)
                } else {
                    frame.rt_in_seconds
                };
                let raw_frame = RawTdfFrame::from_frame(frame, rt_seconds);

                let ingest = decode_raw_frame(local_spectrum_id, raw_frame, ctx)?;
                let spectrum = ingest_converter
                    .convert(ingest)
                    .map_err(|e| TdfError::PeakConversionError(format!("{e}")))?;
                batch_peaks += spectrum.peak_count();
                batch.push(spectrum);
                if batch_peaks >= target_peaks_per_batch {
                    writer
                        .write_spectra_owned(batch)
                        .map_err(|e| TdfError::ReadError(format!("Failed to write batch: {e}")))?;
                    batch = Vec::with_capacity(256);
                    batch_peaks = 0;
                }
                local_spectrum_id += 1;
            }
            Err(e) => {
                let err_str = format!("{e}");
                if err_str.contains("Decompression") {
                    eprintln!("⚠️  Skipping frame {} (decompression error): {}", frame_idx, e);
                    continue;
                }
                return Err(TdfError::FrameParsingError(format!(
                    "Failed to read frame {frame_idx}: {e}"
                )));
            }
        }
    }

    if !batch.is_empty() {
        writer
            .write_spectra_owned(batch)
            .map_err(|e| TdfError::ReadError(format!("Failed to write final batch: {e}")))?;
    }

    // Finish writing
    let stats = writer
        .finish()
        .map_err(|e| TdfError::ReadError(format!("Failed to finish shard: {e}")))?;

    Ok(ShardStats {
        shard_id,
        spectra_written: stats.spectra_written,
        peaks_written: stats.peaks_written,
        path: output_path.to_path_buf(),
    })
}

/// Decode a raw TDF frame into an IngestSpectrum.
fn decode_raw_frame(
    spectrum_id: i64,
    frame: RawTdfFrame,
    ctx: &SharedDecodeContext,
) -> Result<IngestSpectrum, TdfError> {
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

    // Calculate stats inline during decode
    let mut tic: f64 = 0.0;
    let mut max_intensity: f32 = 0.0;
    let mut max_mz: f64 = 0.0;

    // Convert TOF -> m/z and apply intensity correction
    for (&tof_idx, &intensity) in frame.tof_indices.iter().zip(frame.intensities.iter()) {
        let mz = ctx.tof_to_mz.convert(tof_idx);
        let corrected_intensity = (intensity as f64 * frame.intensity_correction_factor) as f32;
        
        mz_values.push(mz);
        intensities.push(corrected_intensity);

        // Update stats
        tic += corrected_intensity as f64;
        if corrected_intensity > max_intensity {
            max_intensity = corrected_intensity;
            max_mz = mz;
        }
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

    // Precursor / isolation information
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

    // MALDI spatial metadata
    let (pixel_x, pixel_y) = frame
        .maldi_info
        .as_ref()
        .map(|m| (Some(m.pixel_x as i32), Some(m.pixel_y as i32)))
        .unwrap_or((None, None));

    Ok(IngestSpectrum {
        spectrum_id,
        scan_number: frame.frame_index as i64,
        ms_level,
        retention_time: frame.rt_seconds as f32, 
        polarity: 1, 
        precursor_mz,
        precursor_charge,
        precursor_intensity,
        isolation_window_lower,
        isolation_window_upper,
        collision_energy,
        total_ion_current: Some(tic),         // Set pre-calculated TIC
        base_peak_mz: Some(max_mz),           // Set pre-calculated BPC m/z
        base_peak_intensity: Some(max_intensity), // Set pre-calculated BPC intensity
        injection_time: None,
        pixel_x,
        pixel_y,
        pixel_z: None,
        peaks: PeakArrays {
            mz: mz_values,
            intensity: intensities,
            ion_mobility,
        },
    })
}

/// Build a map of frame ID -> precursors for MS2 annotation.
fn build_precursor_map(reader: &PrecursorReader) -> HashMap<usize, Vec<Precursor>> {
    let mut map: HashMap<usize, Vec<Precursor>> = HashMap::new();
    for idx in 0..reader.len() {
        if let Some(prec) = reader.get(idx) {
            map.entry(prec.frame_index).or_default().push(prec);
        }
    }
    map
}

/// Merge multiple Parquet shard files into a single file.
/// This is a simple concatenation - all shards are read and written to a single file.
fn merge_shards(shards: &[ShardStats], output_path: &Path) -> Result<(), TdfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    if shards.is_empty() {
        return Ok(());
    }

    // Open first shard to get schema
    let first_file = File::open(&shards[0].path)
        .map_err(|e| TdfError::ReadError(format!("Failed to open shard: {e}")))?;
    let first_reader = ParquetRecordBatchReaderBuilder::try_new(first_file)
        .map_err(|e| TdfError::ReadError(format!("Failed to read shard: {e}")))?;
    let schema = first_reader.schema().clone();

    // Create output writer
    let output_file = File::create(output_path)
        .map_err(|e| TdfError::ReadError(format!("Failed to create merged file: {e}")))?;
    let mut writer = ArrowWriter::try_new(output_file, schema, None)
        .map_err(|e| TdfError::ReadError(format!("Failed to create writer: {e}")))?;

    // Copy all batches from all shards
    for shard in shards {
        let file = File::open(&shard.path)
            .map_err(|e| TdfError::ReadError(format!("Failed to open shard: {e}")))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| TdfError::ReadError(format!("Failed to read shard: {e}")))?
            .build()
            .map_err(|e| TdfError::ReadError(format!("Failed to build reader: {e}")))?;

        for batch in reader {
            let batch =
                batch.map_err(|e| TdfError::ReadError(format!("Failed to read batch: {e}")))?;
            writer
                .write(&batch)
                .map_err(|e| TdfError::ReadError(format!("Failed to write batch: {e}")))?;
        }
    }

    writer
        .close()
        .map_err(|e| TdfError::ReadError(format!("Failed to close merged file: {e}")))?;

    Ok(())
}
