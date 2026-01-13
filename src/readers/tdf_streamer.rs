//! Streaming access to Bruker TDF frames with deferred binary data.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use timsrust::converters::{ConvertableDomain, Frame2RtConverter, Scan2ImConverter, Tof2MzConverter};
use timsrust::readers::{FrameReader, MetadataReader};

use crate::tdf::error::TdfError;

use super::RawTdfFrame;

/// A partition of frames for parallel processing.
#[derive(Debug, Clone)]
pub struct FramePartition {
    /// Range of frame indices in this partition
    pub range: Range<usize>,
    /// Estimated total peaks in this partition (for load balancing info)
    pub estimated_peaks: usize,
}

/// Streaming access to TDF frames with deferred binary data and shared converters.
pub struct TdfStreamer {
    frame_reader: FrameReader,
    rt_converter: Arc<Frame2RtConverter>,
    tof_to_mz: Tof2MzConverter,
    scan_to_im: Scan2ImConverter,
    next_index: usize,
    batch_size: usize,
    is_maldi: bool,
}

impl TdfStreamer {
    /// Create a new streamer over a TDF dataset.
    pub fn new<P: AsRef<Path>>(path: P, batch_size: usize) -> Result<Self, TdfError> {
        let path = path.as_ref();
        let frame_reader = FrameReader::new(path)
            .map_err(|e| TdfError::ReadError(format!("Failed to open TDF frames: {e}")))?;
        let metadata = MetadataReader::new(path)
            .map_err(|e| TdfError::MissingData(format!("Failed to read TDF metadata: {e}")))?;

        let batch_size = batch_size.max(1);

        Ok(Self {
            is_maldi: frame_reader.is_maldi(),
            frame_reader,
            rt_converter: Arc::new(metadata.rt_converter),
            tof_to_mz: metadata.mz_converter,
            scan_to_im: metadata.im_converter,
            next_index: 0,
            batch_size,
        })
    }

    /// Return converters used during decode.
    pub fn converters(&self) -> (&Tof2MzConverter, &Scan2ImConverter, Arc<Frame2RtConverter>) {
        (&self.tof_to_mz, &self.scan_to_im, self.rt_converter.clone())
    }

    /// Whether this dataset contains MALDI imaging frames.
    pub fn is_maldi(&self) -> bool {
        self.is_maldi
    }

    /// Total frame count in the dataset.
    pub fn len(&self) -> usize {
        self.frame_reader.len()
    }

    /// Fetch the next batch of raw frames.
    /// Uses parallel decompression for significantly improved performance.
    pub fn next_batch(&mut self) -> Result<Option<Vec<RawTdfFrame>>, TdfError> {
        if self.next_index >= self.frame_reader.len() {
            return Ok(None);
        }

        let end = (self.next_index + self.batch_size).min(self.frame_reader.len());
        let mut indices: Vec<usize> = Vec::with_capacity(end - self.next_index);
        indices.extend(self.next_index..end);
        
        // Use batch API for parallel zstd decompression
        let frames_results = self.frame_reader.get_batch(&indices);
        
        let mut batch = Vec::with_capacity(frames_results.len());

        for (i, frame_result) in frames_results.into_iter().enumerate() {
            let frame_idx = self.next_index + i;
            match frame_result {
                Ok(frame) => {
                    // Use converter to derive RT to keep consistent with mzPeak contract.
                    // Bounds check: if frame index is out of RT lookup range, use interpolated value
                    let rt_seconds = if frame.index < self.frame_reader.len() {
                        self.rt_converter.convert(frame.index as u32)
                    } else {
                        // Fallback for out-of-bounds frame index (shouldn't happen in normal operation)
                        eprintln!("⚠️  Frame {} has out-of-bounds index, using native RT: {:.2}s", 
                            frame_idx, frame.rt_in_seconds);
                        frame.rt_in_seconds
                    };
                    batch.push(RawTdfFrame::from_frame(frame, rt_seconds));
                }
                Err(e) => {
                    // Skip decompression errors (known issue with some AlphaTims samples)
                    if e.to_string().contains("Decompression") {
                        eprintln!("⚠️  Skipping frame {} (decompression error): {}", frame_idx, e);
                        continue;
                    }
                    // Propagate other errors
                    return Err(TdfError::FrameParsingError(format!(
                        "Failed to read frame {frame_idx}: {e}"
                    )));
                }
            }
        }

        self.next_index = end;
        Ok(Some(batch))
    }

    /// Partition the dataset into N roughly equal parts for parallel processing.
    /// Partitions are balanced by frame count (could be enhanced to use peak count hints).
    pub fn partition(&self, num_workers: usize) -> Vec<FramePartition> {
        let total = self.frame_reader.len();
        if total == 0 || num_workers == 0 {
            return vec![];
        }

        let num_workers = num_workers.min(total);
        let base_size = total / num_workers;
        let remainder = total % num_workers;

        let mut partitions = Vec::with_capacity(num_workers);
        let mut start = 0;

        for i in 0..num_workers {
            // Distribute remainder across first `remainder` partitions
            let extra = if i < remainder { 1 } else { 0 };
            let size = base_size + extra;
            let end = start + size;

            partitions.push(FramePartition {
                range: start..end,
                estimated_peaks: 0, // Could query SQL for NumPeaks hints
            });

            start = end;
        }

        partitions
    }

    /// Read a specific range of frames (for parallel worker processing).
    /// Returns frames with their assigned spectrum IDs based on offset.
    pub fn read_range(
        &self,
        range: Range<usize>,
        spectrum_id_offset: i64,
    ) -> Result<Vec<(i64, RawTdfFrame)>, TdfError> {
        let mut indices: Vec<usize> = Vec::with_capacity(range.len());
        indices.extend(range.clone());
        let frames_results = self.frame_reader.get_batch(&indices);

        let mut result = Vec::with_capacity(frames_results.len());

        for (i, frame_result) in frames_results.into_iter().enumerate() {
            let frame_idx = range.start + i;
            let spectrum_id = spectrum_id_offset + i as i64;

            match frame_result {
                Ok(frame) => {
                    let rt_seconds = if frame.index < self.frame_reader.len() {
                        self.rt_converter.convert(frame.index as u32)
                    } else {
                        frame.rt_in_seconds
                    };
                    result.push((spectrum_id, RawTdfFrame::from_frame(frame, rt_seconds)));
                }
                Err(e) => {
                    if e.to_string().contains("Decompression") {
                        eprintln!("⚠️  Skipping frame {} (decompression error): {}", frame_idx, e);
                        continue;
                    }
                    return Err(TdfError::FrameParsingError(format!(
                        "Failed to read frame {frame_idx}: {e}"
                    )));
                }
            }
        }

        Ok(result)
    }

    /// Get a shared reference to the underlying frame reader.
    /// Useful for parallel workers that need direct access.
    pub fn frame_reader(&self) -> &FrameReader {
        &self.frame_reader
    }

    /// Get the RT converter for use in parallel processing.
    pub fn rt_converter(&self) -> Arc<Frame2RtConverter> {
        self.rt_converter.clone()
    }
}
