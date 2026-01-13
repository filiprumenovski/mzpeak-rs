//! Streaming access to Bruker TDF frames with deferred binary data.

use std::path::Path;
use std::sync::Arc;

use timsrust::converters::{ConvertableDomain, Frame2RtConverter, Scan2ImConverter, Tof2MzConverter};
use timsrust::readers::{FrameReader, MetadataReader};

use crate::tdf::error::TdfError;

use super::RawTdfFrame;

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
    pub fn next_batch(&mut self) -> Result<Option<Vec<RawTdfFrame>>, TdfError> {
        if self.next_index >= self.frame_reader.len() {
            return Ok(None);
        }

        let end = (self.next_index + self.batch_size).min(self.frame_reader.len());
        let mut batch = Vec::with_capacity(end - self.next_index);

        for frame_idx in self.next_index..end {
            let frame = self.frame_reader.get(frame_idx).map_err(|e| {
                TdfError::FrameParsingError(format!(
                    "Failed to read frame {frame_idx}: {e}"
                ))
            })?;

            // Use converter to derive RT to keep consistent with mzPeak contract.
            let rt_seconds = self.rt_converter.convert(frame.index as u32);
            batch.push(RawTdfFrame::from_frame(frame, rt_seconds));
        }

        self.next_index = end;
        Ok(Some(batch))
    }
}
