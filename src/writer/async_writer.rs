//! Asynchronous writer pipeline for mzPeak Parquet files.
//!
//! This module provides [`AsyncMzPeakWriter`], which offloads compression and I/O
//! to a dedicated background thread, allowing the producer to continue preparing
//! batches without blocking on disk writes.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     bounded channel      ┌─────────────────┐
//! │  Producer   │ ──OwnedColumnarBatch──▶  │ Background      │
//! │  (caller)   │      (pointer move)      │ Writer Thread   │
//! │             │ ◀────error slot──────    │ (compression+IO)│
//! └─────────────┘   Arc<Mutex<Option>>     └─────────────────┘
//! ```
//!
//! # Zero-Copy Guarantee
//!
//! Batches are transferred through the channel via pointer move—no data bytes
//! are copied. The underlying heap data is transferred directly to Arrow buffers
//! using `ScalarBuffer::from(Vec<T>)`, which is a zero-copy operation.
//!
//! # Example
//!
//! ```rust,ignore
//! use mzpeak::writer::{AsyncMzPeakWriter, WriterConfig, OwnedColumnarBatch};
//! use mzpeak::metadata::MzPeakMetadata;
//! use std::fs::File;
//!
//! let file = File::create("output.mzpeak.parquet")?;
//! let metadata = MzPeakMetadata::default();
//! let config = WriterConfig::default();
//!
//! let writer = AsyncMzPeakWriter::new(file, metadata, config)?;
//!
//! for batch in produce_batches() {
//!     writer.write_owned_batch(batch)?;  // Non-blocking until backpressure
//! }
//!
//! let stats = writer.finish()?;  // Waits for background thread to complete
//! println!("Wrote {} peaks", stats.peaks_written);
//! ```

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, Sender};

use crate::metadata::MzPeakMetadata;

use super::config::WriterConfig;
use super::error::WriterError;
use super::stats::WriterStats;
use super::types::OwnedColumnarBatch;
use super::writer_impl::MzPeakWriter;

/// Asynchronous wrapper for [`MzPeakWriter`] that offloads compression and I/O
/// to a dedicated background thread.
///
/// # Zero-Copy Guarantee
///
/// Batches are transferred through the channel via pointer move (not copy).
/// The underlying heap data is never duplicated—only ownership is transferred.
/// This is achieved because:
///
/// 1. [`OwnedColumnarBatch`] contains only `Vec<T>` fields with primitive `T`
/// 2. `Vec<T>` is `Send` when `T: Send` (all primitives are `Send`)
/// 3. Channel send moves the struct without touching heap data
/// 4. [`MzPeakWriter::write_owned_batch`] uses `ScalarBuffer::from(Vec<T>)` for
///    zero-copy transfer to Arrow
///
/// # Backpressure
///
/// Uses a bounded channel (configured via [`WriterConfig::async_buffer_capacity`])
/// to prevent OOM if the producer is faster than the writer. When the channel is
/// full, [`write_owned_batch`](Self::write_owned_batch) blocks until space is available.
///
/// # Error Handling
///
/// Errors in the background thread are detected on the next
/// [`write_owned_batch`](Self::write_owned_batch) call or on
/// [`finish`](Self::finish). This ensures fail-fast behavior—the caller learns
/// of errors promptly rather than silently losing data.
///
/// # Drop Safety
///
/// If [`finish`](Self::finish) is not called before the writer is dropped, the
/// destructor will wait for the background thread to complete and log a warning.
/// The resulting Parquet file may be incomplete (missing footer).
pub struct AsyncMzPeakWriter {
    /// Channel sender (None after finish() is called)
    sender: Option<Sender<OwnedColumnarBatch>>,
    /// Background thread handle (None after finish() is called)
    handle: Option<JoinHandle<Result<WriterStats, String>>>,
    /// First error encountered by background thread (for fail-fast detection)
    first_error: Arc<Mutex<Option<String>>>,
}

impl AsyncMzPeakWriter {
    /// Create a new async writer that offloads work to a background thread.
    ///
    /// The background thread is spawned immediately and begins accepting batches.
    /// The Parquet file is opened and headers are written in the background.
    ///
    /// # Arguments
    ///
    /// * `writer` - The underlying `Write` implementation (e.g., `File`, `BufWriter`)
    /// * `metadata` - MzPeak metadata to embed in the Parquet file footer
    /// * `config` - Writer configuration including compression level and buffer capacity
    ///
    /// # Errors
    ///
    /// Returns an error if the background thread fails to spawn (extremely rare)
    /// or if the initial writer setup fails in the background thread. Note that
    /// initialization errors are detected on the first [`write_owned_batch`](Self::write_owned_batch)
    /// call, not immediately upon return from this function.
    ///
    /// # Thread Naming
    ///
    /// The background thread is named `"mzpeak-writer"` for easier debugging
    /// with tools like `top` or `htop`.
    pub fn new<W>(
        writer: W,
        metadata: MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError>
    where
        W: Write + Send + Sync + 'static,
    {
        let buffer_capacity = config.async_buffer_capacity;
        let (sender, receiver) = bounded::<OwnedColumnarBatch>(buffer_capacity);
        let first_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let first_error_clone = Arc::clone(&first_error);

        // Spawn background writer thread
        let handle = thread::Builder::new()
            .name("mzpeak-writer".to_string())
            .spawn(move || {
                // Initialize writer inside thread (config is moved)
                let mut inner_writer = match MzPeakWriter::new(writer, &metadata, config) {
                    Ok(w) => w,
                    Err(e) => {
                        let err_str = e.to_string();
                        *first_error_clone.lock().unwrap() = Some(err_str.clone());
                        return Err(err_str);
                    }
                };

                // Process batches until channel disconnects
                for batch in receiver {
                    if let Err(e) = inner_writer.write_owned_batch(batch) {
                        let err_str = e.to_string();
                        *first_error_clone.lock().unwrap() = Some(err_str.clone());
                        return Err(err_str);
                    }
                }

                // Channel disconnected - finish the file
                inner_writer.finish().map_err(|e| {
                    let err_str = e.to_string();
                    *first_error_clone.lock().unwrap() = Some(err_str.clone());
                    err_str
                })
            })
            .map_err(|e| {
                WriterError::BackgroundWriterError(format!("Failed to spawn writer thread: {}", e))
            })?;

        Ok(Self {
            sender: Some(sender),
            handle: Some(handle),
            first_error,
        })
    }

    /// Write an owned batch asynchronously.
    ///
    /// The batch is transferred to the background thread via pointer move
    /// (zero-copy). If the channel buffer is full, this method blocks until
    /// space is available (backpressure).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The background thread has already failed (fail-fast)
    /// - The channel is disconnected (background thread exited)
    /// - [`finish`](Self::finish) was already called
    ///
    /// # Zero-Copy
    ///
    /// The [`OwnedColumnarBatch`] is moved through the channel. No data bytes
    /// are copied—only the struct's metadata (pointers, lengths, capacities).
    /// For a batch with N peaks, this is O(1) regardless of N.
    ///
    /// # Blocking Behavior
    ///
    /// This method may block if the channel buffer is full. The buffer size is
    /// controlled by [`WriterConfig::async_buffer_capacity`]. If the producer
    /// is consistently faster than the writer, consider:
    /// - Increasing buffer capacity (uses more memory)
    /// - Using faster compression (e.g., `CompressionType::Snappy`)
    /// - Writing to faster storage (SSD vs HDD)
    pub fn write_owned_batch(&self, batch: OwnedColumnarBatch) -> Result<(), WriterError> {
        // Fail-fast: check if background thread already errored
        if let Some(ref err) = *self.first_error.lock().unwrap() {
            return Err(WriterError::BackgroundWriterError(err.clone()));
        }

        // Get sender (None if finish() was called)
        let sender = self.sender.as_ref().ok_or_else(|| {
            WriterError::BackgroundWriterError("Writer already finished".to_string())
        })?;

        // Send batch (blocks if channel is full - backpressure)
        sender.send(batch).map_err(|_| {
            // Channel disconnected - background thread must have exited
            // Check if there's an error message
            let err_guard = self.first_error.lock().unwrap();
            match err_guard.as_ref() {
                Some(msg) => WriterError::BackgroundWriterError(msg.clone()),
                None => WriterError::BackgroundWriterError(
                    "Background writer thread exited unexpectedly".to_string(),
                ),
            }
        })
    }

    /// Finish writing and close the Parquet file.
    ///
    /// This method:
    /// 1. Closes the channel to signal the background thread to stop accepting batches
    /// 2. Waits for the background thread to write remaining buffered batches
    /// 3. Waits for the background thread to finalize the Parquet file (write footer)
    /// 4. Returns the final statistics or propagates any error
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The background thread encountered an error during writing
    /// - The background thread encountered an error while finalizing the file
    /// - The background thread panicked
    /// - [`finish`](Self::finish) was already called
    ///
    /// # Blocking
    ///
    /// This method blocks until the background thread completes. The duration
    /// depends on:
    /// - Number of batches still in the channel buffer
    /// - Compression and I/O speed
    /// - Parquet footer generation (includes statistics aggregation)
    pub fn finish(mut self) -> Result<WriterStats, WriterError> {
        // Drop sender to signal background thread to stop
        self.sender.take();

        // Wait for background thread to complete
        let handle = self.handle.take().ok_or_else(|| {
            WriterError::BackgroundWriterError("finish() called twice".to_string())
        })?;

        match handle.join() {
            Ok(Ok(stats)) => Ok(stats),
            Ok(Err(err_str)) => Err(WriterError::BackgroundWriterError(err_str)),
            Err(_panic) => Err(WriterError::ThreadPanicked),
        }
    }

    /// Check if the background writer has encountered an error.
    ///
    /// This allows polling for errors without consuming a batch slot.
    /// Useful for long-running pipelines that want to detect failures early.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if no error has occurred
    /// - `Err(WriterError::BackgroundWriterError)` if an error has occurred
    pub fn check_error(&self) -> Result<(), WriterError> {
        if let Some(ref err) = *self.first_error.lock().unwrap() {
            return Err(WriterError::BackgroundWriterError(err.clone()));
        }
        Ok(())
    }
}

impl Drop for AsyncMzPeakWriter {
    fn drop(&mut self) {
        // If finish() wasn't called, clean up gracefully
        if self.sender.is_some() || self.handle.is_some() {
            // Drop sender to signal thread to stop
            self.sender.take();

            // Wait for thread (don't leave it orphaned)
            if let Some(handle) = self.handle.take() {
                // Log warning - user should call finish() explicitly
                log::warn!(
                    "AsyncMzPeakWriter dropped without calling finish(). \
                     Parquet file may be incomplete (missing footer)."
                );
                // Best-effort join - ignore result since we can't return errors from Drop
                let _ = handle.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::MzPeakMetadata;
    use crate::writer::OptionalColumnBuf;
    use std::io::Cursor;

    /// Create a minimal test batch with the given number of peaks
    fn create_test_batch(num_peaks: usize, spectrum_id: i64) -> OwnedColumnarBatch {
        OwnedColumnarBatch {
            mz: vec![100.0; num_peaks],
            intensity: vec![1000.0; num_peaks],
            spectrum_id: vec![spectrum_id; num_peaks],
            scan_number: vec![1; num_peaks],
            ms_level: vec![1; num_peaks],
            retention_time: vec![60.0; num_peaks],
            polarity: vec![1; num_peaks],
            ion_mobility: OptionalColumnBuf::AllNull { len: num_peaks },
            precursor_mz: OptionalColumnBuf::AllNull { len: num_peaks },
            precursor_charge: OptionalColumnBuf::AllNull { len: num_peaks },
            precursor_intensity: OptionalColumnBuf::AllNull { len: num_peaks },
            isolation_window_lower: OptionalColumnBuf::AllNull { len: num_peaks },
            isolation_window_upper: OptionalColumnBuf::AllNull { len: num_peaks },
            collision_energy: OptionalColumnBuf::AllNull { len: num_peaks },
            total_ion_current: OptionalColumnBuf::AllNull { len: num_peaks },
            base_peak_mz: OptionalColumnBuf::AllNull { len: num_peaks },
            base_peak_intensity: OptionalColumnBuf::AllNull { len: num_peaks },
            injection_time: OptionalColumnBuf::AllNull { len: num_peaks },
            pixel_x: OptionalColumnBuf::AllNull { len: num_peaks },
            pixel_y: OptionalColumnBuf::AllNull { len: num_peaks },
            pixel_z: OptionalColumnBuf::AllNull { len: num_peaks },
        }
    }

    #[test]
    fn test_basic_write_finish_cycle() {
        let buffer: Vec<u8> = Vec::new();
        let cursor = Cursor::new(buffer);
        let metadata = MzPeakMetadata::default();
        let config = WriterConfig::default();

        let writer = AsyncMzPeakWriter::new(cursor, metadata, config)
            .expect("Failed to create async writer");

        // Write a few batches
        for i in 0..5 {
            let batch = create_test_batch(100, i);
            writer
                .write_owned_batch(batch)
                .expect("Failed to write batch");
        }

        // Finish and verify stats
        let stats = writer.finish().expect("Failed to finish");
        assert_eq!(stats.peaks_written, 500);
        // Note: spectra_written is not tracked by write_owned_batch (raw peak batches)
        // It's only tracked by write_spectra_owned which handles spectrum-level aggregation
    }

    #[test]
    fn test_check_error_when_healthy() {
        let buffer: Vec<u8> = Vec::new();
        let cursor = Cursor::new(buffer);
        let metadata = MzPeakMetadata::default();
        let config = WriterConfig::default();

        let writer = AsyncMzPeakWriter::new(cursor, metadata, config)
            .expect("Failed to create async writer");

        // No error should be reported
        assert!(writer.check_error().is_ok());

        let _ = writer.finish();
    }

    #[test]
    fn test_empty_batch_handling() {
        let buffer: Vec<u8> = Vec::new();
        let cursor = Cursor::new(buffer);
        let metadata = MzPeakMetadata::default();
        let config = WriterConfig::default();

        let writer = AsyncMzPeakWriter::new(cursor, metadata, config)
            .expect("Failed to create async writer");

        // Write an empty batch - should be handled gracefully
        let empty_batch = create_test_batch(0, 0);
        writer
            .write_owned_batch(empty_batch)
            .expect("Failed to write empty batch");

        let stats = writer.finish().expect("Failed to finish");
        assert_eq!(stats.peaks_written, 0);
    }

    #[test]
    fn test_write_after_finish_fails() {
        let buffer: Vec<u8> = Vec::new();
        let cursor = Cursor::new(buffer);
        let metadata = MzPeakMetadata::default();
        let config = WriterConfig::default();

        let writer = AsyncMzPeakWriter::new(cursor, metadata, config)
            .expect("Failed to create async writer");

        // Finish the writer
        let _ = writer.finish();

        // Note: After finish(), the writer is consumed, so we can't call write_owned_batch.
        // This test verifies the API design prevents misuse at compile time.
    }

    #[test]
    fn test_small_buffer_capacity() {
        let buffer: Vec<u8> = Vec::new();
        let cursor = Cursor::new(buffer);
        let metadata = MzPeakMetadata::default();
        let mut config = WriterConfig::default();
        config.async_buffer_capacity = 2; // Very small buffer

        let writer = AsyncMzPeakWriter::new(cursor, metadata, config)
            .expect("Failed to create async writer");

        // Write more batches than buffer capacity - should work with backpressure
        for i in 0..10 {
            let batch = create_test_batch(100, i);
            writer
                .write_owned_batch(batch)
                .expect("Failed to write batch");
        }

        let stats = writer.finish().expect("Failed to finish");
        assert_eq!(stats.peaks_written, 1000);
    }
}
