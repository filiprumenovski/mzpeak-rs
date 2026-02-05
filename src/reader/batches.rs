use std::fs::File;

use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::config::ReaderSource;
use super::{MzPeakReader, ReaderError};

/// Streaming iterator over record batches (Issue 003 fix)
///
/// This iterator provides bounded memory usage by reading batches on-demand
/// rather than loading the entire file into memory.
pub struct RecordBatchIterator {
    inner: Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send>,
}

impl RecordBatchIterator {
    pub(crate) fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send + 'static,
    {
        Self {
            inner: Box::new(iter),
        }
    }
}

impl Iterator for RecordBatchIterator {
    type Item = Result<RecordBatch, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| r.map_err(ReaderError::from))
    }
}

impl MzPeakReader {
    /// Returns a streaming iterator over record batches from the peaks table
    ///
    /// This is the preferred API for large files as it avoids loading all data into memory.
    /// Memory usage is bounded by `batch_size * row_size`.
    ///
    /// # Example
    /// ```rust,no_run
    /// use mzpeak::reader::MzPeakReader;
    ///
    /// let reader = MzPeakReader::open("data.mzpeak")?;
    /// for batch_result in reader.iter_batches()? {
    ///     let batch = batch_result?;
    ///     println!("Processing batch with {} rows", batch.num_rows());
    /// }
    /// # Ok::<(), mzpeak::reader::ReaderError>(())
    /// ```
    pub fn iter_batches(&self) -> Result<RecordBatchIterator, ReaderError> {
        match &self.source {
            ReaderSource::FilePath(path) => {
                let file = File::open(path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                Ok(RecordBatchIterator::new(reader))
            }
            ReaderSource::ZipContainer { chunk_reader, .. } => {
                // Use the seekable chunk reader for streaming access (Issue 002 fix)
                // This avoids loading the entire Parquet file into memory
                let builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader.clone())?
                    .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                Ok(RecordBatchIterator::new(reader))
            }
            ReaderSource::ZipContainerV2 {
                peaks_chunk_reader, ..
            } => {
                // v2 format - read peaks table
                let builder = ParquetRecordBatchReaderBuilder::try_new(peaks_chunk_reader.clone())?
                    .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                Ok(RecordBatchIterator::new(reader))
            }
            ReaderSource::DirectoryV2 { peaks_path, .. } => {
                let file = File::open(peaks_path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                Ok(RecordBatchIterator::new(reader))
            }
        }
    }

    /// Read all record batches from the file (eager, collects all batches)
    ///
    /// Returns the raw Arrow record batches for efficient data access.
    /// Useful for zero-copy integration with data processing libraries.
    ///
    /// **Warning**: This loads all data into memory. For large files, prefer `iter_batches()`.
    pub fn read_all_batches(&self) -> Result<Vec<RecordBatch>, ReaderError> {
        self.iter_batches()?.collect()
    }
}
