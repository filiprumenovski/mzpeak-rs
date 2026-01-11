use std::fs::File;
use std::io::{BufReader, Read};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use zip::ZipArchive;

use super::config::ReaderSource;
use super::utils::{extract_f32_list, extract_f64_list, get_list_column, get_string_column};
use super::{MzPeakReader, ReaderError};

impl MzPeakReader {
    /// Open a sub-parquet file (chromatograms or mobilograms) from the dataset
    fn open_sub_parquet(&self, subpath: &str) -> Result<Option<Vec<RecordBatch>>, ReaderError> {
        match &self.source {
            ReaderSource::FilePath(path) => {
                let sub_file_path = if path.is_dir() {
                    // Directory bundle
                    path.join(subpath)
                } else if path
                    .extension()
                    .map(|e| e == "parquet")
                    .unwrap_or(false)
                {
                    // Single parquet file - could be peaks/peaks.parquet from a directory dataset
                    // Check if parent is peaks/ directory
                    if let Some(parent) = path.parent() {
                        if parent.file_name().and_then(|n| n.to_str()) == Some("peaks") {
                            // This is a directory dataset, go up to dataset root
                            if let Some(dataset_root) = parent.parent() {
                                dataset_root.join(subpath)
                            } else {
                                return Ok(None);
                            }
                        } else {
                            // Single file mode - no chromatograms/mobilograms
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Err(ReaderError::InvalidFormat(format!(
                        "Cannot determine sub-file location for {:?}",
                        path
                    )));
                };

                // If the file doesn't exist, return None (chromatograms/mobilograms are optional)
                if !sub_file_path.exists() {
                    return Ok(None);
                }

                // Read the sub-parquet file
                let file = File::open(&sub_file_path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                let mut batches = Vec::new();
                for batch_result in reader {
                    batches.push(batch_result?);
                }
                Ok(Some(batches))
            }
            ReaderSource::ZipContainer { zip_path, .. } => {
                // ZIP container - re-open and extract the sub-file
                let file = File::open(zip_path)?;
                let mut archive = ZipArchive::new(BufReader::new(file))?;

                // Try to find the sub-file in the ZIP
                let mut sub_file = match archive.by_name(subpath) {
                    Ok(f) => f,
                    Err(_) => return Ok(None), // File doesn't exist in ZIP, return None
                };

                // Read the parquet file into memory
                let mut parquet_bytes = Vec::new();
                sub_file.read_to_end(&mut parquet_bytes)?;

                // Parse as Parquet
                let bytes = Bytes::from(parquet_bytes);
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?
                    .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                let mut batches = Vec::new();
                for batch_result in reader {
                    batches.push(batch_result?);
                }
                Ok(Some(batches))
            }
        }
    }

    /// Read all chromatograms from the dataset
    ///
    /// Returns an empty vector if no chromatogram file exists (chromatograms are optional).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mzpeak::reader::MzPeakReader;
    ///
    /// let reader = MzPeakReader::open("data.mzpeak")?;
    /// let chromatograms = reader.read_chromatograms()?;
    /// for chrom in chromatograms {
    ///     println!("Chromatogram {}: {} points", chrom.chromatogram_id, chrom.time_array.len());
    /// }
    /// # Ok::<(), mzpeak::reader::ReaderError>(())
    /// ```
    pub fn read_chromatograms(
        &self,
    ) -> Result<Vec<crate::chromatogram_writer::Chromatogram>, ReaderError> {
        use crate::schema::chromatogram_columns;

        let batches = match self.open_sub_parquet("chromatograms/chromatograms.parquet")? {
            Some(b) => b,
            None => return Ok(Vec::new()), // No chromatograms file, return empty
        };

        let mut chromatograms = Vec::new();

        for batch in &batches {
            let ids = get_string_column(batch, chromatogram_columns::CHROMATOGRAM_ID)?;
            let types = get_string_column(batch, chromatogram_columns::CHROMATOGRAM_TYPE)?;
            let time_arrays = get_list_column(batch, chromatogram_columns::TIME_ARRAY)?;
            let intensity_arrays = get_list_column(batch, chromatogram_columns::INTENSITY_ARRAY)?;

            for i in 0..batch.num_rows() {
                let chromatogram = crate::chromatogram_writer::Chromatogram {
                    chromatogram_id: ids.value(i).to_string(),
                    chromatogram_type: types.value(i).to_string(),
                    time_array: extract_f64_list(time_arrays, i),
                    intensity_array: extract_f32_list(intensity_arrays, i),
                };
                chromatograms.push(chromatogram);
            }
        }

        Ok(chromatograms)
    }

    /// Read all mobilograms from the dataset
    ///
    /// Returns an empty vector if no mobilogram file exists (mobilograms are optional).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mzpeak::reader::MzPeakReader;
    ///
    /// let reader = MzPeakReader::open("data.mzpeak")?;
    /// let mobilograms = reader.read_mobilograms()?;
    /// for mob in mobilograms {
    ///     println!("Mobilogram {}: {} points", mob.mobilogram_id, mob.mobility_array.len());
    /// }
    /// # Ok::<(), mzpeak::reader::ReaderError>(())
    /// ```
    pub fn read_mobilograms(
        &self,
    ) -> Result<Vec<crate::mobilogram_writer::Mobilogram>, ReaderError> {
        use crate::mobilogram_writer::mobilogram_columns;

        let batches = match self.open_sub_parquet("mobilograms/mobilograms.parquet")? {
            Some(b) => b,
            None => return Ok(Vec::new()), // No mobilograms file, return empty
        };

        let mut mobilograms = Vec::new();

        for batch in &batches {
            let ids = get_string_column(batch, mobilogram_columns::MOBILOGRAM_ID)?;
            let types = get_string_column(batch, mobilogram_columns::MOBILOGRAM_TYPE)?;
            let mobility_arrays = get_list_column(batch, mobilogram_columns::MOBILITY_ARRAY)?;
            let intensity_arrays = get_list_column(batch, mobilogram_columns::INTENSITY_ARRAY)?;

            for i in 0..batch.num_rows() {
                let mobilogram = crate::mobilogram_writer::Mobilogram {
                    mobilogram_id: ids.value(i).to_string(),
                    mobilogram_type: types.value(i).to_string(),
                    mobility_array: extract_f64_list(mobility_arrays, i),
                    intensity_array: extract_f32_list(intensity_arrays, i),
                };
                mobilograms.push(mobilogram);
            }
        }

        Ok(mobilograms)
    }
}
