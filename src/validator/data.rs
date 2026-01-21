use std::fs::File;

use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;

use crate::schema::columns;
use crate::reader::ZipEntryChunkReader;
use crate::schema::spectra_columns;

use super::{ParquetSource, SchemaVersion, ValidationCheck, ValidationReport, ValidationTarget};

/// Step 4: Data sanity validation
pub(crate) fn check_data_sanity(
    validation_target: &ValidationTarget,
    report: &mut ValidationReport,
) -> Result<()> {
    match validation_target.schema_version {
        SchemaVersion::V1 => {
            match &validation_target.peaks {
                ParquetSource::FilePath(path) => {
                    let reader = SerializedFileReader::new(File::open(path)?)?;
                    perform_data_sanity_checks(reader, report)
                }
                ParquetSource::ZipEntry { zip_path, entry_name } => {
                    let reader = ZipEntryChunkReader::new(zip_path, entry_name)?;
                    let reader = SerializedFileReader::new(reader)?;
                    perform_data_sanity_checks(reader, report)
                }
                ParquetSource::InMemory(bytes) => {
                    let reader = SerializedFileReader::new(bytes.clone())?;
                    perform_data_sanity_checks(reader, report)
                }
            }
        }
        SchemaVersion::V2 => {
            match &validation_target.peaks {
                ParquetSource::FilePath(path) => {
                    let reader = SerializedFileReader::new(File::open(path)?)?;
                    perform_v2_peaks_sanity_checks(reader, report)?;
                }
                ParquetSource::ZipEntry { zip_path, entry_name } => {
                    let reader = ZipEntryChunkReader::new(zip_path, entry_name)?;
                    let reader = SerializedFileReader::new(reader)?;
                    perform_v2_peaks_sanity_checks(reader, report)?;
                }
                ParquetSource::InMemory(bytes) => {
                    let reader = SerializedFileReader::new(bytes.clone())?;
                    perform_v2_peaks_sanity_checks(reader, report)?;
                }
            }

            if let Some(spectra_source) = &validation_target.spectra {
                match spectra_source {
                    ParquetSource::FilePath(path) => {
                        let reader = SerializedFileReader::new(File::open(path)?)?;
                        perform_v2_spectra_sanity_checks(reader, report)?;
                    }
                    ParquetSource::ZipEntry { zip_path, entry_name } => {
                        let reader = ZipEntryChunkReader::new(zip_path, entry_name)?;
                        let reader = SerializedFileReader::new(reader)?;
                        perform_v2_spectra_sanity_checks(reader, report)?;
                    }
                    ParquetSource::InMemory(bytes) => {
                        let reader = SerializedFileReader::new(bytes.clone())?;
                        perform_v2_spectra_sanity_checks(reader, report)?;
                    }
                }
            } else {
                report.add_check(ValidationCheck::failed(
                    "spectra.parquet available",
                    "Missing spectra.parquet for v2 data sanity checks",
                ));
            }

            Ok(())
        }
    }
}

/// Perform actual data sanity checks on a reader
fn perform_data_sanity_checks<R: parquet::file::reader::ChunkReader + 'static>(
    reader: SerializedFileReader<R>,
    report: &mut ValidationReport,
) -> Result<()> {
    let metadata = reader.metadata();
    let num_rows = metadata.file_metadata().num_rows();
    let schema_descriptor = metadata.file_metadata().schema_descr();

    report.add_check(ValidationCheck::ok(format!("Total rows: {}", num_rows)));

    if num_rows == 0 {
        report.add_check(ValidationCheck::warning(
            "Data rows",
            "File contains no data rows",
        ));
        return Ok(());
    }

    // Find column indices
    let mut spectrum_id_idx = None;
    let mut ms_level_idx = None;
    let mut retention_time_idx = None;
    let mut mz_idx = None;
    let mut intensity_idx = None;

    for i in 0..schema_descriptor.num_columns() {
        let col = schema_descriptor.column(i);
        match col.name() {
            columns::SPECTRUM_ID => spectrum_id_idx = Some(i),
            columns::MS_LEVEL => ms_level_idx = Some(i),
            columns::RETENTION_TIME => retention_time_idx = Some(i),
            columns::MZ => mz_idx = Some(i),
            columns::INTENSITY => intensity_idx = Some(i),
            _ => {}
        }
    }

    // Read a sample of rows (first 1000 or all if fewer)
    let sample_size = std::cmp::min(1000, num_rows as usize);
    let mut row_iter = reader.get_row_iter(None)?;

    let mut mz_positive_count = 0;
    let mut intensity_non_negative_count = 0;
    let mut ms_level_valid_count = 0;
    let mut last_rt: Option<f32> = None;
    let mut rt_non_decreasing = true;
    let mut prev_spectrum_id: Option<i64> = None;

    for _i in 0..sample_size {
        if let Some(row_result) = row_iter.next() {
            let row = row_result?;

            // Check mz > 0
            if let Some(idx) = mz_idx {
                if let Ok(mz) = row.get_double(idx) {
                    if mz > 0.0 {
                        mz_positive_count += 1;
                    }
                }
            }

            // Check intensity >= 0
            if let Some(idx) = intensity_idx {
                if let Ok(intensity) = row.get_float(idx) {
                    if intensity >= 0.0 {
                        intensity_non_negative_count += 1;
                    }
                }
            }

            // Check ms_level >= 1
            if let Some(idx) = ms_level_idx {
                // ms_level is Int16, so use get_short()
                match row.get_short(idx) {
                    Ok(ms_level) => {
                        if ms_level >= 1 {
                            ms_level_valid_count += 1;
                        }
                    }
                    Err(_) => {
                        // Try get_int() as fallback for compatibility
                        if let Ok(ms_level) = row.get_int(idx) {
                            if ms_level >= 1 {
                                ms_level_valid_count += 1;
                            }
                        }
                    }
                }
            }

            // Check retention_time non-decreasing (per spectrum)
            if let Some(spec_idx) = spectrum_id_idx {
                if let Some(rt_idx) = retention_time_idx {
                    if let Ok(spectrum_id) = row.get_long(spec_idx) {
                        if let Ok(rt) = row.get_float(rt_idx) {
                            // New spectrum
                            if prev_spectrum_id != Some(spectrum_id) {
                                if let Some(prev_rt) = last_rt {
                                    if rt < prev_rt {
                                        rt_non_decreasing = false;
                                    }
                                }
                                last_rt = Some(rt);
                                prev_spectrum_id = Some(spectrum_id);
                            }
                        }
                    }
                }
            }
        } else {
            break;
        }
    }

    // Report findings
    if mz_positive_count == sample_size {
        report.add_check(ValidationCheck::ok(format!(
            "m/z values positive (sampled {} rows)",
            sample_size
        )));
    } else {
        report.add_check(ValidationCheck::failed(
            "m/z values positive",
            format!(
                "Found {} invalid m/z values (<=0) in sample of {}",
                sample_size - mz_positive_count,
                sample_size
            ),
        ));
    }

    if intensity_non_negative_count == sample_size {
        report.add_check(ValidationCheck::ok(format!(
            "Intensity values non-negative (sampled {} rows)",
            sample_size
        )));
    } else {
        report.add_check(ValidationCheck::failed(
            "Intensity values non-negative",
            format!(
                "Found {} negative intensity values in sample of {}",
                sample_size - intensity_non_negative_count,
                sample_size
            ),
        ));
    }

    if ms_level_valid_count == sample_size {
        report.add_check(ValidationCheck::ok(format!(
            "MS level values >= 1 (sampled {} rows)",
            sample_size
        )));
    } else {
        report.add_check(ValidationCheck::failed(
            "MS level values >= 1",
            format!(
                "Found {} invalid ms_level values (<1) in sample of {}",
                sample_size - ms_level_valid_count,
                sample_size
            ),
        ));
    }

    if rt_non_decreasing {
        report.add_check(ValidationCheck::ok("Retention time non-decreasing"));
    } else {
        report.add_check(ValidationCheck::warning(
            "Retention time non-decreasing",
            "Retention time decreases between spectra (may be intentional)",
        ));
    }

    Ok(())
}

fn perform_v2_peaks_sanity_checks<R: parquet::file::reader::ChunkReader + 'static>(
    reader: SerializedFileReader<R>,
    report: &mut ValidationReport,
) -> Result<()> {
    let metadata = reader.metadata();
    let num_rows = metadata.file_metadata().num_rows();
    let schema_descriptor = metadata.file_metadata().schema_descr();

    report.add_check(ValidationCheck::ok(format!(
        "V2 peaks rows: {}",
        num_rows
    )));

    if num_rows == 0 {
        report.add_check(ValidationCheck::warning(
            "V2 peaks rows",
            "peaks.parquet contains no data rows",
        ));
        return Ok(());
    }

    let mut spectrum_id_idx = None;
    let mut mz_idx = None;
    let mut intensity_idx = None;

    for i in 0..schema_descriptor.num_columns() {
        let col = schema_descriptor.column(i);
        match col.name() {
            columns::SPECTRUM_ID => spectrum_id_idx = Some(i),
            columns::MZ => mz_idx = Some(i),
            columns::INTENSITY => intensity_idx = Some(i),
            _ => {}
        }
    }

    let sample_size = std::cmp::min(1000, num_rows as usize);
    let mut row_iter = reader.get_row_iter(None)?;

    let mut mz_positive_count = 0;
    let mut intensity_non_negative_count = 0;
    let mut spectrum_id_valid_count = 0;

    for _i in 0..sample_size {
        if let Some(row_result) = row_iter.next() {
            let row = row_result?;

            if let Some(idx) = mz_idx {
                if let Ok(mz) = row.get_double(idx) {
                    if mz > 0.0 {
                        mz_positive_count += 1;
                    }
                }
            }

            if let Some(idx) = intensity_idx {
                if let Ok(intensity) = row.get_float(idx) {
                    if intensity >= 0.0 {
                        intensity_non_negative_count += 1;
                    }
                }
            }

            if let Some(idx) = spectrum_id_idx {
                if let Ok(spectrum_id) = row.get_int(idx) {
                    if spectrum_id >= 0 {
                        spectrum_id_valid_count += 1;
                    }
                } else if let Ok(spectrum_id) = row.get_long(idx) {
                    if spectrum_id >= 0 {
                        spectrum_id_valid_count += 1;
                    }
                }
            }
        } else {
            break;
        }
    }

    if mz_positive_count == sample_size {
        report.add_check(ValidationCheck::ok(format!(
            "V2 m/z values positive (sampled {} rows)",
            sample_size
        )));
    } else {
        report.add_check(ValidationCheck::failed(
            "V2 m/z values positive",
            format!(
                "Found {} invalid m/z values (<=0) in sample of {}",
                sample_size - mz_positive_count,
                sample_size
            ),
        ));
    }

    if intensity_non_negative_count == sample_size {
        report.add_check(ValidationCheck::ok(format!(
            "V2 intensity values non-negative (sampled {} rows)",
            sample_size
        )));
    } else {
        report.add_check(ValidationCheck::failed(
            "V2 intensity values non-negative",
            format!(
                "Found {} negative intensity values in sample of {}",
                sample_size - intensity_non_negative_count,
                sample_size
            ),
        ));
    }

    if spectrum_id_valid_count == sample_size {
        report.add_check(ValidationCheck::ok(format!(
            "V2 spectrum_id values non-negative (sampled {} rows)",
            sample_size
        )));
    } else {
        report.add_check(ValidationCheck::warning(
            "V2 spectrum_id values non-negative",
            format!(
                "Found {} invalid spectrum_id values in sample of {}",
                sample_size - spectrum_id_valid_count,
                sample_size
            ),
        ));
    }

    Ok(())
}

fn perform_v2_spectra_sanity_checks<R: parquet::file::reader::ChunkReader + 'static>(
    reader: SerializedFileReader<R>,
    report: &mut ValidationReport,
) -> Result<()> {
    let metadata = reader.metadata();
    let num_rows = metadata.file_metadata().num_rows();
    let schema_descriptor = metadata.file_metadata().schema_descr();

    report.add_check(ValidationCheck::ok(format!(
        "V2 spectra rows: {}",
        num_rows
    )));

    if num_rows == 0 {
        report.add_check(ValidationCheck::warning(
            "V2 spectra rows",
            "spectra.parquet contains no data rows",
        ));
        return Ok(());
    }

    let mut ms_level_idx = None;
    let mut retention_time_idx = None;
    let mut polarity_idx = None;
    let mut spectrum_id_idx = None;

    for i in 0..schema_descriptor.num_columns() {
        let col = schema_descriptor.column(i);
        match col.name() {
            spectra_columns::MS_LEVEL => ms_level_idx = Some(i),
            spectra_columns::RETENTION_TIME => retention_time_idx = Some(i),
            spectra_columns::POLARITY => polarity_idx = Some(i),
            spectra_columns::SPECTRUM_ID => spectrum_id_idx = Some(i),
            _ => {}
        }
    }

    let sample_size = std::cmp::min(1000, num_rows as usize);
    let mut row_iter = reader.get_row_iter(None)?;

    let mut ms_level_valid_count = 0;
    let mut polarity_valid_count = 0;
    let mut rt_valid_count = 0;
    let mut last_rt: Option<f32> = None;
    let mut rt_non_decreasing = true;
    let mut last_spectrum_id: Option<i64> = None;
    let mut spectrum_id_non_decreasing = true;

    for _i in 0..sample_size {
        if let Some(row_result) = row_iter.next() {
            let row = row_result?;

            if let Some(idx) = ms_level_idx {
                if let Ok(ms_level) = row.get_byte(idx) {
                    if ms_level >= 1 {
                        ms_level_valid_count += 1;
                    }
                } else if let Ok(ms_level) = row.get_int(idx) {
                    if ms_level >= 1 {
                        ms_level_valid_count += 1;
                    }
                }
            }

            if let Some(idx) = polarity_idx {
                if let Ok(polarity) = row.get_byte(idx) {
                    if matches!(polarity, -1 | 0 | 1) {
                        polarity_valid_count += 1;
                    }
                } else if let Ok(polarity) = row.get_int(idx) {
                    if matches!(polarity, -1 | 0 | 1) {
                        polarity_valid_count += 1;
                    }
                }
            }

            if let Some(idx) = retention_time_idx {
                if let Ok(rt) = row.get_float(idx) {
                    if rt.is_finite() {
                        rt_valid_count += 1;
                    }

                    if let Some(prev_rt) = last_rt {
                        if rt < prev_rt {
                            rt_non_decreasing = false;
                        }
                    }
                    last_rt = Some(rt);
                }
            }

            if let Some(idx) = spectrum_id_idx {
                let spectrum_id = if let Ok(value) = row.get_int(idx) {
                    Some(value as i64)
                } else if let Ok(value) = row.get_long(idx) {
                    Some(value)
                } else {
                    None
                };

                if let Some(current) = spectrum_id {
                    if let Some(prev) = last_spectrum_id {
                        if current < prev {
                            spectrum_id_non_decreasing = false;
                        }
                    }
                    last_spectrum_id = Some(current);
                }
            }
        } else {
            break;
        }
    }

    if ms_level_valid_count == sample_size {
        report.add_check(ValidationCheck::ok(format!(
            "V2 MS level values >= 1 (sampled {} rows)",
            sample_size
        )));
    } else {
        report.add_check(ValidationCheck::failed(
            "V2 MS level values >= 1",
            format!(
                "Found {} invalid ms_level values (<1) in sample of {}",
                sample_size - ms_level_valid_count,
                sample_size
            ),
        ));
    }

    if polarity_valid_count == sample_size {
        report.add_check(ValidationCheck::ok(format!(
            "V2 polarity values valid (sampled {} rows)",
            sample_size
        )));
    } else {
        report.add_check(ValidationCheck::warning(
            "V2 polarity values valid",
            format!(
                "Found {} invalid polarity values in sample of {}",
                sample_size - polarity_valid_count,
                sample_size
            ),
        ));
    }

    if rt_valid_count == sample_size {
        report.add_check(ValidationCheck::ok(format!(
            "V2 retention time finite (sampled {} rows)",
            sample_size
        )));
    } else {
        report.add_check(ValidationCheck::warning(
            "V2 retention time finite",
            format!(
                "Found {} invalid retention_time values in sample of {}",
                sample_size - rt_valid_count,
                sample_size
            ),
        ));
    }

    if rt_non_decreasing {
        report.add_check(ValidationCheck::ok("V2 retention time non-decreasing"));
    } else {
        report.add_check(ValidationCheck::warning(
            "V2 retention time non-decreasing",
            "Retention time decreases between spectra (may be intentional)",
        ));
    }

    if spectrum_id_non_decreasing {
        report.add_check(ValidationCheck::ok("V2 spectrum_id non-decreasing"));
    } else {
        report.add_check(ValidationCheck::warning(
            "V2 spectrum_id non-decreasing",
            "spectrum_id decreases between spectra (may be intentional)",
        ));
    }

    Ok(())
}
