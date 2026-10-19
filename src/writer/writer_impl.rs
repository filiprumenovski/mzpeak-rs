//! Legacy v1 adapter. All input forms converge on one Arrow batch write.
use super::{
    ColumnarBatch, OptionalColumn, OptionalColumnBuf, OwnedColumnarBatch, SpectrumArrays,
    WriterConfig, WriterError, WriterStats,
};
use crate::{metadata::MzPeakMetadata, schema::create_mzpeak_schema_arc};
use arrow::{
    array::{ArrayRef, PrimitiveArray, PrimitiveBuilder},
    buffer::{Buffer, NullBuffer, ScalarBuffer},
    datatypes::{
        ArrowPrimitiveType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    },
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::{fs::File, io::Write, path::Path, sync::Arc};

/// Compatibility writer for the v1 peak-major Parquet schema.
pub struct MzPeakWriter<W: Write + Send + Sync> {
    writer: ArrowWriter<W>,
    schema: Arc<arrow::datatypes::Schema>,
    spectra_written: usize,
    peaks_written: usize,
}

impl MzPeakWriter<File> {
    /// Creates a legacy Parquet file.
    pub fn new_file(
        path: impl AsRef<Path>,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        Self::new(File::create(path)?, metadata, config)
    }
}

impl<W: Write + Send + Sync> MzPeakWriter<W> {
    /// Creates a legacy writer over an arbitrary sink.
    pub fn new(
        writer: W,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        if config.row_group_size == 0 {
            return Err(WriterError::InvalidData(
                "row_group_size must be positive".into(),
            ));
        }
        let schema = create_mzpeak_schema_arc();
        let props = config.to_writer_properties(&metadata.to_parquet_metadata()?);
        Ok(Self {
            writer: ArrowWriter::try_new(writer, schema.clone(), Some(props))?,
            schema,
            spectra_written: 0,
            peaks_written: 0,
        })
    }

    pub(super) fn peaks_written(&self) -> usize {
        self.peaks_written
    }

    /// Copies borrowed columns at the ownership boundary and writes one batch.
    pub fn write_columnar_batch(&mut self, batch: &ColumnarBatch<'_>) -> Result<(), WriterError> {
        let len = batch.len();
        self.write_owned_batch(OwnedColumnarBatch {
            spectrum_id: batch.spectrum_id.to_vec(),
            scan_number: batch.scan_number.to_vec(),
            ms_level: batch.ms_level.to_vec(),
            retention_time: batch.retention_time.to_vec(),
            polarity: batch.polarity.to_vec(),
            mz: batch.mz.to_vec(),
            intensity: batch.intensity.to_vec(),
            ion_mobility: copy_optional(&batch.ion_mobility, len),
            precursor_mz: copy_optional(&batch.precursor_mz, len),
            precursor_charge: copy_optional(&batch.precursor_charge, len),
            precursor_intensity: copy_optional(&batch.precursor_intensity, len),
            isolation_window_lower: copy_optional(&batch.isolation_window_lower, len),
            isolation_window_upper: copy_optional(&batch.isolation_window_upper, len),
            collision_energy: copy_optional(&batch.collision_energy, len),
            total_ion_current: copy_optional(&batch.total_ion_current, len),
            base_peak_mz: copy_optional(&batch.base_peak_mz, len),
            base_peak_intensity: copy_optional(&batch.base_peak_intensity, len),
            injection_time: copy_optional(&batch.injection_time, len),
            pixel_x: copy_optional(&batch.pixel_x, len),
            pixel_y: copy_optional(&batch.pixel_y, len),
            pixel_z: copy_optional(&batch.pixel_z, len),
        })
    }

    /// Transfers owned column buffers into Arrow without copying their payload.
    pub fn write_owned_batch(&mut self, batch: OwnedColumnarBatch) -> Result<(), WriterError> {
        let len = batch.len();
        for (name, actual) in [
            ("spectrum_id", batch.spectrum_id.len()),
            ("scan_number", batch.scan_number.len()),
            ("ms_level", batch.ms_level.len()),
            ("retention_time", batch.retention_time.len()),
            ("polarity", batch.polarity.len()),
            ("mz", batch.mz.len()),
            ("intensity", batch.intensity.len()),
        ] {
            if actual != len {
                return Err(WriterError::InvalidData(format!(
                    "{name} has {actual} rows, expected {len}"
                )));
            }
        }
        let mut null_storage = None;
        let arrays = vec![
            primitive::<Int64Type>(batch.spectrum_id),
            primitive::<Int64Type>(batch.scan_number),
            primitive::<Int16Type>(batch.ms_level),
            primitive::<Float32Type>(batch.retention_time),
            primitive::<Int8Type>(batch.polarity),
            primitive::<Float64Type>(batch.mz),
            primitive::<Float32Type>(batch.intensity),
            optional::<Float64Type>(batch.ion_mobility, len, &mut null_storage)?,
            optional::<Float64Type>(batch.precursor_mz, len, &mut null_storage)?,
            optional::<Int16Type>(batch.precursor_charge, len, &mut null_storage)?,
            optional::<Float32Type>(batch.precursor_intensity, len, &mut null_storage)?,
            optional::<Float32Type>(batch.isolation_window_lower, len, &mut null_storage)?,
            optional::<Float32Type>(batch.isolation_window_upper, len, &mut null_storage)?,
            optional::<Float32Type>(batch.collision_energy, len, &mut null_storage)?,
            optional::<Float64Type>(batch.total_ion_current, len, &mut null_storage)?,
            optional::<Float64Type>(batch.base_peak_mz, len, &mut null_storage)?,
            optional::<Float32Type>(batch.base_peak_intensity, len, &mut null_storage)?,
            optional::<Float32Type>(batch.injection_time, len, &mut null_storage)?,
            optional::<Int32Type>(batch.pixel_x, len, &mut null_storage)?,
            optional::<Int32Type>(batch.pixel_y, len, &mut null_storage)?,
            optional::<Int32Type>(batch.pixel_z, len, &mut null_storage)?,
        ];
        self.write_arrays(arrays, len)
    }

    fn write_arrays(&mut self, arrays: Vec<ArrayRef>, len: usize) -> Result<(), WriterError> {
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        if len != 0 {
            self.writer.write(&batch)?;
            self.peaks_written += len;
        }
        Ok(())
    }

    /// Writes a borrowed spectrum batch using bulk column appends.
    pub fn write_spectra_arrays(&mut self, spectra: &[SpectrumArrays]) -> Result<(), WriterError> {
        for spectrum in spectra {
            validate_peaks(spectrum)?;
        }
        let len = spectra.iter().map(SpectrumArrays::peak_count).sum();
        if len == 0 {
            return Ok(());
        }
        let mut mz = PrimitiveBuilder::<Float64Type>::with_capacity(len);
        let mut intensity = PrimitiveBuilder::<Float32Type>::with_capacity(len);
        let mut mobility = PrimitiveBuilder::<Float64Type>::with_capacity(len);
        for spectrum in spectra {
            mz.append_slice(&spectrum.peaks.mz);
            intensity.append_slice(&spectrum.peaks.intensity);
            match &spectrum.peaks.ion_mobility {
                OptionalColumnBuf::AllPresent(values) => mobility.append_slice(values),
                OptionalColumnBuf::AllNull { len } => mobility.append_nulls(*len),
                OptionalColumnBuf::WithValidity { values, validity } => {
                    mobility.append_values(values, validity)
                }
            }
        }
        let arrays = vec![
            repeated::<Int64Type>(spectra, len, |s| Some(s.spectrum_id)),
            repeated::<Int64Type>(spectra, len, |s| Some(s.scan_number)),
            repeated::<Int16Type>(spectra, len, |s| Some(s.ms_level)),
            repeated::<Float32Type>(spectra, len, |s| Some(s.retention_time)),
            repeated::<Int8Type>(spectra, len, |s| Some(s.polarity)),
            Arc::new(mz.finish()) as ArrayRef,
            Arc::new(intensity.finish()) as ArrayRef,
            Arc::new(mobility.finish()) as ArrayRef,
            repeated::<Float64Type>(spectra, len, |s| s.precursor_mz),
            repeated::<Int16Type>(spectra, len, |s| s.precursor_charge),
            repeated::<Float32Type>(spectra, len, |s| s.precursor_intensity),
            repeated::<Float32Type>(spectra, len, |s| s.isolation_window_lower),
            repeated::<Float32Type>(spectra, len, |s| s.isolation_window_upper),
            repeated::<Float32Type>(spectra, len, |s| s.collision_energy),
            repeated::<Float64Type>(spectra, len, |s| s.total_ion_current),
            repeated::<Float64Type>(spectra, len, |s| s.base_peak_mz),
            repeated::<Float32Type>(spectra, len, |s| s.base_peak_intensity),
            repeated::<Float32Type>(spectra, len, |s| s.injection_time),
            repeated::<Int32Type>(spectra, len, |s| s.pixel_x),
            repeated::<Int32Type>(spectra, len, |s| s.pixel_y),
            repeated::<Int32Type>(spectra, len, |s| s.pixel_z),
        ];
        self.write_arrays(arrays, len)?;
        self.spectra_written += spectra.len();
        Ok(())
    }

    /// Writes owned spectra; a single spectrum transfers its peak allocation.
    pub fn write_spectra_owned(
        &mut self,
        mut spectra: Vec<SpectrumArrays>,
    ) -> Result<(), WriterError> {
        if spectra.len() == 1 {
            return self.write_spectrum_owned(spectra.remove(0));
        }
        self.write_spectra_arrays(&spectra)
    }

    /// Transfers one spectrum's peak buffers into the owned batch path.
    pub fn write_spectrum_owned(&mut self, spectrum: SpectrumArrays) -> Result<(), WriterError> {
        validate_peaks(&spectrum)?;
        let nonempty = spectrum.peak_count() != 0;
        self.write_owned_batch(OwnedColumnarBatch::from_spectrum_arrays(spectrum))?;
        self.spectra_written += usize::from(nonempty);
        Ok(())
    }

    /// Writes one borrowed spectrum.
    pub fn write_spectrum_arrays(&mut self, spectrum: &SpectrumArrays) -> Result<(), WriterError> {
        self.write_spectra_arrays(std::slice::from_ref(spectrum))
    }

    /// Finalizes Parquet and returns its statistics.
    pub fn finish(self) -> Result<WriterStats, WriterError> {
        let metadata = self.writer.close()?;
        Ok(WriterStats {
            spectra_written: self.spectra_written,
            peaks_written: self.peaks_written,
            row_groups_written: metadata.row_groups.len(),
            file_size_bytes: metadata
                .row_groups
                .iter()
                .map(|g| g.total_byte_size as u64)
                .sum(),
        })
    }

    /// Finalizes Parquet and returns its sink.
    pub fn finish_into_inner(self) -> Result<W, WriterError> {
        Ok(self.writer.into_inner()?)
    }

    /// Current counts; final size and row groups are available after finish.
    pub fn stats(&self) -> WriterStats {
        WriterStats {
            spectra_written: self.spectra_written,
            peaks_written: self.peaks_written,
            row_groups_written: 0,
            file_size_bytes: 0,
        }
    }
}

fn primitive<T: ArrowPrimitiveType>(values: Vec<T::Native>) -> ArrayRef {
    Arc::new(PrimitiveArray::<T>::new(values.into(), None))
}

fn optional<T: ArrowPrimitiveType>(
    column: OptionalColumnBuf<T::Native>,
    len: usize,
    null_storage: &mut Option<(Buffer, NullBuffer)>,
) -> Result<ArrayRef, WriterError> {
    if column.len() != len {
        return Err(WriterError::InvalidData(
            "optional column length mismatch".into(),
        ));
    }
    Ok(match column {
        OptionalColumnBuf::AllPresent(values) => primitive::<T>(values),
        OptionalColumnBuf::AllNull { .. } => {
            // One aligned zero buffer and null bitmap serve every absent column.
            let (values, nulls) = null_storage.get_or_insert_with(|| {
                (
                    Buffer::from_vec(vec![0_u64; len]),
                    NullBuffer::new_null(len),
                )
            });
            Arc::new(PrimitiveArray::<T>::new(
                ScalarBuffer::new(values.clone(), 0, len),
                Some(nulls.clone()),
            ))
        }
        OptionalColumnBuf::WithValidity { values, validity } => {
            if validity.len() != len {
                return Err(WriterError::InvalidData("validity length mismatch".into()));
            }
            Arc::new(PrimitiveArray::<T>::new(
                values.into(),
                Some(validity.into()),
            ))
        }
    })
}

fn copy_optional<T: Copy>(column: &OptionalColumn<'_, T>, len: usize) -> OptionalColumnBuf<T> {
    match column {
        OptionalColumn::AllNull => OptionalColumnBuf::all_null(len),
        OptionalColumn::AllPresent(values) => OptionalColumnBuf::AllPresent(values.to_vec()),
        OptionalColumn::WithValidity { values, validity } => OptionalColumnBuf::WithValidity {
            values: values.to_vec(),
            validity: validity.to_vec(),
        },
    }
}

fn repeated<T: ArrowPrimitiveType>(
    spectra: &[SpectrumArrays],
    len: usize,
    value: impl Fn(&SpectrumArrays) -> Option<T::Native>,
) -> ArrayRef {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(len);
    for spectrum in spectra {
        match value(spectrum) {
            Some(value) => builder.append_value_n(value, spectrum.peak_count()),
            None => builder.append_nulls(spectrum.peak_count()),
        }
    }
    Arc::new(builder.finish())
}

fn validate_peaks(spectrum: &SpectrumArrays) -> Result<(), WriterError> {
    spectrum
        .peaks
        .validate()
        .map_err(WriterError::InvalidData)?;
    if let OptionalColumnBuf::WithValidity { values, validity } = &spectrum.peaks.ion_mobility {
        if values.len() != validity.len() {
            return Err(WriterError::InvalidData(
                "ion mobility validity length mismatch".into(),
            ));
        }
    }
    Ok(())
}
