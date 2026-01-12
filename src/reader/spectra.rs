use std::collections::HashSet;
use std::fs::File;

use arrow::array::{Array, Float32Array, Float64Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;

use crate::schema::columns;
use crate::writer::{OptionalColumnBuf, PeakArrays, SpectrumArrays};

use super::config::ReaderSource;
use super::utils::{
    get_float32_column, get_float64_column, get_int16_column, get_int64_column, get_int8_column,
    get_optional_f32, get_optional_f64, get_optional_float32_column, get_optional_float64_column,
    get_optional_i16, get_optional_i32, get_optional_int16_column, get_optional_int32_column,
};
use super::{MzPeakReader, ReaderError, RecordBatchIterator};

fn spectrum_id_column_index(metadata: &ParquetMetaData) -> Option<usize> {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.name() == columns::SPECTRUM_ID)
}

fn row_groups_for_spectrum_id_range(
    metadata: &ParquetMetaData,
    column_index: usize,
    min_id: i64,
    max_id: i64,
) -> Vec<usize> {
    let mut row_groups = Vec::new();
    let num_row_groups = metadata.num_row_groups();

    for i in 0..num_row_groups {
        let column = metadata.row_group(i).column(column_index);
        match column.statistics() {
            Some(Statistics::Int64(stats)) => {
                let min = stats.min_opt();
                let max = stats.max_opt();
                if stats.min_is_exact() && stats.max_is_exact() {
                    if let (Some(min), Some(max)) = (min, max) {
                        if max_id >= *min && min_id <= *max {
                            row_groups.push(i);
                        }
                    } else {
                        row_groups.push(i);
                    }
                } else {
                    row_groups.push(i);
                }
            }
            _ => row_groups.push(i),
        }
    }

    row_groups
}

impl MzPeakReader {
    fn build_iter_for_spectrum_id_range<T: parquet::file::reader::ChunkReader + 'static>(
        &self,
        builder: ParquetRecordBatchReaderBuilder<T>,
        min_id: i64,
        max_id: i64,
    ) -> Result<RecordBatchIterator, ReaderError> {
        let metadata = builder.metadata();
        let row_groups = spectrum_id_column_index(metadata)
            .map(|column_index| {
                row_groups_for_spectrum_id_range(metadata, column_index, min_id, max_id)
            })
            .unwrap_or_else(|| (0..metadata.num_row_groups()).collect());

        if row_groups.is_empty() {
            let empty = std::iter::empty::<Result<RecordBatch, arrow::error::ArrowError>>();
            return Ok(RecordBatchIterator::new(empty));
        }

        let builder = builder
            .with_batch_size(self.config.batch_size)
            .with_row_groups(row_groups);
        let reader = builder.build()?;
        Ok(RecordBatchIterator::new(reader))
    }

    fn iter_batches_for_spectrum_id_range(
        &self,
        min_id: i64,
        max_id: i64,
    ) -> Result<RecordBatchIterator, ReaderError> {
        match &self.source {
            ReaderSource::FilePath(path) => {
                let file = File::open(path)?;
                self.build_iter_for_spectrum_id_range(
                    ParquetRecordBatchReaderBuilder::try_new(file)?,
                    min_id,
                    max_id,
                )
            }
            ReaderSource::ZipContainer { chunk_reader, .. } => self.build_iter_for_spectrum_id_range(
                ParquetRecordBatchReaderBuilder::try_new(chunk_reader.clone())?,
                min_id,
                max_id,
            ),
        }
    }

    /// Iterate over all spectra in the file as SoA array views (eager)
    ///
    /// This yields view-backed spectra that reference Arrow buffers directly.
    /// Materialize with `to_owned()` when needed.
    pub fn iter_spectra_arrays(&self) -> Result<Vec<SpectrumArraysView>, ReaderError> {
        let iter = self.iter_spectra_arrays_streaming()?;
        iter.collect()
    }

    /// Streaming iterator over spectra as SoA array views
    ///
    /// Returns a lazy iterator that yields view-backed spectra referencing
    /// Arrow buffers directly. Memory usage is bounded by batch_size.
    pub fn iter_spectra_arrays_streaming(
        &self,
    ) -> Result<StreamingSpectrumArraysViewIterator, ReaderError> {
        let batch_iter = self.iter_batches()?;
        Ok(StreamingSpectrumArraysViewIterator::new(batch_iter))
    }

    /// Query spectra by retention time range (inclusive), SoA layout
    pub fn spectra_by_rt_range_arrays(
        &self,
        start_rt: f32,
        end_rt: f32,
    ) -> Result<Vec<SpectrumArraysView>, ReaderError> {
        let all_spectra = self.iter_spectra_arrays()?;
        Ok(all_spectra
            .into_iter()
            .filter(|s| s.retention_time >= start_rt && s.retention_time <= end_rt)
            .collect())
    }

    /// Query spectra by MS level, SoA layout
    pub fn spectra_by_ms_level_arrays(
        &self,
        ms_level: i16,
    ) -> Result<Vec<SpectrumArraysView>, ReaderError> {
        let all_spectra = self.iter_spectra_arrays()?;
        Ok(all_spectra
            .into_iter()
            .filter(|s| s.ms_level == ms_level)
            .collect())
    }

    /// Get a specific spectrum by ID, SoA layout
    pub fn get_spectrum_arrays(
        &self,
        spectrum_id: i64,
    ) -> Result<Option<SpectrumArraysView>, ReaderError> {
        let batch_iter = self.iter_batches_for_spectrum_id_range(spectrum_id, spectrum_id)?;
        let iter = StreamingSpectrumArraysViewIterator::new(batch_iter);
        for spectrum in iter {
            let spectrum = spectrum?;
            if spectrum.spectrum_id == spectrum_id {
                return Ok(Some(spectrum));
            }
        }
        Ok(None)
    }

    /// Get multiple spectra by their IDs, SoA layout
    pub fn get_spectra_arrays(
        &self,
        spectrum_ids: &[i64],
    ) -> Result<Vec<SpectrumArraysView>, ReaderError> {
        let id_set: HashSet<_> = spectrum_ids.iter().collect();
        if id_set.is_empty() {
            return Ok(Vec::new());
        }

        let min_id = **id_set.iter().min().unwrap();
        let max_id = **id_set.iter().max().unwrap();
        let batch_iter = self.iter_batches_for_spectrum_id_range(min_id, max_id)?;
        let iter = StreamingSpectrumArraysViewIterator::new(batch_iter);
        let mut matches = Vec::new();
        for spectrum in iter {
            let spectrum = spectrum?;
            if id_set.contains(&spectrum.spectrum_id) {
                matches.push(spectrum);
            }
        }
        Ok(matches)
    }

    /// Get all unique spectrum IDs in the file
    pub fn spectrum_ids(&self) -> Result<Vec<i64>, ReaderError> {
        let spectra = self.iter_spectra_arrays()?;
        Ok(spectra.into_iter().map(|s| s.spectrum_id).collect())
    }
}

/// View-backed SoA spectrum that references Arrow buffers.
#[derive(Debug, Clone)]
pub struct SpectrumArraysView {
    segments: Vec<SpectrumArraysViewSegment>,
    /// Unique spectrum identifier.
    pub spectrum_id: i64,
    /// Native scan number from the instrument.
    pub scan_number: i64,
    /// MS level (1, 2, 3, ...).
    pub ms_level: i16,
    /// Retention time in seconds.
    pub retention_time: f32,
    /// Polarity: 1 for positive, -1 for negative.
    pub polarity: i8,
    /// Precursor m/z (for MS2+).
    pub precursor_mz: Option<f64>,
    /// Precursor charge state.
    pub precursor_charge: Option<i16>,
    /// Precursor intensity.
    pub precursor_intensity: Option<f32>,
    /// Isolation window lower offset.
    pub isolation_window_lower: Option<f32>,
    /// Isolation window upper offset.
    pub isolation_window_upper: Option<f32>,
    /// Collision energy in eV.
    pub collision_energy: Option<f32>,
    /// Total ion current.
    pub total_ion_current: Option<f64>,
    /// Base peak m/z.
    pub base_peak_mz: Option<f64>,
    /// Base peak intensity.
    pub base_peak_intensity: Option<f32>,
    /// Ion injection time in ms.
    pub injection_time: Option<f32>,
    /// MSI X pixel coordinate.
    pub pixel_x: Option<i32>,
    /// MSI Y pixel coordinate.
    pub pixel_y: Option<i32>,
    /// MSI Z pixel coordinate.
    pub pixel_z: Option<i32>,
    num_peaks: usize,
}

#[derive(Debug, Clone)]
struct SpectrumArraysViewSegment {
    batch: RecordBatch,
    start: usize,
    len: usize,
}

impl SpectrumArraysView {
    fn from_segments(segments: Vec<SpectrumArraysViewSegment>) -> Result<Self, ReaderError> {
        let (batch, row) = {
            let first = segments.first().ok_or_else(|| {
                ReaderError::InvalidFormat("empty spectrum view segments".to_string())
            })?;
            (first.batch.clone(), first.start)
        };

        let spectrum_ids = get_int64_column(&batch, columns::SPECTRUM_ID)?;
        let scan_numbers = get_int64_column(&batch, columns::SCAN_NUMBER)?;
        let ms_levels = get_int16_column(&batch, columns::MS_LEVEL)?;
        let retention_times = get_float32_column(&batch, columns::RETENTION_TIME)?;
        let polarities = get_int8_column(&batch, columns::POLARITY)?;

        let precursor_mzs = get_optional_float64_column(&batch, columns::PRECURSOR_MZ);
        let precursor_charges = get_optional_int16_column(&batch, columns::PRECURSOR_CHARGE);
        let precursor_intensities =
            get_optional_float32_column(&batch, columns::PRECURSOR_INTENSITY);
        let isolation_lowers =
            get_optional_float32_column(&batch, columns::ISOLATION_WINDOW_LOWER);
        let isolation_uppers =
            get_optional_float32_column(&batch, columns::ISOLATION_WINDOW_UPPER);
        let collision_energies = get_optional_float32_column(&batch, columns::COLLISION_ENERGY);
        let tics = get_optional_float64_column(&batch, columns::TOTAL_ION_CURRENT);
        let base_peak_mzs = get_optional_float64_column(&batch, columns::BASE_PEAK_MZ);
        let base_peak_intensities =
            get_optional_float32_column(&batch, columns::BASE_PEAK_INTENSITY);
        let injection_times = get_optional_float32_column(&batch, columns::INJECTION_TIME);
        let pixel_xs = get_optional_int32_column(&batch, columns::PIXEL_X);
        let pixel_ys = get_optional_int32_column(&batch, columns::PIXEL_Y);
        let pixel_zs = get_optional_int32_column(&batch, columns::PIXEL_Z);

        let num_peaks = segments.iter().map(|s| s.len).sum();

        Ok(Self {
            segments,
            spectrum_id: spectrum_ids.value(row),
            scan_number: scan_numbers.value(row),
            ms_level: ms_levels.value(row),
            retention_time: retention_times.value(row),
            polarity: polarities.value(row),
            precursor_mz: get_optional_f64(precursor_mzs, row),
            precursor_charge: get_optional_i16(precursor_charges, row),
            precursor_intensity: get_optional_f32(precursor_intensities, row),
            isolation_window_lower: get_optional_f32(isolation_lowers, row),
            isolation_window_upper: get_optional_f32(isolation_uppers, row),
            collision_energy: get_optional_f32(collision_energies, row),
            total_ion_current: get_optional_f64(tics, row),
            base_peak_mz: get_optional_f64(base_peak_mzs, row),
            base_peak_intensity: get_optional_f32(base_peak_intensities, row),
            injection_time: get_optional_f32(injection_times, row),
            pixel_x: get_optional_i32(pixel_xs, row),
            pixel_y: get_optional_i32(pixel_ys, row),
            pixel_z: get_optional_i32(pixel_zs, row),
            num_peaks,
        })
    }

    /// Number of peaks in this spectrum.
    pub fn peak_count(&self) -> usize {
        self.num_peaks
    }

    /// Return m/z arrays for each segment (zero-copy slices).
    pub fn mz_arrays(&self) -> Result<Vec<Float64Array>, ReaderError> {
        self.segments
            .iter()
            .map(|seg| slice_float64_column(&seg.batch, columns::MZ, seg.start, seg.len))
            .collect()
    }

    /// Return intensity arrays for each segment (zero-copy slices).
    pub fn intensity_arrays(&self) -> Result<Vec<Float32Array>, ReaderError> {
        self.segments
            .iter()
            .map(|seg| slice_float32_column(&seg.batch, columns::INTENSITY, seg.start, seg.len))
            .collect()
    }

    /// Return ion mobility arrays for each segment (zero-copy slices), if present.
    pub fn ion_mobility_arrays(&self) -> Result<Option<Vec<Float64Array>>, ReaderError> {
        let mut arrays = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            match slice_optional_float64_column(&seg.batch, columns::ION_MOBILITY, seg.start, seg.len)? {
                Some(array) => arrays.push(array),
                None => return Ok(None),
            }
        }
        Ok(Some(arrays))
    }

    /// Materialize this view into an owned SpectrumArrays.
    pub fn to_owned(&self) -> Result<SpectrumArrays, ReaderError> {
        let has_ion_mobility = self
            .segments
            .first()
            .and_then(|seg| get_optional_float64_column(&seg.batch, columns::ION_MOBILITY))
            .is_some();

        let mut builder = SpectrumArraysBuilder::new(
            self.spectrum_id,
            self.scan_number,
            self.ms_level,
            self.retention_time,
            self.polarity,
            self.precursor_mz,
            self.precursor_charge,
            self.precursor_intensity,
            self.isolation_window_lower,
            self.isolation_window_upper,
            self.collision_energy,
            self.total_ion_current,
            self.base_peak_mz,
            self.base_peak_intensity,
            self.injection_time,
            self.pixel_x,
            self.pixel_y,
            self.pixel_z,
            has_ion_mobility,
        );

        for seg in &self.segments {
            let batch = &seg.batch;
            let mzs = get_float64_column(batch, columns::MZ)?;
            let intensities = get_float32_column(batch, columns::INTENSITY)?;
            let ion_mobilities = get_optional_float64_column(batch, columns::ION_MOBILITY);

            for i in seg.start..seg.start + seg.len {
                builder.push_peak(
                    mzs.value(i),
                    intensities.value(i),
                    get_optional_f64(ion_mobilities, i),
                );
            }
        }

        Ok(builder.finish())
    }
}

fn slice_float64_column(
    batch: &RecordBatch,
    name: &str,
    start: usize,
    len: usize,
) -> Result<Float64Array, ReaderError> {
    let column = get_float64_column(batch, name)?;
    let array = column.slice(start, len);
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Float64", name)))
        .cloned()
}

fn slice_float32_column(
    batch: &RecordBatch,
    name: &str,
    start: usize,
    len: usize,
) -> Result<Float32Array, ReaderError> {
    let column = get_float32_column(batch, name)?;
    let array = column.slice(start, len);
    array
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Float32", name)))
        .cloned()
}

fn slice_optional_float64_column(
    batch: &RecordBatch,
    name: &str,
    start: usize,
    len: usize,
) -> Result<Option<Float64Array>, ReaderError> {
    let column = match get_optional_float64_column(batch, name) {
        Some(column) => column,
        None => return Ok(None),
    };
    let array = column.slice(start, len);
    let array = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Float64", name)))?
        .clone();
    Ok(Some(array))
}

/// Streaming iterator over spectra as view-backed SoA layout
pub struct StreamingSpectrumArraysViewIterator {
    batch_iter: super::batches::RecordBatchIterator,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    pending: Option<SpectrumArraysViewBuilder>,
    ready: std::collections::VecDeque<SpectrumArraysView>,
    exhausted: bool,
}

impl StreamingSpectrumArraysViewIterator {
    pub(super) fn new(batch_iter: super::batches::RecordBatchIterator) -> Self {
        Self {
            batch_iter,
            current_batch: None,
            current_row: 0,
            pending: None,
            ready: std::collections::VecDeque::new(),
            exhausted: false,
        }
    }

    fn load_next_batch(&mut self) -> Option<RecordBatch> {
        match self.batch_iter.next() {
            Some(Ok(batch)) => {
                self.current_row = 0;
                Some(batch)
            }
            Some(Err(e)) => {
                log::error!("Error reading batch: {}", e);
                self.exhausted = true;
                None
            }
            None => {
                self.exhausted = true;
                None
            }
        }
    }
}

impl Iterator for StreamingSpectrumArraysViewIterator {
    type Item = Result<SpectrumArraysView, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(view) = self.ready.pop_front() {
                return Some(Ok(view));
            }

            if self.current_batch.is_none() {
                if self.exhausted {
                    return self
                        .pending
                        .take()
                        .map(|pending| pending.finish().map_err(|e| e));
                }
                self.current_batch = self.load_next_batch();
                if self.current_batch.is_none() {
                    return self
                        .pending
                        .take()
                        .map(|pending| pending.finish().map_err(|e| e));
                }
            }

            let batch = self.current_batch.as_ref()?;

            if self.current_row >= batch.num_rows() {
                self.current_batch = None;
                continue;
            }

            let spectrum_ids = match get_int64_column(batch, columns::SPECTRUM_ID) {
                Ok(col) => col,
                Err(e) => return Some(Err(e)),
            };

            let start = self.current_row;
            let spectrum_id = spectrum_ids.value(start);
            let mut end = start + 1;
            while end < batch.num_rows() && spectrum_ids.value(end) == spectrum_id {
                end += 1;
            }

            let len = end - start;

            match &mut self.pending {
                None => {
                    self.pending = Some(SpectrumArraysViewBuilder::new(spectrum_id));
                }
                Some(pending) if pending.spectrum_id != spectrum_id => {
                    let completed = match self.pending.take().unwrap().finish() {
                        Ok(view) => view,
                        Err(e) => return Some(Err(e)),
                    };
                    self.ready.push_back(completed);
                    self.pending = Some(SpectrumArraysViewBuilder::new(spectrum_id));
                }
                _ => {}
            }

            if let Some(pending) = &mut self.pending {
                pending.push_segment(batch, start, len);
            }

            self.current_row = end;
        }
    }
}

struct SpectrumArraysViewBuilder {
    spectrum_id: i64,
    segments: Vec<SpectrumArraysViewSegment>,
}

impl SpectrumArraysViewBuilder {
    fn new(spectrum_id: i64) -> Self {
        Self {
            spectrum_id,
            segments: Vec::new(),
        }
    }

    fn push_segment(&mut self, batch: &RecordBatch, start: usize, len: usize) {
        self.segments.push(SpectrumArraysViewSegment {
            batch: batch.clone(),
            start,
            len,
        });
    }

    fn finish(self) -> Result<SpectrumArraysView, ReaderError> {
        SpectrumArraysView::from_segments(self.segments)
    }
}

struct SpectrumArraysBuilder {
    spectrum_id: i64,
    scan_number: i64,
    ms_level: i16,
    retention_time: f32,
    polarity: i8,
    precursor_mz: Option<f64>,
    precursor_charge: Option<i16>,
    precursor_intensity: Option<f32>,
    isolation_window_lower: Option<f32>,
    isolation_window_upper: Option<f32>,
    collision_energy: Option<f32>,
    total_ion_current: Option<f64>,
    base_peak_mz: Option<f64>,
    base_peak_intensity: Option<f32>,
    injection_time: Option<f32>,
    pixel_x: Option<i32>,
    pixel_y: Option<i32>,
    pixel_z: Option<i32>,
    mz: Vec<f64>,
    intensity: Vec<f32>,
    ion_mobility: Option<IonMobilityBuffer>,
}

impl SpectrumArraysBuilder {
    #[allow(clippy::too_many_arguments)]
    fn new(
        spectrum_id: i64,
        scan_number: i64,
        ms_level: i16,
        retention_time: f32,
        polarity: i8,
        precursor_mz: Option<f64>,
        precursor_charge: Option<i16>,
        precursor_intensity: Option<f32>,
        isolation_window_lower: Option<f32>,
        isolation_window_upper: Option<f32>,
        collision_energy: Option<f32>,
        total_ion_current: Option<f64>,
        base_peak_mz: Option<f64>,
        base_peak_intensity: Option<f32>,
        injection_time: Option<f32>,
        pixel_x: Option<i32>,
        pixel_y: Option<i32>,
        pixel_z: Option<i32>,
        has_ion_mobility: bool,
    ) -> Self {
        Self {
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            precursor_mz,
            precursor_charge,
            precursor_intensity,
            isolation_window_lower,
            isolation_window_upper,
            collision_energy,
            total_ion_current,
            base_peak_mz,
            base_peak_intensity,
            injection_time,
            pixel_x,
            pixel_y,
            pixel_z,
            mz: Vec::new(),
            intensity: Vec::new(),
            ion_mobility: if has_ion_mobility {
                Some(IonMobilityBuffer::default())
            } else {
                None
            },
        }
    }

    fn push_peak(&mut self, mz: f64, intensity: f32, ion_mobility: Option<f64>) {
        self.mz.push(mz);
        self.intensity.push(intensity);
        if let Some(ref mut buffer) = self.ion_mobility {
            buffer.push(ion_mobility);
        }
    }

    fn finish(self) -> SpectrumArrays {
        let len = self.mz.len();
        let ion_mobility = match self.ion_mobility {
            None => OptionalColumnBuf::all_null(len),
            Some(buffer) => buffer.finish(len),
        };

        SpectrumArrays {
            spectrum_id: self.spectrum_id,
            scan_number: self.scan_number,
            ms_level: self.ms_level,
            retention_time: self.retention_time,
            polarity: self.polarity,
            precursor_mz: self.precursor_mz,
            precursor_charge: self.precursor_charge,
            precursor_intensity: self.precursor_intensity,
            isolation_window_lower: self.isolation_window_lower,
            isolation_window_upper: self.isolation_window_upper,
            collision_energy: self.collision_energy,
            total_ion_current: self.total_ion_current,
            base_peak_mz: self.base_peak_mz,
            base_peak_intensity: self.base_peak_intensity,
            injection_time: self.injection_time,
            pixel_x: self.pixel_x,
            pixel_y: self.pixel_y,
            pixel_z: self.pixel_z,
            peaks: PeakArrays {
                mz: self.mz,
                intensity: self.intensity,
                ion_mobility,
            },
        }
    }
}

#[derive(Default)]
struct IonMobilityBuffer {
    values: Vec<f64>,
    validity: Vec<bool>,
    has_any: bool,
    all_present: bool,
}

impl IonMobilityBuffer {
    fn push(&mut self, value: Option<f64>) {
        if self.values.is_empty() {
            self.all_present = true;
        }
        match value {
            Some(v) => {
                self.values.push(v);
                self.validity.push(true);
                self.has_any = true;
            }
            None => {
                self.values.push(0.0);
                self.validity.push(false);
                self.all_present = false;
            }
        }
    }

    fn finish(self, len: usize) -> OptionalColumnBuf<f64> {
        if !self.has_any {
            OptionalColumnBuf::all_null(len)
        } else if self.all_present {
            OptionalColumnBuf::AllPresent(self.values)
        } else {
            OptionalColumnBuf::WithValidity {
                values: self.values,
                validity: self.validity,
            }
        }
    }
}
