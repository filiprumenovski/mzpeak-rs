//! Streaming v2 table joins over seekable ZIP members, without extraction.
use crate::{
    dataset::MZPEAK_V2_MIMETYPE,
    metadata::MzPeakMetadata,
    schema::manifest::Manifest,
    writer::{PeakArraysV2, SpectrumMetadata, SpectrumV2},
};
use arrow::{
    array::{Array, Float32Array, Float64Array, PrimitiveArray},
    datatypes::*,
    record_batch::RecordBatch,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};
mod error;
pub mod zip_chunk_reader;
pub use error::ReaderError;
pub use zip_chunk_reader::ZipEntryChunkReader;
pub type RecordBatchIterator = Box<dyn Iterator<Item = Result<RecordBatch, ReaderError>> + Send>;
#[derive(Debug, Clone, Copy)]
pub struct ReaderConfig {
    pub batch_size: usize,
}
impl Default for ReaderConfig {
    fn default() -> Self {
        Self { batch_size: 65536 }
    }
}
pub struct MzPeakReader {
    path: PathBuf,
    config: ReaderConfig,
    manifest: Manifest,
    metadata: MzPeakMetadata,
}
fn invalid(s: impl Into<String>) -> ReaderError {
    ReaderError::InvalidFormat(s.into())
}
fn json_member<T: serde::de::DeserializeOwned>(
    zip: &mut zip::ZipArchive<BufReader<File>>,
    name: &str,
) -> Result<T, ReaderError> {
    let entry = zip.by_name(name)?;
    if entry.size() > 16 * 1024 * 1024 {
        return Err(invalid(format!("{name} exceeds 16 MiB")));
    }
    Ok(serde_json::from_reader(entry)?)
}
impl MzPeakReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReaderError> {
        Self::open_with_config(path, ReaderConfig::default())
    }
    pub fn open_with_config(
        path: impl AsRef<Path>,
        config: ReaderConfig,
    ) -> Result<Self, ReaderError> {
        if config.batch_size == 0 {
            return Err(invalid("batch_size must be positive"));
        }
        let path = std::path::absolute(path)?;
        let mut zip = zip::ZipArchive::new(BufReader::new(File::open(&path)?))?;
        {
            let mut first = zip.by_index(0)?;
            if first.name() != "mimetype"
                || first.compression() != zip::CompressionMethod::Stored
                || first.size() != MZPEAK_V2_MIMETYPE.len() as u64
            {
                return Err(invalid(
                    "expected a v2 container with a first, stored mimetype member",
                ));
            }
            let mut mime = String::new();
            first.read_to_string(&mut mime)?;
            if mime != MZPEAK_V2_MIMETYPE {
                return Err(invalid("only v2 containers are supported"));
            }
        }
        let manifest: Manifest = json_member(&mut zip, "manifest.json")?;
        if manifest.format_version != "2.0"
            || manifest.schema_version != "2.0"
            || manifest.has_ion_mobility != manifest.modality.has_ion_mobility()
            || manifest.has_imaging != manifest.modality.has_imaging()
        {
            return Err(invalid("invalid or unsupported v2 manifest"));
        }
        let metadata = json_member(&mut zip, "metadata.json")?;
        let reader = Self {
            path,
            config,
            manifest,
            metadata,
        };
        // Validate both table schemas and footer counts before returning a reader.
        drop(reader.table(
            "spectra/spectra.parquet",
            Some((
                &crate::schema::create_spectra_schema(),
                reader.manifest.spectrum_count,
            )),
        )?);
        drop(reader.table(
            "peaks/peaks.parquet",
            Some((
                &crate::schema::create_peaks_schema_v2(reader.manifest.has_ion_mobility),
                reader.manifest.peak_count,
            )),
        )?);
        Ok(reader)
    }
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub fn metadata(&self) -> &MzPeakMetadata {
        &self.metadata
    }
    pub fn total_peaks(&self) -> u64 {
        self.manifest.peak_count
    }
    pub fn total_spectra(&self) -> u64 {
        self.manifest.spectrum_count
    }
    fn table(
        &self,
        name: &str,
        expected: Option<(&Schema, u64)>,
    ) -> Result<RecordBatchIterator, ReaderError> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(ZipEntryChunkReader::new(&self.path, name)?)?;
        if let Some((schema, rows)) = expected {
            if builder.schema().fields() != schema.fields() {
                return Err(invalid(format!("invalid schema for {name}")));
            }
            if builder.metadata().file_metadata().num_rows() as u64 != rows {
                return Err(invalid(format!("manifest count disagrees with {name}")));
            }
        }
        Ok(Box::new(
            builder
                .with_batch_size(self.config.batch_size)
                .build()?
                .map(|b| b.map_err(Into::into)),
        ))
    }
    pub fn iter_batches(&self) -> Result<RecordBatchIterator, ReaderError> {
        self.table("peaks/peaks.parquet", None)
    }
    pub fn iter_spectra_metadata(&self) -> Result<SpectrumMetadataIterator, ReaderError> {
        Ok(SpectrumMetadataIterator {
            batches: self.table("spectra/spectra.parquet", None)?,
            batch: None,
            row: 0,
            done: false,
        })
    }
    pub fn iter_spectra_arrays_streaming(
        &self,
    ) -> Result<StreamingSpectrumArraysViewIterator, ReaderError> {
        Ok(StreamingSpectrumArraysViewIterator {
            metadata: self.iter_spectra_metadata()?,
            peaks: self.iter_batches()?,
            batch: None,
            row: 0,
            offset: 0,
            count: 0,
            expected_spectra: self.total_spectra(),
            expected_peaks: self.total_peaks(),
            mobility: self.manifest.has_ion_mobility,
            last_id: None,
            done: false,
        })
    }
    pub fn iter_spectra_arrays(&self) -> Result<Vec<SpectrumArraysView>, ReaderError> {
        self.iter_spectra_arrays_streaming()?.collect()
    }
    pub fn get_spectrum_arrays(&self, id: u32) -> Result<Option<SpectrumArraysView>, ReaderError> {
        // shortcut: ID queries scan both tables; add row-group selection for random-access workloads.
        for s in self.iter_spectra_arrays_streaming()? {
            let s = s?;
            if s.metadata.spectrum_id == id {
                return Ok(Some(s));
            }
        }
        Ok(None)
    }
    pub fn get_spectra_arrays(&self, ids: &[u32]) -> Result<Vec<SpectrumArraysView>, ReaderError> {
        let ids: HashSet<_> = ids.iter().copied().collect();
        self.iter_spectra_arrays_streaming()?
            .filter_map(|s| match s {
                Ok(s) if ids.contains(&s.metadata.spectrum_id) => Some(Ok(s)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }
    pub fn read_chromatograms(
        &self,
    ) -> Result<Option<Vec<crate::chromatogram_writer::Chromatogram>>, ReaderError> {
        use arrow::array::{ListArray, StringArray};
        let mut zip = zip::ZipArchive::new(BufReader::new(File::open(&self.path)?))?;
        match zip.by_name("chromatograms/chromatograms.parquet") {
            Ok(_) => {}
            Err(zip::result::ZipError::FileNotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let mut result = Vec::new();
        for batch in self.table("chromatograms/chromatograms.parquet", None)? {
            let batch = batch?;
            if batch.columns().iter().any(|a| a.null_count() != 0) {
                return Err(invalid("null chromatogram data"));
            }
            let strings = |name: &str| -> Result<&StringArray, ReaderError> {
                batch
                    .column_by_name(name)
                    .and_then(|a| a.as_any().downcast_ref())
                    .ok_or_else(|| invalid(name))
            };
            let lists = |name: &str| -> Result<&ListArray, ReaderError> {
                batch
                    .column_by_name(name)
                    .and_then(|a| a.as_any().downcast_ref())
                    .ok_or_else(|| invalid(name))
            };
            for row in 0..batch.num_rows() {
                let times = lists("time_array")?.value(row);
                let intensities = lists("intensity_array")?.value(row);
                let times = times
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| invalid("chromatogram time type"))?;
                let intensities = intensities
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| invalid("chromatogram intensity type"))?;
                if times.null_count() != 0 || intensities.null_count() != 0 {
                    return Err(invalid("null chromatogram array values"));
                }
                result.push(
                    crate::chromatogram_writer::Chromatogram::new(
                        strings("chromatogram_id")?.value(row).into(),
                        strings("chromatogram_type")?.value(row).into(),
                        times.values().to_vec(),
                        intensities.values().to_vec(),
                    )
                    .map_err(|e| invalid(e.to_string()))?,
                );
            }
        }
        Ok(Some(result))
    }
    pub fn verify_archive_crc(&self) -> Result<(), ReaderError> {
        let mut zip = zip::ZipArchive::new(BufReader::new(File::open(&self.path)?))?;
        for i in 0..zip.len() {
            std::io::copy(&mut zip.by_index(i)?, &mut std::io::sink())?;
        }
        Ok(())
    }
}
fn typed<'a, T: ArrowPrimitiveType>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a PrimitiveArray<T>, ReaderError> {
    batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref())
        .ok_or_else(|| invalid(format!("missing or mistyped column {name}")))
}
fn optional<T: ArrowPrimitiveType>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<T::Native>, ReaderError> {
    let a = typed::<T>(batch, name)?;
    Ok((!a.is_null(row)).then(|| a.value(row)))
}
fn required<T: ArrowPrimitiveType>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<T::Native, ReaderError> {
    optional::<T>(batch, name, row)?.ok_or_else(|| invalid(format!("null {name}")))
}
pub struct SpectrumMetadataView {
    pub metadata: SpectrumMetadata,
    pub peak_offset: u64,
}
impl std::ops::Deref for SpectrumMetadataView {
    type Target = SpectrumMetadata;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}
pub struct SpectrumMetadataIterator {
    batches: RecordBatchIterator,
    batch: Option<RecordBatch>,
    row: usize,
    done: bool,
}
impl SpectrumMetadataIterator {
    fn read(&mut self) -> Result<Option<SpectrumMetadataView>, ReaderError> {
        while self.batch.as_ref().is_none_or(|b| self.row == b.num_rows()) {
            self.batch = self.batches.next().transpose()?;
            self.row = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
        let b = self
            .batch
            .as_ref()
            .ok_or_else(|| invalid("metadata batch missing"))?;
        let r = self.row;
        self.row += 1;
        macro_rules! req {
            ($t:ty,$n:literal) => {
                required::<$t>(b, $n, r)?
            };
        }
        macro_rules! opt {
            ($t:ty,$n:literal) => {
                optional::<$t>(b, $n, r)?
            };
        }
        let metadata = SpectrumMetadata {
            spectrum_id: req!(UInt32Type, "spectrum_id"),
            scan_number: opt!(Int32Type, "scan_number"),
            ms_level: req!(UInt8Type, "ms_level"),
            retention_time: req!(Float32Type, "retention_time"),
            polarity: req!(Int8Type, "polarity"),
            peak_count: req!(UInt32Type, "peak_count"),
            precursor_mz: opt!(Float64Type, "precursor_mz"),
            precursor_charge: opt!(Int8Type, "precursor_charge"),
            precursor_intensity: opt!(Float32Type, "precursor_intensity"),
            isolation_window_lower: opt!(Float32Type, "isolation_window_lower"),
            isolation_window_upper: opt!(Float32Type, "isolation_window_upper"),
            collision_energy: opt!(Float32Type, "collision_energy"),
            total_ion_current: opt!(Float64Type, "total_ion_current"),
            base_peak_mz: opt!(Float64Type, "base_peak_mz"),
            base_peak_intensity: opt!(Float32Type, "base_peak_intensity"),
            injection_time: opt!(Float32Type, "injection_time"),
            pixel_x: opt!(UInt16Type, "pixel_x"),
            pixel_y: opt!(UInt16Type, "pixel_y"),
            pixel_z: opt!(UInt16Type, "pixel_z"),
        };
        Ok(Some(SpectrumMetadataView {
            metadata,
            peak_offset: req!(UInt64Type, "peak_offset"),
        }))
    }
}
impl Iterator for SpectrumMetadataIterator {
    type Item = Result<SpectrumMetadataView, ReaderError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.read() {
            Ok(Some(s)) => Some(Ok(s)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}
#[derive(Debug, Clone)]
struct Segment {
    mz: Float64Array,
    intensity: Float32Array,
    mobility: Option<Float64Array>,
}
#[derive(Debug, Clone)]
pub struct SpectrumArraysView {
    pub metadata: SpectrumMetadata,
    segments: Vec<Segment>,
    mobility: bool,
}
impl std::ops::Deref for SpectrumArraysView {
    type Target = SpectrumMetadata;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}
impl SpectrumArraysView {
    pub fn peak_count(&self) -> usize {
        self.metadata.peak_count as usize
    }
    pub fn mz(&self) -> impl Iterator<Item = f64> + '_ {
        self.segments
            .iter()
            .flat_map(|s| s.mz.values().iter().copied())
    }
    pub fn intensity(&self) -> impl Iterator<Item = f32> + '_ {
        self.segments
            .iter()
            .flat_map(|s| s.intensity.values().iter().copied())
    }
    pub fn ion_mobility(&self) -> impl Iterator<Item = f64> + '_ {
        self.segments
            .iter()
            .flat_map(|s| s.mobility.iter().flat_map(|a| a.values().iter().copied()))
    }
    pub fn to_owned(&self) -> Result<SpectrumV2, ReaderError> {
        Ok(SpectrumV2::new(
            self.metadata.clone(),
            PeakArraysV2 {
                mz: self.mz().collect(),
                intensity: self.intensity().collect(),
                ion_mobility: self.mobility.then(|| self.ion_mobility().collect()),
            },
        ))
    }
}
pub struct StreamingSpectrumArraysViewIterator {
    metadata: SpectrumMetadataIterator,
    peaks: RecordBatchIterator,
    batch: Option<RecordBatch>,
    row: usize,
    offset: u64,
    count: u64,
    expected_spectra: u64,
    expected_peaks: u64,
    mobility: bool,
    last_id: Option<u32>,
    done: bool,
}
impl StreamingSpectrumArraysViewIterator {
    fn read(&mut self) -> Result<Option<SpectrumArraysView>, ReaderError> {
        let Some(metadata) = self.metadata.next().transpose()? else {
            if self.count != self.expected_spectra
                || self.offset != self.expected_peaks
                || self.batch.as_ref().is_some_and(|b| self.row < b.num_rows())
            {
                return Err(invalid("v2 row counts or peak references do not match"));
            }
            for b in self.peaks.by_ref() {
                if b?.num_rows() != 0 {
                    return Err(invalid("unreferenced peak rows"));
                }
            }
            return Ok(None);
        };
        if metadata.peak_offset != self.offset
            || self.last_id.is_some_and(|id| metadata.spectrum_id <= id)
        {
            return Err(invalid("invalid peak offset or spectrum ID order"));
        }
        let mut remaining = metadata.peak_count as usize;
        let mut segments = Vec::new();
        while remaining > 0 {
            if self.batch.as_ref().is_none_or(|b| self.row == b.num_rows()) {
                self.batch = self.peaks.next().transpose()?;
                self.row = 0;
                if let Some(batch) = &self.batch {
                    if batch.columns().iter().any(|a| a.null_count() != 0) {
                        return Err(invalid("null value in required peak column"));
                    }
                }
            }
            let batch = self
                .batch
                .as_ref()
                .ok_or_else(|| invalid("truncated peaks table"))?;
            let length = remaining.min(batch.num_rows() - self.row);
            if length == 0 {
                continue;
            }
            let slice = batch.slice(self.row, length);
            if typed::<UInt32Type>(&slice, "spectrum_id")?
                .values()
                .iter()
                .any(|id| *id != metadata.spectrum_id)
            {
                return Err(invalid("peak IDs do not match spectrum metadata"));
            }
            segments.push(Segment {
                mz: typed::<Float64Type>(&slice, "mz")?.clone(),
                intensity: typed::<Float32Type>(&slice, "intensity")?.clone(),
                mobility: if self.mobility {
                    Some(typed::<Float64Type>(&slice, "ion_mobility")?.clone())
                } else {
                    None
                },
            });
            self.row += length;
            self.offset += length as u64;
            remaining -= length;
        }
        self.count += 1;
        self.last_id = Some(metadata.spectrum_id);
        Ok(Some(SpectrumArraysView {
            metadata: metadata.metadata,
            segments,
            mobility: self.mobility,
        }))
    }
}
impl Iterator for StreamingSpectrumArraysViewIterator {
    type Item = Result<SpectrumArraysView, ReaderError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.read() {
            Ok(Some(s)) => Some(Ok(s)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}
