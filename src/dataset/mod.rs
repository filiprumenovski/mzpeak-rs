//! One staged writer for v2 containers. Failed or dropped writes publish nothing.
use crate::{
    chromatogram_writer::{Chromatogram, ChromatogramWriter, ChromatogramWriterConfig},
    metadata::{MzPeakMetadata, VendorHints},
    schema::manifest::{Manifest, Modality},
    writer::{
        PeakArraysV2, PeaksWriterV2, PeaksWriterV2Config, PeaksWriterV2Stats, SpectraWriter,
        SpectraWriterConfig, SpectraWriterStats, SpectrumMetadata, SpectrumV2, WriterConfig,
        WriterError,
    },
};
use std::{
    fs::File,
    io::{BufReader, Write},
    path::{Path, PathBuf},
};
use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};
mod error;
pub use error::DatasetError;
pub const MZPEAK_V2_MIMETYPE: &str = "application/vnd.mzpeak+v2";
#[derive(Debug, Clone, Default)]
pub struct DatasetWriterV2Config {
    pub spectra_config: SpectraWriterConfig,
    pub peaks_config: PeaksWriterV2Config,
}
impl From<&WriterConfig> for DatasetWriterV2Config {
    fn from(config: &WriterConfig) -> Self {
        Self {
            spectra_config: SpectraWriterConfig {
                compression: config.compression,
                data_page_size: config.data_page_size,
                write_statistics: config.write_statistics,
                dictionary_page_size_limit: config.dictionary_page_size_limit,
                ..Default::default()
            },
            peaks_config: PeaksWriterV2Config {
                compression: config.compression,
                row_group_size: config.row_group_size,
                data_page_size: config.data_page_size,
                write_statistics: config.write_statistics,
                use_byte_stream_split: config.use_byte_stream_split,
                ..Default::default()
            },
        }
    }
}
#[derive(Debug)]
pub struct DatasetV2Stats {
    pub spectra_stats: SpectraWriterStats,
    pub peaks_stats: PeaksWriterV2Stats,
    pub output_file_size: u64,
}
pub struct MzPeakDatasetWriterV2 {
    output: PathBuf,
    staging: tempfile::TempDir,
    spectra: SpectraWriter<File>,
    peaks: PeaksWriterV2<File>,
    chromatograms: Option<ChromatogramWriter<File>>,
    metadata: MzPeakMetadata,
    modality: Modality,
    vendor_hints: Option<VendorHints>,
    spectra_count: u64,
    peak_count: u64,
    last_id: Option<u32>,
    has_precursor: bool,
    failed: bool,
}
impl MzPeakDatasetWriterV2 {
    pub fn new(
        path: impl AsRef<Path>,
        modality: Modality,
        vendor_hints: Option<VendorHints>,
    ) -> Result<Self, DatasetError> {
        Self::with_config(
            path,
            modality,
            vendor_hints,
            DatasetWriterV2Config::default(),
        )
    }
    pub fn with_config(
        path: impl AsRef<Path>,
        modality: Modality,
        vendor_hints: Option<VendorHints>,
        config: DatasetWriterV2Config,
    ) -> Result<Self, DatasetError> {
        let output = std::path::absolute(path)?;
        if output.exists() {
            return Err(DatasetError::AlreadyExists(output.display().to_string()));
        }
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)?;
        let staging = tempfile::tempdir_in(parent)?;
        let spectra = SpectraWriter::new(
            File::create(staging.path().join("spectra.parquet"))?,
            &config.spectra_config,
        )?;
        let peaks = PeaksWriterV2::new(
            File::create(staging.path().join("peaks.parquet"))?,
            &config.peaks_config,
            modality.has_ion_mobility(),
        )?;
        Ok(Self {
            output,
            staging,
            spectra,
            peaks,
            chromatograms: None,
            metadata: MzPeakMetadata::new(),
            modality,
            vendor_hints,
            spectra_count: 0,
            peak_count: 0,
            last_id: None,
            has_precursor: false,
            failed: false,
        })
    }
    pub fn set_metadata(&mut self, metadata: MzPeakMetadata) {
        self.metadata = metadata;
    }
    pub fn stats(&self) -> (u64, u64) {
        (self.spectra_count, self.peak_count)
    }
    pub fn write_spectrum(&mut self, spectrum: &SpectrumV2) -> Result<(), DatasetError> {
        self.write_spectrum_v2(&spectrum.metadata, &spectrum.peaks)
    }
    pub fn write_spectrum_v2(
        &mut self,
        metadata: &SpectrumMetadata,
        peaks: &PeakArraysV2,
    ) -> Result<(), DatasetError> {
        let invalid = |s: &str| DatasetError::WriterError(WriterError::InvalidData(s.into()));
        peaks.validate().map_err(WriterError::InvalidData)?;
        if metadata.peak_count as usize != peaks.len() {
            return Err(invalid("peak_count does not match peak arrays"));
        }
        if metadata.ms_level == 0
            || !metadata.retention_time.is_finite()
            || metadata.retention_time < 0.0
            || !matches!(metadata.polarity, -1..=1)
        {
            return Err(invalid("invalid MS level, retention time or polarity"));
        }
        if self.last_id.is_some_and(|id| metadata.spectrum_id <= id) {
            return Err(invalid("spectrum IDs must be strictly increasing"));
        }
        if self.modality.has_ion_mobility() != peaks.ion_mobility.is_some() {
            return Err(invalid("ion mobility does not match dataset modality"));
        }
        if self.modality.has_imaging() != (metadata.pixel_x.is_some() && metadata.pixel_y.is_some())
            || metadata.pixel_x.is_some() != metadata.pixel_y.is_some()
            || (!self.modality.has_imaging() && metadata.pixel_z.is_some())
        {
            return Err(invalid("imaging coordinates do not match dataset modality"));
        }
        if self.failed {
            return Err(invalid("writer failed during an earlier write"));
        }
        self.spectra.set_peak_offset(self.peak_count);
        if let Err(error) = self
            .peaks
            .write_peaks(metadata.spectrum_id, peaks)
            .and_then(|()| self.spectra.write_spectrum_metadata(metadata))
        {
            self.failed = true;
            return Err(error.into());
        }
        self.spectra_count += 1;
        self.peak_count += peaks.len() as u64;
        self.last_id = Some(metadata.spectrum_id);
        self.has_precursor |= metadata.precursor_mz.is_some();
        Ok(())
    }
    pub fn write_chromatogram(&mut self, chromatogram: &Chromatogram) -> Result<(), DatasetError> {
        if self.failed {
            return Err(
                WriterError::InvalidData("writer failed during an earlier write".into()).into(),
            );
        }
        let result = (|| {
            if self.chromatograms.is_none() {
                self.chromatograms = Some(
                    ChromatogramWriter::new(
                        File::create(self.staging.path().join("chromatograms.parquet"))?,
                        &self.metadata,
                        ChromatogramWriterConfig::default(),
                    )
                    .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?,
                );
            }
            self.chromatograms
                .as_mut()
                .ok_or(DatasetError::NotInitialized)?
                .write_chromatogram(chromatogram)
                .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))
        })();
        self.failed |= result.is_err();
        result
    }

    pub fn close(self) -> Result<DatasetV2Stats, DatasetError> {
        if self.failed {
            return Err(
                WriterError::InvalidData("writer failed during an earlier write".into()).into(),
            );
        }
        let spectra_stats = self.spectra.finish()?;
        let peaks_stats = self.peaks.finish()?;
        let has_chromatograms = self.chromatograms.is_some();
        if let Some(writer) = self.chromatograms {
            writer
                .finish()
                .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?;
        }
        let mut manifest = Manifest::new(
            self.modality,
            self.has_precursor,
            self.spectra_count,
            self.peak_count,
            chrono::Utc::now().to_rfc3339(),
            format!("mzpeak {}", env!("CARGO_PKG_VERSION")),
        );
        manifest.vendor_hints = self.vendor_hints;
        let temporary = tempfile::NamedTempFile::new_in(self.staging.path())?;
        let mut zip = ZipWriter::new(temporary.reopen()?);
        let stored = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        let compressed =
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        zip.start_file("mimetype", stored)?;
        zip.write_all(MZPEAK_V2_MIMETYPE.as_bytes())?;
        for (name, json) in [
            ("manifest.json", serde_json::to_vec(&manifest)?),
            ("metadata.json", serde_json::to_vec(&self.metadata)?),
        ] {
            zip.start_file(name, compressed)?;
            zip.write_all(&json)?;
        }
        let mut members = vec![
            ("spectra.parquet", "spectra/spectra.parquet"),
            ("peaks.parquet", "peaks/peaks.parquet"),
        ];
        if has_chromatograms {
            members.push((
                "chromatograms.parquet",
                "chromatograms/chromatograms.parquet",
            ));
        }
        for (file, member) in members {
            zip.start_file(member, stored)?;
            std::io::copy(
                &mut BufReader::new(File::open(self.staging.path().join(file))?),
                &mut zip,
            )?;
        }
        let file = zip.finish()?;
        let output_file_size = file.metadata()?.len();
        drop(file);
        temporary
            .persist_noclobber(&self.output)
            .map_err(|e| e.error)?;
        Ok(DatasetV2Stats {
            spectra_stats,
            peaks_stats,
            output_file_size,
        })
    }
}
