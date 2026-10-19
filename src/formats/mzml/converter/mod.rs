//! One bounded conversion pipeline for sequential and parallel mzML decoding.
use super::{MzMLError, MzMLStreamer};
use crate::{
    chromatogram_writer::Chromatogram,
    dataset::{DatasetWriterV2Config, MzPeakDatasetWriterV2},
    ingest::IngestSpectrumConverter,
    schema::manifest::Modality,
    writer::{SpectrumV2, WriterConfig, WriterError},
};
use std::path::Path;
mod metadata;
mod spectrum;
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error(transparent)]
    MzML(#[from] MzMLError),
    #[error(transparent)]
    WriterError(#[from] WriterError),
    #[error(transparent)]
    Dataset(#[from] crate::dataset::DatasetError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Metadata(#[from] crate::metadata::MetadataError),
    #[error("scan {index} ({id}): {source}")]
    BinaryDecodeError {
        index: i64,
        id: String,
        #[source]
        source: super::binary::BinaryDecodeError,
    },
}
#[derive(Debug, Clone)]
pub struct ConversionConfig {
    pub writer_config: WriterConfig,
    pub batch_size: usize,
    pub include_chromatograms: bool,
    pub modality: Option<Modality>,
}
impl Default for ConversionConfig {
    fn default() -> Self {
        Self {
            writer_config: WriterConfig::default(),
            batch_size: 256,
            include_chromatograms: true,
            modality: None,
        }
    }
}
pub use crate::convert::ConversionStats;
pub struct MzMLConverter {
    config: ConversionConfig,
}
impl Default for MzMLConverter {
    fn default() -> Self {
        Self::new()
    }
}
impl MzMLConverter {
    pub fn new() -> Self {
        Self::with_config(ConversionConfig::default())
    }
    pub fn with_config(config: ConversionConfig) -> Self {
        Self { config }
    }
    pub fn convert(
        &self,
        input: impl AsRef<Path>,
        output: impl AsRef<Path>,
    ) -> Result<ConversionStats, ConversionError> {
        let input = input.as_ref();
        let output = output.as_ref();
        let imaging = input
            .extension()
            .is_some_and(|s| s.eq_ignore_ascii_case("imzml"));
        let mut stream = if imaging {
            MzMLStreamer::open_imzml(input)?
        } else {
            MzMLStreamer::open_with_buffer_size(input, 1 << 20)?
        };
        let metadata = self.convert_metadata(stream.read_metadata()?, input)?;
        let expected = stream.spectrum_count();
        let mut pending = stream.next_raw_spectrum()?;
        let modality = self.config.modality.unwrap_or_else(|| {
            Modality::from_flags(
                pending
                    .as_ref()
                    .is_some_and(|s| s.ion_mobility_data.is_some()),
                imaging || pending.as_ref().is_some_and(|s| s.pixel_x.is_some()),
            )
        });
        let config = DatasetWriterV2Config::from(&self.config.writer_config);
        let mut writer = MzPeakDatasetWriterV2::with_config(
            output,
            modality,
            metadata.vendor_hints.clone(),
            config,
        )?;
        writer.set_metadata(metadata);
        let mut stats = ConversionStats {
            source_file_size: std::fs::metadata(input)?.len(),
            ..Default::default()
        };
        let mut converter = IngestSpectrumConverter::new();
        let (mut times, mut tic, mut bpc) = (Vec::new(), Vec::new(), Vec::new());
        loop {
            let mut batch = Vec::new();
            while batch.len() < self.config.batch_size.max(1) {
                match if let Some(raw) = pending.take() {
                    Some(raw)
                } else {
                    stream.next_raw_spectrum()?
                } {
                    Some(raw) => batch.push(raw),
                    None => break,
                }
            }
            if batch.is_empty() {
                break;
            }
            #[cfg(feature = "mzml-parallel")]
            let decoded = {
                use rayon::prelude::*;
                batch
                    .into_par_iter()
                    .map(|raw| self.build_ingest_spectrum_raw(raw))
                    .collect::<Result<Vec<_>, _>>()?
            };
            #[cfg(not(feature = "mzml-parallel"))]
            let decoded = batch
                .into_iter()
                .map(|raw| self.build_ingest_spectrum_raw(raw))
                .collect::<Result<Vec<_>, _>>()?;
            for raw in decoded {
                let spectrum = SpectrumV2::try_from_spectrum_arrays(
                    converter.convert(raw.ingest).map_err(WriterError::from)?,
                )?;
                match spectrum.metadata.ms_level {
                    1 => stats.ms1_spectra += 1,
                    2 => stats.ms2_spectra += 1,
                    _ => stats.msn_spectra += 1,
                }
                if self.config.include_chromatograms && spectrum.metadata.ms_level == 1 {
                    times.push(spectrum.metadata.retention_time as f64);
                    tic.push(spectrum.metadata.total_ion_current.unwrap_or(0.0) as f32);
                    bpc.push(spectrum.metadata.base_peak_intensity.unwrap_or(0.0));
                }
                stats.spectra_count += 1;
                stats.peak_count += spectrum.peaks.len();
                writer.write_spectrum(&spectrum)?;
            }
        }
        if expected.is_some_and(|count| count != stats.spectra_count) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "mzML spectrum count mismatch",
            )
            .into());
        }
        // Drain the XML even when chromatograms are disabled, so malformed trailers fail.
        while let Some(raw) = stream.next_chromatogram()? {
            if self.config.include_chromatograms {
                writer.write_chromatogram(
                    &Chromatogram::new(
                        raw.id,
                        format!("{:?}", raw.chromatogram_type),
                        raw.time_array,
                        raw.intensity_array.into_iter().map(|v| v as f32).collect(),
                    )
                    .map_err(|e| WriterError::InvalidData(e.to_string()))?,
                )?;
                stats.chromatograms_converted += 1;
            }
        }
        stream.finish_document()?;
        if self.config.include_chromatograms
            && stats.chromatograms_converted == 0
            && !times.is_empty()
        {
            for (name, values) in [("TIC", tic), ("BPC", bpc)] {
                writer.write_chromatogram(
                    &Chromatogram::new(name.into(), name.into(), times.clone(), values)
                        .map_err(|e| WriterError::InvalidData(e.to_string()))?,
                )?;
                stats.chromatograms_converted += 1;
            }
        }
        stats.output_file_size = writer.close()?.output_file_size;
        stats.compression_ratio = stats.source_file_size as f64 / stats.output_file_size as f64;
        Ok(stats)
    }
}
