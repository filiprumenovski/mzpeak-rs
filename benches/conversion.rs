use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mzpeak::dataset::MzPeakDatasetWriter;
use mzpeak::metadata::MzPeakMetadata;
use mzpeak::mzml::converter::{ConversionConfig, MzMLConverter};
use mzpeak::writer::{
    OptionalColumnBuf, PeakArrays, SpectrumArrays, WriterConfig,
};
use std::fs;
use tempfile::TempDir;

/// Generate synthetic mzML data for benchmarking
fn generate_test_mzml(path: &std::path::Path, num_spectra: usize, peaks_per_spectrum: usize) {
    let mut content = String::from(
        r#"<?xml version="1.0" encoding="utf-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1">
  <cvList count="1">
    <cv id="MS" fullName="Proteomics Standards Initiative Mass Spectrometry Ontology" version="4.1.0" URI="https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo"/>
  </cvList>
  <run id="test_run" defaultInstrumentConfigurationRef="IC1">
    <spectrumList count=""#,
    );
    content.push_str(&format!("{}\">", num_spectra));

    for i in 0..num_spectra {
        let rt = i as f64 * 0.5;
        let ms_level = if i % 10 == 0 { 1 } else { 2 };

        content.push_str(&format!(
            r#"
      <spectrum index="{}" id="scan={}" defaultArrayLength="{}">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="{}"/>
        <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="{}" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
        <cvParam cvRef="MS" accession="MS:1000129" name="negative scan" value=""/>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="0">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>"#,
            i, i + 1, peaks_per_spectrum, ms_level, rt
        ));

        // Generate base64-encoded m/z values
        let mz_data: Vec<f64> = (0..peaks_per_spectrum)
            .map(|j| 200.0 + j as f64 * 10.0)
            .collect();
        let mz_bytes: Vec<u8> = mz_data
            .iter()
            .flat_map(|&v| v.to_le_bytes())
            .collect();
        content.push_str(&base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &mz_bytes,
        ));

        content.push_str(
            r#"</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="0">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>"#,
        );

        // Generate base64-encoded intensity values
        let intensity_data: Vec<f32> = (0..peaks_per_spectrum)
            .map(|j| 1000.0 + (j as f32 * 100.0))
            .collect();
        let intensity_bytes: Vec<u8> = intensity_data
            .iter()
            .flat_map(|&v| v.to_le_bytes())
            .collect();
        content.push_str(&base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &intensity_bytes,
        ));

        content.push_str(
            r#"</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>"#,
        );
    }

    content.push_str(
        r#"
    </spectrumList>
  </run>
</mzML>"#,
    );

    fs::write(path, content).expect("Failed to write test mzML");
}

/// Benchmark mzML to mzPeak conversion
fn bench_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("mzml_conversion");

    for num_spectra in [100, 500, 1000] {
        let peaks_per_spectrum = 100;
        let total_peaks = num_spectra * peaks_per_spectrum;

        group.throughput(Throughput::Elements(total_peaks as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}spectra_{}peaks", num_spectra, peaks_per_spectrum)),
            &num_spectra,
            |b, &num_spectra| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let mzml_path = temp_dir.path().join("test.mzML");
                        let output_path = temp_dir.path().join("test.mzpeak");
                        generate_test_mzml(&mzml_path, num_spectra, peaks_per_spectrum);
                        (temp_dir, mzml_path, output_path)
                    },
                    |(temp_dir, mzml_path, output_path)| {
                        let config = ConversionConfig {
                            batch_size: 100,
                            ..Default::default()
                        };
                        let converter = MzMLConverter::with_config(config);
                        let _stats = converter
                            .convert(&mzml_path, &output_path)
                            .expect("Conversion failed");
                        drop(temp_dir);
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark peak writing throughput
fn bench_peak_writing(c: &mut Criterion) {
    let mut group = c.benchmark_group("peak_writing");

    for num_peaks in [1000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_peaks as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}peaks", num_peaks)),
            &num_peaks,
            |b, &num_peaks| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let output_path = temp_dir.path().join("test.mzpeak");
                        let metadata = MzPeakMetadata::new();
                        let config = WriterConfig::default();
                        let writer =
                            MzPeakDatasetWriter::new(&output_path, &metadata, config).unwrap();

                        // Generate spectra with peaks
                        let peaks_per_spectrum = 100;
                        let num_spectra = num_peaks / peaks_per_spectrum;
                        let mut spectra = Vec::new();

                        for i in 0..num_spectra {
                            let mut mz = Vec::with_capacity(peaks_per_spectrum);
                            let mut intensity = Vec::with_capacity(peaks_per_spectrum);
                            for j in 0..peaks_per_spectrum {
                                mz.push(200.0 + j as f64 * 10.0);
                                intensity.push(1000.0 + j as f32 * 100.0);
                            }
                            let peaks = PeakArrays::new(mz, intensity);
                            spectra.push(SpectrumArrays::new_ms1(
                                i as i64,
                                i as i64 + 1,
                                i as f32 * 0.5,
                                1,
                                peaks,
                            ));
                        }

                        (temp_dir, writer, spectra)
                    },
                    |(temp_dir, mut writer, spectra)| {
                        for spectrum in spectra {
                            writer.write_spectrum_arrays(&spectrum).unwrap();
                        }
                        writer.close().unwrap();
                        drop(temp_dir);
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark peak writing throughput using SoA arrays
fn bench_peak_writing_arrays(c: &mut Criterion) {
    let mut group = c.benchmark_group("peak_writing_arrays");

    for num_peaks in [1000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_peaks as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}peaks", num_peaks)),
            &num_peaks,
            |b, &num_peaks| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let output_path = temp_dir.path().join("test.mzpeak");
                        let metadata = MzPeakMetadata::new();
                        let config = WriterConfig::default();
                        let writer =
                            MzPeakDatasetWriter::new(&output_path, &metadata, config).unwrap();

                        let peaks_per_spectrum = 100;
                        let num_spectra = num_peaks / peaks_per_spectrum;
                        let mut spectra = Vec::with_capacity(num_spectra);

                        for i in 0..num_spectra {
                            let mz: Vec<f64> = (0..peaks_per_spectrum)
                                .map(|j| 200.0 + j as f64 * 10.0)
                                .collect();
                            let intensity: Vec<f32> = (0..peaks_per_spectrum)
                                .map(|j| 1000.0 + j as f32 * 100.0)
                                .collect();
                            let mut peaks = PeakArrays::new(mz, intensity);
                            peaks.ion_mobility = OptionalColumnBuf::all_null(peaks_per_spectrum);

                            let spectrum = SpectrumArrays::new_ms1(
                                i as i64,
                                i as i64 + 1,
                                i as f32 * 0.5,
                                1,
                                peaks,
                            );
                            spectra.push(spectrum);
                        }

                        (temp_dir, writer, spectra)
                    },
                    |(temp_dir, mut writer, spectra)| {
                        writer.write_spectra_arrays(&spectra).unwrap();
                        writer.close().unwrap();
                        drop(temp_dir);
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark per-peak processing time
fn bench_per_peak_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("per_peak_overhead");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_peak_write", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let output_path = temp_dir.path().join("test.mzpeak");
                let metadata = MzPeakMetadata::new();
                let config = WriterConfig::default();
                let writer = MzPeakDatasetWriter::new(&output_path, &metadata, config).unwrap();

                let peaks = PeakArrays::new(vec![500.0], vec![10000.0]);
                let spectrum = SpectrumArrays::new_ms1(0, 1, 60.0, 1, peaks);

                (temp_dir, writer, spectrum)
            },
            |(temp_dir, mut writer, spectrum)| {
                writer.write_spectrum_arrays(&spectrum).unwrap();
                writer.close().unwrap();
                drop(temp_dir);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_conversion,
    bench_peak_writing,
    bench_peak_writing_arrays,
    bench_per_peak_overhead
);
criterion_main!(benches);
