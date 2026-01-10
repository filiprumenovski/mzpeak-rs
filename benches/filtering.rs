use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mzpeak::dataset::MzPeakDatasetWriter;
use mzpeak::metadata::MzPeakMetadata;
use mzpeak::reader::MzPeakReader;
use mzpeak::writer::{SpectrumBuilder, WriterConfig};
use tempfile::TempDir;

/// Create a test file with MS2 peaks for filtering
fn create_ms2_test_file(path: &std::path::Path, num_spectra: usize, peaks_per_spectrum: usize) {
    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut writer = MzPeakDatasetWriter::new(path, &metadata, config).unwrap();

    for i in 0..num_spectra {
        let ms_level = if i % 3 == 0 { 1 } else { 2 };
        let mut builder = SpectrumBuilder::new(i as i64, i as i64 + 1)
            .ms_level(ms_level)
            .retention_time(i as f32 * 0.5)
            .polarity(1);

        if ms_level == 2 {
            builder = builder.precursor(500.0 + (i % 100) as f64, Some(2), Some(1e6));
        }

        for j in 0..peaks_per_spectrum {
            builder = builder.add_peak(200.0 + j as f64 * 10.0, 1000.0 + j as f32 * 100.0);
        }

        writer.write_spectrum(&builder.build()).unwrap();
    }

    writer.close().unwrap();
}

/// Benchmark extracting only MS2 peaks
fn bench_ms2_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("ms2_filtering");

    for num_spectra in [500, 1000, 2000] {
        let peaks_per_spectrum = 100;
        // Approximately 2/3 will be MS2
        let expected_ms2_peaks = (num_spectra * 2 / 3) * peaks_per_spectrum;

        group.throughput(Throughput::Elements(expected_ms2_peaks as u64));

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.mzpeak");
        create_ms2_test_file(&file_path, num_spectra, peaks_per_spectrum);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}spectra", num_spectra)),
            &num_spectra,
            |b, _| {
                let reader = MzPeakReader::open(&file_path).unwrap();

                b.iter(|| {
                    let ms2_spectra = reader
                        .spectra_by_ms_level(black_box(2))
                        .unwrap();
                    black_box(ms2_spectra);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark precursor m/z range filtering
fn bench_precursor_mz_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("precursor_mz_filter");

    let num_spectra = 1000;
    let peaks_per_spectrum = 100;

    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.mzpeak");
    create_ms2_test_file(&file_path, num_spectra, peaks_per_spectrum);

    for mz_range in [10.0, 50.0, 100.0] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}Da_range", mz_range)),
            &mz_range,
            |b, &mz_range| {
                let reader = MzPeakReader::open(&file_path).unwrap();

                b.iter(|| {
                    let min_mz = 500.0;
                    let max_mz = 500.0 + mz_range;

                    // Filter MS2 spectra by precursor m/z range
                    let filtered: Vec<_> = reader
                        .spectra_by_ms_level(2)
                        .unwrap()
                        .into_iter()
                        .filter(|s| {
                            if let Some(prec_mz) = s.precursor_mz {
                                prec_mz >= min_mz && prec_mz <= max_mz
                            } else {
                                false
                            }
                        })
                        .collect();
                    black_box(filtered);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark intensity threshold filtering
fn bench_intensity_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("intensity_filter");

    let num_spectra = 500;
    let peaks_per_spectrum = 200;

    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.mzpeak");
    create_ms2_test_file(&file_path, num_spectra, peaks_per_spectrum);

    for threshold in [5000.0, 10000.0, 15000.0] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("threshold_{}", threshold)),
            &threshold,
            |b, &threshold| {
                let reader = MzPeakReader::open(&file_path).unwrap();

                b.iter(|| {
                    let mut filtered_count = 0;

                    for spectrum in reader.iter_spectra().unwrap() {
                        for peak in spectrum.peaks {
                            if peak.intensity >= threshold {
                                filtered_count += 1;
                            }
                        }
                    }

                    black_box(filtered_count);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark combined filtering (MS2 + RT range + intensity)
fn bench_combined_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("combined_filter");

    let num_spectra = 1000;
    let peaks_per_spectrum = 100;

    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.mzpeak");
    create_ms2_test_file(&file_path, num_spectra, peaks_per_spectrum);

    group.bench_function("ms2_rt_intensity", |b| {
        let reader = MzPeakReader::open(&file_path).unwrap();

        b.iter(|| {
            let rt_min = 100.0;
            let rt_max = 200.0;
            let intensity_threshold = 5000.0;

            let filtered: Vec<_> = reader
                .spectra_by_ms_level(2)
                .unwrap()
                .into_iter()
                .filter(|s| s.retention_time >= rt_min && s.retention_time <= rt_max)
                .map(|mut s| {
                    s.peaks.retain(|p| p.intensity >= intensity_threshold);
                    s
                })
                .filter(|s| !s.peaks.is_empty())
                .collect();

            black_box(filtered);
        });
    });

    group.finish();
}

/// Benchmark top-N peak extraction per spectrum
fn bench_top_n_peaks(c: &mut Criterion) {
    let mut group = c.benchmark_group("top_n_peaks");

    let num_spectra = 500;
    let peaks_per_spectrum = 200;

    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.mzpeak");
    create_ms2_test_file(&file_path, num_spectra, peaks_per_spectrum);

    for top_n in [10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("top_{}", top_n)),
            &top_n,
            |b, &top_n| {
                let reader = MzPeakReader::open(&file_path).unwrap();

                b.iter(|| {
                    let filtered: Vec<_> = reader
                        .iter_spectra()
                        .unwrap()
                        .into_iter()
                        .map(|mut s| {
                            // Sort by intensity descending and take top N
                            s.peaks.sort_by(|a, b| {
                                b.intensity
                                    .partial_cmp(&a.intensity)
                                    .unwrap_or(std::cmp::Ordering::Equal)
                            });
                            s.peaks.truncate(top_n);
                            s
                        })
                        .collect();

                    black_box(filtered);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_ms2_filtering,
    bench_precursor_mz_filter,
    bench_intensity_filter,
    bench_combined_filter,
    bench_top_n_peaks
);
criterion_main!(benches);
