use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mzpeak::dataset::MzPeakDatasetWriter;
use mzpeak::metadata::MzPeakMetadata;
use mzpeak::reader::MzPeakReader;
use mzpeak::writer::{SpectrumBuilder, WriterConfig};
use tempfile::TempDir;

/// Create a test mzPeak file with known data
fn create_test_file(path: &std::path::Path, num_spectra: usize, peaks_per_spectrum: usize) {
    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut writer = MzPeakDatasetWriter::new(path, &metadata, config).unwrap();

    for i in 0..num_spectra {
        let mut builder = SpectrumBuilder::new(i as i64, i as i64 + 1)
            .ms_level(if i % 10 == 0 { 1 } else { 2 })
            .retention_time(i as f32 * 0.5)
            .polarity(1);

        for j in 0..peaks_per_spectrum {
            builder = builder.add_peak(200.0 + j as f64 * 10.0, 1000.0 + j as f32 * 100.0);
        }

        writer.write_spectrum(&builder.build()).unwrap();
    }

    writer.close().unwrap();
}

/// Benchmark random access by spectrum ID
fn bench_random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_access");

    for num_spectra in [100, 500, 1000] {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.mzpeak");
        create_test_file(&file_path, num_spectra, 100);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}spectra", num_spectra)),
            &num_spectra,
            |b, &num_spectra| {
                let reader = MzPeakReader::open(&file_path).unwrap();
                let target_id = (num_spectra / 2) as i64; // Seek to middle

                b.iter(|| {
                    let spectrum = reader
                        .get_spectrum(black_box(target_id))
                        .unwrap()
                        .expect("Spectrum not found");
                    black_box(spectrum);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark retention time range queries
fn bench_rt_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("rt_range_query");

    let num_spectra = 1000;
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.mzpeak");
    create_test_file(&file_path, num_spectra, 100);

    for range_size in [10.0, 50.0, 100.0] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}s_range", range_size)),
            &range_size,
            |b, &range_size| {
                let reader = MzPeakReader::open(&file_path).unwrap();

                b.iter(|| {
                    let spectra = reader
                        .spectra_by_rt_range(black_box(100.0), black_box(100.0 + range_size))
                        .unwrap();
                    black_box(spectra);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark MS level filtering
fn bench_ms_level_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("ms_level_filter");

    let num_spectra = 1000;
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.mzpeak");
    create_test_file(&file_path, num_spectra, 100);

    for ms_level in [1, 2] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("MS{}", ms_level)),
            &ms_level,
            |b, &ms_level| {
                let reader = MzPeakReader::open(&file_path).unwrap();

                b.iter(|| {
                    let spectra = reader
                        .spectra_by_ms_level(black_box(ms_level as i16))
                        .unwrap();
                    black_box(spectra);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark full file scan (streaming)
fn bench_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_scan");

    for num_spectra in [100, 500, 1000] {
        let peaks_per_spectrum = 100;
        let total_peaks = num_spectra * peaks_per_spectrum;

        group.throughput(Throughput::Elements(total_peaks as u64));

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.mzpeak");
        create_test_file(&file_path, num_spectra, peaks_per_spectrum);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}spectra", num_spectra)),
            &num_spectra,
            |b, _| {
                let reader = MzPeakReader::open(&file_path).unwrap();

                b.iter(|| {
                    let mut count = 0;
                    for spectrum in reader.iter_spectra().unwrap() {
                        count += spectrum.peaks.len();
                    }
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark metadata access (no peak data)
fn bench_metadata_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_only");

    let num_spectra = 1000;
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.mzpeak");
    create_test_file(&file_path, num_spectra, 100);

    group.bench_function("read_metadata", |b| {
        b.iter(|| {
            let reader = MzPeakReader::open(&file_path).unwrap();
            let metadata = reader.metadata();
            black_box(metadata);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_random_access,
    bench_rt_range_query,
    bench_ms_level_filter,
    bench_full_scan,
    bench_metadata_only
);
criterion_main!(benches);
