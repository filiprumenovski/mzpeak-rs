use std::io::{BufReader, Cursor};
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use mzpeak::mzml::MzMLStreamer;

fn generate_test_mzml(num_spectra: usize, peaks_per_spectrum: usize) -> Vec<u8> {
    let mut mzml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <run id="bench_run">
    <spectrumList count=""#);
    mzml.push_str(&num_spectra.to_string());
    mzml.push_str(r#"">"#);

    for i in 0..num_spectra {
        let ms_level = if i % 5 == 0 { 1 } else { 2 };
        let rt = (i as f64) * 0.5;

        let mz_values: Vec<f64> = (0..peaks_per_spectrum)
            .map(|j| 100.0 + (j as f64) * 10.0 + (i as f64) * 0.1)
            .collect();
        let intensity_values: Vec<f32> = (0..peaks_per_spectrum)
            .map(|j| 1000.0 + (j as f32) * 50.0)
            .collect();

        let mz_bytes: Vec<u8> = mz_values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let intensity_bytes: Vec<u8> = intensity_values
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();

        let mz_base64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &mz_bytes);
        let intensity_base64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &intensity_bytes);

        mzml.push_str(&format!(
            r#"
      <spectrum index="{}" id="scan={}" defaultArrayLength="{}">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="{}"/>
        <cvParam cvRef="MS" accession="MS:1000130" name="positive scan"/>
        <scanList count="1">
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="{}" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <binary>{}</binary>
          </binaryDataArray>
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <binary>{}</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>"#,
            i,
            i + 1,
            peaks_per_spectrum,
            ms_level,
            rt,
            mz_base64,
            intensity_base64
        ));
    }

    mzml.push_str(
        r#"
    </spectrumList>
  </run>
</mzML>"#,
    );

    mzml.into_bytes()
}

fn bench_next_spectrum(c: &mut Criterion) {
    let mut group = c.benchmark_group("mzml_streamer_next_spectrum");

    for num_spectra in [100, 500, 1000] {
        let peaks_per_spectrum = 50;
        let total_peaks = num_spectra * peaks_per_spectrum;
        let mzml_bytes = Arc::new(generate_test_mzml(num_spectra, peaks_per_spectrum));

        group.throughput(Throughput::Elements(total_peaks as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_spectra),
            &mzml_bytes,
            |b, bytes| {
                b.iter_batched(
                    || {
                        let reader = BufReader::new(Cursor::new(bytes.as_ref().clone()));
                        MzMLStreamer::new(reader).unwrap()
                    },
                    |mut streamer| {
                        let mut count = 0usize;
                        while let Some(spectrum) = streamer.next_spectrum().unwrap() {
                            count += spectrum.mz_array.len();
                        }
                        black_box(count);
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_next_raw_spectrum(c: &mut Criterion) {
    let mut group = c.benchmark_group("mzml_streamer_next_raw_spectrum");

    for num_spectra in [100, 500, 1000] {
        let peaks_per_spectrum = 50;
        let total_peaks = num_spectra * peaks_per_spectrum;
        let mzml_bytes = Arc::new(generate_test_mzml(num_spectra, peaks_per_spectrum));

        group.throughput(Throughput::Elements(total_peaks as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_spectra),
            &mzml_bytes,
            |b, bytes| {
                b.iter_batched(
                    || {
                        let reader = BufReader::new(Cursor::new(bytes.as_ref().clone()));
                        MzMLStreamer::new(reader).unwrap()
                    },
                    |mut streamer| {
                        let mut count = 0usize;
                        while let Some(raw) = streamer.next_raw_spectrum().unwrap() {
                            count += raw.default_array_length as usize;
                        }
                        black_box(count);
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_next_spectrum, bench_next_raw_spectrum);
criterion_main!(benches);
