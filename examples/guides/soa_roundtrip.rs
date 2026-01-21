//! Example demonstrating SoA (SpectrumArrays) write/read
//!
//! Run with:
//! ```
//! cargo run --example soa_roundtrip
//! ```

use mzpeak::prelude::*;
use std::error::Error;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let path = dir.path().join("soa_example.mzpeak.parquet");

    let metadata = MzPeakMetadata::new();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, WriterConfig::default())?;

    let spectrum1 = SpectrumArrays::new_ms1(
        0,
        1,
        10.0,
        1,
        PeakArrays::new(vec![100.0, 200.0], vec![10.0, 20.0]),
    );

    let mut peaks2 = PeakArrays::new(vec![150.0, 250.0, 350.0], vec![15.0, 25.0, 35.0]);
    peaks2.ion_mobility = OptionalColumnBuf::WithValidity {
        values: vec![1.1, 1.2, 1.3],
        validity: vec![true, false, true],
    };
    let mut spectrum2 = SpectrumArrays::new_ms2(1, 2, 11.0, 1, 500.0, peaks2);
    spectrum2.precursor_charge = Some(2);

    let spectra = vec![spectrum1, spectrum2];
    writer.write_spectra_arrays(&spectra)?;
    let stats = writer.finish()?;

    println!(
        "Wrote {} spectra with {} peaks",
        stats.spectra_written, stats.peaks_written
    );

    let reader = MzPeakReader::open(&path)?;
    let spectra = reader.iter_spectra_arrays()?;

    for spectrum in spectra {
        println!(
            "Spectrum {} (ms_level {}) has {} peaks",
            spectrum.spectrum_id,
            spectrum.ms_level,
            spectrum.peak_count()
        );

        let has_ion = spectrum.ion_mobility_arrays()?.is_some();
        if has_ion {
            println!("  ion_mobility: present");
        } else {
            println!("  ion_mobility: none");
        }
    }

    Ok(())
}
