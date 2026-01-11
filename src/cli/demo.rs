use anyhow::{Context, Result};
use log::info;
use std::path::PathBuf;

use mzpeak::controlled_vocabulary::ms_terms;
use mzpeak::metadata::{
    ColumnInfo, GradientProgram, GradientStep, InstrumentConfig, LcConfig, MassAnalyzerConfig,
    MobilePhase, MzPeakMetadata, PressureTrace, ProcessingHistory, ProcessingStep, RunParameters,
    SdrfMetadata, SourceFileInfo,
};
use mzpeak::writer::{CompressionType, MzPeakWriter, Peak, SpectrumBuilder, WriterConfig};

/// Generate demo LC-MS data
pub fn run(output: PathBuf, compression_level: i32) -> Result<()> {
    info!("mzPeak Reference Implementation - LC-MS Converter Demo");
    info!("=======================================================");

    // Build comprehensive metadata as emphasized in the whitepaper
    let metadata = build_demo_metadata()?;

    // Configure writer for optimal compression
    let config = WriterConfig {
        compression: CompressionType::Zstd(compression_level),
        row_group_size: 100_000,
        ..Default::default()
    };

    info!("Creating mzPeak file: {}", output.display());

    // Create the writer
    let mut writer = MzPeakWriter::new_file(&output, &metadata, config)
        .context("Failed to create mzPeak writer")?;

    // Generate mock LC-MS run data
    info!("Generating mock LC-MS data...");
    let spectra = generate_mock_lcms_run();

    info!(
        "Writing {} spectra ({} total peaks)...",
        spectra.len(),
        spectra.iter().map(|s| s.peak_count()).sum::<usize>()
    );

    // Write spectra in batches for memory efficiency
    const BATCH_SIZE: usize = 100;
    for (batch_idx, batch) in spectra.chunks(BATCH_SIZE).enumerate() {
        writer
            .write_spectra(batch)
            .context("Failed to write spectrum batch")?;

        if (batch_idx + 1) % 10 == 0 {
            info!("  Written {} spectra...", (batch_idx + 1) * BATCH_SIZE);
        }
    }

    // Finalize and get statistics
    let stats = writer.finish().context("Failed to finalize mzPeak file")?;

    info!("Conversion complete!");
    info!("  Output file: {}", output.display());
    info!("  Spectra written: {}", stats.spectra_written);
    info!("  Peaks written: {}", stats.peaks_written);
    info!("  Row groups: {}", stats.row_groups_written);

    // Verify the file was created
    let file_size = std::fs::metadata(&output).map(|m| m.len()).unwrap_or(0);
    info!(
        "  File size: {} bytes ({:.2} MB)",
        file_size,
        file_size as f64 / 1024.0 / 1024.0
    );

    // Calculate compression ratio estimate
    let uncompressed_estimate = stats.peaks_written * (8 + 4 + 8 + 8 + 4 + 2 + 1);
    let ratio = uncompressed_estimate as f64 / file_size as f64;
    info!("  Estimated compression ratio: {:.1}x", ratio);

    info!("\nFile can be read with any Parquet-compatible tool:");
    info!(
        "  - Python: pyarrow.parquet.read_table('{}').to_pandas()",
        output.display()
    );
    info!("  - R: arrow::read_parquet('{}')", output.display());
    info!(
        "  - DuckDB: SELECT * FROM read_parquet('{}')",
        output.display()
    );

    Ok(())
}

/// Build comprehensive metadata demonstrating all mzPeak metadata capabilities
fn build_demo_metadata() -> Result<MzPeakMetadata> {
    let mut metadata = MzPeakMetadata::new();

    // SDRF Metadata - following SDRF-Proteomics standard
    let mut sdrf = SdrfMetadata::new("HeLa_Digest_Sample_01");
    sdrf.organism = Some("Homo sapiens".to_string());
    sdrf.organism_part = Some("cell culture".to_string());
    sdrf.cell_type = Some("HeLa".to_string());
    sdrf.disease = Some("cervical adenocarcinoma".to_string());
    sdrf.instrument = Some("Orbitrap Exploris 480".to_string());
    sdrf.cleavage_agent = Some("Trypsin".to_string());
    sdrf.modifications = vec![
        "Carbamidomethyl (C)".to_string(),
        "Oxidation (M)".to_string(),
    ];
    sdrf.label = Some("label free sample".to_string());
    sdrf.fraction = Some("1".to_string());
    sdrf.technical_replicate = Some(1);
    sdrf.biological_replicate = Some(1);
    sdrf.factor_values
        .insert("treatment".to_string(), "control".to_string());
    sdrf.comments
        .insert("sample preparation".to_string(), "FASP digestion".to_string());
    sdrf.raw_file = Some("HeLa_Digest_01.raw".to_string());
    metadata.sdrf = Some(sdrf);

    // Instrument Configuration
    let mut instrument = InstrumentConfig::new();
    instrument.model = Some("Orbitrap Exploris 480".to_string());
    instrument.vendor = Some("Thermo Fisher Scientific".to_string());
    instrument.serial_number = Some("EXPL-12345".to_string());
    instrument.software_version = Some("Xcalibur 4.5".to_string());
    instrument.ion_source = Some("electrospray ionization".to_string());
    instrument.detector = Some("inductive detector".to_string());

    // Mass analyzers configuration
    instrument.mass_analyzers = vec![
        MassAnalyzerConfig {
            analyzer_type: "quadrupole".to_string(),
            order: 1,
            resolution: None,
            resolution_mz: None,
            cv_params: Default::default(),
        },
        MassAnalyzerConfig {
            analyzer_type: "orbitrap".to_string(),
            order: 2,
            resolution: Some(120000.0),
            resolution_mz: Some(200.0),
            cv_params: Default::default(),
        },
    ];

    instrument.cv_params.add(ms_terms::thermo_instrument());
    instrument.cv_params.add(ms_terms::orbitrap());
    metadata.instrument = Some(instrument);

    // LC Configuration
    let mut lc = LcConfig::new();
    lc.system_model = Some("Dionex UltiMate 3000".to_string());
    lc.flow_rate_ul_min = Some(300.0);
    lc.column_temperature_celsius = Some(40.0);
    lc.injection_volume_ul = Some(2.0);

    lc.column = Some(ColumnInfo {
        name: Some("Acclaim PepMap RSLC".to_string()),
        manufacturer: Some("Thermo Fisher Scientific".to_string()),
        length_mm: Some(250.0),
        inner_diameter_um: Some(75.0),
        particle_size_um: Some(2.0),
        pore_size_angstrom: Some(100.0),
        stationary_phase: Some("C18".to_string()),
    });

    lc.mobile_phases = vec![
        MobilePhase {
            channel: "A".to_string(),
            composition: "0.1% formic acid in water".to_string(),
            ph: Some(2.7),
        },
        MobilePhase {
            channel: "B".to_string(),
            composition: "0.1% formic acid in 80% acetonitrile".to_string(),
            ph: None,
        },
    ];

    lc.gradient = Some(GradientProgram {
        steps: vec![
            GradientStep {
                time_min: 0.0,
                percent_b: 2.0,
                flow_rate_ul_min: Some(300.0),
            },
            GradientStep {
                time_min: 5.0,
                percent_b: 2.0,
                flow_rate_ul_min: Some(300.0),
            },
            GradientStep {
                time_min: 90.0,
                percent_b: 35.0,
                flow_rate_ul_min: Some(300.0),
            },
            GradientStep {
                time_min: 100.0,
                percent_b: 95.0,
                flow_rate_ul_min: Some(300.0),
            },
            GradientStep {
                time_min: 105.0,
                percent_b: 95.0,
                flow_rate_ul_min: Some(300.0),
            },
            GradientStep {
                time_min: 106.0,
                percent_b: 2.0,
                flow_rate_ul_min: Some(300.0),
            },
            GradientStep {
                time_min: 120.0,
                percent_b: 2.0,
                flow_rate_ul_min: Some(300.0),
            },
        ],
    });

    metadata.lc_config = Some(lc);

    // Run Parameters - lossless technical metadata
    let mut run_params = RunParameters::new();
    run_params.start_time = Some("2024-01-15T10:30:00Z".to_string());
    run_params.end_time = Some("2024-01-15T12:30:00Z".to_string());
    run_params.operator = Some("Dr. Jane Smith".to_string());
    run_params.sample_name = Some("HeLa_Digest_Control_Rep1".to_string());
    run_params.sample_position = Some("P1-A1".to_string());
    run_params.method_name = Some("DDA_TopN_120min.meth".to_string());
    run_params.tune_file = Some("Exploris_Standard.mstune".to_string());
    run_params.calibration_info = Some("FlexMix calibration 2024-01-14".to_string());

    // Spray/source parameters
    run_params.spray_voltage_kv = Some(2.1);
    run_params.spray_current_ua = Some(0.5);
    run_params.capillary_temp_celsius = Some(275.0);
    run_params.source_temp_celsius = Some(300.0);
    run_params.sheath_gas = Some(40.0);
    run_params.aux_gas = Some(10.0);
    run_params.sweep_gas = Some(1.0);
    run_params.funnel_rf_level = Some(50.0);

    // AGC settings
    run_params
        .agc_settings
        .insert("MS1_target".to_string(), "3e6".to_string());
    run_params
        .agc_settings
        .insert("MS1_max_IT".to_string(), "50ms".to_string());
    run_params
        .agc_settings
        .insert("MS2_target".to_string(), "1e5".to_string());
    run_params
        .agc_settings
        .insert("MS2_max_IT".to_string(), "100ms".to_string());

    // Mock pump pressure trace
    run_params.pressure_traces = vec![PressureTrace {
        name: "Pump A Pressure".to_string(),
        unit: "bar".to_string(),
        times_min: (0..120).map(|i| i as f64).collect(),
        values: (0..120)
            .map(|i| {
                let base = 250.0;
                let gradient_effect = (i as f64 / 120.0) * 50.0;
                let noise = (i as f64 * 0.1).sin() * 5.0;
                base + gradient_effect + noise
            })
            .collect(),
    }];

    // Vendor-specific parameters
    run_params.add_vendor_param("ThermoRawFileVersion", "3.0.0");
    run_params.add_vendor_param("DataDependentMode", "TopN");
    run_params.add_vendor_param("TopNValue", "20");
    run_params.add_vendor_param("DynamicExclusion", "30s");
    run_params.add_vendor_param("IsolationWidth", "1.6 m/z");
    run_params.add_vendor_param("NormalizationCE", "30%");
    run_params.add_vendor_param("ResolutionMS1", "120000");
    run_params.add_vendor_param("ResolutionMS2", "30000");

    metadata.run_parameters = Some(run_params);

    // Source file information for provenance
    let mut source = SourceFileInfo::new("HeLa_Digest_01.raw");
    source.path = Some("/data/raw/2024/01/HeLa_Digest_01.raw".to_string());
    source.format = Some("Thermo RAW".to_string());
    source.size_bytes = Some(2_500_000_000);
    source.sha256 = Some("a1b2c3d4e5f6...".to_string());
    source.format_version = Some("3.0".to_string());
    metadata.source_file = Some(source);

    // Processing history
    let mut history = ProcessingHistory::new();
    history.add_step(ProcessingStep {
        order: 1,
        software: "mzpeak-rs".to_string(),
        version: Some(env!("CARGO_PKG_VERSION").to_string()),
        processing_type: "Conversion to mzPeak".to_string(),
        timestamp: Some(chrono::Utc::now().to_rfc3339()),
        parameters: std::collections::HashMap::new(),
        cv_params: Default::default(),
    });
    metadata.processing_history = Some(history);

    Ok(metadata)
}

/// Generate a mock LC-MS run with realistic data patterns
fn generate_mock_lcms_run() -> Vec<mzpeak::writer::Spectrum> {
    let mut spectra = Vec::new();
    let mut spectrum_id: i64 = 0;

    let run_duration_sec = 120.0 * 60.0;
    let cycle_time = 3.0;

    let mut current_time = 0.0;

    while current_time < run_duration_sec {
        // MS1 survey scan
        let ms1_peaks = generate_ms1_peaks(current_time, run_duration_sec);
        let ms1_spectrum = SpectrumBuilder::new(spectrum_id, spectrum_id + 1)
            .ms_level(1)
            .retention_time(current_time as f32)
            .polarity(1)
            .injection_time(50.0)
            .peaks(ms1_peaks)
            .build();

        spectra.push(ms1_spectrum);
        spectrum_id += 1;

        // Select top N precursors for MS2 (simulate DDA)
        let num_ms2 = 20;
        let precursors = select_precursors(current_time, run_duration_sec, num_ms2);

        for (precursor_mz, charge) in precursors {
            let ms2_peaks = generate_ms2_peaks(precursor_mz);

            let ms2_spectrum = SpectrumBuilder::new(spectrum_id, spectrum_id + 1)
                .ms_level(2)
                .retention_time(current_time as f32)
                .polarity(1)
                .precursor(precursor_mz, Some(charge), Some(1e6))
                .isolation_window(0.8, 0.8)
                .collision_energy(30.0)
                .injection_time(100.0)
                .peaks(ms2_peaks)
                .build();

            spectra.push(ms2_spectrum);
            spectrum_id += 1;
        }

        current_time += cycle_time;
    }

    spectra
}

/// Generate realistic MS1 peaks based on retention time
fn generate_ms1_peaks(rt_sec: f64, total_duration: f64) -> Vec<Peak> {
    let mut peaks = Vec::new();

    let gradient_position = rt_sec / total_duration;
    let intensity_modifier = 1.0 - (gradient_position - 0.5).abs() * 2.0;
    let base_intensity = 1e6 * (0.5 + intensity_modifier * 0.5);

    let num_peaks = 200 + (intensity_modifier * 300.0) as usize;

    for i in 0..num_peaks {
        let mz = 300.0 + (i as f64 / num_peaks as f64) * 1500.0;
        let mz_noise = (i as f64 * 0.123).sin() * 0.01;
        let intensity = base_intensity * (0.1 + (i as f64 * 0.456).sin().abs() * 0.9);

        peaks.push(Peak {
            mz: mz + mz_noise,
            intensity: intensity as f32,
            ion_mobility: None,
        });
    }

    peaks.sort_by(|a, b| a.mz.total_cmp(&b.mz));

    peaks
}

/// Select precursors for MS2 fragmentation (mock DDA selection)
fn select_precursors(rt_sec: f64, total_duration: f64, num_precursors: usize) -> Vec<(f64, i16)> {
    let gradient_position = rt_sec / total_duration;
    let mut precursors = Vec::new();

    for i in 0..num_precursors {
        let base_mz = 400.0 + (i as f64 / num_precursors as f64) * 1200.0;
        let rt_offset = gradient_position * 100.0;
        let mz = base_mz + rt_offset + (i as f64 * 0.789).sin() * 10.0;

        let charge = if i % 5 == 0 { 3 } else { 2 };

        precursors.push((mz, charge));
    }

    precursors
}

/// Generate MS2 fragment peaks for a given precursor
fn generate_ms2_peaks(precursor_mz: f64) -> Vec<Peak> {
    let mut peaks = Vec::new();

    let num_fragments = 30 + (precursor_mz / 50.0) as usize;

    for i in 0..num_fragments {
        let frag_mz = 100.0 + (i as f64 / num_fragments as f64) * (precursor_mz - 150.0);
        let intensity = 1e5 * (0.2 + (i as f64 * 0.321).sin().abs() * 0.8);

        if frag_mz < precursor_mz - 50.0 {
            peaks.push(Peak {
                mz: frag_mz,
                intensity: intensity as f32,
                ion_mobility: None,
            });
        }
    }

    // Add some common reporter ions
    peaks.push(Peak {
        mz: 110.0712,
        intensity: 5e4,
        ion_mobility: None,
    });
    peaks.push(Peak {
        mz: 120.0808,
        intensity: 3e4,
        ion_mobility: None,
    });
    peaks.push(Peak {
        mz: 136.0757,
        intensity: 4e4,
        ion_mobility: None,
    });

    peaks.sort_by(|a, b| a.mz.total_cmp(&b.mz));

    peaks
}
