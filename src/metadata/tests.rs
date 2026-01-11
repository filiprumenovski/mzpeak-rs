use super::*;

const SAMPLE_SDRF: &str = r#"source name	characteristics[organism]	characteristics[organism part]	comment[data file]	comment[instrument]
Sample1	Homo sapiens	liver	sample1.raw	Orbitrap Exploris 480
Sample2	Homo sapiens	kidney	sample2.raw	Orbitrap Exploris 480"#;

#[test]
fn test_sdrf_parsing() {
    let reader = std::io::Cursor::new(SAMPLE_SDRF);
    let metadata = SdrfMetadata::from_reader(reader).unwrap();

    assert_eq!(metadata.len(), 2);
    assert_eq!(metadata[0].source_name, "Sample1");
    assert_eq!(metadata[0].organism, Some("Homo sapiens".to_string()));
    assert_eq!(metadata[0].organism_part, Some("liver".to_string()));
}

#[test]
fn test_metadata_json_roundtrip() {
    let mut sdrf = SdrfMetadata::new("TestSample");
    sdrf.organism = Some("Mus musculus".to_string());
    sdrf.instrument = Some("Q Exactive HF".to_string());

    let json = sdrf.to_json().unwrap();
    let restored = SdrfMetadata::from_json(&json).unwrap();

    assert_eq!(restored.source_name, "TestSample");
    assert_eq!(restored.organism, Some("Mus musculus".to_string()));
}

#[test]
fn test_run_parameters() {
    let mut params = RunParameters::new();
    params.spray_voltage_kv = Some(3.5);
    params.add_vendor_param("ThermoSpecific", "SomeValue");

    let json = params.to_json().unwrap();
    let restored = RunParameters::from_json(&json).unwrap();

    assert_eq!(restored.spray_voltage_kv, Some(3.5));
    assert_eq!(
        restored.vendor_params.get("ThermoSpecific"),
        Some(&"SomeValue".to_string())
    );
}
