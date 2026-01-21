use anyhow::{Context, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::path::PathBuf;

/// Display information about an mzPeak file
pub fn run(file: PathBuf) -> Result<()> {
    use std::fs::File;
    use mzpeak::reader::ZipEntryChunkReader;
    use zip::ZipArchive;

    if !file.exists() {
        anyhow::bail!("File does not exist: {}", file.display());
    }

    println!("mzPeak File Information");
    println!("=======================");
    println!("File: {}", file.display());
    println!();

    if file.extension().map(|e| e == "mzpeak").unwrap_or(false) {
        let mut archive =
            ZipArchive::new(File::open(&file).context("Failed to open container")?)?;
        let is_v2 = archive.by_name("manifest.json").is_ok();

        println!(
            "Container format: {}",
            if is_v2 { "v2" } else { "v1" }
        );
        println!();

        let peaks_reader = ZipEntryChunkReader::new(&file, "peaks/peaks.parquet")
            .context("Failed to open peaks/peaks.parquet")?;
        let peaks_reader =
            SerializedFileReader::new(peaks_reader).context("Failed to read peaks.parquet")?;
        print_parquet_info("peaks/peaks.parquet", &peaks_reader);

        if let Ok(spectra_chunk) = ZipEntryChunkReader::new(&file, "spectra/spectra.parquet") {
            let spectra_reader = SerializedFileReader::new(spectra_chunk)
                .context("Failed to read spectra.parquet")?;
            print_parquet_info("spectra/spectra.parquet", &spectra_reader);
        }

        return Ok(());
    }

    let file_handle = File::open(&file).context("Failed to open file")?;
    let reader = SerializedFileReader::new(file_handle).context("Failed to read Parquet file")?;
    print_parquet_info(file.to_string_lossy().as_ref(), &reader);

    Ok(())
}

fn print_parquet_info<T: parquet::file::reader::ChunkReader + 'static>(
    label: &str,
    reader: &parquet::file::reader::SerializedFileReader<T>,
) {
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();

    println!("Parquet: {}", label);
    println!("  Row groups: {}", metadata.num_row_groups());
    println!("  Total rows: {}", file_metadata.num_rows());
    println!(
        "  Schema columns: {}",
        file_metadata.schema_descr().num_columns()
    );

    if let Some(kv_metadata) = file_metadata.key_value_metadata() {
        println!("  Metadata keys:");
        for kv in kv_metadata {
            let value_preview = match kv.value.as_deref() {
                Some(value) => {
                    if value.len() > 100 {
                        let preview: String = value.chars().take(100).collect();
                        format!("{}... ({} bytes)", preview, value.len())
                    } else {
                        value.to_string()
                    }
                }
                None => "<null>".to_string(),
            };
            println!("    {}: {}", kv.key, value_preview);
        }
    }

    println!("  Schema:");
    for i in 0..file_metadata.schema_descr().num_columns() {
        let col = file_metadata.schema_descr().column(i);
        println!("    {:3}. {} ({})", i + 1, col.name(), col.physical_type());
    }
    println!();
}
