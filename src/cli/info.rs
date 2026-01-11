use anyhow::{Context, Result};
use std::path::PathBuf;

/// Display information about an mzPeak file
pub fn run(file: PathBuf) -> Result<()> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    if !file.exists() {
        anyhow::bail!("File does not exist: {}", file.display());
    }

    let file_handle = File::open(&file).context("Failed to open file")?;
    let reader = SerializedFileReader::new(file_handle).context("Failed to read Parquet file")?;

    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();

    println!("mzPeak File Information");
    println!("=======================");
    println!("File: {}", file.display());
    println!();

    // File statistics
    println!("File Statistics:");
    println!("  Row groups: {}", metadata.num_row_groups());
    println!("  Total rows: {}", file_metadata.num_rows());
    println!(
        "  Schema columns: {}",
        file_metadata.schema_descr().num_columns()
    );
    println!();

    // Key-value metadata
    if let Some(kv_metadata) = file_metadata.key_value_metadata() {
        println!("Metadata Keys:");
        for kv in kv_metadata {
            let value_preview = kv
                .value
                .as_ref()
                .map(|v| {
                    if v.len() > 100 {
                        format!("{}... ({} bytes)", &v[..100], v.len())
                    } else {
                        v.clone()
                    }
                })
                .unwrap_or_else(|| "<null>".to_string());
            println!("  {}: {}", kv.key, value_preview);
        }
        println!();
    }

    // Schema
    println!("Schema:");
    for i in 0..file_metadata.schema_descr().num_columns() {
        let col = file_metadata.schema_descr().column(i);
        println!("  {:3}. {} ({})", i + 1, col.name(), col.physical_type());
    }

    Ok(())
}
