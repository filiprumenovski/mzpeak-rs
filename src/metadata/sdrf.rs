use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use super::MetadataError;

/// SDRF-Proteomics metadata following the community standard
///
/// Reference: <https://github.com/bigbio/proteomics-sample-metadata>
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SdrfMetadata {
    /// Source file name (required)
    pub source_name: String,

    /// Organism (NCBI taxonomy, e.g., "Homo sapiens")
    pub organism: Option<String>,

    /// Organism part / tissue
    pub organism_part: Option<String>,

    /// Cell type
    pub cell_type: Option<String>,

    /// Disease state
    pub disease: Option<String>,

    /// Instrument model (e.g., "Orbitrap Exploris 480")
    pub instrument: Option<String>,

    /// Cleavage agent (e.g., "Trypsin")
    pub cleavage_agent: Option<String>,

    /// Modification parameters (e.g., "Carbamidomethyl")
    pub modifications: Vec<String>,

    /// Label (e.g., "TMT126", "label free")
    pub label: Option<String>,

    /// Fraction identifier
    pub fraction: Option<String>,

    /// Technical replicate number
    pub technical_replicate: Option<i32>,

    /// Biological replicate number
    pub biological_replicate: Option<i32>,

    /// Factor values (experimental conditions)
    pub factor_values: HashMap<String, String>,

    /// Comment fields (free-form annotations)
    pub comments: HashMap<String, String>,

    /// Raw file name reference
    pub raw_file: Option<String>,

    /// Additional custom attributes
    pub custom_attributes: HashMap<String, String>,
}

impl SdrfMetadata {
    /// Create new SDRF metadata with the given source name
    pub fn new(source_name: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            ..Default::default()
        }
    }

    /// Parse SDRF metadata from a TSV file
    pub fn from_tsv_file<P: AsRef<Path>>(path: P) -> Result<Vec<Self>, MetadataError> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Self::from_reader(reader)
    }

    /// Parse SDRF metadata from a reader
    pub fn from_reader<R: BufRead>(reader: R) -> Result<Vec<Self>, MetadataError> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .flexible(true)
            .has_headers(true)
            .from_reader(reader);

        let headers: Vec<String> = csv_reader
            .headers()?
            .iter()
            .map(|s| s.to_lowercase().trim().to_string())
            .collect();

        // Validate required column
        if !headers.iter().any(|h| h.contains("source name")) {
            return Err(MetadataError::MissingColumn("source name".to_string()));
        }

        let mut results = Vec::new();

        for record in csv_reader.records() {
            let record = record?;
            let mut metadata = SdrfMetadata::default();

            for (i, value) in record.iter().enumerate() {
                if i >= headers.len() {
                    break;
                }

                let header = &headers[i];
                let value = value.trim();

                if value.is_empty() {
                    continue;
                }

                // Map SDRF column names to struct fields
                match header.as_str() {
                    h if h.contains("source name") => {
                        metadata.source_name = value.to_string();
                    }
                    h if h.contains("organism") && !h.contains("part") => {
                        metadata.organism = Some(value.to_string());
                    }
                    h if h.contains("organism part") || h.contains("tissue") => {
                        metadata.organism_part = Some(value.to_string());
                    }
                    h if h.contains("cell type") => {
                        metadata.cell_type = Some(value.to_string());
                    }
                    h if h.contains("disease") => {
                        metadata.disease = Some(value.to_string());
                    }
                    h if h.contains("instrument") => {
                        metadata.instrument = Some(value.to_string());
                    }
                    h if h.contains("cleavage agent") || h.contains("enzyme") => {
                        metadata.cleavage_agent = Some(value.to_string());
                    }
                    h if h.contains("modification") => {
                        metadata.modifications.push(value.to_string());
                    }
                    h if h.contains("label") => {
                        metadata.label = Some(value.to_string());
                    }
                    h if h.contains("fraction") => {
                        metadata.fraction = Some(value.to_string());
                    }
                    h if h.contains("technical replicate") => {
                        metadata.technical_replicate = value.parse().ok();
                    }
                    h if h.contains("biological replicate") => {
                        metadata.biological_replicate = value.parse().ok();
                    }
                    h if h.starts_with("factor value") => {
                        // Extract factor name from brackets: "factor value[treatment]"
                        if let Some(start) = h.find('[') {
                            if let Some(end) = h.find(']') {
                                let factor_name = &h[start + 1..end];
                                metadata
                                    .factor_values
                                    .insert(factor_name.to_string(), value.to_string());
                            }
                        }
                    }
                    h if h.starts_with("comment") => {
                        if let Some(start) = h.find('[') {
                            if let Some(end) = h.find(']') {
                                let comment_name = &h[start + 1..end];
                                metadata
                                    .comments
                                    .insert(comment_name.to_string(), value.to_string());
                            }
                        }
                    }
                    h if h.contains("file") || h.contains("data file") => {
                        metadata.raw_file = Some(value.to_string());
                    }
                    _ => {
                        // Store unknown columns as custom attributes
                        metadata
                            .custom_attributes
                            .insert(header.clone(), value.to_string());
                    }
                }
            }

            if !metadata.source_name.is_empty() {
                results.push(metadata);
            }
        }

        Ok(results)
    }

    /// Serialize to JSON for storage in Parquet footer
    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    /// Deserialize from JSON stored in Parquet footer
    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
}
