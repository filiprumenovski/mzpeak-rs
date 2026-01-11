/// Index entry for indexed mzML files
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// Spectrum or chromatogram ID
    pub id: String,
    /// Byte offset in the file
    pub offset: u64,
}

/// Complete index from indexedmzML
#[derive(Debug, Clone, Default)]
pub struct MzMLIndex {
    /// Spectrum index entries
    pub spectrum_index: Vec<IndexEntry>,
    /// Chromatogram index entries
    pub chromatogram_index: Vec<IndexEntry>,
    /// Byte offset of the index list
    pub index_list_offset: Option<u64>,
}

impl MzMLIndex {
    /// Check if this is an indexed file
    pub fn is_indexed(&self) -> bool {
        self.index_list_offset.is_some()
    }

    /// Get spectrum count
    pub fn spectrum_count(&self) -> usize {
        self.spectrum_index.len()
    }

    /// Get chromatogram count
    pub fn chromatogram_count(&self) -> usize {
        self.chromatogram_index.len()
    }
}
