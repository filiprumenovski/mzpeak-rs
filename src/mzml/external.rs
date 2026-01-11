use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::mzml::MzMLError;

/// Reader for external binary data (imzML .ibd files).
pub struct ExternalBinaryReader {
    file: File,
}

impl ExternalBinaryReader {
    /// Open an external binary file for reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MzMLError> {
        let file = File::open(path)?;
        Ok(Self { file })
    }

    /// Read a byte range from the external binary file.
    pub fn read_bytes(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, MzMLError> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buffer = vec![0u8; length];
        self.file.read_exact(&mut buffer)?;
        Ok(buffer)
    }
}
