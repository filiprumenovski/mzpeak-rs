use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use quick_xml::events::Event;
use quick_xml::Reader;

use super::helpers::get_attribute;
use super::{MzMLError, MzMLStreamer};
use crate::mzml::ExternalBinaryReader;
use crate::mzml::models::{IndexEntry, MzMLIndex};

/// Default input buffer size for mzML parsing (64KB)
pub const DEFAULT_INPUT_BUFFER_SIZE: usize = 64 * 1024;

impl MzMLStreamer<BufReader<File>> {
    /// Open an mzML file for streaming with default buffer size (64KB)
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MzMLError> {
        Self::open_with_buffer_size(path, DEFAULT_INPUT_BUFFER_SIZE)
    }

    /// Open an mzML file for streaming with custom buffer size
    ///
    /// # Arguments
    /// * `path` - Path to the mzML file
    /// * `buffer_size` - Size of the input buffer in bytes
    ///
    /// # Example
    /// ```rust,no_run
    /// use mzpeak::mzml::streamer::MzMLStreamer;
    ///
    /// // Use 256KB buffer for better throughput
    /// let streamer = MzMLStreamer::open_with_buffer_size("data.mzML", 256 * 1024)?;
    /// # Ok::<(), mzpeak::mzml::streamer::MzMLError>(())
    /// ```
    pub fn open_with_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> Result<Self, MzMLError> {
        let file = File::open(path.as_ref())?;
        let reader = BufReader::with_capacity(buffer_size, file);
        Self::new(reader)
    }

    /// Open an imzML file for streaming (with external .ibd binary data)
    pub fn open_imzml<P: AsRef<Path>>(path: P) -> Result<Self, MzMLError> {
        Self::open_imzml_with_buffer_size(path, DEFAULT_INPUT_BUFFER_SIZE)
    }

    /// Open an imzML file for streaming with custom buffer size
    pub fn open_imzml_with_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> Result<Self, MzMLError> {
        let xml_path = path.as_ref();
        let file = File::open(xml_path)?;
        let reader = BufReader::with_capacity(buffer_size, file);
        let mut streamer = Self::new(reader)?;

        let ibd_path = find_ibd_path(xml_path).ok_or_else(|| {
            MzMLError::InvalidStructure(format!(
                "Missing .ibd file for imzML: {}",
                xml_path.display()
            ))
        })?;
        streamer.external_binary = Some(ExternalBinaryReader::open(&ibd_path)?);

        Ok(streamer)
    }

    /// Open an indexed mzML file and read the index first
    pub fn open_indexed<P: AsRef<Path>>(path: P) -> Result<Self, MzMLError> {
        Self::open_indexed_with_buffer_size(path, DEFAULT_INPUT_BUFFER_SIZE)
    }

    /// Open an indexed mzML file with custom buffer size
    pub fn open_indexed_with_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> Result<Self, MzMLError> {
        let mut file = File::open(path.as_ref())?;

        // Try to read the index from the end of the file
        let index = Self::read_index_from_file(&mut file)?;

        // Reset to beginning
        file.seek(SeekFrom::Start(0))?;

        let reader = BufReader::with_capacity(buffer_size, file);
        let mut streamer = Self::new(reader)?;
        streamer.index = index;

        Ok(streamer)
    }

    /// Read the index from the end of an indexed mzML file
    fn read_index_from_file(file: &mut File) -> Result<MzMLIndex, MzMLError> {
        let file_size = file.seek(SeekFrom::End(0))?;

        // Read last 1KB to find indexListOffset
        let read_size = std::cmp::min(1024, file_size) as usize;
        file.seek(SeekFrom::End(-(read_size as i64)))?;

        let mut tail = vec![0u8; read_size];
        file.read_exact(&mut tail)?;

        let tail_str = String::from_utf8_lossy(&tail);

        // Look for indexListOffset
        let mut index = MzMLIndex::default();
        if let Some(pos) = tail_str.find("<indexListOffset>") {
            let start = pos + 17;
            if let Some(end) = tail_str[start..].find("</indexListOffset>") {
                if let Ok(offset) = tail_str[start..start + end].trim().parse::<u64>() {
                    index.index_list_offset = Some(offset);

                    // Seek to index and parse it
                    file.seek(SeekFrom::Start(offset))?;
                    let mut index_data = Vec::new();
                    file.read_to_end(&mut index_data)?;

                    index = Self::parse_index_data(&index_data, offset)?;
                }
            }
        }

        Ok(index)
    }
}

fn find_ibd_path(path: &Path) -> Option<PathBuf> {
    let lower = path.with_extension("ibd");
    if lower.exists() {
        return Some(lower);
    }
    let upper = path.with_extension("IBD");
    if upper.exists() {
        return Some(upper);
    }
    None
}

impl<R: BufRead> MzMLStreamer<R> {
    /// Parse the indexList from raw XML data
    fn parse_index_data(data: &[u8], offset: u64) -> Result<MzMLIndex, MzMLError> {
        let mut reader = Reader::from_reader(data);
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        let mut index = MzMLIndex {
            index_list_offset: Some(offset),
            ..Default::default()
        };

        let mut current_index_name: Option<String> = None;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => match e.name().as_ref() {
                    b"index" => {
                        current_index_name = get_attribute(e, "name")?;
                    }
                    b"offset" => {
                        let id = get_attribute(e, "idRef")?;
                        // Read the offset value
                        let mut offset_buf = Vec::new();
                        let offset_val = match reader.read_event_into(&mut offset_buf) {
                            Ok(Event::Text(t)) => t.unescape()?.trim().parse::<u64>().unwrap_or(0),
                            _ => 0,
                        };

                        if let Some(ref name) = current_index_name {
                            let entry = IndexEntry {
                                id: id.unwrap_or_default(),
                                offset: offset_val,
                            };
                            match name.as_str() {
                                "spectrum" => index.spectrum_index.push(entry),
                                "chromatogram" => index.chromatogram_index.push(entry),
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                },
                Ok(Event::End(ref e)) => {
                    if e.name().as_ref() == b"index" {
                        current_index_name = None;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(index)
    }
}
