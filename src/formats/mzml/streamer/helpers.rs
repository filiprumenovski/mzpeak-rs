use quick_xml::events::BytesStart;

use super::MzMLError;
use crate::mzml::cv_params::CvParam;

/// Helper function to get an attribute value from a BytesStart
pub(super) fn get_attribute(e: &BytesStart, name: &str) -> Result<Option<String>, MzMLError> {
    for attr in e.attributes() {
        let attr = attr.map_err(|e| MzMLError::XmlError(quick_xml::Error::from(e)))?;
        if attr.key.as_ref() == name.as_bytes() {
            let value = std::str::from_utf8(&attr.value)?.to_string();
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// Parse a cvParam element
pub(super) fn parse_cv_param(e: &BytesStart) -> Result<CvParam, MzMLError> {
    Ok(CvParam {
        cv_ref: get_attribute(e, "cvRef")?.unwrap_or_default(),
        accession: get_attribute(e, "accession")?.unwrap_or_default(),
        name: get_attribute(e, "name")?.unwrap_or_default(),
        value: get_attribute(e, "value")?,
        unit_cv_ref: get_attribute(e, "unitCvRef")?,
        unit_accession: get_attribute(e, "unitAccession")?,
        unit_name: get_attribute(e, "unitName")?,
    })
}
