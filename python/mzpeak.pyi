"""
mzPeak - High-performance mass spectrometry data format

Type stub file providing IDE support and static type checking.
"""

from __future__ import annotations
from typing import Any, Optional, List, Dict, Tuple, Iterator, Union
from types import TracebackType
from os import PathLike

# Version and format constants
__version__: str
FORMAT_VERSION: str
MIMETYPE: str

# Exception classes
class MzPeakException(Exception):
    """Base exception for all mzPeak errors."""
    ...

class MzPeakIOError(MzPeakException):
    """I/O error during file operations."""
    ...

class MzPeakFormatError(MzPeakException):
    """Invalid or corrupted file format."""
    ...

class MzPeakValidationError(MzPeakException):
    """Data validation failed."""
    ...

# Data classes
class Peak:
    """A single mass spectrometry peak (m/z, intensity pair)."""
    
    def __init__(
        self,
        mz: float,
        intensity: float,
        ion_mobility: Optional[float] = None
    ) -> None: ...
    
    @property
    def mz(self) -> float:
        """Mass-to-charge ratio."""
        ...
    
    @property
    def intensity(self) -> float:
        """Signal intensity."""
        ...
    
    @property
    def ion_mobility(self) -> Optional[float]:
        """Ion mobility value (if available)."""
        ...

class Spectrum:
    """A mass spectrum containing peaks and metadata."""
    
    def __init__(
        self,
        spectrum_id: int,
        scan_number: int,
        ms_level: int,
        retention_time: float,
        polarity: int,
        peaks: Optional[List[Peak]] = None
    ) -> None: ...
    
    @property
    def spectrum_id(self) -> int:
        """Unique spectrum identifier."""
        ...
    
    @property
    def scan_number(self) -> int:
        """Native scan number."""
        ...
    
    @property
    def ms_level(self) -> int:
        """MS level (1 for MS1, 2 for MS2, etc.)."""
        ...
    
    @property
    def retention_time(self) -> float:
        """Retention time in seconds."""
        ...
    
    @property
    def polarity(self) -> int:
        """Polarity (1 for positive, -1 for negative)."""
        ...
    
    @property
    def peaks(self) -> List[Peak]:
        """List of peaks in this spectrum."""
        ...
    
    @property
    def num_peaks(self) -> int:
        """Number of peaks in this spectrum."""
        ...
    
    @property
    def precursor_mz(self) -> Optional[float]:
        """Precursor m/z (for MS2+ spectra)."""
        ...
    
    @property
    def precursor_charge(self) -> Optional[int]:
        """Precursor charge state."""
        ...
    
    @property
    def precursor_intensity(self) -> Optional[float]:
        """Precursor intensity."""
        ...
    
    @property
    def isolation_window_lower(self) -> Optional[float]:
        """Lower isolation window offset."""
        ...
    
    @property
    def isolation_window_upper(self) -> Optional[float]:
        """Upper isolation window offset."""
        ...
    
    @property
    def collision_energy(self) -> Optional[float]:
        """Collision energy in eV."""
        ...
    
    @property
    def total_ion_current(self) -> Optional[float]:
        """Total ion current."""
        ...
    
    @property
    def base_peak_mz(self) -> Optional[float]:
        """Base peak m/z."""
        ...
    
    @property
    def base_peak_intensity(self) -> Optional[float]:
        """Base peak intensity."""
        ...
    
    @property
    def injection_time(self) -> Optional[float]:
        """Ion injection time in milliseconds."""
        ...
    
    @property
    def pixel_x(self) -> Optional[int]:
        """MSI pixel X coordinate."""
        ...
    
    @property
    def pixel_y(self) -> Optional[int]:
        """MSI pixel Y coordinate."""
        ...
    
    @property
    def pixel_z(self) -> Optional[int]:
        """MSI pixel Z coordinate."""
        ...
    
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class SpectrumArrays:
    """A mass spectrum with SoA arrays."""

    def __init__(
        self,
        spectrum_id: int,
        scan_number: int,
        ms_level: int,
        retention_time: float,
        polarity: int,
        mz: Any,
        intensity: Any,
        ion_mobility: Optional[Any] = None,
        ion_mobility_validity: Optional[Any] = None,
        precursor_mz: Optional[float] = None,
        precursor_charge: Optional[int] = None,
        precursor_intensity: Optional[float] = None,
        isolation_window_lower: Optional[float] = None,
        isolation_window_upper: Optional[float] = None,
        collision_energy: Optional[float] = None,
        total_ion_current: Optional[float] = None,
        base_peak_mz: Optional[float] = None,
        base_peak_intensity: Optional[float] = None,
        injection_time: Optional[float] = None,
        pixel_x: Optional[int] = None,
        pixel_y: Optional[int] = None,
        pixel_z: Optional[int] = None,
    ) -> None: ...

    @property
    def spectrum_id(self) -> int: ...

    @property
    def scan_number(self) -> int: ...

    @property
    def ms_level(self) -> int: ...

    @property
    def retention_time(self) -> float: ...

    @property
    def polarity(self) -> int: ...

    @property
    def mz_array(self) -> Any: ...

    @property
    def intensity_array(self) -> Any: ...

    @property
    def ion_mobility_array(self) -> Any: ...

    @property
    def num_peaks(self) -> int: ...

    @property
    def precursor_mz(self) -> Optional[float]: ...

    @property
    def precursor_charge(self) -> Optional[int]: ...

    @property
    def precursor_intensity(self) -> Optional[float]: ...

    @property
    def isolation_window_lower(self) -> Optional[float]: ...

    @property
    def isolation_window_upper(self) -> Optional[float]: ...

    @property
    def collision_energy(self) -> Optional[float]: ...

    @property
    def total_ion_current(self) -> Optional[float]: ...

    @property
    def base_peak_mz(self) -> Optional[float]: ...

    @property
    def base_peak_intensity(self) -> Optional[float]: ...

    @property
    def injection_time(self) -> Optional[float]: ...

    @property
    def pixel_x(self) -> Optional[int]: ...

    @property
    def pixel_y(self) -> Optional[int]: ...

    @property
    def pixel_z(self) -> Optional[int]: ...

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class SpectrumArraysView:
    """View-backed SoA spectrum with zero-copy array access."""

    @property
    def spectrum_id(self) -> int: ...

    @property
    def scan_number(self) -> int: ...

    @property
    def ms_level(self) -> int: ...

    @property
    def retention_time(self) -> float: ...

    @property
    def polarity(self) -> int: ...

    @property
    def num_peaks(self) -> int: ...

    @property
    def mz_array_view(self) -> Any: ...

    @property
    def intensity_array_view(self) -> Any: ...

    @property
    def mz_array_views(self) -> List[Any]: ...

    @property
    def intensity_array_views(self) -> List[Any]: ...

    def to_owned(self) -> SpectrumArrays: ...

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class SpectrumMetadata:
    """Spectrum-level metadata for mzPeak v2.0 datasets."""

    def __init__(
        self,
        spectrum_id: int,
        scan_number: Optional[int],
        ms_level: int,
        retention_time: float,
        polarity: int,
        peak_count: int,
        precursor_mz: Optional[float] = None,
        precursor_charge: Optional[int] = None,
        precursor_intensity: Optional[float] = None,
        isolation_window_lower: Optional[float] = None,
        isolation_window_upper: Optional[float] = None,
        collision_energy: Optional[float] = None,
        total_ion_current: Optional[float] = None,
        base_peak_mz: Optional[float] = None,
        base_peak_intensity: Optional[float] = None,
        injection_time: Optional[float] = None,
        pixel_x: Optional[int] = None,
        pixel_y: Optional[int] = None,
        pixel_z: Optional[int] = None,
    ) -> None: ...

    @staticmethod
    def new_ms1(
        spectrum_id: int,
        scan_number: Optional[int],
        retention_time: float,
        polarity: int,
        peak_count: int,
    ) -> SpectrumMetadata: ...

    @staticmethod
    def new_ms2(
        spectrum_id: int,
        scan_number: Optional[int],
        retention_time: float,
        polarity: int,
        peak_count: int,
        precursor_mz: float,
    ) -> SpectrumMetadata: ...

    @property
    def spectrum_id(self) -> int: ...

    @property
    def scan_number(self) -> Optional[int]: ...

    @property
    def ms_level(self) -> int: ...

    @property
    def retention_time(self) -> float: ...

    @property
    def polarity(self) -> int: ...

    @property
    def peak_count(self) -> int: ...

    @property
    def precursor_mz(self) -> Optional[float]: ...

    @property
    def precursor_charge(self) -> Optional[int]: ...

    @property
    def precursor_intensity(self) -> Optional[float]: ...

    @property
    def isolation_window_lower(self) -> Optional[float]: ...

    @property
    def isolation_window_upper(self) -> Optional[float]: ...

    @property
    def collision_energy(self) -> Optional[float]: ...

    @property
    def total_ion_current(self) -> Optional[float]: ...

    @property
    def base_peak_mz(self) -> Optional[float]: ...

    @property
    def base_peak_intensity(self) -> Optional[float]: ...

    @property
    def injection_time(self) -> Optional[float]: ...

    @property
    def pixel_x(self) -> Optional[int]: ...

    @property
    def pixel_y(self) -> Optional[int]: ...

    @property
    def pixel_z(self) -> Optional[int]: ...

class PeakArraysV2:
    """Peak arrays for mzPeak v2.0 datasets."""

    def __init__(
        self,
        mz: Any,
        intensity: Any,
        ion_mobility: Optional[Any] = None,
    ) -> None: ...

    @property
    def mz_array(self) -> Any: ...

    @property
    def intensity_array(self) -> Any: ...

    @property
    def ion_mobility_array(self) -> Optional[Any]: ...

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class SpectrumV2:
    """Combined spectrum for mzPeak v2.0 datasets."""

    def __init__(self, metadata: SpectrumMetadata, peaks: PeakArraysV2) -> None: ...

    @property
    def metadata(self) -> SpectrumMetadata: ...

    @property
    def peaks(self) -> PeakArraysV2: ...

    @property
    def peak_count(self) -> int: ...

    def __repr__(self) -> str: ...

class FileSummary:
    """Summary statistics for an mzPeak file."""
    
    @property
    def total_peaks(self) -> int:
        """Total number of peaks across all spectra."""
        ...
    
    @property
    def num_spectra(self) -> int:
        """Total number of spectra."""
        ...
    
    @property
    def num_ms1_spectra(self) -> int:
        """Number of MS1 spectra."""
        ...
    
    @property
    def num_ms2_spectra(self) -> int:
        """Number of MS2 spectra."""
        ...
    
    @property
    def rt_range(self) -> Optional[Tuple[float, float]]:
        """Retention time range as (min, max) tuple in seconds."""
        ...
    
    @property
    def mz_range(self) -> Optional[Tuple[float, float]]:
        """m/z range as (min, max) tuple."""
        ...
    
    @property
    def format_version(self) -> str:
        """Format version string."""
        ...

class FileMetadata:
    """Metadata from an mzPeak file."""
    
    @property
    def format_version(self) -> str:
        """Format version string."""
        ...
    
    @property
    def total_rows(self) -> int:
        """Total number of rows (peaks) in the file."""
        ...
    
    @property
    def num_row_groups(self) -> int:
        """Number of row groups in the Parquet file."""
        ...
    
    @property
    def key_value_metadata(self) -> Dict[str, str]:
        """Key-value metadata from the file."""
        ...
    
    @property
    def parsed_metadata(self) -> Optional[MzPeakMetadata]:
        """Get the parsed structured metadata (if available)."""
        ...
    
    def has_parsed_metadata(self) -> bool:
        """Check if this file has parsed structured metadata."""
        ...

class Chromatogram:
    """A chromatogram (time-intensity trace)."""
    
    def __init__(
        self,
        chromatogram_id: str,
        chromatogram_type: str,
        time_array: List[float],
        intensity_array: List[float]
    ) -> None: ...
    
    @property
    def chromatogram_id(self) -> str:
        """Chromatogram identifier."""
        ...
    
    @property
    def chromatogram_type(self) -> str:
        """Chromatogram type (e.g., 'TIC', 'BPC')."""
        ...
    
    @property
    def time_array(self) -> List[float]:
        """Time values in seconds."""
        ...
    
    @property
    def intensity_array(self) -> List[float]:
        """Intensity values."""
        ...
    
    def __len__(self) -> int: ...

class Mobilogram:
    """A mobilogram (ion mobility-intensity trace)."""
    
    def __init__(
        self,
        mobilogram_id: str,
        mobilogram_type: str,
        mobility_array: List[float],
        intensity_array: List[float]
    ) -> None: ...
    
    @property
    def mobilogram_id(self) -> str:
        """Mobilogram identifier."""
        ...
    
    @property
    def mobilogram_type(self) -> str:
        """Mobilogram type."""
        ...
    
    @property
    def mobility_array(self) -> List[float]:
        """Ion mobility values."""
        ...
    
    @property
    def intensity_array(self) -> List[float]:
        """Intensity values."""
        ...
    
    def __len__(self) -> int: ...

# ============================================================================
# Metadata classes
# ============================================================================

class VendorHints:
    """Vendor hints for files converted via intermediate formats (e.g., mzML)."""
    
    def __init__(self, original_vendor: Optional[str] = None) -> None: ...
    
    @property
    def original_vendor(self) -> Optional[str]:
        """Original vendor name (e.g., 'Waters', 'Sciex', 'Agilent')."""
        ...
    
    @property
    def original_format(self) -> Optional[str]:
        """Original file format (e.g., 'waters_raw', 'wiff', 'agilent_d')."""
        ...
    
    @property
    def instrument_model(self) -> Optional[str]:
        """Instrument model from original file."""
        ...
    
    @property
    def conversion_path(self) -> List[str]:
        """Conversion path taken (e.g., ['waters_raw', 'mzML', 'mzpeak'])."""
        ...
    
    def is_empty(self) -> bool:
        """Check if any vendor hints are present."""
        ...

class ImagingMetadata:
    """MALDI/imaging grid metadata for spatial indexing."""
    
    def __init__(
        self,
        grid_width: Optional[int] = None,
        grid_height: Optional[int] = None,
        pixel_size_x_um: Optional[float] = None,
        pixel_size_y_um: Optional[float] = None
    ) -> None: ...
    
    @property
    def grid_width(self) -> Optional[int]:
        """Width of the pixel grid (X dimension)."""
        ...
    
    @property
    def grid_height(self) -> Optional[int]:
        """Height of the pixel grid (Y dimension)."""
        ...
    
    @property
    def pixel_size_x_um(self) -> Optional[float]:
        """Pixel size along X in micrometers."""
        ...
    
    @property
    def pixel_size_y_um(self) -> Optional[float]:
        """Pixel size along Y in micrometers."""
        ...

class SourceFileInfo:
    """Source file information for provenance tracking."""
    
    def __init__(self, name: str) -> None: ...
    
    @property
    def name(self) -> str:
        """Original file name."""
        ...
    
    @property
    def path(self) -> Optional[str]:
        """Original file path."""
        ...
    
    @property
    def format(self) -> Optional[str]:
        """File format (e.g., 'Thermo RAW', 'Bruker .d')."""
        ...
    
    @property
    def size_bytes(self) -> Optional[int]:
        """File size in bytes."""
        ...
    
    @property
    def sha256(self) -> Optional[str]:
        """SHA-256 checksum of the original file."""
        ...
    
    @property
    def md5(self) -> Optional[str]:
        """MD5 checksum (for legacy compatibility)."""
        ...
    
    @property
    def format_version(self) -> Optional[str]:
        """Vendor file version/format version."""
        ...

class ProcessingStep:
    """A single data processing step in the processing history."""
    
    def __init__(self, order: int, software: str, processing_type: str) -> None: ...
    
    @property
    def order(self) -> int:
        """Step order (1-indexed)."""
        ...
    
    @property
    def software(self) -> str:
        """Software name."""
        ...
    
    @property
    def version(self) -> Optional[str]:
        """Software version."""
        ...
    
    @property
    def processing_type(self) -> str:
        """Processing type (e.g., 'conversion', 'peak picking')."""
        ...
    
    @property
    def timestamp(self) -> Optional[str]:
        """Timestamp when processing was performed."""
        ...
    
    @property
    def parameters(self) -> Dict[str, str]:
        """Processing parameters."""
        ...

class ProcessingHistory:
    """Data processing history for audit trail."""
    
    def __init__(self) -> None: ...
    
    @property
    def steps(self) -> List[ProcessingStep]:
        """List of processing steps applied."""
        ...
    
    def __len__(self) -> int: ...

class MassAnalyzerConfig:
    """Mass analyzer configuration."""
    
    def __init__(self, analyzer_type: str, order: int) -> None: ...
    
    @property
    def analyzer_type(self) -> str:
        """Analyzer type (e.g., 'orbitrap', 'quadrupole', 'ion trap')."""
        ...
    
    @property
    def order(self) -> int:
        """Analyzer order (1 = first analyzer, 2 = second, etc.)."""
        ...
    
    @property
    def resolution(self) -> Optional[float]:
        """Resolution at a given m/z (if applicable)."""
        ...
    
    @property
    def resolution_mz(self) -> Optional[float]:
        """Reference m/z for resolution."""
        ...

class InstrumentConfig:
    """Instrument configuration metadata."""
    
    def __init__(self) -> None: ...
    
    @property
    def model(self) -> Optional[str]:
        """Instrument model name (CV: MS:1000031)."""
        ...
    
    @property
    def serial_number(self) -> Optional[str]:
        """Instrument serial number (CV: MS:1000529)."""
        ...
    
    @property
    def vendor(self) -> Optional[str]:
        """Vendor name."""
        ...
    
    @property
    def software_version(self) -> Optional[str]:
        """Software version."""
        ...
    
    @property
    def ion_source(self) -> Optional[str]:
        """Ion source type (e.g., ESI, MALDI)."""
        ...
    
    @property
    def mass_analyzers(self) -> List[MassAnalyzerConfig]:
        """Mass analyzer configurations."""
        ...
    
    @property
    def detector(self) -> Optional[str]:
        """Detector configuration."""
        ...

class GradientStep:
    """A single step in an LC gradient program."""
    
    def __init__(
        self,
        time_min: float,
        percent_b: float,
        flow_rate_ul_min: Optional[float] = None
    ) -> None: ...
    
    @property
    def time_min(self) -> float:
        """Time in minutes."""
        ...
    
    @property
    def percent_b(self) -> float:
        """Percentage of mobile phase B."""
        ...
    
    @property
    def flow_rate_ul_min(self) -> Optional[float]:
        """Flow rate at this step (if variable)."""
        ...

class GradientProgram:
    """LC gradient program definition."""
    
    def __init__(self) -> None: ...
    
    @property
    def steps(self) -> List[GradientStep]:
        """Gradient steps."""
        ...
    
    def __len__(self) -> int: ...

class MobilePhase:
    """Mobile phase solvent configuration."""
    
    def __init__(
        self,
        channel: str,
        composition: str,
        ph: Optional[float] = None
    ) -> None: ...
    
    @property
    def channel(self) -> str:
        """Channel identifier (A, B, C, D)."""
        ...
    
    @property
    def composition(self) -> str:
        """Composition description."""
        ...
    
    @property
    def ph(self) -> Optional[float]:
        """pH (if applicable)."""
        ...

class ColumnInfo:
    """Information about an LC column."""
    
    def __init__(self) -> None: ...
    
    @property
    def name(self) -> Optional[str]:
        """Column name/model."""
        ...
    
    @property
    def manufacturer(self) -> Optional[str]:
        """Column manufacturer."""
        ...
    
    @property
    def length_mm(self) -> Optional[float]:
        """Column length in mm."""
        ...
    
    @property
    def inner_diameter_um(self) -> Optional[float]:
        """Column inner diameter in um."""
        ...
    
    @property
    def particle_size_um(self) -> Optional[float]:
        """Particle size in um."""
        ...
    
    @property
    def pore_size_angstrom(self) -> Optional[float]:
        """Pore size in Angstrom."""
        ...
    
    @property
    def stationary_phase(self) -> Optional[str]:
        """Stationary phase type."""
        ...

class LcConfig:
    """Liquid Chromatography configuration."""
    
    def __init__(self) -> None: ...
    
    @property
    def system_model(self) -> Optional[str]:
        """LC system model."""
        ...
    
    @property
    def column(self) -> Optional[ColumnInfo]:
        """Column information."""
        ...
    
    @property
    def mobile_phases(self) -> List[MobilePhase]:
        """Mobile phases."""
        ...
    
    @property
    def gradient(self) -> Optional[GradientProgram]:
        """Gradient program."""
        ...
    
    @property
    def flow_rate_ul_min(self) -> Optional[float]:
        """Flow rate in uL/min."""
        ...
    
    @property
    def column_temperature_celsius(self) -> Optional[float]:
        """Column temperature in Celsius."""
        ...
    
    @property
    def injection_volume_ul(self) -> Optional[float]:
        """Injection volume in uL."""
        ...

class RunParameters:
    """Technical run parameters - lossless storage of vendor-specific data."""
    
    def __init__(self) -> None: ...
    
    @property
    def start_time(self) -> Optional[str]:
        """Run start timestamp (ISO 8601)."""
        ...
    
    @property
    def end_time(self) -> Optional[str]:
        """Run end timestamp (ISO 8601)."""
        ...
    
    @property
    def operator(self) -> Optional[str]:
        """Operator name."""
        ...
    
    @property
    def sample_name(self) -> Optional[str]:
        """Sample name as entered in instrument."""
        ...
    
    @property
    def sample_position(self) -> Optional[str]:
        """Sample vial/position."""
        ...
    
    @property
    def method_name(self) -> Optional[str]:
        """Method file name."""
        ...
    
    @property
    def tune_file(self) -> Optional[str]:
        """Tune file name."""
        ...
    
    @property
    def calibration_info(self) -> Optional[str]:
        """Calibration file or date."""
        ...
    
    @property
    def spray_voltage_kv(self) -> Optional[float]:
        """Spray voltage in kV (for ESI)."""
        ...
    
    @property
    def spray_current_ua(self) -> Optional[float]:
        """Spray current in uA."""
        ...
    
    @property
    def capillary_temp_celsius(self) -> Optional[float]:
        """Capillary temperature in Celsius."""
        ...
    
    @property
    def source_temp_celsius(self) -> Optional[float]:
        """Source/desolvation temperature in Celsius."""
        ...
    
    @property
    def sheath_gas(self) -> Optional[float]:
        """Sheath gas flow."""
        ...
    
    @property
    def aux_gas(self) -> Optional[float]:
        """Auxiliary gas flow."""
        ...
    
    @property
    def sweep_gas(self) -> Optional[float]:
        """Sweep gas flow."""
        ...
    
    @property
    def funnel_rf_level(self) -> Optional[float]:
        """S-lens/funnel RF level."""
        ...
    
    @property
    def agc_settings(self) -> Dict[str, str]:
        """AGC (Automatic Gain Control) settings."""
        ...
    
    @property
    def vendor_params(self) -> Dict[str, str]:
        """Free-form vendor-specific parameters."""
        ...

class SdrfMetadata:
    """SDRF-Proteomics metadata following the community standard."""
    
    def __init__(self, source_name: str) -> None: ...
    
    @property
    def source_name(self) -> str:
        """Source file name (required)."""
        ...
    
    @property
    def organism(self) -> Optional[str]:
        """Organism (NCBI taxonomy, e.g., 'Homo sapiens')."""
        ...
    
    @property
    def organism_part(self) -> Optional[str]:
        """Organism part / tissue."""
        ...
    
    @property
    def cell_type(self) -> Optional[str]:
        """Cell type."""
        ...
    
    @property
    def disease(self) -> Optional[str]:
        """Disease state."""
        ...
    
    @property
    def instrument(self) -> Optional[str]:
        """Instrument model."""
        ...
    
    @property
    def cleavage_agent(self) -> Optional[str]:
        """Cleavage agent (e.g., 'Trypsin')."""
        ...
    
    @property
    def modifications(self) -> List[str]:
        """Modification parameters (e.g., 'Carbamidomethyl')."""
        ...
    
    @property
    def label(self) -> Optional[str]:
        """Label (e.g., 'TMT126', 'label free')."""
        ...
    
    @property
    def fraction(self) -> Optional[str]:
        """Fraction identifier."""
        ...
    
    @property
    def technical_replicate(self) -> Optional[int]:
        """Technical replicate number."""
        ...
    
    @property
    def biological_replicate(self) -> Optional[int]:
        """Biological replicate number."""
        ...
    
    @property
    def factor_values(self) -> Dict[str, str]:
        """Factor values (experimental conditions)."""
        ...
    
    @property
    def comments(self) -> Dict[str, str]:
        """Comment fields (free-form annotations)."""
        ...
    
    @property
    def raw_file(self) -> Optional[str]:
        """Raw file name reference."""
        ...
    
    @property
    def custom_attributes(self) -> Dict[str, str]:
        """Additional custom attributes."""
        ...

class MzPeakMetadata:
    """Complete metadata container for an mzPeak file."""
    
    def __init__(self) -> None: ...
    
    @property
    def sdrf(self) -> Optional[SdrfMetadata]:
        """SDRF experimental metadata."""
        ...
    
    @property
    def instrument(self) -> Optional[InstrumentConfig]:
        """Instrument configuration."""
        ...
    
    @property
    def lc_config(self) -> Optional[LcConfig]:
        """LC configuration."""
        ...
    
    @property
    def run_parameters(self) -> Optional[RunParameters]:
        """Run-level technical parameters."""
        ...
    
    @property
    def source_file(self) -> Optional[SourceFileInfo]:
        """Source file information."""
        ...
    
    @property
    def processing_history(self) -> Optional[ProcessingHistory]:
        """Processing history."""
        ...
    
    @property
    def raw_file_checksum(self) -> Optional[str]:
        """SHA-256 checksum of the original raw file."""
        ...
    
    @property
    def imaging(self) -> Optional[ImagingMetadata]:
        """MALDI/imaging spatial metadata (if available)."""
        ...
    
    @property
    def vendor_hints(self) -> Optional[VendorHints]:
        """Vendor hints for files converted via intermediate formats."""
        ...
    
    def has_sdrf(self) -> bool:
        """Check if this metadata has SDRF information."""
        ...
    
    def has_instrument(self) -> bool:
        """Check if this metadata has instrument configuration."""
        ...
    
    def has_lc_config(self) -> bool:
        """Check if this metadata has LC configuration."""
        ...
    
    def has_run_parameters(self) -> bool:
        """Check if this metadata has run parameters."""
        ...
    
    def has_imaging(self) -> bool:
        """Check if this metadata has imaging information."""
        ...

# ============================================================================
# Validation classes
# ============================================================================

class CheckStatus:
    """Validation check status enum."""
    
    Ok: CheckStatus
    """Check passed."""
    
    Warning: CheckStatus
    """Check passed with warnings."""
    
    Failed: CheckStatus
    """Check failed."""
    
    def is_ok(self) -> bool:
        """Check if the status represents a passing check."""
        ...
    
    def is_warning(self) -> bool:
        """Check if the status represents a warning."""
        ...
    
    def is_failed(self) -> bool:
        """Check if the status represents a failure."""
        ...

class ValidationCheck:
    """Individual validation check result."""
    
    @property
    def name(self) -> str:
        """Name of the validation check."""
        ...
    
    @property
    def status(self) -> CheckStatus:
        """Result status of the check."""
        ...
    
    @property
    def message(self) -> Optional[str]:
        """Message for warnings and failures (None if OK)."""
        ...
    
    def is_ok(self) -> bool:
        """Check if this check passed."""
        ...
    
    def is_warning(self) -> bool:
        """Check if this check produced a warning."""
        ...
    
    def is_failed(self) -> bool:
        """Check if this check failed."""
        ...

class ValidationReport:
    """Complete validation report for an mzPeak file."""
    
    @property
    def checks(self) -> List[ValidationCheck]:
        """List of individual validation check results."""
        ...
    
    @property
    def file_path(self) -> str:
        """Path of the file that was validated."""
        ...
    
    def has_failures(self) -> bool:
        """Check if any validation checks failed."""
        ...
    
    def has_warnings(self) -> bool:
        """Check if any validation checks produced warnings."""
        ...
    
    def is_valid(self) -> bool:
        """Check if all validation checks passed (no failures)."""
        ...
    
    def success_count(self) -> int:
        """Count the number of successful checks."""
        ...
    
    def warning_count(self) -> int:
        """Count the number of warnings."""
        ...
    
    def failure_count(self) -> int:
        """Count the number of failures."""
        ...
    
    def failed_checks(self) -> List[ValidationCheck]:
        """Get all failed checks."""
        ...
    
    def warning_checks(self) -> List[ValidationCheck]:
        """Get all warnings."""
        ...
    
    def passed_checks(self) -> List[ValidationCheck]:
        """Get all passed checks."""
        ...
    
    def summary(self) -> str:
        """Get a summary string."""
        ...
    
    def __len__(self) -> int: ...
    def __str__(self) -> str: ...

def validate_mzpeak_file(path: Union[str, PathLike]) -> ValidationReport:
    """
    Validate an mzPeak file for compliance with the format specification.
    
    This function performs deep integrity validation including:
    - Structure check: validates file/directory structure
    - Metadata integrity: validates metadata.json against schema
    - Schema contract: verifies Parquet schema matches specification
    - Data sanity: performs semantic checks on data values
    
    Args:
        path: Path to the .mzpeak file or directory to validate
    
    Returns:
        ValidationReport with detailed results of all checks
    
    Raises:
        MzPeakIOError: If the file cannot be read
        MzPeakValidationError: If validation encounters a critical error
    
    Example:
        >>> import mzpeak
        >>> report = mzpeak.validate_mzpeak_file("data.mzpeak")
        >>> if report.is_valid():
        ...     print("File is valid!")
        >>> else:
        ...     for check in report.failed_checks():
        ...         print(f"Failed: {check.name} - {check.message}")
    """
    ...

# ============================================================================
# Controlled Vocabulary (CV) classes
# ============================================================================

class CvTerm:
    """A controlled vocabulary term with accession and name."""
    
    def __init__(self, accession: str, name: str) -> None:
        """
        Create a new CV term with accession and name.
        
        Args:
            accession: CV accession (e.g., 'MS:1000040')
            name: Human-readable name
        """
        ...
    
    @property
    def accession(self) -> str:
        """CV accession (e.g., 'MS:1000040')."""
        ...
    
    @property
    def name(self) -> str:
        """Human-readable name."""
        ...
    
    @property
    def value(self) -> Optional[str]:
        """Optional value associated with the term."""
        ...
    
    @property
    def unit_accession(self) -> Optional[str]:
        """Optional unit accession for the value."""
        ...
    
    @property
    def unit_name(self) -> Optional[str]:
        """Optional unit name."""
        ...
    
    def with_value(self, value: str) -> CvTerm:
        """Add a value to the CV term, returning a new term."""
        ...
    
    def with_unit(self, unit_accession: str, unit_name: str) -> CvTerm:
        """Add a unit to the CV term value, returning a new term."""
        ...

class CvParamList:
    """A parameter list containing multiple CV terms."""
    
    def __init__(self) -> None: ...
    
    def add(self, term: CvTerm) -> None:
        """Add a CV term to the list."""
        ...
    
    def get(self, accession: str) -> Optional[CvTerm]:
        """Get a CV term by accession."""
        ...
    
    def terms(self) -> List[CvTerm]:
        """Get all CV terms as a list."""
        ...
    
    def is_empty(self) -> bool:
        """Check if the list is empty."""
        ...
    
    def __len__(self) -> int: ...

class MsTerms:
    """HUPO-PSI Mass Spectrometry CV terms factory."""
    
    @staticmethod
    def ms_level(level: int) -> CvTerm:
        """MS:1000511 - ms level"""
        ...
    
    @staticmethod
    def scan_start_time(time_seconds: float) -> CvTerm:
        """MS:1000016 - scan start time (in seconds)"""
        ...
    
    @staticmethod
    def spectrum_title(title: str) -> CvTerm:
        """MS:1000796 - spectrum title"""
        ...
    
    @staticmethod
    def positive_scan() -> CvTerm:
        """MS:1000130 - positive scan"""
        ...
    
    @staticmethod
    def negative_scan() -> CvTerm:
        """MS:1000129 - negative scan"""
        ...
    
    @staticmethod
    def scan_polarity(is_positive: bool) -> CvTerm:
        """MS:1000465 - scan polarity"""
        ...
    
    @staticmethod
    def mz() -> CvTerm:
        """MS:1000040 - m/z"""
        ...
    
    @staticmethod
    def peak_intensity() -> CvTerm:
        """MS:1000042 - peak intensity"""
        ...
    
    @staticmethod
    def selected_ion_mz(mz: float) -> CvTerm:
        """MS:1000744 - selected ion m/z"""
        ...
    
    @staticmethod
    def charge_state(charge: int) -> CvTerm:
        """MS:1000041 - charge state"""
        ...
    
    @staticmethod
    def isolation_window_lower_offset(offset: float) -> CvTerm:
        """MS:1000828 - isolation window lower offset"""
        ...
    
    @staticmethod
    def isolation_window_upper_offset(offset: float) -> CvTerm:
        """MS:1000829 - isolation window upper offset"""
        ...
    
    @staticmethod
    def collision_energy(energy: float) -> CvTerm:
        """MS:1000045 - collision energy (in eV)"""
        ...
    
    @staticmethod
    def cid() -> CvTerm:
        """MS:1000133 - collision-induced dissociation"""
        ...
    
    @staticmethod
    def hcd() -> CvTerm:
        """MS:1000422 - beam-type collision-induced dissociation (HCD)"""
        ...
    
    @staticmethod
    def etd() -> CvTerm:
        """MS:1000598 - electron transfer dissociation"""
        ...
    
    @staticmethod
    def total_ion_current(tic: float) -> CvTerm:
        """MS:1000285 - total ion current"""
        ...
    
    @staticmethod
    def base_peak_mz(mz: float) -> CvTerm:
        """MS:1000504 - base peak m/z"""
        ...
    
    @staticmethod
    def base_peak_intensity(intensity: float) -> CvTerm:
        """MS:1000505 - base peak intensity"""
        ...
    
    @staticmethod
    def ion_injection_time(time_ms: float) -> CvTerm:
        """MS:1000927 - ion injection time (in ms)"""
        ...
    
    @staticmethod
    def instrument_model(model: str) -> CvTerm:
        """MS:1000031 - instrument model"""
        ...
    
    @staticmethod
    def instrument_serial_number(serial: str) -> CvTerm:
        """MS:1000529 - instrument serial number"""
        ...
    
    @staticmethod
    def thermo_instrument() -> CvTerm:
        """MS:1000557 - Thermo Fisher Scientific instrument model"""
        ...
    
    @staticmethod
    def sciex_instrument() -> CvTerm:
        """MS:1000121 - SCIEX instrument model"""
        ...
    
    @staticmethod
    def waters_instrument() -> CvTerm:
        """MS:1000126 - Waters instrument model"""
        ...
    
    @staticmethod
    def bruker_instrument() -> CvTerm:
        """MS:1000122 - Bruker Daltonics instrument model"""
        ...
    
    @staticmethod
    def agilent_instrument() -> CvTerm:
        """MS:1000123 - Agilent instrument model"""
        ...
    
    @staticmethod
    def orbitrap() -> CvTerm:
        """MS:1000484 - Orbitrap"""
        ...
    
    @staticmethod
    def ion_trap() -> CvTerm:
        """MS:1000264 - ion trap"""
        ...
    
    @staticmethod
    def quadrupole() -> CvTerm:
        """MS:1000081 - quadrupole"""
        ...
    
    @staticmethod
    def tof() -> CvTerm:
        """MS:1000084 - time-of-flight"""
        ...
    
    @staticmethod
    def conversion_to_mzml() -> CvTerm:
        """MS:1000544 - Conversion to mzML"""
        ...
    
    @staticmethod
    def peak_picking() -> CvTerm:
        """MS:1000035 - peak picking"""
        ...
    
    @staticmethod
    def retention_time_alignment() -> CvTerm:
        """MS:1000745 - retention time alignment"""
        ...

class UnitTerms:
    """Unit ontology terms factory."""
    
    @staticmethod
    def second() -> CvTerm:
        """UO:0000010 - second"""
        ...
    
    @staticmethod
    def minute() -> CvTerm:
        """UO:0000031 - minute"""
        ...
    
    @staticmethod
    def millisecond() -> CvTerm:
        """UO:0000028 - millisecond"""
        ...
    
    @staticmethod
    def electronvolt() -> CvTerm:
        """UO:0000266 - electronvolt"""
        ...
    
    @staticmethod
    def ppm() -> CvTerm:
        """UO:0000169 - parts per million"""
        ...
    
    @staticmethod
    def percent() -> CvTerm:
        """UO:0000187 - percent"""
        ...
    
    @staticmethod
    def gram() -> CvTerm:
        """UO:0000175 - gram"""
        ...
    
    @staticmethod
    def bar() -> CvTerm:
        """UO:0000101 - bar (pressure)"""
        ...
    
    @staticmethod
    def pascal() -> CvTerm:
        """UO:0000110 - pascal"""
        ...

# Configuration classes
class WriterConfig:
    """Configuration for mzPeak writers."""
    
    def __init__(
        self,
        compression: str = "zstd",
        compression_level: int = 9,
        row_group_size: int = 100000,
        data_page_size: int = 1048576
    ) -> None:
        """
        Create a new writer configuration.
        
        Args:
            compression: Compression type ("zstd", "snappy", or "none")
            compression_level: ZSTD compression level (1-22, default 9)
            row_group_size: Number of rows per row group (default 100000)
            data_page_size: Data page size in bytes (default 1MB)
        """
        ...
    
    @staticmethod
    def default() -> WriterConfig:
        """Create default configuration."""
        ...
    
    @property
    def row_group_size(self) -> int:
        """Row group size."""
        ...
    
    @property
    def data_page_size(self) -> int:
        """Data page size in bytes."""
        ...

class WriterStats:
    """Statistics from a writer operation."""
    
    @property
    def spectra_written(self) -> int:
        """Number of spectra written."""
        ...
    
    @property
    def peaks_written(self) -> int:
        """Number of peaks written."""
        ...
    
    @property
    def row_groups_written(self) -> int:
        """Number of row groups written."""
        ...
    
    @property
    def file_size_bytes(self) -> int:
        """Output file size in bytes."""
        ...

class DatasetV2Stats:
    """Statistics from a v2 dataset writer operation."""

    @property
    def spectra_written(self) -> int: ...

    @property
    def peaks_written(self) -> int: ...

    @property
    def spectra_row_groups(self) -> int: ...

    @property
    def peaks_row_groups(self) -> int: ...

    @property
    def spectra_file_size_bytes(self) -> int: ...

    @property
    def peaks_file_size_bytes(self) -> int: ...

    @property
    def total_size_bytes(self) -> int: ...

# Conversion enums and configuration
class OutputFormat:
    """Output format for conversion (v1 legacy or v2 container)."""
    
    V2Container: OutputFormat
    """mzPeak v2.0 container format (default, recommended)."""
    
    V1Parquet: OutputFormat
    """Legacy v1 Parquet file (.mzpeak.parquet)."""

class Modality:
    """Data modality for conversion output."""
    
    LcMs: Modality
    """LC-MS: 3D data (RT, m/z, intensity)."""
    
    LcImsMs: Modality
    """LC-IMS-MS: 4D data with ion mobility."""
    
    Msi: Modality
    """MSI: Mass spectrometry imaging without ion mobility."""
    
    MsiIms: Modality
    """MSI-IMS: Mass spectrometry imaging with ion mobility."""
    
    def has_ion_mobility(self) -> bool:
        """Check if this modality includes ion mobility data."""
        ...
    
    def has_imaging(self) -> bool:
        """Check if this modality includes imaging data."""
        ...
    
    @staticmethod
    def from_flags(has_ion_mobility: bool, has_imaging: bool) -> Modality:
        """Create modality from flags."""
        ...

class StreamingConfig:
    """Streaming configuration for memory-bounded conversion."""
    
    def __init__(
        self,
        input_buffer_size: int = 65536,
        streaming_mode: bool = True
    ) -> None:
        """
        Create a new streaming configuration.
        
        Args:
            input_buffer_size: Size of input buffer in bytes (default 64KB)
            streaming_mode: Enable streaming mode for bounded memory (default True)
        """
        ...
    
    @staticmethod
    def default() -> StreamingConfig:
        """Create default streaming configuration."""
        ...
    
    @staticmethod
    def low_memory() -> StreamingConfig:
        """Create config optimized for low memory usage."""
        ...
    
    @staticmethod
    def high_throughput() -> StreamingConfig:
        """Create config optimized for throughput."""
        ...
    
    @property
    def input_buffer_size(self) -> int:
        """Input buffer size in bytes."""
        ...
    
    @property
    def streaming_mode(self) -> bool:
        """Whether streaming mode is enabled."""
        ...

class ConversionConfig:
    """Configuration for mzML conversion.
    
    Provides full control over conversion settings including output format,
    compression, streaming behavior, and metadata options.
    
    Example:
        >>> config = ConversionConfig(
        ...     batch_size=200,
        ...     output_format=OutputFormat.V2Container,
        ...     compression_level=9,
        ... )
        >>> stats = mzpeak.convert("input.mzML", "output.mzpeak", config)
    """
    
    def __init__(
        self,
        batch_size: int = 100,
        preserve_precision: bool = True,
        include_chromatograms: bool = True,
        progress_interval: int = 1000,
        output_format: Optional[OutputFormat] = None,
        modality: Optional[Modality] = None,
        compression_level: Optional[int] = None,
        row_group_size: Optional[int] = None,
        sdrf_path: Optional[str] = None,
        streaming_config: Optional[StreamingConfig] = None
    ) -> None:
        """
        Create a new conversion configuration.
        
        Args:
            batch_size: Number of spectra to process per batch (default 100)
            preserve_precision: Keep original numeric precision (default True)
            include_chromatograms: Include chromatogram data (default True)
            progress_interval: Log progress every N spectra (default 1000)
            output_format: Output format (V2Container or V1Parquet, default V2Container)
            modality: Data modality override (auto-detect if None)
            compression_level: ZSTD compression level 1-22 (default 9)
            row_group_size: Rows per Parquet row group (default 100000)
            sdrf_path: Optional path to SDRF metadata file
            streaming_config: Optional streaming configuration
        """
        ...
    
    @staticmethod
    def default() -> ConversionConfig:
        """Create default configuration."""
        ...
    
    @property
    def batch_size(self) -> int:
        """Batch size for processing."""
        ...
    
    @staticmethod
    def max_compression() -> ConversionConfig:
        """Create configuration optimized for maximum compression."""
        ...
    
    @staticmethod
    def fast_write() -> ConversionConfig:
        """Create configuration optimized for fast writing."""
        ...
    
    @property
    def preserve_precision(self) -> bool:
        """Whether to preserve original numeric precision."""
        ...
    
    @property
    def include_chromatograms(self) -> bool:
        """Whether to include chromatogram data."""
        ...
    
    @property
    def progress_interval(self) -> int:
        """Progress logging interval."""
        ...
    
    @property
    def output_format(self) -> OutputFormat:
        """Output format (V2Container or V1Parquet)."""
        ...
    
    @property
    def modality(self) -> Optional[Modality]:
        """Data modality override (None for auto-detect)."""
        ...
    
    @property
    def compression_level(self) -> Optional[int]:
        """Compression level (for ZSTD)."""
        ...
    
    @property
    def row_group_size(self) -> int:
        """Row group size for Parquet output."""
        ...
    
    @property
    def sdrf_path(self) -> Optional[str]:
        """SDRF metadata file path."""
        ...

class ConversionStats:
    """Statistics from a conversion operation."""
    
    @property
    def spectra_count(self) -> int:
        """Total number of spectra converted."""
        ...
    
    @property
    def peak_count(self) -> int:
        """Total number of peaks converted."""
        ...
    
    @property
    def ms1_spectra(self) -> int:
        """Number of MS1 spectra."""
        ...
    
    @property
    def ms2_spectra(self) -> int:
        """Number of MS2 spectra."""
        ...
    
    @property
    def msn_spectra(self) -> int:
        """Number of MSn spectra (n > 2)."""
        ...
    
    @property
    def chromatograms_converted(self) -> int:
        """Number of chromatograms converted."""
        ...
    
    @property
    def source_file_size(self) -> int:
        """Source file size in bytes."""
        ...
    
    @property
    def output_file_size(self) -> int:
        """Output file size in bytes."""
        ...
    
    @property
    def compression_ratio(self) -> float:
        """Compression ratio achieved."""
        ...

# Reader classes
class SpectrumIterator:
    """Iterator over spectra."""
    
    def __iter__(self) -> SpectrumIterator: ...
    def __next__(self) -> Spectrum: ...
    def __len__(self) -> int: ...

class SpectrumArraysIterator:
    """Iterator over spectra (SoA arrays)."""

    def __iter__(self) -> SpectrumArraysIterator: ...
    def __next__(self) -> SpectrumArrays: ...
    def __len__(self) -> int: ...

class SpectrumArraysViewIterator:
    """Iterator over spectra (SoA view arrays)."""

    def __iter__(self) -> SpectrumArraysViewIterator: ...
    def __next__(self) -> SpectrumArraysView: ...
    def __len__(self) -> int: ...

class MzPeakReader:
    """
    Reader for mzPeak format files.
    
    Supports reading from single Parquet files, dataset bundles (directories),
    and ZIP container files.
    
    Example:
        >>> with mzpeak.MzPeakReader("data.mzpeak") as reader:
        ...     summary = reader.summary()
        ...     print(f"Total spectra: {summary.num_spectra}")
        ...     table = reader.to_arrow()
    """
    
    def __init__(
        self,
        path: Union[str, PathLike],
        batch_size: Optional[int] = None
    ) -> None:
        """
        Open an mzPeak file for reading.
        
        Args:
            path: Path to the mzPeak file, directory, or ZIP container
            batch_size: Optional batch size for reading (default: 65536)
        """
        ...
    
    @staticmethod
    def open(
        path: Union[str, PathLike],
        batch_size: Optional[int] = None
    ) -> MzPeakReader:
        """Open an mzPeak file (alternative constructor)."""
        ...
    
    def metadata(self) -> FileMetadata:
        """Get file metadata."""
        ...
    
    def summary(self) -> FileSummary:
        """Get file summary statistics."""
        ...
    
    def total_peaks(self) -> int:
        """Get total number of peaks in the file."""
        ...
    
    def get_spectrum(self, spectrum_id: int) -> Optional[Spectrum]:
        """
        Get a single spectrum by ID.
        
        Args:
            spectrum_id: The spectrum identifier
            
        Returns:
            Spectrum object or None if not found
        """
        ...

    def get_spectrum_arrays(self, spectrum_id: int) -> Optional[SpectrumArrays]:
        """Get a single spectrum by ID as SoA arrays."""
        ...

    def get_spectrum_arrays_view(self, spectrum_id: int) -> Optional[SpectrumArraysView]:
        """Get a single spectrum by ID as SoA array views."""
        ...
    
    def get_spectra(self, spectrum_ids: List[int]) -> List[Spectrum]:
        """
        Get multiple spectra by their IDs.
        
        Args:
            spectrum_ids: List of spectrum identifiers
            
        Returns:
            List of Spectrum objects
        """
        ...

    def get_spectra_arrays(self, spectrum_ids: List[int]) -> List[SpectrumArrays]:
        """Get multiple spectra by their IDs as SoA arrays."""
        ...

    def get_spectra_arrays_views(self, spectrum_ids: List[int]) -> List[SpectrumArraysView]:
        """Get multiple spectra by their IDs as SoA array views."""
        ...
    
    def all_spectra(self) -> List[Spectrum]:
        """
        Get all spectra from the file.
        
        Warning: This loads all spectra into memory. For large files,
        consider using iter_spectra() or to_arrow() instead.
        """
        ...

    def all_spectra_arrays(self) -> List[SpectrumArrays]:
        """Get all spectra from the file as SoA arrays."""
        ...

    def all_spectra_arrays_views(self) -> List[SpectrumArraysView]:
        """Get all spectra from the file as SoA array views."""
        ...
    
    def spectra_by_rt_range(self, min_rt: float, max_rt: float) -> List[Spectrum]:
        """
        Get spectra within a retention time range.
        
        Args:
            min_rt: Minimum retention time in seconds
            max_rt: Maximum retention time in seconds
        """
        ...

    def spectra_by_rt_range_arrays(self, min_rt: float, max_rt: float) -> List[SpectrumArrays]:
        """Get spectra within a retention time range as SoA arrays."""
        ...
    
    def spectra_by_ms_level(self, ms_level: int) -> List[Spectrum]:
        """
        Get spectra by MS level.
        
        Args:
            ms_level: MS level (1, 2, etc.)
        """
        ...

    def spectra_by_ms_level_arrays(self, ms_level: int) -> List[SpectrumArrays]:
        """Get spectra by MS level as SoA arrays."""
        ...
    
    def spectrum_ids(self) -> List[int]:
        """Get all spectrum IDs in the file."""
        ...
    
    def read_chromatograms(self) -> List[Chromatogram]:
        """Read chromatogram data (empty list if no chromatograms present)."""
        ...
    
    def read_mobilograms(self) -> List[Mobilogram]:
        """Read mobilogram data (empty list if no mobilograms present)."""
        ...
    
    def iter_spectra(self) -> SpectrumIterator:
        """
        Return an iterator over all spectra.
        
        This is memory-efficient for large files as it reads spectra lazily.
        """
        ...

    def iter_spectra_arrays(self) -> SpectrumArraysIterator:
        """Return an iterator over all spectra as SoA arrays."""
        ...

    def iter_spectra_arrays_views(self) -> SpectrumArraysViewIterator:
        """Return an iterator over all spectra as SoA array views."""
        ...
    
    def to_arrow(self) -> "pyarrow.Table":
        """
        Export data as a PyArrow Table (zero-copy).
        
        Uses the Arrow C Data Interface to pass memory directly to PyArrow
        without serialization overhead.
        
        Returns:
            pyarrow.Table containing all peak data
            
        Raises:
            ImportError: If pyarrow is not installed
        """
        ...
    
    def to_pandas(self) -> "pandas.DataFrame":
        """
        Export data as a pandas DataFrame.
        
        Internally uses zero-copy Arrow handoff for efficiency.
        
        Returns:
            pandas.DataFrame containing all peak data
            
        Raises:
            ImportError: If pandas or pyarrow is not installed
        """
        ...
    
    def to_polars(self) -> "polars.DataFrame":
        """
        Export data as a polars DataFrame.
        
        Internally uses zero-copy Arrow handoff for efficiency.
        
        Returns:
            polars.DataFrame containing all peak data
            
        Raises:
            ImportError: If polars is not installed
        """
        ...
    
    def close(self) -> None:
        """Close the reader and release resources."""
        ...
    
    def is_open(self) -> bool:
        """Check if the reader is open."""
        ...
    
    def __enter__(self) -> MzPeakReader: ...
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool: ...
    def __repr__(self) -> str: ...

# Writer classes
class MzPeakWriter:
    """
    Writer for creating mzPeak Parquet files.
    
    Supports streaming writes with automatic batching and compression.
    Use as a context manager to ensure proper file finalization.
    
    Example:
        >>> with mzpeak.MzPeakWriter("output.parquet") as writer:
        ...     spectrum = mzpeak.SpectrumBuilder(1, 1) \\
        ...         .ms_level(1) \\
        ...         .retention_time(60.0) \\
        ...         .add_peak(400.0, 10000.0) \\
        ...         .build()
        ...     writer.write_spectrum(spectrum)
    """
    
    def __init__(
        self,
        path: Union[str, PathLike],
        config: Optional[WriterConfig] = None
    ) -> None:
        """
        Create a new mzPeak writer.
        
        Args:
            path: Output file path
            config: Optional WriterConfig for compression and batching settings
        """
        ...
    
    def write_spectrum(self, spectrum: Spectrum) -> None:
        """Write a single spectrum."""
        ...

    def write_spectrum_arrays(self, spectrum: SpectrumArrays) -> None:
        """Write a single spectrum using SoA arrays."""
        ...

    def write_spectrum_arrays(self, spectrum: SpectrumArrays) -> None:
        """Write a single spectrum using SoA arrays."""
        ...
    
    def write_spectra(self, spectra: List[Spectrum]) -> None:
        """Write multiple spectra in a batch."""
        ...

    def write_spectra_arrays(self, spectra: List[SpectrumArrays]) -> None:
        """Write multiple spectra using SoA arrays."""
        ...

    def write_spectra_arrays(self, spectra: List[SpectrumArrays]) -> None:
        """Write multiple spectra using SoA arrays."""
        ...
    
    def stats(self) -> WriterStats:
        """Get current writer statistics."""
        ...
    
    def close(self) -> WriterStats:
        """
        Finalize and close the writer.
        
        Returns:
            WriterStats with final statistics
        """
        ...
    
    def is_open(self) -> bool:
        """Check if the writer is open."""
        ...
    
    def __enter__(self) -> MzPeakWriter: ...
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool: ...
    def __repr__(self) -> str: ...

class MzPeakDatasetWriter:
    """
    Writer for creating mzPeak dataset bundles.
    
    Creates directory-based or ZIP container datasets with separate files
    for peaks, chromatograms, mobilograms, and metadata.
    
    Example:
        >>> with mzpeak.MzPeakDatasetWriter("output.mzpeak") as writer:
        ...     spectrum = mzpeak.SpectrumBuilder(1, 1) \\
        ...         .ms_level(1) \\
        ...         .retention_time(60.0) \\
        ...         .add_peak(400.0, 10000.0) \\
        ...         .build()
        ...     writer.write_spectrum(spectrum)
    """
    
    def __init__(
        self,
        path: Union[str, PathLike],
        config: Optional[WriterConfig] = None,
        use_container: bool = True
    ) -> None:
        """
        Create a new dataset writer.
        
        Args:
            path: Output path (will create .mzpeak directory or ZIP file)
            config: Optional WriterConfig for compression settings
            use_container: If True, create a ZIP container; otherwise create a directory
        """
        ...
    
    def write_spectrum(self, spectrum: Spectrum) -> None:
        """Write a single spectrum."""
        ...
    
    def write_spectra(self, spectra: List[Spectrum]) -> None:
        """Write multiple spectra in a batch."""
        ...
    
    def write_chromatogram(self, chromatogram: Chromatogram) -> None:
        """Write a chromatogram."""
        ...
    
    def write_chromatograms(self, chromatograms: List[Chromatogram]) -> None:
        """Write multiple chromatograms."""
        ...
    
    def write_mobilogram(self, mobilogram: Mobilogram) -> None:
        """Write a mobilogram."""
        ...
    
    def write_mobilograms(self, mobilograms: List[Mobilogram]) -> None:
        """Write multiple mobilograms."""
        ...
    
    def output_mode(self) -> str:
        """Get the output mode (directory or container)."""
        ...
    
    def close(self) -> Dict[str, int]:
        """
        Finalize and close the dataset writer.
        
        Returns:
            Dictionary with final statistics
        """
        ...
    
    def is_open(self) -> bool:
        """Check if the writer is open."""
        ...
    
    def __enter__(self) -> MzPeakDatasetWriter: ...
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool: ...
    def __repr__(self) -> str: ...

class MzPeakDatasetWriterV2:
    """
    Writer for creating mzPeak v2.0 dataset containers.

    Creates a normalized two-table container with spectra and peaks parquet files.
    """

    def __init__(
        self,
        path: Union[str, PathLike],
        modality: str = "lc-ms",
        config: Optional[WriterConfig] = None,
    ) -> None:
        """
        Create a new v2 dataset writer.

        Args:
            path: Output .mzpeak container path
            modality: Data modality ("lc-ms", "lc-ims-ms", "msi", "msi-ims")
            config: Optional WriterConfig for compression settings
        """
        ...

    def write_spectrum_v2(self, metadata: SpectrumMetadata, peaks: PeakArraysV2) -> None:
        """Write a single spectrum using v2 metadata + peaks."""
        ...

    def write_spectrum(self, spectrum: SpectrumV2) -> None:
        """Write a single SpectrumV2."""
        ...

    def write_spectra(self, spectra: List[SpectrumV2]) -> None:
        """Write multiple SpectrumV2 objects."""
        ...

    def stats(self) -> Dict[str, int]:
        """Get current stats (spectra/peaks counts)."""
        ...

    def close(self) -> DatasetV2Stats:
        """Finalize and close the dataset writer."""
        ...

    def modality(self) -> str:
        """Get the modality for this writer."""
        ...

    def is_open(self) -> bool:
        """Check if the writer is open."""
        ...

    def __enter__(self) -> MzPeakDatasetWriterV2: ...
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool: ...
    def __repr__(self) -> str: ...

class SpectrumBuilder:
    """
    Builder for creating Spectrum objects with a fluent API.
    
    Example:
        >>> spectrum = mzpeak.SpectrumBuilder(1, 1) \\
        ...     .ms_level(1) \\
        ...     .retention_time(60.0) \\
        ...     .polarity(1) \\
        ...     .add_peak(400.0, 10000.0) \\
        ...     .add_peak(500.0, 20000.0) \\
        ...     .build()
    """
    
    def __init__(self, spectrum_id: int, scan_number: int) -> None:
        """
        Create a new spectrum builder.
        
        Args:
            spectrum_id: Unique spectrum identifier
            scan_number: Native scan number
        """
        ...
    
    def ms_level(self, level: int) -> SpectrumBuilder:
        """Set the MS level."""
        ...
    
    def retention_time(self, rt: float) -> SpectrumBuilder:
        """Set the retention time in seconds."""
        ...
    
    def polarity(self, polarity: int) -> SpectrumBuilder:
        """Set the polarity (1 for positive, -1 for negative)."""
        ...
    
    def precursor(
        self,
        mz: float,
        charge: Optional[int] = None,
        intensity: Optional[float] = None
    ) -> SpectrumBuilder:
        """
        Set precursor information.
        
        Args:
            mz: Precursor m/z
            charge: Optional charge state
            intensity: Optional precursor intensity
        """
        ...
    
    def isolation_window(self, lower: float, upper: float) -> SpectrumBuilder:
        """
        Set the isolation window.
        
        Args:
            lower: Lower offset from precursor m/z
            upper: Upper offset from precursor m/z
        """
        ...
    
    def collision_energy(self, ce: float) -> SpectrumBuilder:
        """Set the collision energy in eV."""
        ...
    
    def injection_time(self, time_ms: float) -> SpectrumBuilder:
        """Set the ion injection time in milliseconds."""
        ...
    
    def pixel(self, x: int, y: int) -> SpectrumBuilder:
        """Set MSI pixel coordinates (2D)."""
        ...
    
    def pixel_3d(self, x: int, y: int, z: int) -> SpectrumBuilder:
        """Set MSI pixel coordinates (3D)."""
        ...
    
    def peaks(self, peaks: List[Peak]) -> SpectrumBuilder:
        """Set all peaks at once."""
        ...
    
    def add_peak(self, mz: float, intensity: float) -> SpectrumBuilder:
        """Add a single peak."""
        ...
    
    def add_peak_with_im(
        self,
        mz: float,
        intensity: float,
        ion_mobility: float
    ) -> SpectrumBuilder:
        """Add a peak with ion mobility."""
        ...
    
    def build(self) -> Spectrum:
        """Build the final Spectrum object."""
        ...

# Converter classes
class MzMLConverter:
    """
    Converter for mzML files to mzPeak format.
    
    Example:
        >>> converter = mzpeak.MzMLConverter()
        >>> stats = converter.convert("input.mzML", "output.mzpeak")
        >>> print(f"Converted {stats.spectra_count} spectra")
    """
    
    def __init__(self, config: Optional[ConversionConfig] = None) -> None:
        """
        Create a new converter with optional configuration.
        
        Args:
            config: Optional ConversionConfig for batch size, precision settings, etc.
        """
        ...
    
    def with_batch_size(self, batch_size: int) -> MzMLConverter:
        """Set the batch size for processing."""
        ...
    
    def convert(
        self,
        input_path: Union[str, PathLike],
        output_path: Union[str, PathLike]
    ) -> ConversionStats:
        """
        Convert an mzML file to mzPeak format.
        
        Args:
            input_path: Path to input mzML file
            output_path: Path for output mzPeak file/directory
            
        Returns:
            ConversionStats with details about the conversion
        """
        ...
    
    def convert_with_sharding(
        self,
        input_path: Union[str, PathLike],
        output_path: Union[str, PathLike],
        max_peaks_per_file: int = 50_000_000
    ) -> ConversionStats:
        """
        Convert an mzML file with automatic file sharding.
        
        Args:
            input_path: Path to input mzML file
            output_path: Base path for output files
            max_peaks_per_file: Maximum peaks per output file
            
        Returns:
            ConversionStats with details about the conversion
        """
        ...

# Module-level convenience functions
def convert(
    input_path: Union[str, PathLike],
    output_path: Union[str, PathLike],
    config: Optional[ConversionConfig] = None
) -> ConversionStats:
    """
    Convert an mzML file to mzPeak format (convenience function).
    
    Args:
        input_path: Path to input mzML file
        output_path: Path for output mzPeak file/directory
        config: Optional ConversionConfig
        
    Returns:
        ConversionStats with details about the conversion
        
    Example:
        >>> import mzpeak
        >>> stats = mzpeak.convert("input.mzML", "output.mzpeak")
        >>> print(f"Converted {stats.spectra_count} spectra")
    """
    ...

def convert_with_sharding(
    input_path: Union[str, PathLike],
    output_path: Union[str, PathLike],
    max_peaks_per_file: int = 50_000_000,
    config: Optional[ConversionConfig] = None
) -> ConversionStats:
    """
    Convert an mzML file with automatic file sharding (convenience function).
    
    Args:
        input_path: Path to input mzML file
        output_path: Base path for output files
        max_peaks_per_file: Maximum peaks per output file
        config: Optional ConversionConfig
        
    Returns:
        ConversionStats with details about the conversion
        
    Example:
        >>> import mzpeak
        >>> stats = mzpeak.convert_with_sharding("large.mzML", "output", max_peaks_per_file=10_000_000)
    """
    ...

# ============================================================================
# TDF Converter (Bruker TimsTOF) - requires 'tdf' feature
# ============================================================================

class TdfConversionStats:
    """Statistics from TDF conversion."""
    
    @property
    def spectra_read(self) -> int:
        """Number of spectra converted."""
        ...
    
    @property
    def peaks_total(self) -> int:
        """Total peak count processed."""
        ...
    
    @property
    def ms1_count(self) -> int:
        """Count of MS1 spectra."""
        ...
    
    @property
    def ms2_count(self) -> int:
        """Count of MS2 spectra."""
        ...
    
    @property
    def imaging_frames(self) -> int:
        """Number of frames with MALDI imaging metadata."""
        ...
    
    def __repr__(self) -> str: ...

class TdfConverter:
    """
    Converter for Bruker TDF (TimsTOF) datasets to mzPeak format.
    
    Converts Bruker .d directories containing TDF data to mzPeak v2 containers.
    Supports LC-TIMS-MS, PASEF, diaPASEF, and MALDI-TIMS-MSI data.
    
    Note: Requires the 'tdf' feature to be enabled at compile time.
    
    Example:
        >>> converter = mzpeak.TdfConverter()
        >>> stats = converter.convert("sample.d", "output.mzpeak")
        >>> print(f"Converted {stats.spectra_read} spectra")
    """
    
    def __init__(
        self,
        include_extended_metadata: bool = True,
        batch_size: int = 256,
        compression_level: int = 9,
        row_group_size: int = 100000
    ) -> None:
        """
        Create a new TDF converter.
        
        Args:
            include_extended_metadata: Include TIC/base peak prepopulation (default True)
            batch_size: Batch size for streaming + parallel decode (default 256)
            compression_level: ZSTD compression level 1-22 (default 9)
            row_group_size: Rows per Parquet row group (default 100000)
        """
        ...
    
    def convert(
        self,
        input_path: Union[str, PathLike],
        output_path: Union[str, PathLike]
    ) -> TdfConversionStats:
        """
        Convert a Bruker TDF dataset to mzPeak v2 container.
        
        Args:
            input_path: Path to Bruker .d directory
            output_path: Path for output .mzpeak container
            
        Returns:
            TdfConversionStats with details about the conversion
        """
        ...
    
    def __repr__(self) -> str: ...

def convert_tdf(
    input_path: Union[str, PathLike],
    output_path: Union[str, PathLike],
    batch_size: int = 256,
    compression_level: int = 9
) -> TdfConversionStats:
    """
    Convert a Bruker TDF dataset to mzPeak format (convenience function).
    
    Note: Requires the 'tdf' feature to be enabled at compile time.
    
    Args:
        input_path: Path to Bruker .d directory
        output_path: Path for output .mzpeak container
        batch_size: Batch size for streaming (default 256)
        compression_level: ZSTD compression level 1-22 (default 9)
        
    Returns:
        TdfConversionStats with details about the conversion
        
    Example:
        >>> import mzpeak
        >>> stats = mzpeak.convert_tdf("sample.d", "output.mzpeak")
        >>> print(f"Converted {stats.spectra_read} spectra")
    """
    ...

# ============================================================================
# Thermo Converter - requires 'thermo' feature
# ============================================================================

class ThermoConversionStats:
    """Statistics from Thermo RAW conversion."""
    
    @property
    def spectra_count(self) -> int:
        """Total number of spectra converted."""
        ...
    
    @property
    def peak_count(self) -> int:
        """Total number of peaks converted."""
        ...
    
    @property
    def ms1_spectra(self) -> int:
        """Number of MS1 spectra."""
        ...
    
    @property
    def ms2_spectra(self) -> int:
        """Number of MS2 spectra."""
        ...
    
    @property
    def msn_spectra(self) -> int:
        """Number of MSn spectra (n > 2)."""
        ...
    
    @property
    def source_file_size(self) -> int:
        """Source file size in bytes."""
        ...
    
    @property
    def output_file_size(self) -> int:
        """Output file size in bytes."""
        ...
    
    @property
    def compression_ratio(self) -> float:
        """Compression ratio achieved."""
        ...
    
    def __repr__(self) -> str: ...

class ThermoConverter:
    """
    Converter for Thermo RAW files to mzPeak format.
    
    Converts Thermo Fisher RAW files to mzPeak format. Supports both legacy
    v1 Parquet output and v2 container format.
    
    Note: Requires the 'thermo' feature and .NET 8 runtime.
          Only supported on Windows x86_64, Linux x86_64, and macOS x86_64.
    
    Example:
        >>> converter = mzpeak.ThermoConverter()
        >>> stats = converter.convert("sample.raw", "output.mzpeak")
        >>> print(f"Converted {stats.spectra_count} spectra")
    """
    
    def __init__(
        self,
        batch_size: int = 1000,
        compression_level: int = 9,
        row_group_size: int = 100000,
        legacy_format: bool = False,
        centroid_spectra: bool = True
    ) -> None:
        """
        Create a new Thermo RAW converter.
        
        Args:
            batch_size: Number of spectra to process per batch (default 1000)
            compression_level: ZSTD compression level 1-22 (default 9)
            row_group_size: Rows per Parquet row group (default 100000)
            legacy_format: Output legacy v1 Parquet instead of v2 container (default False)
            centroid_spectra: Centroid profile spectra during conversion (default True)
        """
        ...
    
    def convert(
        self,
        input_path: Union[str, PathLike],
        output_path: Union[str, PathLike]
    ) -> ThermoConversionStats:
        """
        Convert a Thermo RAW file to mzPeak format.
        
        Args:
            input_path: Path to Thermo RAW file
            output_path: Path for output file (.mzpeak or .mzpeak.parquet)
            
        Returns:
            ThermoConversionStats with details about the conversion
        """
        ...
    
    def __repr__(self) -> str: ...

def convert_thermo(
    input_path: Union[str, PathLike],
    output_path: Union[str, PathLike],
    batch_size: int = 1000,
    compression_level: int = 9
) -> ThermoConversionStats:
    """
    Convert a Thermo RAW file to mzPeak format (convenience function).
    
    Note: Requires the 'thermo' feature and .NET 8 runtime.
    
    Args:
        input_path: Path to Thermo RAW file
        output_path: Path for output .mzpeak container
        batch_size: Number of spectra per batch (default 1000)
        compression_level: ZSTD compression level 1-22 (default 9)
        
    Returns:
        ThermoConversionStats with details about the conversion
        
    Example:
        >>> import mzpeak
        >>> stats = mzpeak.convert_thermo("sample.raw", "output.mzpeak")
        >>> print(f"Converted {stats.spectra_count} spectra")
    """
    ...

# ============================================================================
# Advanced Writer Classes (Phase 5 - Performance / Advanced Writes)
# ============================================================================

class RollingWriterStats:
    """Statistics from a rolling writer operation."""
    
    @property
    def total_spectra_written(self) -> int:
        """Total number of spectra written across all files."""
        ...
    
    @property
    def total_peaks_written(self) -> int:
        """Total number of peaks written across all files."""
        ...
    
    @property
    def files_written(self) -> int:
        """Number of output files created."""
        ...
    
    @property
    def part_stats(self) -> List[WriterStats]:
        """Statistics for each individual file part."""
        ...
    
    def __repr__(self) -> str: ...

class RollingWriter:
    """
    Rolling writer that automatically shards output into multiple files.
    
    Useful for processing very large datasets that need to be split across
    multiple files based on peak count limits.
    
    Example:
        >>> with mzpeak.RollingWriter("output.parquet", max_peaks_per_file=10_000_000) as writer:
        ...     for spectrum in spectra:
        ...         writer.write_spectrum(spectrum)
        ...     stats = writer.finish()
        >>> print(f"Wrote {stats.files_written} files")
    """
    
    def __init__(
        self,
        base_path: Union[str, PathLike],
        max_peaks_per_file: int = 50_000_000,
        config: Optional[WriterConfig] = None
    ) -> None:
        """
        Create a new rolling writer.
        
        Args:
            base_path: Base output file path (files will be named base-part-NNNN.parquet)
            max_peaks_per_file: Maximum peaks per output file (default 50M)
            config: Optional WriterConfig for compression settings
        """
        ...
    
    def write_spectrum(self, spectrum: Spectrum) -> None:
        """Write a single spectrum."""
        ...
    
    def write_spectrum_arrays(self, spectrum: SpectrumArrays) -> None:
        """Write a single spectrum using SoA arrays."""
        ...
    
    def write_spectra(self, spectra: List[Spectrum]) -> None:
        """Write multiple spectra in a batch."""
        ...
    
    def write_spectra_arrays(self, spectra: List[SpectrumArrays]) -> None:
        """Write multiple spectra using SoA arrays."""
        ...
    
    def stats(self) -> RollingWriterStats:
        """Get current writer statistics."""
        ...
    
    def finish(self) -> RollingWriterStats:
        """
        Finalize and close the writer.
        
        Returns:
            RollingWriterStats with final statistics
        """
        ...
    
    def is_open(self) -> bool:
        """Check if the writer is open."""
        ...
    
    def __enter__(self) -> RollingWriter: ...
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool: ...
    def __repr__(self) -> str: ...

class OwnedColumnarBatch:
    """
    Owned columnar batch for zero-copy writing to Arrow/Parquet.
    
    This struct takes full ownership of all data vectors, enabling true zero-copy
    transfer to the underlying Arrow backend. Use this for maximum write performance.
    
    Example:
        >>> import numpy as np
        >>> batch = mzpeak.OwnedColumnarBatch(
        ...     mz=np.array([100.0, 200.0, 300.0], dtype=np.float64),
        ...     intensity=np.array([1000.0, 2000.0, 500.0], dtype=np.float32),
        ...     spectrum_id=np.array([0, 0, 0], dtype=np.int64),
        ...     scan_number=np.array([1, 1, 1], dtype=np.int64),
        ...     ms_level=np.array([1, 1, 1], dtype=np.int16),
        ...     retention_time=np.array([60.0, 60.0, 60.0], dtype=np.float32),
        ...     polarity=np.array([1, 1, 1], dtype=np.int8),
        ... )
        >>> writer.write_batch(batch)
    """
    
    def __init__(
        self,
        mz: Any,
        intensity: Any,
        spectrum_id: Any,
        scan_number: Any,
        ms_level: Any,
        retention_time: Any,
        polarity: Any,
        ion_mobility: Optional[Any] = None,
        precursor_mz: Optional[Any] = None,
        precursor_charge: Optional[Any] = None,
        precursor_intensity: Optional[Any] = None,
        isolation_window_lower: Optional[Any] = None,
        isolation_window_upper: Optional[Any] = None,
        collision_energy: Optional[Any] = None,
        total_ion_current: Optional[Any] = None,
        base_peak_mz: Optional[Any] = None,
        base_peak_intensity: Optional[Any] = None,
        injection_time: Optional[Any] = None,
        pixel_x: Optional[Any] = None,
        pixel_y: Optional[Any] = None,
        pixel_z: Optional[Any] = None,
    ) -> None:
        """
        Create a new owned columnar batch from numpy arrays.
        
        Args:
            mz: Float64 array of m/z values
            intensity: Float32 array of intensity values
            spectrum_id: Int64 array of spectrum IDs
            scan_number: Int64 array of scan numbers
            ms_level: Int16 array of MS levels
            retention_time: Float32 array of retention times
            polarity: Int8 array of polarity values (1 or -1)
            ion_mobility: Optional Float64 array of ion mobility values
            precursor_mz: Optional Float64 array of precursor m/z
            precursor_charge: Optional Int16 array of precursor charges
            precursor_intensity: Optional Float32 array of precursor intensities
            isolation_window_lower: Optional Float32 array
            isolation_window_upper: Optional Float32 array
            collision_energy: Optional Float32 array
            total_ion_current: Optional Float64 array
            base_peak_mz: Optional Float64 array
            base_peak_intensity: Optional Float32 array
            injection_time: Optional Float32 array
            pixel_x: Optional Int32 array (MSI)
            pixel_y: Optional Int32 array (MSI)
            pixel_z: Optional Int32 array (MSI)
        """
        ...
    
    @staticmethod
    def with_required(
        mz: Any,
        intensity: Any,
        spectrum_id: Any,
        scan_number: Any,
        ms_level: Any,
        retention_time: Any,
        polarity: Any,
    ) -> OwnedColumnarBatch:
        """Create a batch with only required columns (optional columns set to all-null)."""
        ...
    
    @property
    def num_peaks(self) -> int:
        """Number of peaks in this batch."""
        ...
    
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class AsyncMzPeakWriter:
    """
    Async writer that offloads compression and I/O to a background thread.
    
    Ideal for high-throughput pipelines where the producer can prepare batches
    while previous batches are being written. Uses zero-copy transfer of batches.
    
    Example:
        >>> writer = mzpeak.AsyncMzPeakWriter("output.parquet")
        >>> for batch in batches:
        ...     writer.write_batch(batch)  # Non-blocking until backpressure
        >>> stats = writer.finish()  # Wait for background thread
    """
    
    def __init__(
        self,
        path: Union[str, PathLike],
        config: Optional[WriterConfig] = None,
        buffer_capacity: int = 8
    ) -> None:
        """
        Create a new async writer.
        
        Args:
            path: Output file path
            config: Optional WriterConfig for compression settings
            buffer_capacity: Number of batches to buffer (default 8)
        """
        ...
    
    def write_batch(self, batch: OwnedColumnarBatch) -> None:
        """
        Write an owned columnar batch (zero-copy transfer).
        
        Note: This may block if the buffer is full (backpressure)
        """
        ...
    
    def check_error(self) -> None:
        """
        Check if the background writer has encountered an error.
        
        Raises exception if an error has occurred.
        """
        ...
    
    def finish(self) -> WriterStats:
        """
        Finalize and close the writer.
        
        Waits for the background thread to complete all pending writes.
        
        Returns:
            WriterStats with final statistics
        """
        ...
    
    def is_open(self) -> bool:
        """Check if the writer is open."""
        ...
    
    def __enter__(self) -> AsyncMzPeakWriter: ...
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool: ...
    def __repr__(self) -> str: ...

class IngestSpectrum:
    """
    Thin-waist ingestion spectrum for contract-enforced writing.
    
    This provides a validated spectrum type that enforces the ingestion contract
    invariants (valid ms_level, polarity, finite retention_time, matching array lengths).
    
    Example:
        >>> import numpy as np
        >>> converter = mzpeak.IngestSpectrumConverter()
        >>> ingest = mzpeak.IngestSpectrum(
        ...     spectrum_id=0,
        ...     scan_number=1,
        ...     ms_level=1,
        ...     retention_time=60.0,
        ...     polarity=1,
        ...     mz=np.array([100.0, 200.0], dtype=np.float64),
        ...     intensity=np.array([1000.0, 2000.0], dtype=np.float32),
        ... )
        >>> spectrum_arrays = converter.convert(ingest)
    """
    
    def __init__(
        self,
        spectrum_id: int,
        scan_number: int,
        ms_level: int,
        retention_time: float,
        polarity: int,
        mz: Any,
        intensity: Any,
        ion_mobility: Optional[Any] = None,
        precursor_mz: Optional[float] = None,
        precursor_charge: Optional[int] = None,
        precursor_intensity: Optional[float] = None,
        isolation_window_lower: Optional[float] = None,
        isolation_window_upper: Optional[float] = None,
        collision_energy: Optional[float] = None,
        total_ion_current: Optional[float] = None,
        base_peak_mz: Optional[float] = None,
        base_peak_intensity: Optional[float] = None,
        injection_time: Optional[float] = None,
        pixel_x: Optional[int] = None,
        pixel_y: Optional[int] = None,
        pixel_z: Optional[int] = None,
    ) -> None:
        """
        Create a new ingestion spectrum.
        
        Args:
            spectrum_id: Unique spectrum identifier (typically 0-indexed)
            scan_number: Native scan number from the instrument
            ms_level: MS level (1, 2, 3, ...)
            retention_time: Retention time in seconds
            polarity: Polarity (1 for positive, -1 for negative, 0 for unknown)
            mz: Float64 array of m/z values
            intensity: Float32 array of intensity values
            ion_mobility: Optional Float64 array of ion mobility values
        """
        ...
    
    @property
    def spectrum_id(self) -> int: ...
    
    @property
    def scan_number(self) -> int: ...
    
    @property
    def ms_level(self) -> int: ...
    
    @property
    def retention_time(self) -> float: ...
    
    @property
    def polarity(self) -> int: ...
    
    @property
    def mz_array(self) -> Any: ...
    
    @property
    def intensity_array(self) -> Any: ...
    
    def __repr__(self) -> str: ...

class IngestSpectrumConverter:
    """
    Stateful converter from IngestSpectrum to SpectrumArrays with contract enforcement.
    
    Validates that spectrum IDs are contiguous and enforces all ingestion contract
    invariants (ms_level >= 1, valid polarity, finite retention_time, etc.).
    
    Example:
        >>> converter = mzpeak.IngestSpectrumConverter()
        >>> for ingest_spectrum in spectra:
        ...     spectrum_arrays = converter.convert(ingest_spectrum)
        ...     writer.write_spectrum_arrays(spectrum_arrays)
    """
    
    def __init__(self) -> None:
        """Create a new contract-enforcing converter."""
        ...
    
    def convert(self, ingest: IngestSpectrum) -> SpectrumArrays:
        """
        Convert an ingestion spectrum to SpectrumArrays with contract validation.
        
        Args:
            ingest: IngestSpectrum to convert
            
        Returns:
            SpectrumArrays suitable for writing
            
        Raises:
            MzPeakValidationError: If contract validation fails
        """
        ...
    
    def __repr__(self) -> str: ...

