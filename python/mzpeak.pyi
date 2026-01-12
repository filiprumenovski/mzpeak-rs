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

class ConversionConfig:
    """Configuration for mzML conversion."""
    
    def __init__(
        self,
        batch_size: int = 100,
        preserve_precision: bool = True,
        include_chromatograms: bool = True,
        progress_interval: int = 1000
    ) -> None:
        """
        Create a new conversion configuration.
        
        Args:
            batch_size: Number of spectra to process per batch
            preserve_precision: Keep original numeric precision
            include_chromatograms: Include chromatogram data
            progress_interval: Log progress every N spectra
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
