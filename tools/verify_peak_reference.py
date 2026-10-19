"""Compare all centroid peaks against an independent mzML reference.

Usage: python tools/verify_peak_reference.py reference.mzML converted.mzpeak
Requires pyarrow and numpy. Values are compared at the reference's precision;
this also permits checking an older f32 mzML against preserved f64 m/z data.
"""
import base64
import json
from pathlib import Path
import re
import sys
import tempfile
import zipfile
import zlib

import numpy as np
import pyarrow.parquet as pq

from verify_raw_export import PeakCursor, spectra


def main(reference, converted):
    count = total_peaks = 0
    with tempfile.TemporaryDirectory(dir=Path(converted).resolve().parent) as directory:
        with zipfile.ZipFile(converted) as archive:
            paths = [archive.extract(name, directory) for name in (
                "spectra/spectra.parquet", "peaks/peaks.parquet",
            )]
        with pq.ParquetFile(paths[0]) as metadata_file, pq.ParquetFile(paths[1]) as peaks_file:
            metadata = (row for batch in metadata_file.iter_batches(batch_size=1024) for row in batch.to_pylist())
            peaks = PeakCursor(peaks_file)
            for expected in spectra(reference):
                actual = next(metadata)
                assert actual["scan_number"] == int(re.search(r"scan=(\d+)", expected.attrib["id"])[1])
                assert actual["peak_count"] == int(expected.attrib["defaultArrayLength"])
                assert actual["peak_offset"] == total_peaks
                stored = peaks.take(actual["peak_count"], actual["spectrum_id"])
                seen = set()
                for array in expected.findall(".//{*}binaryDataArray"):
                    terms = {p.attrib["accession"] for p in array.findall("{*}cvParam")}
                    if "MS:1000514" in terms:
                        column, stored_dtype = "mz", "<f8"
                    elif "MS:1000515" in terms:
                        column, stored_dtype = "intensity", "<f4"
                    else:
                        continue
                    assert column not in seen
                    seen.add(column)
                    reference_dtype = "<f8" if "MS:1000523" in terms else "<f4"
                    binary = base64.b64decode(array.findtext("{*}binary") or "")
                    if "MS:1000574" in terms:
                        binary = zlib.decompress(binary)
                    assert len(binary) == actual["peak_count"] * np.dtype(reference_dtype).itemsize
                    represented = np.frombuffer(stored[column], dtype=stored_dtype).astype(reference_dtype, copy=False)
                    assert represented.tobytes() == binary, (actual["scan_number"], column)
                assert seen == {"mz", "intensity"}
                total_peaks += actual["peak_count"]
                count += 1
            assert next(metadata, None) is None
            assert count == metadata_file.metadata.num_rows
            assert total_peaks == peaks_file.metadata.num_rows
    print(json.dumps({"spectra": count, "peaks": total_peaks, "peak_fidelity": "bit exact at reference precision", "native_scan_numbers": "matched"}, indent=2))


if __name__ == "__main__":
    main(*sys.argv[1:])
