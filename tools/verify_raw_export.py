"""Compare rawr's mzML export to mzpeak's tables with an independent reader.

Usage: python tools/verify_raw_export.py reference.mzML converted.mzpeak
Requires pyarrow. All peak values are compared at the destination's bit width.
"""
import base64
import json
import re
import struct
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile
import zlib
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq


def spectra(reference):
    """Retain only the current XML spectrum, including for multi-GB exports."""
    stack = []
    for event, element in ET.iterparse(reference, events=("start", "end")):
        if event == "start":
            stack.append(element)
        else:
            if element.tag.rsplit("}", 1)[-1] == "spectrum":
                yield element
                stack[-2].remove(element)
                element.clear()
            stack.pop()


class PeakCursor:
    """Compare contiguous peak slices without materializing the full table."""
    def __init__(self, parquet):
        self.batches = iter(parquet.iter_batches(batch_size=65536))
        self.batch = None
        self.row = 0

    def take(self, count, spectrum_id):
        columns = {"mz": [], "intensity": []}
        while count:
            if self.batch is None or self.row == self.batch.num_rows:
                self.batch = next(self.batches)
                self.row = 0
            length = min(count, self.batch.num_rows - self.row)
            batch = self.batch.slice(self.row, length)
            ids = batch.column("spectrum_id")
            assert ids.null_count == 0 and pc.all(pc.equal(ids, spectrum_id)).as_py()
            for name, width in (("mz", 8), ("intensity", 4)):
                array = batch.column(name)
                assert array.null_count == 0
                start = array.offset * width
                columns[name].append(memoryview(array.buffers()[1])[start:start + length * width].tobytes())
            self.row += length
            count -= length
        return {name: b"".join(chunks) for name, chunks in columns.items()}


def compare(reference, metadata_file, peaks_file):
    metadata = (row for batch in metadata_file.iter_batches(batch_size=1024) for row in batch.to_pylist())
    peaks = PeakCursor(peaks_file)
    total_peaks = 0
    total_spectra = 0
    for expected in spectra(reference):
        actual = next(metadata)
        parameters = {p.attrib["accession"]: p.attrib for p in expected.findall(".//{*}cvParam")}
        assert actual["spectrum_id"] == int(expected.attrib["index"])
        assert actual["scan_number"] == int(re.search(r"scan=(\d+)", expected.attrib["id"])[1])
        assert actual["ms_level"] == int(parameters["MS:1000511"]["value"])
        rt = parameters["MS:1000016"]
        seconds = float(rt["value"]) * (60 if rt.get("unitAccession") == "UO:0000031" else 1)
        assert struct.pack("<f", actual["retention_time"]) == struct.pack("<f", seconds)
        expected_polarity = 1 if "MS:1000130" in parameters else -1 if "MS:1000129" in parameters else 0
        assert actual["polarity"] == expected_polarity
        for column, accession, code in [
            ("precursor_mz", "MS:1000744", "d"),
            ("precursor_charge", "MS:1000041", "b"),
            ("isolation_window_lower", "MS:1000828", "f"),
            ("isolation_window_upper", "MS:1000829", "f"),
            ("collision_energy", "MS:1000045", "f"),
            ("total_ion_current", "MS:1000285", "d"),
            ("base_peak_mz", "MS:1000504", "d"),
            ("base_peak_intensity", "MS:1000505", "f"),
            ("injection_time", "MS:1000927", "f"),
        ]:
            if accession in parameters:
                value = float(parameters[accession]["value"])
                if code == "b":
                    value = int(value)
                assert struct.pack("<" + code, actual[column]) == struct.pack("<" + code, value), (actual["scan_number"], column)
        offset, count = actual["peak_offset"], actual["peak_count"]
        assert count == int(expected.attrib["defaultArrayLength"])
        assert offset == total_peaks
        stored = peaks.take(count, actual["spectrum_id"])
        arrays_seen = set()
        for array in expected.findall(".//{*}binaryDataArray"):
            terms = {p.attrib["accession"] for p in array.findall("{*}cvParam")}
            if "MS:1000514" in terms:
                column, output_code = "mz", "d"
            elif "MS:1000515" in terms:
                column, output_code = "intensity", "f"
            else:
                raise AssertionError("unexpected auxiliary binary array")
            assert column not in arrays_seen
            arrays_seen.add(column)
            source_code = "d" if "MS:1000523" in terms else "f"
            binary = base64.b64decode(array.findtext("{*}binary") or "")
            if "MS:1000574" in terms:
                binary = zlib.decompress(binary)
            values = struct.unpack("<" + source_code * count, binary)
            assert stored[column] == struct.pack("<" + output_code * count, *values), (actual["scan_number"], column)
        assert arrays_seen == {"mz", "intensity"}
        total_peaks += count
        total_spectra += 1
    assert next(metadata, None) is None
    assert total_spectra == metadata_file.metadata.num_rows
    assert total_peaks == peaks_file.metadata.num_rows
    print(json.dumps({"spectra": total_spectra, "peaks": total_peaks, "peak_fidelity": "bit exact at f64 m/z and f32 intensity", "scan_metadata": "matched all exported fields"}, indent=2))


def main(reference, converted):
    # Extract fixed members to sibling temporary storage for seekable Parquet I/O.
    with tempfile.TemporaryDirectory(dir=Path(converted).resolve().parent) as directory:
        with zipfile.ZipFile(converted) as archive:
            paths = [archive.extract(name, directory) for name in (
                "spectra/spectra.parquet", "peaks/peaks.parquet",
            )]
        with pq.ParquetFile(paths[0]) as metadata, pq.ParquetFile(paths[1]) as peaks:
            compare(reference, metadata, peaks)


if __name__ == "__main__":
    main(*sys.argv[1:])
