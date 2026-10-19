"""Compare rawr's mzML export to mzpeak's tables with an independent reader.

Usage: python tools/verify_raw_export.py reference.mzML converted.mzpeak
Requires pyarrow. All peak values are compared at the destination's bit width.
"""
import base64
import json
import re
import struct
import sys
import xml.etree.ElementTree as ET
import zipfile
import zlib

import pyarrow as pa
import pyarrow.parquet as pq


def main(reference, converted):
    with zipfile.ZipFile(converted) as archive:
        metadata = pq.read_table(pa.BufferReader(archive.read("spectra/spectra.parquet"))).to_pylist()
        peaks = pq.read_table(pa.BufferReader(archive.read("peaks/peaks.parquet"))).to_pydict()
    expected_spectra = ET.parse(reference).findall(".//{*}spectrum")
    assert len(metadata) == len(expected_spectra)
    total_peaks = 0
    for expected, actual in zip(expected_spectra, metadata):
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
        for array in expected.findall(".//{*}binaryDataArray"):
            terms = {p.attrib["accession"] for p in array.findall("{*}cvParam")}
            column, output_code = ("mz", "d") if "MS:1000514" in terms else ("intensity", "f")
            source_code = "d" if "MS:1000523" in terms else "f"
            binary = base64.b64decode(array.findtext("{*}binary") or "")
            if "MS:1000574" in terms:
                binary = zlib.decompress(binary)
            values = struct.unpack("<" + source_code * count, binary)
            stored = peaks[column][offset:offset + count]
            assert struct.pack("<" + output_code * count, *stored) == struct.pack("<" + output_code * count, *values), (actual["scan_number"], column)
        total_peaks += count
    assert total_peaks == len(peaks["mz"]) == len(peaks["intensity"])
    print(json.dumps({"spectra": len(metadata), "peaks": total_peaks, "peak_fidelity": "bit exact at f64 m/z and f32 intensity", "scan_metadata": "matched all exported fields"}, indent=2))


if __name__ == "__main__":
    main(*sys.argv[1:])
