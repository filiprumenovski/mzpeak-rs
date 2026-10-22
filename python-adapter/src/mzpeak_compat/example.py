"""Run a same-query comparison using the installed synthetic example files."""
from contextlib import closing
from importlib.resources import as_file, files
import json

import numpy as np
from numpy.testing import assert_array_equal
from mzpeak import MzPeakFile as OfficialFile

from . import MzPeakFile


def compare():
    data = files('mzpeak_compat') / 'examples'
    source = json.loads((data / 'source.json').read_text())
    total = retained = zeros = 0
    max_mz_ppm = max_intensity_relative = 0.
    with as_file(data / 'synthetic.integer.zip') as native_path, as_file(data / 'synthetic.official.mzpeak') as official_path:
        with official_path.open("rb") as handle, MzPeakFile(native_path) as native, MzPeakFile(native_path, filter_profile_zeros=True) as filtered, closing(OfficialFile(handle)) as official:
            assert len(native) == len(filtered) == len(official) == len(source)
            for expected in source:
                i = expected['index']
                full, actual, reference = native[i], filtered[i], official[i]
                mz = np.array(expected['mz'])
                intensity = np.array(expected['intensity'])
                assert_array_equal(full['intensity array'] == 0, intensity == 0)
                assert len(full['m/z array']) == len(mz)
                keep = expected['retained_indices']
                assert_array_equal(reference['m/z array'], mz[keep])
                assert_array_equal(reference['intensity array'], intensity[keep])
                assert_array_equal(actual['intensity array'] == 0, reference['intensity array'] == 0)
                assert len(actual['m/z array']) == len(keep)
                for read, masses, values in ((full,mz,intensity), (actual,mz[keep],intensity[keep])):
                    if len(masses):
                        mz_error = float(np.max(np.abs(read['m/z array']-masses)/masses*1e6))
                        assert mz_error <= .01
                        max_mz_ppm = max(max_mz_ppm,mz_error)
                    nonzero = values > 0
                    if nonzero.any():
                        intensity_error = float(np.max(np.abs(read['intensity array'][nonzero]-values[nonzero])/values[nonzero]))
                        assert intensity_error <= .001
                        max_intensity_relative = max(max_intensity_relative,intensity_error)
                total += len(mz)
                retained += len(keep)
                zeros += int((intensity == 0).sum())
            assert native.check_archive_integrity() == (True, [])
    return dict(scans=len(source), source_points=total, retained_points=retained,
                source_zeros=zeros, max_mz_error_ppm=max_mz_ppm,
                max_intensity_error_percent=100*max_intensity_relative,
                status='passed')


def main():
    print(json.dumps(compare(),indent=2))


if __name__ == '__main__':
    main()
