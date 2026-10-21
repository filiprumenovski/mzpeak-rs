"""Complete finite-time checks after auditing the pinned reader's +inf error."""
import json
import os
from pathlib import Path
import sys
import time
import traceback
import numpy as np
from mzpeak import MzPeakFile as Official
from mzpeak_compat import MzPeakFile
from validate_astral_adapter import scalar, reference_query, REPORT as SHARED, sha


def main(output):
    result = {'status':'running', 'scope':'Retention-time and empty-query checks; no signal code or numerical bounds changed.'}
    started = time.perf_counter()
    f = MzPeakFile(os.environ['MZPEAK_COMPAT_ARCHIVE'],filter_profile_zeros=True)
    o = Official(os.environ['MZPEAK_OFFICIAL_ARCHIVE'])
    try:
        ts = o.spectra['time'].to_numpy(dtype=float)
        ids = sorted(set([0,1,len(o)//2,len(o)-2,len(o)-1]+np.random.default_rng(20261021).integers(0,len(o),16).tolist()))
        queries = [float(ts[0]-1),float(ts[-1]+1)]
        for i in ids:
            queries.extend([float(ts[i]),float(np.nextafter(ts[i],-np.inf)),float(np.nextafter(ts[i],np.inf))])
            if i+1 < len(ts): queries.append(float((ts[i]+ts[i+1])/2))
        for t in queries:
            assert f.time.resolve(t) == o.time.resolve(t),t
            scalar(f.time[t],o.time[t])
        result['finite_time_queries_passed'] = len(queries)
        errors=[]
        for r in (f,o):
            try:
                r.time.resolve(float('inf'))
                raise AssertionError('Pinned +inf behavior changed; inspect before changing expectation')
            except KeyError as e:
                assert e.args == (-1,)
                errors.append({'type':type(e).__name__,'args':list(e.args)})
        result['positive_infinity'] = {'both_raise_same_error':errors[0]==errors[1], 'error':errors[0], 'supported':False}
        for i in (0,len(o)//2,len(o)-4):
            query=slice(float(ts[i]),float(ts[min(i+3,len(o)-1)]))
            assert f.time.resolve(query)==o.time.resolve(query)
            a=f.time[query]
            b=reference_query('time slice '+repr(query),lambda:o.time[query])
            if b is None: b=[o[j] for j in range(*o.time.resolve(query).indices(len(o)))]
            assert len(a)==len(b)
            for x,y in zip(a,b): scalar(x,y)
        result['time_ranges_checked']=3
        result['reference_timeouts']=SHARED.get('reference_timeouts',[])
        assert f[[]] == o[[]] == []
        a=f.spectra_signal_for_indices([])
        assert all(len(v)==0 for v in a.values())
        try:
            o.spectra_signal_for_indices([])
            raise AssertionError('Pinned empty-array-range behavior changed')
        except ValueError as e:
            result['empty_array_range']={'native_points':0,'official_error':str(e),'same_behavior':False}
        for i in (-1,len(o)):
            errors=[]
            for r in (f,o):
                try:
                    r[i]
                    raise AssertionError('Expected an invalid spectrum index to raise')
                except KeyError as e:
                    errors.append((type(e).__name__,e.args))
            assert errors[0]==errors[1]
        result['invalid_spectrum_indices_same_error']=[-1,len(o)]
        result['status']='passed_with_documented_reference_limits'
    except Exception:
        result['status']='failed'
        result['error']=traceback.format_exc()
        traceback.print_exc()
    finally:
        f.close();o.close()
        result['elapsed_seconds']=time.perf_counter()-started
        result['source_sha256']=sha(__file__)
        result['adapter_sha256']=sha(Path(__file__).with_name('mzpeak_compat.py'))
        Path(output).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)
    return result['status']=='failed'


if __name__=='__main__':
    sys.exit(main(sys.argv[1]))
