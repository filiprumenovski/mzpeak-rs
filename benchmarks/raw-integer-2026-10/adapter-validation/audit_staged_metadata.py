"""Audit the known staging-name differences between the saved Astral archives.

No signal bounds or adapter behavior change. The all-point validator establishes
full archive SHA-256 identity separately. This check records member hashes and
permits only converter provenance, configuration order, and the known source
name/location and derived run ID changes.
"""
import hashlib
import json
import os
from pathlib import Path
import sys
import zipfile


def audit():
    files = [(Path(os.environ['MZPEAK_OFFICIAL_ARCHIVE']), 'mzpeak_index.json'),
             (Path(os.environ['MZPEAK_COMPAT_ARCHIVE']), 'acquisition/acquisition.json')]
    metadata, hashes = [], []
    for path, member in files:
        with zipfile.ZipFile(path) as archive:
            raw = archive.read(member)
            hashes.append(hashlib.sha256(raw).hexdigest())
            metadata.append(json.loads(raw)['metadata'])
    official, native = metadata
    official['software_list'] = [x for x in official['software_list'] if x['id'] != 'mzpeak_prototyping_convert1']
    official['data_processing_method_list'] = [x for x in official['data_processing_method_list'] if x['id'] != 'mzpeak_conversion1']
    for m in metadata:
        configs = m['instrument_configuration_list']
        assert len({x['id'] for x in configs}) == len(configs)
        m['instrument_configuration_list'] = sorted(configs, key=lambda x:x['id'])
    a,b = [m['file_description']['source_files'] for m in metadata]
    assert len(a) == len(b) == 1
    a,b = a[0],b[0]
    original_name = '20231206_HAP1_1ug_60min_DIA_2Th_5e4_3p5ms_rep01.raw'
    assert a['name'] == 'astral.raw' and b['name'] == original_name
    assert official['run']['id'] == 'astral' and native['run']['id'] == Path(original_name).stem
    assert {k:v for k,v in a.items() if k not in ('name','location')} == {k:v for k,v in b.items() if k not in ('name','location')}
    sha1 = [x['value'] for x in a['parameters'] if x['accession'] == 'MS:1000569']
    assert sha1 == ['272b6e7f39a812050bdb3969d045af34643cbe13']
    changes = {'source_name': [a['name'],b['name']], 'source_location':[a['location'],b['location']],
               'derived_run_id':[official['run']['id'],native['run']['id']]}
    a['name'],a['location'] = b['name'],b['location']
    official['run']['id'] = native['run']['id']
    assert official == native, [k for k in official if official[k] != native.get(k)]
    return {'status':'passed', 'source_sha1':sha1[0], 'audited_changes_official_native':changes,
            'all_other_acquisition_metadata_equal':True,
            'metadata_member_sha256':dict(zip(['official','native'],hashes)),
            'audit_source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'scope':'File-level metadata audit only; combine with separately recorded full-archive identity and signal validation.'}


if __name__ == '__main__':
    report = audit()
    Path(sys.argv[1]).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))
