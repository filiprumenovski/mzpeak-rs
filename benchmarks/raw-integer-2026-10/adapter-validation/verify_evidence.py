"""Verify the Astral adapter evidence and derive its publication summary.

The original full-run report is immutable. Two test expectations are resolved
by explicit supplemental audits, not by changing signal bounds or hiding errors.
"""
import argparse
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
EVIDENCE = HERE.parent/'evidence'
DATA = EVIDENCE/'astral-adapter'


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def derive():
    run = read(DATA/'contract-run.json')
    times = read(DATA/'time-contract.json')
    metadata = read(DATA/'staging-metadata-audit.json')
    provenance = read(DATA/'provenance.json')
    previous = read(EVIDENCE/'astral-queries.json')
    for name, expected in provenance['published_file_sha256'].items():
        assert sha(DATA/name) == expected, name
    assert run['adapter_sha256'] == times['adapter_sha256'] == sha(HERE/'mzpeak_compat.py')
    assert run['source_sha256'] == sha(HERE/'validate_astral_adapter.py')
    assert times['source_sha256'] == sha(HERE/'validate_time_contract.py')
    assert metadata['audit_source_sha256'] == sha(HERE/'audit_staged_metadata.py')
    assert run['status'] == 'failed'
    corrected = ['acquisition_metadata_and_source_identity', 'retention_time_boundaries']
    assert run['failures'] == corrected
    sections = run['sections']
    assert sections[corrected[0]]['error'] == "['run']"
    assert sections[corrected[1]]['error'] == '-1'
    required = {'fixture_identity','all_metadata_frames','zero_filter_edge_cases','scalar_id_list_slice',
                'iterator_forward_seek_and_prefer_peaks','range_list_duplicate_and_slice_contract',
                'same_xic_and_ms2_queries','all_scans_all_points_and_zero_policy',
                'inclusive_mz_boundaries_and_quantization_crossings','chromatograms_tic_bpc',
                'integrity_and_lifetime','flattened_columns'}
    assert set(sections) == required | set(corrected)
    assert all(sections[k]['status'] == 'passed' for k in required)
    identity = sections['fixture_identity']['result']
    assert identity['native']['sha256'] == previous['sha256']['native.zip']
    assert identity['official']['sha256'] == previous['sha256']['official.mzpeak']
    assert metadata['status'] == 'passed' and metadata['all_other_acquisition_metadata_equal']
    assert metadata['source_sha1'] == '272b6e7f39a812050bdb3969d045af34643cbe13'
    assert set(metadata['audited_changes_official_native']) == {'source_name','source_location','derived_run_id'}
    assert times['status'] == 'passed_with_documented_reference_limits'
    assert times['finite_time_queries_passed'] == 85 and times['time_ranges_checked'] == 3
    assert times['positive_infinity'] == {'both_raise_same_error':True,'error':{'type':'KeyError','args':[-1]},'supported':False}
    assert times['invalid_spectrum_indices_same_error'] == [-1,744726]
    points = sections['all_scans_all_points_and_zero_policy']['result']
    assert points['scans_checked'] == 744726
    assert points['native_points'] == 1511811155 and points['native_zeros'] == 162582445
    assert points['native_points'] - points['official_points'] == points['removed_profile_zeros']
    assert points['native_zeros'] - points['official_zeros'] == points['removed_profile_zeros']
    assert 0 <= points['max_mz_error_ppm'] <= .01
    assert 0 <= points['max_intensity_relative_error'] <= .001
    assert points['maximum_native_batch_points'] <= 8000000
    workloads = sections['same_xic_and_ms2_queries']['result']
    assert workloads['xic']['scans'] == 494 and workloads['ms2_batch']['scans'] == 50208
    for result in workloads.values():
        assert result['max_mz_error_ppm'] <= .01 and result['max_intensity_relative_error'] <= .001
    assert workloads['xic']['boundary_membership_differences'] == 0
    assert workloads['xic']['max_xic_relative_error'] <= .001
    boundaries = sections['inclusive_mz_boundaries_and_quantization_crossings']['result']
    assert boundaries['scan_probes'] == 8 and boundaries['windows'] == 176
    assert boundaries['max_crossing_distance_ppm'] <= .01
    assert boundaries['official_coordinate_argument_ignored'] > 0
    timeouts = run.get('reference_timeouts',[]) + times['reference_timeouts']
    assert len(timeouts) == 4 and all(x['limit_seconds']==30 for x in timeouts)
    return {
        'status':'validated_with_documented_api_limits',
        'scope':'Same saved Astral archives as the query benchmark. Full retained-signal comparison, plus public API and zero-policy checks. Not a new timing or independent official-to-RAW benchmark.',
        'fixture_identity':identity,
        'adapter_sha256':run['adapter_sha256'],
        'signal':points,
        'metadata_frames':sections['all_metadata_frames']['result'],
        'acquisition_staging_audit':metadata,
        'workloads':workloads,
        'boundaries':boundaries,
        'finite_time_queries':times['finite_time_queries_passed'],
        'time_ranges':times['time_ranges_checked'],
        'reference_timeouts':timeouts,
        'empty_array_range':times['empty_array_range'],
        'positive_infinity':times['positive_infinity'],
        'additional_checks':{k:sections[k]['result'] for k in ('scalar_id_list_slice','iterator_forward_seek_and_prefer_peaks',
            'range_list_duplicate_and_slice_contract','chromatograms_tic_bpc','integrity_and_lifetime','flattened_columns')},
        'test_expectation_corrections':{
            'acquisition_metadata_and_source_identity':'A separate strict audit verifies the known staged source name, location, and derived run ID; all other acquisition metadata agrees.',
            'retention_time_boundaries':'Both readers reject +infinity with the same inherited KeyError. Supplemental tests complete all finite-time queries and record the unsupported case.'},
        'provenance':'astral-adapter/provenance.json',
    }


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--write-summary',action='store_true')
    args=parser.parse_args()
    summary=derive()
    path=EVIDENCE/'astral-adapter.json'
    if args.write_summary:
        path.write_text(json.dumps(summary,indent=2)+'\n')
    else:
        assert read(path)==summary, 'Published adapter summary differs from evidence'
    p=summary['signal']
    print(f"Astral adapter: {p['scans_checked']:,} scans; {p['official_points']:,} retained points compared; zero policy checked on {p['native_points']:,} native points.")
    print('Fixed numerical bounds; four reference tail-query timeouts and exact-cutoff behavior remain explicit.')


if __name__=='__main__':
    main()
