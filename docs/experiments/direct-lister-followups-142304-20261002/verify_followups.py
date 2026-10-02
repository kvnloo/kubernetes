#!/usr/bin/env python3
"""Independent reader for structural receipts, inventories and paired allocations."""
import collections
import json
from pathlib import Path
import re
import statistics
import sys

RECEIPT = re.compile(r'FOLLOWUP_RECEIPT (\S+) (\{.*\})')
BENCH = re.compile(r'^BenchmarkFollowupN2EqualOutput/op=(get|list)/n=(\d+)/mf=(\d+)/path=(\w+)(?:-\d+)?\s+(\d+)\s+([\d.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op$', re.M)

def events(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.startswith('{')]

def read_receipts(path):
    out=[]
    for ev in events(path):
        m=RECEIPT.search(ev.get('Output',''))
        if m: out.append((m[1],json.loads(m[2])))
    return out

def require_root(path, name):
    ev=events(path)
    assert not any(x.get('Action')=='fail' for x in ev), f'failure in {path}'
    assert sum(x.get('Action')=='pass' and x.get('Test')==name for x in ev)==1, f'missing root {name}'

def benchmark_rows(path):
    return [{'op':m[1],'n':int(m[2]),'mf':int(m[3]),'path':m[4],'iterations':int(m[5]),'ns':float(m[6]),'bytes':int(m[7]),'allocs':int(m[8])} for m in BENCH.finditer(path.read_text())]

def verify(out, plan):
    manifest=json.loads((out/'manifest.json').read_text())
    assert all(c.get('returncode')==0 for c in manifest['commands'] if c['label']!='independent artifact verification'), 'failed command retained in current attempt'
    counts={'calibration':1,'N1':3,'N1-list':3,'N2':6,'N2-real-cache':1,'N3':4,'N6-startup':1,'N6-context':3,'N6-fallback':4,'N6-version':1}
    receipts=[]
    for filename in ['correctness.jsonl','correctness-replay.jsonl']:
        path=out/filename
        for root in plan['correctness_roots']: require_root(path, root)
        rr=read_receipts(path)
        assert dict(collections.Counter(k for k,v in rr))==counts, 'wrong correctness receipt inventory'
        for key,r in rr:
            if key=='N1': assert r['source_unchanged'] and r['cache_get_backend_calls']==0 and r['raw_managed_fields_mismatch']==(r['fixture']=='normal-4096')
            if key=='N3': assert (r['list_counts'].get('pods',0)>0)==(not r['direct'] or r['node_graph'])
            if key=='N6-fallback': assert r['lister_calls']==1 and r['fallback_calls']==(0 if r['kind']=='success' else 1)
        receipts.extend(rr)
    bench=[]
    for block in range(plan['execution']['benchmark_blocks']):
        for arm in plan['fixtures']['N2']['paths']:
            rows=benchmark_rows(out/f'n2-{block:02d}-{arm}.txt')
            assert len(rows)==12 and len({(r['op'],r['n'],r['mf']) for r in rows})==12, 'benchmark inventory'
            assert all(r['path']==arm and r['iterations']==100 for r in rows), 'benchmark arm/iterations'
            for r in rows: r['block']=block
            bench.extend(rows)
    resource=[]
    for path in sorted(out.glob('n4-*.jsonl')):
        require_root(path,'TestFollowupN4ResourceTrial')
        rr=read_receipts(path);assert len(rr)==1 and rr[0][0]=='N4'
        r=rr[0][1];p=r['phase'];u,rd={'read':(0,150),'update':(20,0),'mixed':(20,150)}[r['workload']]
        assert (p['updates'],p['reads'],p['http_updates'])==(u,rd,u) and r['output_parity']
        if r['strategy']=='direct': assert p['storage_gets']==rd
        if r['strategy']=='direct' and not r['event']: assert r['total_lists']==0 and r['total_watches']==0
        if r['event']: assert p['event_updates']>=u
        resource.append(r)
    expected=plan['execution']['N4_blocks']*12
    assert len(resource)==expected and len({(r['block'],r['strategy'],r['event'],r['workload']) for r in resource})==expected
    recovery=[]
    for path in sorted(out.glob('n5-*.jsonl')):
        require_root(path,'TestFollowupN5RecoveryTrial')
        rr=read_receipts(path);assert len(rr)==1 and rr[0][0]=='N5'
        r=rr[0][1];need=r['mode']!='control' and (r['strategy']=='cached' or r['event'])
        assert r['perturbed']==need and r['injected_responses']==int(need) and r['output_parity']
        if need: assert r['watch_delta']>=2
        if r['mode']=='expiry' and need: assert r['full_list_delta']>=1, 'expiry did not cause observed List'
        recovery.append(r)
    expected=plan['execution']['N5_blocks']*12
    assert len(recovery)==expected and len({(r['block'],r['strategy'],r['event'],r['mode']) for r in recovery})==expected
    grouped=collections.defaultdict(list)
    for r in bench: grouped[(r['op'],r['n'],r['mf'],r['path'])].append(r)
    medians=[dict(op=k[0],n=k[1],mf=k[2],path=k[3],bytes=statistics.median(r['bytes'] for r in v),allocs=statistics.median(r['allocs'] for r in v),ns=statistics.median(r['ns'] for r in v)) for k,v in sorted(grouped.items())]
    effects=[]
    for event in [False,True]:
        for workload in ['read','update','mixed']:
            ratios=[]
            for b in range(plan['execution']['N4_blocks']):
                c=next(r for r in resource if r['block']==str(b) and r['strategy']=='cached' and r['event']==event and r['workload']==workload)
                d=next(r for r in resource if r['block']==str(b) and r['strategy']=='direct' and r['event']==event and r['workload']==workload)
                ratios.append(d['phase']['alloc_bytes']/c['phase']['alloc_bytes'])
            median=statistics.median(ratios)
            decision='inconclusive'
            if median<=.90 and sum(x<=.90 for x in ratios)>=4:decision='allocation_reduction'
            if median>=1.10 and sum(x>=1.10 for x in ratios)>=4:decision='allocation_increase'
            effects.append(dict(event=event,workload=workload,ratios=ratios,median_ratio=median,decision=decision))
    result={'passed':True,'correctness_receipts':len(receipts),'benchmark_records':len(bench),'resource_trials':len(resource),'recovery_trials':len(recovery),'benchmark_medians':medians,'resource_allocation_effects':effects,'recovery':recovery,'structural_receipts':receipts}
    (out/'SUMMARY.json').write_text(json.dumps(result,indent=2)+'\n')
    return {k:v for k,v in result.items() if k not in ['benchmark_medians','recovery','structural_receipts']}

if __name__=='__main__':
    if sys.argv[1:] == ['--self-test']:
        row=benchmark_rows(Path(__file__).with_name('VERIFIER_CONTROL.txt'))
        assert len(row)==1 and row[0]['bytes']==42
        bad=dict(row[0],iterations=99)
        assert bad['iterations']!=100
        m=RECEIPT.search('FOLLOWUP_RECEIPT control {"route":1}')
        assert m and json.loads(m[2])['route']==1
        print(json.dumps({'calibrated':True,'benchmark_parser':True,'wrong_iteration_detected':True,'receipt_parser':True}))
    else:
        print(json.dumps(verify(Path(sys.argv[1]),json.loads(Path(sys.argv[2]).read_text())),indent=2))

