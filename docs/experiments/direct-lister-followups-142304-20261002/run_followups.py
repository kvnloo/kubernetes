#!/usr/bin/env python3
"""Execute frozen follow-up probes; never overwrite an earlier attempt."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = Path.cwd()
PLAN = json.loads((HERE / 'PREREG.json').read_text())
OUT = ROOT / os.environ.get('K8S_FOLLOW_OUT', 'followup-evidence/r1')
OUT.mkdir(parents=True, exist_ok=False)
START = time.monotonic()
manifest = {'status': 'running', 'source_sha': os.environ.get('K8S_FOLLOW_SOURCE_SHA', subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()), 'checkout_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(), 'go_version': subprocess.check_output(['go', 'version'], text=True).strip(), 'platform': platform.platform(), 'cpu_count': os.cpu_count(), 'run_id': os.environ.get('GITHUB_RUN_ID'), 'commands': [], 'source_hashes': {}, 'prereg_sha256': hashlib.sha256((HERE/'PREREG.json').read_bytes()).hexdigest()}
files = sorted((ROOT/'pkg/kubeapiserver/direct').glob('*.go')) + sorted(HERE.glob('*.py')) + [HERE/'PREREG.json'] + sorted(HERE.glob('AMENDMENT*.json'))
manifest['source_hashes'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}

def save():
    manifest['elapsed_seconds'] = time.monotonic()-START
    manifest['named_command_seconds'] = sum(c.get('elapsed_seconds', 0) for c in manifest['commands'])
    manifest['residual_seconds'] = manifest['elapsed_seconds']-manifest['named_command_seconds']
    (OUT/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')

def run(label, argv, filename, env=None, timeout=180):
    print('RUN', label, flush=True)
    before = time.monotonic()
    receipt = {'label': label, 'argv': [str(x) for x in argv], 'file': filename, 'env_delta': env or {}, 'start_offset_seconds': before-START}
    manifest['commands'].append(receipt)
    code = -1
    try:
        with (OUT/filename).open('w') as h:
            result = subprocess.run(receipt['argv'], env=dict(os.environ, **(env or {})), stdout=h, stderr=subprocess.STDOUT, timeout=timeout)
        code = result.returncode
    finally:
        receipt.update(returncode=code, elapsed_seconds=time.monotonic()-before)
        save()
    if code:
        print((OUT/filename).read_text()[-12000:], flush=True)
        raise RuntimeError(f'{label} failed: {code}')

try:
    run('independent verifier calibration', [sys.executable, HERE/'verify_followups.py', '--self-test'], 'verifier-calibration.json')
    run('frozen harness format', ['gofmt', '-d', *sorted((ROOT/'pkg/kubeapiserver/direct').glob('followup*test.go'))], 'format.diff')
    binary = OUT/'followups.test'
    run('build frozen binary', ['go', 'test', '-p=2', '-c', '-o', binary, './pkg/kubeapiserver/direct'], 'build.log', timeout=PLAN['execution']['build_timeout_seconds'])
    manifest['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
    save()
    common = ['go','tool','test2json','-t','-p','k8s.io/kubernetes/pkg/kubeapiserver/direct',binary,'-test.v=test2json','-test.count=1','-test.timeout=120s']
    roots = PLAN['correctness_roots']
    run('structural correctness and controls', common+['-test.run=^('+'|'.join(roots)+')$'], 'correctness.jsonl')
    run('fresh process structural replay', common+['-test.run=^('+'|'.join(roots)+')$'], 'correctness-replay.jsonl')
    paths = PLAN['fixtures']['N2']['paths']
    for block in range(PLAN['execution']['benchmark_blocks']):
        for path in paths if block % 2 == 0 else list(reversed(paths)):
            run(f'N2 block {block} {path}', [binary,'-test.run=^$','-test.bench=^BenchmarkFollowupN2EqualOutput$/op=.*/n=.*/mf=.*/path='+path+'$','-test.benchmem','-test.benchtime=100x','-test.cpu=1','-test.count=1','-test.timeout=60s'], f'n2-{block:02d}-{path}.txt')
    cells = [(s,e,w) for s in ['cached','direct'] for e in [False,True] for w in ['read','update','mixed']]
    for block in range(PLAN['execution']['N4_blocks']):
        for strategy,event,workload in cells if block % 2 == 0 else list(reversed(cells)):
            env = {'K8S_FOLLOW_STRATEGY':strategy,'K8S_FOLLOW_EVENT':str(event).lower(),'K8S_FOLLOW_WORKLOAD':workload,'K8S_FOLLOW_BLOCK':str(block)}
            run(f'N4 {block} {strategy} event={event} {workload}', common+['-test.run=^TestFollowupN4ResourceTrial$'], f'n4-{block:02d}-{strategy}-{str(event).lower()}-{workload}.jsonl', env)
    cells = [(s,e,m) for s in ['cached','direct'] for e in [False,True] for m in ['control','restart','expiry']]
    for block in range(PLAN['execution']['N5_blocks']):
        for strategy,event,mode in cells if block % 2 == 0 else list(reversed(cells)):
            env = {'K8S_FOLLOW_STRATEGY':strategy,'K8S_FOLLOW_EVENT':str(event).lower(),'K8S_FOLLOW_RECOVERY':mode,'K8S_FOLLOW_BLOCK':str(block)}
            run(f'N5 {block} {strategy} event={event} {mode}', common+['-test.run=^TestFollowupN5RecoveryTrial$'], f'n5-{block:02d}-{strategy}-{str(event).lower()}-{mode}.jsonl', env)
    manifest['status'] = 'measurements_complete'
    save()
    run('independent artifact verification', [sys.executable, HERE/'verify_followups.py', OUT, HERE/'PREREG.json'], 'verification.log')
    manifest['status'] = 'verified'
except BaseException as exc:
    manifest.update(status='blocked', error=repr(exc))
    raise
finally:
    save()

