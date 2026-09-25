#!/usr/bin/env python3
"""Source-pinned AST discovery, not taint analysis or a safety proof.
A selects one function, B records AST evidence, A' expands local symbol edges,
and B' preserves unresolved reachability. Automated checks are not manual reviews.
AI-assisted research tooling. No upstream code changes or automatic comments.
"""
import argparse
import collections
import hashlib
import heapq
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import time

PIN = 'dff9a1db40999d73ed9e838b5c8d75d14c6528d5'
ROOTS = ['plugin/pkg/admission', 'staging/src/k8s.io/apiserver/pkg/admission',
         'staging/src/k8s.io/apiserver/pkg/endpoints/handlers',
         'staging/src/k8s.io/apimachinery/pkg/apis/meta/v1',
         'staging/src/k8s.io/apimachinery/pkg/runtime',
         'staging/src/k8s.io/apimachinery/pkg/util/managedfields',
         'staging/src/k8s.io/client-go/util/csaupgrade', 'pkg/kubeapiserver/options']
GO_SOURCE = r'''
package main
import (
 "bytes"
 "encoding/json"
 "fmt"
 "go/ast"
 "go/format"
 "go/parser"
 "go/token"
 "os"
 "path/filepath"
 "sort"
 "strconv"
 "strings"
)
type Call struct { Name string `json:"name"`; Args []string `json:"args"`; Line int `json:"line"` }
type Assign struct { Left []string `json:"left"`; Right []string `json:"right"`; Line int `json:"line"` }
type Node struct {
 ID string `json:"id"`; Path string `json:"path"`; Pkg string `json:"package"`; Name string `json:"name"`; Receiver string `json:"receiver"`
 Start int `json:"start"`; End int `json:"end"`; Calls []Call `json:"calls"`; Writes []Assign `json:"writes"`; Selectors []string `json:"selectors"`; Identifiers []string `json:"identifiers"`; Imports map[string]string `json:"imports"`; AsyncLines []int `json:"async_lines"`
}
type Output struct { Nodes []Node `json:"nodes"`; Errors []string `json:"errors"`; Files int `json:"files"` }
func main() {
 var files []string
 if err:=json.NewDecoder(os.Stdin).Decode(&files); err!=nil { panic(err) }
 root:=os.Args[1]
 out:=Output{Nodes: []Node{}, Errors: []string{}}
 for _,path:=range files {
  data,err:=os.ReadFile(filepath.Join(root,path)); if err!=nil { out.Errors=append(out.Errors,path+": "+err.Error()); continue }
  fs:=token.NewFileSet()
  f,err:=parser.ParseFile(fs,path,data,parser.SkipObjectResolution); if err!=nil { out.Errors=append(out.Errors,path+": "+err.Error()); continue }; out.Files++
  render:=func(n ast.Node) string { var b bytes.Buffer; if err:=format.Node(&b,fs,n); err!=nil {return "<format-error>"}; return b.String() }
  imports:=map[string]string{}
  for _,i:=range f.Imports { p,_:=strconv.Unquote(i.Path.Value); a:=filepath.Base(p); if i.Name!=nil {a=i.Name.Name}; imports[a]=p }
  for _,d:=range f.Decls {
   fn,ok:=d.(*ast.FuncDecl); if !ok||fn.Body==nil {continue}
   n:=Node{Path:path,Pkg:filepath.Dir(path),Name:fn.Name.Name,Start:fs.Position(fn.Pos()).Line,End:fs.Position(fn.End()).Line,Imports:imports,Calls:[]Call{},Writes:[]Assign{},Selectors:[]string{},Identifiers:[]string{},AsyncLines:[]int{}}
   if fn.Recv!=nil {for _,r:=range fn.Recv.List {n.Receiver=render(r.Type)}}
   n.ID=fmt.Sprintf("%s:%d:%s.%s",path,n.Start,n.Receiver,n.Name)
   ids:=map[string]bool{}; sels:=map[string]bool{}
   ast.Inspect(fn.Body,func(x ast.Node)bool{
    switch x:=x.(type) {
    case *ast.CallExpr:
     c:=Call{Name:render(x.Fun),Line:fs.Position(x.Pos()).Line,Args:[]string{}}
     for _,a:=range x.Args {c.Args=append(c.Args,render(a))}; n.Calls=append(n.Calls,c)
    case *ast.AssignStmt:
     a:=Assign{Line:fs.Position(x.Pos()).Line,Left:[]string{},Right:[]string{}}
     for _,v:=range x.Lhs {a.Left=append(a.Left,render(v))}; for _,v:=range x.Rhs {a.Right=append(a.Right,render(v))}; n.Writes=append(n.Writes,a)
    case *ast.SelectorExpr: sels[x.Sel.Name]=true
    case *ast.Ident: ids[x.Name]=true
    case *ast.GoStmt: n.AsyncLines=append(n.AsyncLines,fs.Position(x.Pos()).Line)
    }
    return true
   })
   for s:=range sels {n.Selectors=append(n.Selectors,s)}; sort.Strings(n.Selectors)
   for s:=range ids {n.Identifiers=append(n.Identifiers,s)}; sort.Strings(n.Identifiers)
   out.Nodes=append(out.Nodes,n)
  }
 }
 sort.Slice(out.Nodes,func(i,j int)bool{return strings.Compare(out.Nodes[i].ID,out.Nodes[j].ID)<0})
 if err:=json.NewEncoder(os.Stdout).Encode(out);err!=nil{panic(err)}
}
'''


def cmd(argv, cwd, **kw):
    return subprocess.run(argv, cwd=cwd, check=True, text=True, capture_output=True, **kw).stdout


def blob_sha(data):
    return hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()


def inspect_signals(node):
    ids = set(node['identifiers']) | set(node['selectors'])
    if 'FieldsV1' in node['receiver']:
        ids.add('FieldsV1')
    calls = node['calls']
    names = {c['name'].rsplit('.', 1)[-1] for c in calls} | {node['name']}
    sinks = {'SetRawBytes', 'SetRawString', 'UnmarshalJSON', 'UnmarshalCBOR', 'Unmarshal'}
    decoding = {'Decode', 'Unmarshal', 'UnmarshalJSON', 'UnmarshalCBOR', 'FromUnstructured', 'FromUnstructuredWithValidation'}
    s = {}
    if ids & {'FieldsV1', 'ManagedFields', 'GetManagedFields', 'SetManagedFields'}:
        s['managed_fields_reference'] = sorted(ids & {'FieldsV1', 'ManagedFields', 'GetManagedFields', 'SetManagedFields'})
    if names & sinks:
        s['possible_decoder_or_setter'] = [c for c in calls if c['name'].rsplit('.',1)[-1] in sinks]
    if names & decoding:
        s['decode_destination_to_trace'] = [c for c in calls if c['name'].rsplit('.',1)[-1] in decoding]
    replacement = {'Convert', 'ConvertToVersion', 'UnsafeConvertToVersion', 'UpdateObject', 'SetUnstructuredContent'}
    if names & replacement:
        s['conversion_or_object_replacement'] = [c for c in calls if c['name'].rsplit('.',1)[-1] in replacement]
    writes = [a for a in node['writes'] if any(t in lhs for lhs in a['left'] for t in ('FieldsV1', 'ManagedFields', 'ObjectMeta', '.Raw', '*'))]
    if writes:
        s['possible_alias_or_metadata_write'] = writes
    allocations = {'DeepCopy', 'DeepCopyInto', 'DeepCopyObject', 'New', 'MakeSlice', 'MakeMap'}
    if names & allocations:
        s['possible_allocation_boundary_not_proven'] = [c for c in calls if c['name'].rsplit('.',1)[-1] in allocations]
    if ids & {'reflect', 'unsafe'}:
        s['reflection_or_unsafe'] = sorted(ids & {'reflect','unsafe'})
    if node['async_lines']:
        s['goroutine_requires_object_identity_trace'] = node['async_lines']
    if node['name'] in {'Admit', 'Dispatch', 'dispatchOne', 'dispatchInvocations', 'applyAdmission', 'ValidateManagedFields'}:
        s['admission_or_validation_entry'] = node['name']
    return s


def priority(node, signals):
    weights = {'managed_fields_reference':130, 'possible_decoder_or_setter':65,
               'decode_destination_to_trace':40, 'possible_alias_or_metadata_write':25,
               'admission_or_validation_entry':100, 'conversion_or_object_replacement':35,
               'reflection_or_unsafe':15}
    p = sum(v for k,v in weights.items() if k in signals)
    if '/admission/' in node['path'] or node['path'].startswith('plugin/pkg/admission/'):
        p += 25
    if '/csaupgrade/' in node['path']:
        p -= 15
    return p


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--repo', type=Path, default=Path.cwd())
    ap.add_argument('--out', type=Path, default=Path('audit-output'))
    ap.add_argument('--limit', type=int, default=500)
    args = ap.parse_args()
    repo, out = args.repo.resolve(), args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    actual = cmd(['git', 'rev-parse', 'HEAD'], repo).strip()
    cmd(['git', 'cat-file', '-e', PIN+'^{commit}'], repo)
    tree = cmd(['git', 'ls-tree', '-r', PIN, '--', *ROOTS], repo)
    files, manifest = [], []
    for line in tree.splitlines():
        meta, path = line.split('\t',1)
        mode, typ, sha = meta.split()
        if typ!='blob' or not path.endswith('.go') or path.endswith('_test.go') or '/testdata/' in path:
            continue
        if Path(path).name.startswith('zz_generated.') or Path(path).name == 'generated.pb.go':
            continue
        data = (repo/path).read_bytes()
        observed = blob_sha(data)
        if observed != sha:
            raise RuntimeError(f'Source drift: {path}: {sha} != {observed}')
        files.append(path)
        manifest.append({'path':path,'git_blob_sha':sha,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'lines':len(data.splitlines())})
    (out/'source-manifest.json').write_text(json.dumps(manifest,indent=2))
    with tempfile.TemporaryDirectory(prefix='mf-parser-') as tmp:
        temp = Path(tmp)
        (temp/'main.go').write_text(GO_SOURCE)
        env = {**os.environ,'GOTOOLCHAIN':'local','GOWORK':'off','GO111MODULE':'off'}
        toolchain = cmd(['go','version'], temp, env=env).strip()
        parsed = json.loads(cmd(['go','run','main.go',str(repo)],temp,env=env,input=json.dumps(files)))
    (out/'ast-index.json').write_text(json.dumps(parsed,separators=(',',':')))
    if parsed['errors']:
        print(json.dumps({'parse_errors':parsed['errors']},indent=2))
        raise RuntimeError('Partial parse: refusing an apparently complete discovery count')
    nodes = {n['id']:n for n in parsed['nodes']}
    signals = {key:inspect_signals(n) for key,n in nodes.items()}
    by_symbol = collections.defaultdict(list)
    for key,n in nodes.items():
        if not n['receiver']:
            by_symbol[(n['package'],n['name'])].append(key)
    callees, callers, unresolved = collections.defaultdict(set), collections.defaultdict(set), collections.defaultdict(list)
    for key,n in nodes.items():
        for c in n['calls']:
            name = c['name']
            target = None
            if name.isidentifier():
                target = (n['package'],name)
            elif name.count('.') == 1:
                alias, symbol = name.split('.')
                pkg = n['imports'].get(alias)
                if pkg and pkg.startswith('k8s.io/kubernetes/'):
                    target = (pkg[len('k8s.io/kubernetes/'):],symbol)
                elif pkg and pkg.startswith('k8s.io/'):
                    target = ('staging/src/'+pkg,symbol)
            resolved = by_symbol.get(target, []) if target else []
            if resolved:
                for other in resolved:
                    callees[key].add(other)
                    callers[other].add(key)
            else:
                unresolved[key].append({'name':name,'line':c['line']})
    base = {key:priority(n,signals[key]) for key,n in nodes.items()}
    heap = [(-p,key,'initial AST signals') for key,p in base.items()]
    heapq.heapify(heap)
    seen, journal, checkpoints = set(), [], []
    hist = collections.Counter()
    while heap and len(journal) < args.limit:
        negp,key,why = heapq.heappop(heap)
        if key in seen:
            continue
        seen.add(key)
        n, s = nodes[key], signals[key]
        hit = bool(set(s)-{'possible_allocation_boundary_not_proven'})
        if 'managed_fields_reference' in s and ('possible_decoder_or_setter' in s or 'possible_alias_or_metadata_write' in s):
            verdict = 'candidate_identity_and_admission_reachability_unproven'
        elif hit:
            verdict = 'boundary_or_callsite_for_manual_trace'
        else:
            verdict = 'no_local_signal_transitive_effects_unchecked'
        new_frontier = []
        if hit:
            for other in sorted(callees[key] | callers[key]):
                if other not in seen:
                    boost = 70 if 'managed_fields_reference' in s else 25
                    heapq.heappush(heap,(-base[other]-boost,other,'local symbol edge from '+key))
                    new_frontier.append(other)
        rec = {
            'iteration':len(journal)+1,'kind':'automated_AST_discovery_not_manual_review','source_commit':PIN,
            'A':{'candidate':key,'hypothesis':'Could this function participate in a FieldsV1 payload change that preserves the admission snapshot comparison?','selection':why,'priority':-negp},
            'B':{'path':n['path'],'start':n['start'],'end':n['end'],'signals':s},
            'A_prime':{'new_frontier':new_frontier,'resolved_local_callees':sorted(callees[key]),'resolved_local_callers':sorted(callers[key])},
            'B_prime':{'verdict':verdict,'unresolved_calls':unresolved[key],'proof_gate':'Need same admitted object, execution during wrapped Admit, payload changed, comparison equal, and differential consequence versus parent. AST signals alone establish none of this.'}
        }
        journal.append(rec)
        hist[verdict]+=1
        if len(journal)%50==0:
            cp = {'completed':len(journal),'unique_functions':len(seen),'verdicts':dict(hist),'remaining_unique':len(nodes)-len(seen),'adaptive_frontier_entries':len(heap)}
            checkpoints.append(cp)
            print('CHECKPOINT '+json.dumps(cp),flush=True)
    if len(journal)!=args.limit:
        raise RuntimeError(f'Only {len(journal)} distinct functions available; will not pad to {args.limit}')
    with (out/'discovery.jsonl').open('w') as f:
        for rec in journal:
            f.write(json.dumps(rec,separators=(',',':'))+'\n')
    (out/'checkpoints.json').write_text(json.dumps(checkpoints,indent=2))
    graph = {'resolved_local_edges':[[a,b] for a in sorted(callees) for b in sorted(callees[a])], 'warning':'Method and interface dispatch unresolved; not a call graph proof.'}
    (out/'local-symbol-graph.json').write_text(json.dumps(graph,indent=2))
    summary = {'pin':PIN,'checkout_head':actual,'source_files':len(files),'source_functions':len(nodes),'completed_discovery_checks':len(journal),'deep_manual_reviews':0,'verdicts':dict(hist),'go_version':toolchain,'elapsed_seconds':round(time.time()-started,3),'sources_match_pinned_git_tree':True,'tests_run':False,'parse_errors':parsed['errors'],'limitations':['No type checking or alias analysis','No interface-dispatch resolution','No build-tag filtering: alternatives recorded separately','No runtime/admission reproduction','Not an exhaustive safety proof','No test/generated source functions in this corpus']}
    (out/'summary.json').write_text(json.dumps(summary,indent=2))
    with tarfile.open(out/'source-corpus.tar.gz','w:gz') as tf:
        for path in files+['LICENSE','.go-version','go.mod','go.work']:
            tf.add(repo/path,arcname=path)
    print('SUMMARY '+json.dumps(summary),flush=True)
    interesting = [r for r in journal if r['B_prime']['verdict'].startswith('candidate_')]
    for r in interesting[:50]:
        print('CANDIDATE '+json.dumps(r),flush=True)
    text = '# Pinned managedFields discovery audit\n\n'+json.dumps(summary,indent=2)+'\n\nAutomated discovery evidence, not bug findings.\n'
    (out/'README.md').write_text(text)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as f:
            f.write(text)

if __name__=='__main__':
    main()
