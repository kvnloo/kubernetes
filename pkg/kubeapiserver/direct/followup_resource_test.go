/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package direct

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type followPhase struct {
	Name              string `json:"phase"`
	Updates           int    `json:"updates"`
	Reads             int    `json:"reads"`
	ElapsedNS         int64  `json:"elapsed_ns"`
	AllocBytes        uint64 `json:"alloc_bytes"`
	AllocObjects      uint64 `json:"alloc_objects"`
	CPUUS             int64  `json:"cpu_us"`
	GCPauseNS         uint64 `json:"gc_pause_ns"`
	GCCycles          uint32 `json:"gc_cycles"`
	RetainedHeapBytes int64  `json:"retained_heap_delta"`
	HTTPLIsts         int64  `json:"http_lists"`
	HTTPWatches       int64  `json:"http_watches"`
	HTTPUpdates       int64  `json:"http_updates"`
	HTTPBytes         int64  `json:"http_bytes"`
	DirectGets        int64  `json:"storage_gets"`
	EventUpdates      int64  `json:"event_updates"`
	PropagationNS     int64  `json:"propagation_ns"`
}

func followCPU() int64 {
	var r syscall.Rusage
	if e := syscall.Getrusage(syscall.RUSAGE_SELF, &r); e != nil {
		panic(e)
	}
	return r.Utime.Sec*1000000 + r.Utime.Usec + r.Stime.Sec*1000000 + r.Stime.Usec
}

func followSetupResource(t *testing.T, directMode, event bool) (*followFixture, []*corev1.Pod, *atomic.Int64) {
	f := newFollowFixture(t, directMode, true, false)
	pods := make([]*corev1.Pod, 10)
	for i := range pods {
		pods[i] = f.create(fmt.Sprintf("pod-%04d", i), 0)
		f.barrier(pods[i].Name, pods[i].ResourceVersion)
	}
	events := &atomic.Int64{}
	if !directMode || event {
		f.informer = f.factory.Core().V1().Pods().Informer()
		if event {
			_, e := f.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{UpdateFunc: func(_, next any) { events.Add(1) }})
			if e != nil {
				t.Fatal(e)
			}
		}
		f.startInformer()
		for _, p := range pods {
			f.barrier(p.Name, p.ResourceVersion)
		}
	}
	return f, pods, events
}

func followResourceParity(t *testing.T, f *followFixture, pods []*corev1.Pod) {
	for _, p := range pods {
		got, e := f.lister.Pods("ns").Get(p.Name)
		if e != nil {
			t.Fatal(e)
		}
		followAssertSame(t, p, got, true)
		if f.informer != nil {
			cached, e := corelisters.NewPodLister(f.informer.GetIndexer()).Pods("ns").Get(p.Name)
			if e != nil {
				t.Fatal(e)
			}
			followAssertSame(t, p, cached, true)
		}
	}
}

func followRunPhase(t *testing.T, f *followFixture, pods []*corev1.Pod, events *atomic.Int64, name string, updates, reads int) followPhase {
	t.Helper()
	followResourceParity(t, f, pods)
	// The post-GC retained heap is reported separately from measured allocation/CPU.
	runtime.GC()
	var before, after, retained runtime.MemStats
	runtime.ReadMemStats(&before)
	lb, wb, ub, bb, gb, eb := f.httpLists.Load(), f.httpWatches.Load(), f.httpUpdates.Load(), f.httpBytes.Load(), f.front.gets.Load(), events.Load()
	cpu := followCPU()
	start := time.Now()
	propagation := int64(0)
	for i := 0; i < updates; i++ {
		p := pods[0]
		seq := int(f.httpUpdates.Load()) + 1
		p = f.update(p, seq)
		pods[0] = p
		catchup := time.Now()
		f.barrier(p.Name, p.ResourceVersion)
		if f.informer != nil && events != nil && eb+int64(i+1) > 0 && os.Getenv("K8S_FOLLOW_EVENT") == "true" {
			followPoll(t, func() (bool, error) { return events.Load() >= eb+int64(i+1), nil })
		}
		propagation += time.Since(catchup).Nanoseconds()
	}
	for i := 0; i < reads; i++ {
		expected := pods[i%len(pods)]
		p, e := f.lister.Pods("ns").Get(expected.Name)
		if e != nil {
			t.Fatal(e)
		}
		if p.UID != expected.UID || p.ResourceVersion != expected.ResourceVersion || p.Annotations["sequence"] != expected.Annotations["sequence"] {
			t.Fatal("measured read identity/version/sequence differs")
		}
		// Only the direct owned return is trimmed; cached values are already trimmed.
		if _, ok := f.factory.(*directSharedInformerFactory); ok {
			p.ManagedFields = nil
		}
		readCostPodSink = p
	}
	elapsed := time.Since(start).Nanoseconds()
	cpu = followCPU() - cpu
	runtime.ReadMemStats(&after)
	result := followPhase{Name: name, Updates: updates, Reads: reads, ElapsedNS: elapsed, AllocBytes: after.TotalAlloc - before.TotalAlloc, AllocObjects: after.Mallocs - before.Mallocs, CPUUS: cpu, GCPauseNS: after.PauseTotalNs - before.PauseTotalNs, GCCycles: after.NumGC - before.NumGC, HTTPLIsts: f.httpLists.Load() - lb, HTTPWatches: f.httpWatches.Load() - wb, HTTPUpdates: f.httpUpdates.Load() - ub, HTTPBytes: f.httpBytes.Load() - bb, DirectGets: f.front.gets.Load() - gb, EventUpdates: events.Load() - eb, PropagationNS: propagation}
	runtime.GC()
	runtime.ReadMemStats(&retained)
	result.RetainedHeapBytes = int64(retained.HeapAlloc) - int64(before.HeapAlloc)
	followResourceParity(t, f, pods)
	if result.HTTPUpdates != int64(updates) {
		t.Fatal("measured HTTP update count differs")
	}
	return result
}

func TestFollowupN4ResourceTrial(t *testing.T) {
	strategy := os.Getenv("K8S_FOLLOW_STRATEGY")
	if strategy == "" {
		t.Skip("runner supplies one fresh-process factorial cell")
	}
	if strategy != "cached" && strategy != "direct" {
		t.Fatal("invalid strategy")
	}
	event := os.Getenv("K8S_FOLLOW_EVENT") == "true"
	workload := os.Getenv("K8S_FOLLOW_WORKLOAD")
	f, pods, events := followSetupResource(t, strategy == "direct", event)
	followRunPhase(t, f, pods, events, "warmup", 1, 20)
	updates, reads := 0, 0
	switch workload {
	case "read":
		reads = 150
	case "update":
		updates = 20
	case "mixed":
		updates = 20
		reads = 150
	default:
		t.Fatal("invalid workload")
	}
	result := followRunPhase(t, f, pods, events, workload, updates, reads)
	if strategy == "direct" && !event && (f.httpLists.Load() != 0 || f.httpWatches.Load() != 0) {
		t.Fatal("no-consumer direct arm started informer")
	}
	if strategy == "direct" && result.DirectGets != int64(reads) {
		t.Fatal("direct read route count differs")
	}
	followReceipt(t, "N4", map[string]any{"strategy": strategy, "event": event, "workload": workload, "block": os.Getenv("K8S_FOLLOW_BLOCK"), "process_scope": "embedded etcd + HTTP Pod handlers + real store/cache + client + informer; no full control-plane filter chain", "phase": result, "total_lists": f.httpLists.Load(), "total_watches": f.httpWatches.Load(), "population": 10, "output_parity": true})
}

func TestFollowupN5RecoveryTrial(t *testing.T) {
	strategy := os.Getenv("K8S_FOLLOW_STRATEGY")
	if strategy == "" {
		t.Skip("runner supplies one fresh-process recovery cell")
	}
	event := os.Getenv("K8S_FOLLOW_EVENT") == "true"
	mode := os.Getenv("K8S_FOLLOW_RECOVERY")
	if mode != "control" && mode != "restart" && mode != "expiry" {
		t.Fatal("invalid recovery mode")
	}
	f, pods, events := followSetupResource(t, strategy == "direct", event)
	followRunPhase(t, f, pods, events, "warmup", 1, 20)
	a1 := followRunPhase(t, f, pods, events, "A1", 0, 200)
	listBefore, watchBefore := f.httpLists.Load(), f.httpWatches.Load()
	begin := time.Now()
	perturbed := false
	if mode != "control" && f.informer != nil {
		followPoll(t, func() (bool, error) { f.traceMu.Lock(); defer f.traceMu.Unlock(); return f.watchCancel != nil, nil })
		f.traceMu.Lock()
		cancel := f.watchCancel
		f.traceMu.Unlock()
		if mode == "expiry" {
			f.expireWatch.Store(true)
		} else {
			f.closeWatch.Store(true)
		}
		cancel()
		perturbed = true
		followPoll(t, func() (bool, error) { return f.watchInterrupted.Load() == 1, nil })
		// Wait for a following live watch after the single injected response.
		followPoll(t, func() (bool, error) { return f.httpWatches.Load() >= watchBefore+2, nil })
	}
	recoveryNS := time.Since(begin).Nanoseconds()
	b := followRunPhase(t, f, pods, events, "B", 10, 200)
	a2 := followRunPhase(t, f, pods, events, "A2", 0, 200)
	f.traceMu.Lock()
	rvs := append([]string(nil), f.watchRVs...)
	f.traceMu.Unlock()
	if perturbed && f.watchInterrupted.Load() != 1 {
		t.Fatal("intended perturbation not observed")
	}
	if !perturbed && f.watchInterrupted.Load() != 0 {
		t.Fatal("unexpected watch interruption")
	}
	// Report recovery tolerance as an observation, not a statistical system claim.
	tolerance := a2.ElapsedNS <= a1.ElapsedNS*11/10
	followReceipt(t, "N5", map[string]any{"strategy": strategy, "event": event, "mode": mode, "block": os.Getenv("K8S_FOLLOW_BLOCK"), "perturbed": perturbed, "injected_responses": f.watchInterrupted.Load(), "watch_delta": f.httpWatches.Load() - watchBefore, "full_list_delta": f.httpLists.Load() - listBefore, "watch_request_rvs": rvs, "recovery_ns": recoveryNS, "A1": a1, "B": b, "A2": a2, "A2_within_10pct": tolerance, "output_parity": true})
}

func TestFollowupCalibration(t *testing.T) {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "control", UID: "control"}}
	q := p.DeepCopy()
	q.Spec.SchedulerName = "different"
	if followJSON(t, followClear(p, true)) == followJSON(t, followClear(q, true)) {
		t.Fatal("spec mismatch hidden by normalization")
	}
	f := newFollowFixture(t, true, true, false)
	original := f.create("calibration", 0)
	f.barrier(original.Name, original.ResourceVersion)
	before := f.front.gets.Load()
	p2, e := f.lister.Pods("ns").Get(original.Name)
	if e != nil {
		t.Fatal(e)
	}
	if f.front.gets.Load() != before+1 || p2.UID != original.UID {
		t.Fatal("actual route positive control failed")
	}
	followReceipt(t, "calibration", map[string]any{"known_mismatch_detected": true, "actual_route_count": 1, "identity_control": true, "watch_list_client": false})
	_ = context.Background()
}
