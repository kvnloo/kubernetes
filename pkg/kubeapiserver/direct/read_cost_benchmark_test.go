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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	core "k8s.io/kubernetes/pkg/apis/core"
)

var (
	readCostPodSink  *corev1.Pod
	readCostPodsSink []*corev1.Pod
)

// TestDirectListerReadCostFixtureParity is a separate correctness gate for the
// benchmark. This isolates the managedFields transform from GVK differences by
// starting with empty internal TypeMeta. The cached fixture explicitly applies
// the managedFields trim used by the ordinary control-plane informer factory.
// It does not exercise informer population, decoding, or the watch cache.
func TestDirectListerReadCostFixtureParity(t *testing.T) {
	for _, count := range []int{1, 100} {
		for _, targetBytes := range []int{0, 4096, 32768} {
			t.Run(fmt.Sprintf("n=%d/managed_fields_target_bytes=%d", count, targetBytes), func(t *testing.T) {
				fixture := newReadCostFixture(t, count, targetBytes)
				fixture.verifyParity(t, true)
			})
		}
	}
}

// BenchmarkDirectListerReadCost compares warmed, read-only client-go lister
// access with the direct lister plus a synthetic REST-storage stub. The stub
// returns prebuilt internal objects: no etcd, watch-cache lookup, filtering,
// synchronization, serialization, or informer population cost is measured.
// Treat direct_synthetic as an optimistic synthetic model, not a system-level
// performance comparison. Its constant-time observation counters are included.
// managed_fields_bytes reports actual FieldsV1.Raw bytes per source Pod.
//
// Alternate K8S_READ_COST_ORDER=AB and BA across external process repetitions.
// This reverses the cached/direct order without changing benchmark names.
func BenchmarkDirectListerReadCost(b *testing.B) {
	order := os.Getenv("K8S_READ_COST_ORDER")
	if order == "" {
		order = "AB"
	}
	if order != "AB" && order != "BA" {
		b.Fatalf("K8S_READ_COST_ORDER must be AB or BA, got %q", order)
	}
	for _, operation := range []string{"get", "list"} {
		for _, count := range []int{1, 100} {
			for _, targetBytes := range []int{0, 4096, 32768} {
				fixture := newReadCostFixture(b, count, targetBytes)
				fixture.verifyParity(b, false)
				paths := []struct {
					name   string
					lister corev1listers.PodNamespaceLister
				}{
					{"cached", fixture.cached},
					{"direct_synthetic", fixture.direct},
				}
				if order == "BA" {
					paths[0], paths[1] = paths[1], paths[0]
				}
				for _, path := range paths {
					name := fmt.Sprintf("op=%s/n=%d/managed_fields_bytes=%d/path=%s", operation, count, fixture.managedFieldsBytes, path.name)
					b.Run(name, func(b *testing.B) {
						getBefore, listBefore := fixture.storage.getCalls, fixture.storage.listCalls
						selector := labels.Everything()
						b.ReportAllocs()
						b.ResetTimer()
						if operation == "get" {
							for i := 0; i < b.N; i++ {
								pod, err := path.lister.Get("pod-0000")
								if err != nil {
									b.Fatal(err)
								}
								readCostPodSink = pod
							}
						} else {
							for i := 0; i < b.N; i++ {
								pods, err := path.lister.List(selector)
								if err != nil {
									b.Fatal(err)
								}
								readCostPodsSink = pods
							}
						}
						b.StopTimer()
						wantGets, wantLists := 0, 0
						if path.name == "direct_synthetic" {
							if operation == "get" {
								wantGets = b.N
							} else {
								wantLists = b.N
							}
						}
						if got := fixture.storage.getCalls - getBefore; got != wantGets {
							b.Fatalf("storage Get calls = %d, want %d", got, wantGets)
						}
						if got := fixture.storage.listCalls - listBefore; got != wantLists {
							b.Fatalf("storage List calls = %d, want %d", got, wantLists)
						}
					})
				}
			}
		}
	}
}

type readCostFixture struct {
	cached                corev1listers.PodNamespaceLister
	direct                corev1listers.PodNamespaceLister
	storage               *readCostStorage
	count                 int
	managedFieldsBytes    int
	expectedManagedFields []metav1.ManagedFieldsEntry
}

func newReadCostFixture(t testing.TB, count, targetBytes int) readCostFixture {
	t.Helper()
	raw := readCostManagedFields(targetBytes)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	list := &core.PodList{Items: make([]core.Pod, count)}
	for i := range list.Items {
		pod := mutationTestPod()
		pod.TypeMeta = metav1.TypeMeta{}
		pod.Name = fmt.Sprintf("pod-%04d", i)
		if len(raw) != 0 {
			pod.ManagedFields = []metav1.ManagedFieldsEntry{{
				Manager:    "synthetic-read-cost-fixture",
				Operation:  metav1.ManagedFieldsOperationApply,
				APIVersion: "v1",
				FieldsType: "FieldsV1",
				FieldsV1:   &metav1.FieldsV1{Raw: append([]byte(nil), raw...)},
			}}
		}
		list.Items[i] = *pod
		external, err := legacyscheme.Scheme.ConvertToVersion(pod, corev1.SchemeGroupVersion)
		if err != nil {
			t.Fatal(err)
		}
		cached := external.(*corev1.Pod)
		cached.TypeMeta = metav1.TypeMeta{}
		// Reproduce the existing factory's managedFields trim, before insertion.
		cached.SetManagedFields(nil)
		if err := indexer.Add(cached); err != nil {
			t.Fatal(err)
		}
	}
	storage := &readCostStorage{getObject: &list.Items[0], listObject: list}
	return readCostFixture{
		cached:                corev1listers.NewPodLister(indexer).Pods("ns"),
		direct:                NewPodLister(mutationTestClient(storage)).Pods("ns"),
		storage:               storage,
		count:                 count,
		managedFieldsBytes:    len(raw),
		expectedManagedFields: list.Items[0].DeepCopy().ManagedFields,
	}
}

func (f readCostFixture) verifyParity(t testing.TB, log bool) {
	t.Helper()
	cachedPod, err := f.cached.Get("pod-0000")
	if err != nil {
		t.Fatal(err)
	}
	directPod, err := f.direct.Get("pod-0000")
	if err != nil {
		t.Fatal(err)
	}
	f.verifyPair(t, "get", cachedPod, directPod)
	if f.storage.getCalls != 1 || f.storage.lastGetName != "pod-0000" || f.storage.lastGetRV != "0" {
		t.Fatalf("unexpected storage Get observation: calls=%d name=%q rv=%q", f.storage.getCalls, f.storage.lastGetName, f.storage.lastGetRV)
	}
	cachedPods, err := f.cached.List(labels.Everything())
	if err != nil {
		t.Fatal(err)
	}
	directPods, err := f.direct.List(labels.Everything())
	if err != nil {
		t.Fatal(err)
	}
	if len(cachedPods) != f.count || len(directPods) != f.count {
		t.Fatalf("list counts: cached=%d direct=%d want=%d", len(cachedPods), len(directPods), f.count)
	}
	byName := make(map[string]*corev1.Pod, len(cachedPods))
	for _, pod := range cachedPods {
		byName[pod.Name] = pod
	}
	for _, pod := range directPods {
		cached := byName[pod.Name]
		if cached == nil {
			t.Fatalf("direct list returned unexpected or duplicate Pod %q", pod.Name)
		}
		f.verifyPair(t, "list", cached, pod)
		delete(byName, pod.Name)
	}
	if f.storage.listCalls != 1 || f.storage.lastListRV != "0" {
		t.Fatalf("unexpected storage List observation: calls=%d rv=%q", f.storage.listCalls, f.storage.lastListRV)
	}
	if log {
		for _, operation := range []string{"get", "list"} {
			returned := 1
			if operation == "list" {
				returned = f.count
			}
			t.Logf("READ_COST_PARITY op=%s n=%d managed_fields_bytes=%d raw_equal=%t normalized_equal=true direct_managed_fields_bytes=%d cached_managed_fields_bytes=0", operation, f.count, f.managedFieldsBytes, f.managedFieldsBytes == 0, returned*f.managedFieldsBytes)
		}
	}
}

func (f readCostFixture) verifyPair(t testing.TB, operation string, cached, direct *corev1.Pod) {
	t.Helper()
	if got := equality.Semantic.DeepEqual(cached, direct); got != (f.managedFieldsBytes == 0) {
		t.Fatalf("%s %s raw equality=%t; want equality only with no managedFields", operation, direct.Name, got)
	}
	if len(cached.ManagedFields) != 0 {
		t.Fatalf("cached %s unexpectedly retains managedFields", cached.Name)
	}
	if f.managedFieldsBytes != 0 && (len(direct.ManagedFields) != 1 || direct.ManagedFields[0].FieldsV1 == nil || len(direct.ManagedFields[0].FieldsV1.Raw) != f.managedFieldsBytes) {
		t.Fatalf("direct %s did not retain the expected managedFields payload", direct.Name)
	}
	if !equality.Semantic.DeepEqual(direct.ManagedFields, f.expectedManagedFields) {
		t.Fatalf("direct %s managedFields contents differ from the fixture", direct.Name)
	}
	normalized := direct.DeepCopy()
	normalized.SetManagedFields(nil)
	if !equality.Semantic.DeepEqual(cached, normalized) {
		t.Fatalf("%s %s differs after normalizing managedFields", operation, direct.Name)
	}
}

// Generate valid FieldsV1 JSON close to the requested size. Ownership keys are
// synthetic: this fixture measures opaque byte copying, not SSA or realistic
// ownership distributions. Pod metadata remains small and identical otherwise.
func readCostManagedFields(targetBytes int) []byte {
	if targetBytes == 0 {
		return nil
	}
	var out strings.Builder
	out.WriteString(`{"f:metadata":{"f:annotations":{`)
	for i := 0; ; i++ {
		prefix := ""
		if i != 0 {
			prefix = ","
		}
		entry := fmt.Sprintf(`%s"f:synthetic-%04d":{}`, prefix, i)
		if out.Len()+len(entry)+3 > targetBytes {
			break
		}
		out.WriteString(entry)
	}
	out.WriteString("}}}")
	return []byte(out.String())
}

type readCostStorage struct {
	getObject   runtime.Object
	listObject  runtime.Object
	getCalls    int
	listCalls   int
	lastGetName string
	lastGetRV   string
	lastListRV  string
}

func (s *readCostStorage) Get(_ context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	s.getCalls++
	s.lastGetName = name
	s.lastGetRV = options.ResourceVersion
	return s.getObject, nil
}

func (s *readCostStorage) List(_ context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	s.listCalls++
	s.lastListRV = options.ResourceVersion
	return s.listObject, nil
}
