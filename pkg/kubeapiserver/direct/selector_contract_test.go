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
	"reflect"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	core "k8s.io/kubernetes/pkg/apis/core"
)

// Nil selectors are excluded: the ordinary cache.ListAll implementation calls
// selector.Empty(), so nil is not a valid shared contract. Selection compares
// sorted namespace/name sets only; order and pointer identity are not promised.
func TestExperimentSelectorParity(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	var sources []*core.Pod
	for _, fixture := range []struct {
		ns, name string
		labels   map[string]string
	}{
		{ns: "ns", name: "web", labels: map[string]string{"app": "web", "tier": "frontend"}},
		{ns: "ns", name: "db", labels: map[string]string{"app": "db", "tier": "backend"}},
		{ns: "ns", name: "empty-value", labels: map[string]string{"app": ""}},
		{ns: "ns", name: "missing-app", labels: map[string]string{"tier": "frontend"}},
		{ns: "other", name: "web", labels: map[string]string{"app": "web"}},
		{ns: "other", name: "unlabelled"},
	} {
		metadata := metav1.ObjectMeta{Namespace: fixture.ns, Name: fixture.name, Labels: fixture.labels}
		if err := indexer.Add(&corev1.Pod{ObjectMeta: *metadata.DeepCopy()}); err != nil {
			t.Fatal(err)
		}
		sources = append(sources, &core.Pod{ObjectMeta: *metadata.DeepCopy()})
	}
	typedOracle := corev1listers.NewPodLister(indexer)
	genericOracle := cache.NewGenericLister(indexer, corev1.Resource("pods"))
	selectors := []struct {
		name     string
		selector labels.Selector
	}{
		{name: "everything", selector: labels.Everything()},
		{name: "nothing", selector: labels.Nothing()},
	}
	for _, expression := range []string{
		"app=web", "app!=web", "app in (web,db)", "app notin (web,db)",
		"app", "!app", "app=", "app=web,app=db", "app=web,tier=frontend",
	} {
		selector, err := labels.Parse(expression)
		if err != nil {
			t.Fatalf("fixture selector %q: %v", expression, err)
		}
		selectors = append(selectors, struct {
			name     string
			selector labels.Selector
		}{name: expression, selector: selector})
	}
	for _, namespace := range []string{"", "ns", "other", "absent"} {
		for _, selector := range selectors {
			for _, route := range []string{"typed", "generic"} {
				t.Run(fmt.Sprintf("%s/namespace=%s/%s", route, namespace, selector.name), func(t *testing.T) {
					storage := &experimentSelectorStorage{pods: sources}
					client := mutationTestClient(storage)
					var got, want []runtime.Object
					if route == "typed" {
						direct := NewPodLister(client)
						readDirect := direct.List
						readOracle := typedOracle.List
						if namespace != "" {
							readDirect = direct.Pods(namespace).List
							readOracle = typedOracle.Pods(namespace).List
						}
						actual, err := readDirect(selector.selector)
						if err != nil {
							t.Fatal(err)
						}
						expected, err := readOracle(selector.selector)
						if err != nil {
							t.Fatal(err)
						}
						for _, pod := range actual {
							got = append(got, pod)
						}
						for _, pod := range expected {
							want = append(want, pod)
						}
					} else {
						direct := NewLister[runtime.Object](client)
						readDirect := direct.List
						readOracle := genericOracle.List
						if namespace != "" {
							readDirect = direct.ByNamespace(namespace).List
							readOracle = genericOracle.ByNamespace(namespace).List
						}
						var err error
						got, err = readDirect(selector.selector)
						if err != nil {
							t.Fatal(err)
						}
						want, err = readOracle(selector.selector)
						if err != nil {
							t.Fatal(err)
						}
					}
					gotKeys, wantKeys := experimentSelectorKeys(t, got), experimentSelectorKeys(t, want)
					if !reflect.DeepEqual(gotKeys, wantKeys) {
						t.Fatalf("direct keys=%v, client-go keys=%v", gotKeys, wantKeys)
					}
					wantCalls := 1
					if selector.name == "nothing" {
						wantCalls = 0
					}
					if storage.calls != wantCalls {
						t.Fatalf("backend calls=%d, want %d", storage.calls, wantCalls)
					}
				})
			}
		}
	}
	t.Run("calibration-empty-string-loses-nothing", func(t *testing.T) {
		// This deliberately lossy selector conversion is contained in the test.
		// It confirms the same key-set oracle detects the expected false positive.
		lossy, err := labels.Parse(labels.Nothing().String())
		if err != nil {
			t.Fatal(err)
		}
		want, err := genericOracle.List(labels.Nothing())
		if err != nil {
			t.Fatal(err)
		}
		wrong, err := genericOracle.List(lossy)
		if err != nil {
			t.Fatal(err)
		}
		if len(want) != 0 || len(wrong) == 0 || reflect.DeepEqual(experimentSelectorKeys(t, wrong), experimentSelectorKeys(t, want)) {
			t.Fatalf("calibration did not expose lossy Nothing serialization: wrong=%d expected=%d", len(wrong), len(want))
		}
		t.Logf("calibration detected %d false-positive items from Nothing -> string -> Parse", len(wrong))
	})
}

func experimentSelectorKeys(t *testing.T, objects []runtime.Object) []string {
	t.Helper()
	keys := make([]string, 0, len(objects))
	for _, obj := range objects {
		accessor, err := meta.Accessor(obj)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, accessor.GetNamespace()+"/"+accessor.GetName())
	}
	sort.Strings(keys)
	return keys
}

type experimentSelectorStorage struct {
	pods  []*core.Pod
	calls int
}

func (s *experimentSelectorStorage) Get(context.Context, string, *metav1.GetOptions) (runtime.Object, error) {
	return nil, fmt.Errorf("unexpected Get in selector experiment")
}

func (s *experimentSelectorStorage) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	s.calls++
	if options.ResourceVersion != "0" {
		return nil, fmt.Errorf("resourceVersion=%q, want 0", options.ResourceVersion)
	}
	namespace := genericapirequest.NamespaceValue(ctx)
	result := &core.PodList{}
	for _, pod := range s.pods {
		if namespace != "" && pod.Namespace != namespace {
			continue
		}
		if options.LabelSelector != nil && !options.LabelSelector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		result.Items = append(result.Items, *pod)
	}
	return result, nil
}
