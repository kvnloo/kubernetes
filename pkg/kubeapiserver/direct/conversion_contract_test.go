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
	"encoding/json"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	core "k8s.io/kubernetes/pkg/apis/core"
)

// This is a characterization experiment for the pinned implementation, not a
// declaration that retained item GVK is the intended contract. A populated item
// is a benign synthetic boundary input; it does not prove live cache provenance.
func TestExperimentConversionGVKIsolation(t *testing.T) {
	for _, tc := range []struct {
		name string
		gvk  schema.GroupVersionKind
	}{
		{name: "empty-item-gvk"},
		{name: "populated-item-gvk", gvk: corev1.SchemeGroupVersion.WithKind("Pod")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("get", func(t *testing.T) {
				source := experimentContractPod()
				source.SetGroupVersionKind(tc.gvk)
				defer experimentImmutableSnapshot(t, "Get source", source)()
				before := source.DeepCopy()
				client := mutationTestClient(&mutationStorage{getObj: source})
				read := func() runtime.Object {
					obj, err := client.Get(context.Background(), "ns", "pod", metav1.GetOptions{ResourceVersion: "0"})
					if err != nil {
						t.Fatalf("Get: %v", err)
					}
					pod, ok := obj.(*corev1.Pod)
					if !ok {
						t.Fatalf("Get type = %T", obj)
					}
					if got := pod.GroupVersionKind(); got != (schema.GroupVersionKind{}) {
						t.Fatalf("returned Pod GVK = %v, want empty", got)
					}
					return obj
				}
				first, second := read(), read()
				defer experimentImmutableSnapshot(t, "Get second result", second)()
				experimentAssertEqual(t, "source after conversion", source, before)
				secondBefore := second.DeepCopyObject()
				experimentMutateContractPod(first.(*corev1.Pod))
				experimentAssertEqual(t, "source after returned-object mutation", source, before)
				experimentAssertEqual(t, "independent second result", second, secondBefore)
				experimentAssertEqual(t, "subsequent read", read(), secondBefore)
				t.Logf("get source GVK=%v; output GVK empty; source and independent reads unchanged", tc.gvk)
			})
			t.Run("list", func(t *testing.T) {
				pod := experimentContractPod()
				pod.SetGroupVersionKind(tc.gvk)
				sibling := experimentContractPod()
				sibling.Name = "sibling"
				sibling.SetGroupVersionKind(schema.GroupVersionKind{})
				source := &core.PodList{
					TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
					Items:    []core.Pod{*pod, *sibling},
				}
				defer experimentImmutableSnapshot(t, "List source", source)()
				before := source.DeepCopy()
				client := mutationTestClient(&mutationStorage{listObj: source})
				read := func() *corev1.PodList {
					obj, err := client.List(context.Background(), "ns", metav1.ListOptions{ResourceVersion: "0"})
					if err != nil {
						t.Fatalf("List: %v", err)
					}
					list, ok := obj.(*corev1.PodList)
					if !ok || len(list.Items) != 2 {
						t.Fatalf("List type/size = %T, want two-item *v1.PodList", obj)
					}
					if got := list.GroupVersionKind(); got != (schema.GroupVersionKind{}) {
						t.Fatalf("outer GVK = %v, want empty", got)
					}
					if got := list.Items[0].GroupVersionKind(); got != tc.gvk {
						t.Fatalf("first item GVK = %v, want input GVK %v", got, tc.gvk)
					}
					if got := list.Items[1].GroupVersionKind(); got != (schema.GroupVersionKind{}) {
						t.Fatalf("sibling GVK = %v, want empty", got)
					}
					return list
				}
				first, second := read(), read()
				defer experimentImmutableSnapshot(t, "List second result", second)()
				experimentAssertEqual(t, "source after list conversion", source, before)
				secondBefore := second.DeepCopy()
				siblingBefore := first.Items[1].DeepCopy()
				first.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ChangedList"})
				experimentMutateContractPod(&first.Items[0])
				experimentAssertEqual(t, "sibling result", &first.Items[1], siblingBefore)
				experimentAssertEqual(t, "source after list mutation", source, before)
				experimentAssertEqual(t, "independent second list", second, secondBefore)
				experimentAssertEqual(t, "subsequent list", read(), secondBefore)
				t.Logf("list outer GVK empty; first item GVK retained as %v; source and independent reads unchanged", tc.gvk)
			})
		})
	}
}

func TestExperimentGenericListIsolation(t *testing.T) {
	source := &core.PodList{Items: []core.Pod{*experimentContractPod()}}
	defer experimentImmutableSnapshot(t, "generic source", source)()
	before := source.DeepCopy()
	lister := NewLister[runtime.Object](mutationTestClient(&mutationStorage{listObj: source})).Namespaced("ns")
	read := func() *corev1.Pod {
		items, err := lister.List(labels.Everything())
		if err != nil || len(items) != 1 {
			t.Fatalf("generic List length=%d, err=%v", len(items), err)
		}
		pod, ok := items[0].(*corev1.Pod)
		if !ok {
			t.Fatalf("generic item type=%T", items[0])
		}
		return pod
	}
	first, second := read(), read()
	defer experimentImmutableSnapshot(t, "generic second result", second)()
	secondBefore := second.DeepCopy()
	experimentAssertEqual(t, "generic source after reads", source, before)
	experimentMutateContractPod(first)
	experimentAssertEqual(t, "generic source after mutation", source, before)
	experimentAssertEqual(t, "generic second read", second, secondBefore)
	experimentAssertEqual(t, "generic subsequent read", read(), secondBefore)
}

func TestExperimentManagedFieldsTransformBoundary(t *testing.T) {
	source := experimentContractPod()
	source.SetGroupVersionKind(schema.GroupVersionKind{})
	defer experimentImmutableSnapshot(t, "managedFields source", source)()
	before := source.DeepCopy()
	// Construct the external fixture independently of storageClient and Scheme
	// conversion. Only fields present in mutationTestPod and the added metadata
	// are populated. No defaulting or serialization equivalence is claimed.
	external := &corev1.Pod{
		ObjectMeta: *source.ObjectMeta.DeepCopy(),
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"zone": "original"},
			Containers: []corev1.Container{{
				Name: "container",
				Env:  []corev1.EnvVar{{Name: "MODE", Value: "original"}},
			}},
		},
	}
	// Two actual informer pipelines make the no-transform arm a positive control:
	// otherwise a fake ListWatch that already dropped the field could pass falsely.
	plain := experimentInformerPod(t, external, false)
	trimmed := experimentInformerPod(t, external, true)
	if len(plain.ManagedFields) != 1 || trimmed.ManagedFields != nil {
		t.Fatalf("informer control: plain managedFields=%d, trimmed=%v", len(plain.ManagedFields), trimmed.ManagedFields)
	}
	experimentAssertEqual(t, "untransformed informer fixture", plain, external)
	client := mutationTestClient(&mutationStorage{
		getObj:  source,
		listObj: &core.PodList{Items: []core.Pod{*source}},
	})
	lister := NewPodLister(client)
	get, err := lister.Pods("ns").Get("pod")
	if err != nil {
		t.Fatal(err)
	}
	list, err := lister.Pods("ns").List(labels.Everything())
	if err != nil || len(list) != 1 {
		t.Fatalf("direct List length=%d, err=%v", len(list), err)
	}
	for _, arm := range []struct {
		name string
		pod  *corev1.Pod
	}{{name: "get", pod: get}, {name: "list", pod: list[0]}} {
		if !reflect.DeepEqual(arm.pod.ManagedFields, external.ManagedFields) {
			t.Errorf("%s did not retain source managedFields", arm.name)
		}
		experimentAssertEqual(t, arm.name+" versus untransformed informer", arm.pod, plain)
		// The ONLY comparison mask is metadata.managedFields. TypeMeta, Spec,
		// Status, nil/empty distinctions and all other metadata compare exactly.
		masked := arm.pod.DeepCopy()
		masked.ManagedFields = nil
		experimentAssertEqual(t, arm.name+" versus transformed informer after managedFields-only mask", masked, trimmed)
		t.Logf("%s managedFields entries: direct=%d, informer=%d; all unmasked fields equal", arm.name, len(arm.pod.ManagedFields), len(trimmed.ManagedFields))
	}
	experimentAssertEqual(t, "storage source after transform comparisons", source, before)
}

func experimentContractPod() *core.Pod {
	pod := mutationTestPod()
	pod.ManagedFields = []metav1.ManagedFieldsEntry{{
		Manager:    "experiment",
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: "v1",
		FieldsType: "FieldsV1",
		FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:labels":{"f:label":{}}}}`)},
	}}
	controller := true
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "owner", Controller: &controller}}
	return pod
}

func experimentMutateContractPod(pod *corev1.Pod) {
	mutateReturnedPod(pod)
	pod.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ChangedPod"})
	pod.ManagedFields[0].Manager = "changed"
	pod.ManagedFields[0].FieldsV1.Raw[0] = '['
	*pod.OwnerReferences[0].Controller = false
}

func experimentAssertEqual(t *testing.T, label string, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s differs: got %#v, want %#v", label, got, want)
	}
}

// JSON provides an immutable snapshot independent of generated DeepCopy, which
// is also used by the implementation under test. The reflect.DeepEqual checks
// above complement it by preserving distinctions JSON omitempty may erase.
func experimentImmutableSnapshot(t *testing.T, label string, obj interface{}) func() {
	t.Helper()
	before, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("%s initial immutable snapshot: %v", label, err)
	}
	return func() {
		t.Helper()
		after, err := json.Marshal(obj)
		if err != nil {
			t.Errorf("%s final immutable snapshot: %v", label, err)
			return
		}
		if string(after) != string(before) {
			t.Errorf("%s changed relative to independent JSON snapshot", label)
		}
	}
}

func experimentInformerPod(t *testing.T, pod *corev1.Pod, trim bool) *corev1.Pod {
	t.Helper()
	client := fake.NewSimpleClientset(pod.DeepCopy())
	var opts []informers.SharedInformerOption
	if trim {
		// Mirrors BuildGenericConfig's WithTransform(trim) at upstream 5b10e94.
		opts = append(opts, informers.WithTransform(func(obj interface{}) (interface{}, error) {
			if accessor, err := meta.Accessor(obj); err == nil && accessor.GetManagedFields() != nil {
				accessor.SetManagedFields(nil)
			}
			return obj, nil
		}))
	}
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0, opts...)
	pi := factory.Core().V1().Pods()
	inf := pi.Informer()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		t.Fatal("informer did not sync before timeout")
	}
	got, err := pi.Lister().Pods("ns").Get("pod")
	if err != nil {
		t.Fatalf("informer Get: %v", err)
	}
	return got.DeepCopy()
}
