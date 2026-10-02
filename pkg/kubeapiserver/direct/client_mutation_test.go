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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	core "k8s.io/kubernetes/pkg/apis/core"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
)

func TestStorageClientReturnedObjectsAreIsolated(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		source := mutationTestPod()
		client := mutationTestClient(&mutationStorage{getObj: source})

		obj, err := client.Get(context.Background(), "ns", "pod", metav1.GetOptions{ResourceVersion: "0"})
		if err != nil {
			t.Fatalf("Get() failed: %v", err)
		}
		got, ok := obj.(*corev1.Pod)
		if !ok {
			t.Fatalf("Get() returned %T, want *v1.Pod", obj)
		}

		mutateReturnedPod(got)
		assertInternalPodUnchanged(t, source)
	})

	t.Run("list", func(t *testing.T) {
		source := &core.PodList{Items: []core.Pod{*mutationTestPod()}}
		client := mutationTestClient(&mutationStorage{listObj: source})

		obj, err := client.List(context.Background(), "ns", metav1.ListOptions{ResourceVersion: "0"})
		if err != nil {
			t.Fatalf("List() failed: %v", err)
		}
		got, ok := obj.(*corev1.PodList)
		if !ok {
			t.Fatalf("List() returned %T, want *v1.PodList", obj)
		}
		if len(got.Items) != 1 {
			t.Fatalf("List() returned %d items, want 1", len(got.Items))
		}

		mutateReturnedPod(&got.Items[0])
		assertInternalPodUnchanged(t, &source.Items[0])
	})
}

func mutationTestClient(storage Storage) *storageClient {
	gvr := corev1.SchemeGroupVersion.WithResource("pods")
	return &storageClient{
		factory: &directSharedInformerFactory{
			storages: map[schema.GroupResource]Storage{
				gvr.GroupResource(): storage,
			},
		},
		gvr: gvr,
	}
}

func mutationTestPod() *core.Pod {
	return &core.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pod",
			Namespace:   "ns",
			Labels:      map[string]string{"label": "original"},
			Annotations: map[string]string{"annotation": "original"},
		},
		Spec: core.PodSpec{
			NodeSelector: map[string]string{"zone": "original"},
			Containers: []core.Container{{
				Name: "container",
				Env: []core.EnvVar{{
					Name:  "MODE",
					Value: "original",
				}},
			}},
		},
	}
}

func mutateReturnedPod(pod *corev1.Pod) {
	pod.Labels["label"] = "mutated"
	pod.Annotations["annotation"] = "mutated"
	pod.Spec.NodeSelector["zone"] = "mutated"
	pod.Spec.Containers[0].Name = "mutated"
	pod.Spec.Containers[0].Env[0].Value = "mutated"
}

func assertInternalPodUnchanged(t *testing.T, pod *core.Pod) {
	t.Helper()
	if got := pod.Labels["label"]; got != "original" {
		t.Errorf("source label mutated: got %q, want %q", got, "original")
	}
	if got := pod.Annotations["annotation"]; got != "original" {
		t.Errorf("source annotation mutated: got %q, want %q", got, "original")
	}
	if got := pod.Spec.NodeSelector["zone"]; got != "original" {
		t.Errorf("source nodeSelector mutated: got %q, want %q", got, "original")
	}
	if got := pod.Spec.Containers[0].Name; got != "container" {
		t.Errorf("source container name mutated: got %q, want %q", got, "container")
	}
	if got := pod.Spec.Containers[0].Env[0].Value; got != "original" {
		t.Errorf("source env value mutated: got %q, want %q", got, "original")
	}
}

type mutationStorage struct {
	getObj  runtime.Object
	listObj runtime.Object
}

func (s *mutationStorage) Get(context.Context, string, *metav1.GetOptions) (runtime.Object, error) {
	return s.getObj, nil
}

func (s *mutationStorage) List(context.Context, *metainternalversion.ListOptions) (runtime.Object, error) {
	return s.listObj, nil
}
