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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestPodListerSelectors(t *testing.T) {
	for _, scope := range []struct {
		name      string
		namespace string
	}{
		{name: "all namespaces"},
		{name: "namespaced", namespace: "ns"},
	} {
		t.Run(scope.name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				selector labels.Selector
				want     int
			}{
				{name: "nothing", selector: labels.Nothing(), want: 0},
				{name: "everything", selector: labels.Everything(), want: 1},
				{name: "nil", selector: nil, want: 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					client := &selectorRecordingClient{}
					lister := NewPodLister(client)
					list := lister.List
					if scope.namespace != "" {
						list = lister.Pods(scope.namespace).List
					}
					pods, err := list(tc.selector)
					if err != nil {
						t.Fatalf("List() failed: %v", err)
					}
					if len(pods) != tc.want {
						t.Errorf("got %d pods, want %d", len(pods), tc.want)
					}
					if client.calls != tc.want {
						t.Errorf("got %d backend calls, want %d", client.calls, tc.want)
					}
					if tc.want == 0 {
						return
					}
					if client.namespace != scope.namespace {
						t.Errorf("backend namespace = %q, want %q", client.namespace, scope.namespace)
					}
					if client.options.ResourceVersion != "0" || client.options.LabelSelector != "" {
						t.Errorf("unexpected backend options: %+v", client.options)
					}
				})
			}
		})
	}
}

type selectorRecordingClient struct {
	calls     int
	namespace string
	options   metav1.ListOptions
}

var _ Client = (*selectorRecordingClient)(nil)

func (c *selectorRecordingClient) Get(context.Context, string, string, metav1.GetOptions) (runtime.Object, error) {
	return nil, fmt.Errorf("unexpected Get call")
}

func (c *selectorRecordingClient) List(_ context.Context, namespace string, options metav1.ListOptions) (runtime.Object, error) {
	c.calls++
	c.namespace = namespace
	c.options = options
	return &corev1.PodList{Items: []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}},
	}}, nil
}
