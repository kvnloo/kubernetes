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
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"
	core "k8s.io/kubernetes/pkg/apis/core"
	psaadmission "k8s.io/pod-security-admission/admission"
)

type experimentContextKey struct{}

type experimentContextStorage struct {
	ctx context.Context
	calls int
	rv string
}

func (s *experimentContextStorage) Get(ctx context.Context, name string, opts *metav1.GetOptions) (runtime.Object, error) {
	s.ctx, s.rv = ctx, opts.ResourceVersion
	s.calls++
	if err := ctx.Err(); err != nil { return nil, err }
	return mutationTestPod(), nil
}

func (s *experimentContextStorage) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	s.ctx, s.rv = ctx, opts.ResourceVersion
	s.calls++
	if err := ctx.Err(); err != nil { return nil, err }
	return &core.PodList{Items: []core.Pod{*mutationTestPod()}}, nil
}

// This characterizes the actual PSA adapter route with harmless cancellation,
// deadline and private-value probes. It does not exercise an HTTP filter chain.
func TestExperimentContextRoutes(t *testing.T) {
	for _, state := range []string{"active", "cancelled", "expired"} {
		for _, route := range []string{"direct-context-aware", "production-psa-adapter"} {
			t.Run(state+"/"+route, func(t *testing.T) {
				deadline := time.Now().Add(time.Hour)
				if state == "expired" { deadline = time.Unix(1, 0) }
				ctx, cancel := context.WithDeadline(context.WithValue(context.Background(), experimentContextKey{}, "sentinel"), deadline)
				defer cancel()
				if state == "cancelled" { cancel() }
				storage := &experimentContextStorage{}
				lister := NewPodLister(mutationTestClient(storage))
				var caller interface { ListPods(context.Context, string) ([]*corev1.Pod, error) }
				if route == "direct-context-aware" {
					var ok bool
					caller, ok = lister.(interface { ListPods(context.Context, string) ([]*corev1.Pod, error) })
					if !ok { t.Fatal("direct ListPods extension missing") }
				} else {
					caller = psaadmission.PodListerFromInformer(lister)
				}
				pods, err := caller.ListPods(ctx, "ns")
				if storage.calls != 1 || storage.ctx == nil { t.Fatalf("Storage route calls=%d", storage.calls) }
				ns, ok := request.NamespaceFrom(storage.ctx)
				if !ok || ns != "ns" || storage.rv != "0" { t.Fatalf("namespace=%q/%v RV=%q", ns, ok, storage.rv) }
				gotDeadline, hasDeadline := storage.ctx.Deadline()
				preserve := route == "direct-context-aware"
				if preserve {
					if !hasDeadline || !gotDeadline.Equal(deadline) || storage.ctx.Value(experimentContextKey{}) != "sentinel" {
						t.Fatal("direct context-aware path lost deadline or sentinel")
					}
					if !errors.Is(err, ctx.Err()) || !errors.Is(storage.ctx.Err(), ctx.Err()) { t.Fatalf("err=%v observed=%v caller=%v", err, storage.ctx.Err(), ctx.Err()) }
				} else {
					if hasDeadline || storage.ctx.Value(experimentContextKey{}) != nil || storage.ctx.Err() != nil || err != nil { t.Fatalf("adapter unexpectedly preserved caller context: err=%v deadline=%v", err, hasDeadline) }
				}
				if (!preserve || state == "active") && (len(pods) != 1 || pods[0].Name != "pod") { t.Fatalf("successful route returned %d pods", len(pods)) }
				t.Logf("route=%s caller=%s calls=%d deadline=%v sentinel=%v storage_error=%v returned_error=%v", route, state, storage.calls, hasDeadline, storage.ctx.Value(experimentContextKey{}) != nil, storage.ctx.Err(), err)
			})
		}
	}
	// Get's generated API has no context argument. This verifies its concrete
	// Storage route, independently of the adapter's List route above.
	t.Run("contextless-get", func(t *testing.T) {
		storage := &experimentContextStorage{}
		pod, err := NewPodLister(mutationTestClient(storage)).Pods("ns").Get("pod")
		if err != nil || pod == nil || storage.calls != 1 { t.Fatalf("Get calls=%d err=%v", storage.calls, err) }
		if _, ok := storage.ctx.Deadline(); ok || storage.ctx.Value(experimentContextKey{}) != nil || storage.rv != "0" { t.Fatal("unexpected Get context/options") }
	})
}
