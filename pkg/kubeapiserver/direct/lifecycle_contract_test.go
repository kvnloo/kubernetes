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
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	core "k8s.io/kubernetes/pkg/apis/core"
)

type experimentLifecycleStorage struct {
	gets  atomic.Int64
	lists atomic.Int64
}

func (s *experimentLifecycleStorage) Get(context.Context, string, *metav1.GetOptions) (runtime.Object, error) {
	s.gets.Add(1)
	pod := mutationTestPod()
	pod.Labels = map[string]string{"experiment-route": "direct-storage"}
	return pod, nil
}

func (s *experimentLifecycleStorage) List(context.Context, *metainternalversion.ListOptions) (runtime.Object, error) {
	s.lists.Add(1)
	pod := mutationTestPod()
	pod.Labels = map[string]string{"experiment-route": "direct-storage"}
	return &core.PodList{Items: []core.Pod{*pod}}, nil
}

// This exercises the production factory with a fake client, not an apiserver
// or a memory benchmark. A distinct object marker proves which path serves
// reads when the event consumer and direct lister coexist.
func TestExperimentInformerLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name          string
		wrapped       bool
		eventConsumer bool
	}{
		{name: "direct-lister-only", wrapped: true},
		{name: "direct-with-event-consumer", wrapped: true, eventConsumer: true},
		{name: "standard-factory-control", eventConsumer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apiPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "pod", Namespace: "ns", ResourceVersion: "1",
				Labels: map[string]string{"experiment-route": "informer-cache"},
			}}
			client := fake.NewSimpleClientset(apiPod)
			var clientLists, clientWatches atomic.Int64
			watchStarted := make(chan struct{})
			var watchOnce sync.Once
			client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
				clientLists.Add(1)
				return false, nil, nil
			})
			client.PrependWatchReactor("pods", func(clienttesting.Action) (bool, watch.Interface, error) {
				clientWatches.Add(1)
				watchOnce.Do(func() { close(watchStarted) })
				return false, nil, nil
			})
			original := informers.NewSharedInformerFactory(client, 0)
			var factory informers.SharedInformerFactory = original
			storage := &experimentLifecycleStorage{}
			if tc.wrapped {
				factory = NewSharedInformerFactory(original)
				SetStorage(factory, corev1.Resource("pods"), storage)
			}
			podInformer := factory.Core().V1().Pods()
			lister := podInformer.Lister()
			var informer cache.SharedIndexInformer
			eventSeen := make(chan struct{}, 1)
			if tc.eventConsumer {
				informer = podInformer.Informer()
				if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(obj interface{}) {
					if pod, ok := obj.(*corev1.Pod); ok && pod.Namespace == "ns" && pod.Name == "pod" {
						select {
						case eventSeen <- struct{}{}:
						default:
						}
					}
				}}); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer func() { cancel(); original.Shutdown() }()
			factory.Start(ctx.Done())
			synced := factory.WaitForCacheSync(ctx.Done())
			if tc.eventConsumer {
				if len(synced) != 1 || !synced[reflect.TypeOf(&corev1.Pod{})] {
					t.Fatalf("sync map=%v, want one synced Pod informer", synced)
				}
				for _, ready := range []<-chan struct{}{watchStarted, eventSeen} {
					select {
					case <-ready:
					case <-ctx.Done():
						t.Fatal("informer watch or event consumer did not start")
					}
				}
				obj, exists, err := informer.GetIndexer().GetByKey("ns/pod")
				if err != nil || !exists {
					t.Fatalf("cached object exists=%v err=%v", exists, err)
				}
				if got := obj.(*corev1.Pod).Labels["experiment-route"]; got != "informer-cache" {
					t.Fatalf("cache marker=%q", got)
				}
				if clientLists.Load() < 1 || clientWatches.Load() < 1 {
					t.Fatalf("client list/watch=%d/%d", clientLists.Load(), clientWatches.Load())
				}
			} else if len(synced) != 0 || clientLists.Load() != 0 || clientWatches.Load() != 0 {
				t.Fatalf("lister-only started informer: sync=%v list/watch=%d/%d", synced, clientLists.Load(), clientWatches.Load())
			}
			pod, err := lister.Pods("ns").Get("pod")
			if err != nil {
				t.Fatal(err)
			}
			pods, err := lister.Pods("ns").List(labels.Everything())
			if err != nil || len(pods) != 1 {
				t.Fatalf("List len=%d err=%v", len(pods), err)
			}
			wantMarker := "informer-cache"
			var wantReads int64
			if tc.wrapped {
				wantMarker, wantReads = "direct-storage", 1
			}
			if pod.Labels["experiment-route"] != wantMarker || pods[0].Labels["experiment-route"] != wantMarker {
				t.Fatalf("Get/List did not use expected route %q", wantMarker)
			}
			if storage.gets.Load() != wantReads || storage.lists.Load() != wantReads {
				t.Fatalf("direct Get/List counts=%d/%d, want %d each", storage.gets.Load(), storage.lists.Load(), wantReads)
			}
			if !tc.eventConsumer {
				// Start again so an informer registered lazily by Get/List cannot
				// hide merely because it missed the first Start call.
				factory.Start(ctx.Done())
				if len(factory.WaitForCacheSync(ctx.Done())) != 0 || clientLists.Load() != 0 || clientWatches.Load() != 0 {
					t.Fatal("direct read registered or started an informer")
				}
			}
			t.Logf("wrapped=%v eventConsumer=%v synced=%d client_list=%d client_watch=%d direct_get=%d direct_list=%d route=%s", tc.wrapped, tc.eventConsumer, len(synced), clientLists.Load(), clientWatches.Load(), storage.gets.Load(), storage.lists.Load(), wantMarker)
		})
	}
}
