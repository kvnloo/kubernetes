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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/admission"
	request "k8s.io/apiserver/pkg/endpoints/request"
	apifeatures "k8s.io/apiserver/pkg/features"
	registryrest "k8s.io/apiserver/pkg/registry/rest"
	apistorage "k8s.io/apiserver/pkg/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturetesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	featuretesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	core "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/auth/nodeidentifier"
	tokengetter "k8s.io/kubernetes/pkg/controller/serviceaccount"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	quota "k8s.io/kubernetes/pkg/quota/v1/evaluator/core"
	"k8s.io/kubernetes/plugin/pkg/admission/noderestriction"
	podsecurity "k8s.io/kubernetes/plugin/pkg/admission/security/podsecurity"
	nodegraph "k8s.io/kubernetes/plugin/pkg/auth/authorizer/node"
	psa "k8s.io/pod-security-admission/admission"
	"k8s.io/utils/ptr"
)

func TestFollowupN1RealStorageParity(t *testing.T) {
	f := newFollowFixture(t, true, true, false)
	for _, size := range []int{0, 4096} {
		f.create(fmt.Sprintf("normal-%d", size), size)
	}
	// This is a registered stored-codec fixture, not a supported HTTP write.
	raw := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"omitted","namespace":"ns","uid":"codec-fixture","labels":{"fixture":"followup"}},"spec":{"containers":[{"name":"container","image":"registry.k8s.io/pause:3.10"}]}}`
	if _, e := f.etcd.V3Client.Put(context.Background(), "/followup/pods/ns/omitted", raw); e != nil {
		t.Fatal(e)
	}
	f.startInformer()
	for _, name := range []string{"normal-0", "normal-4096", "omitted"} {
		t.Run(name, func(t *testing.T) {
			httpPod, e := f.client.CoreV1().Pods("ns").Get(context.Background(), name, metav1.GetOptions{})
			if e != nil {
				t.Fatal(e)
			}
			f.barrier(name, httpPod.ResourceVersion)
			cached, e := corelisters.NewPodLister(f.informer.GetIndexer()).Pods("ns").Get(name)
			if e != nil {
				t.Fatal(e)
			}
			backendBefore := f.backend.gets.Load()
			direct, e := f.lister.Pods("ns").Get(name)
			if e != nil {
				t.Fatal(e)
			}
			if f.backend.gets.Load() != backendBefore {
				t.Fatal("warmed direct Get unexpectedly used backend")
			}
			followAssertSame(t, httpPod, direct, false)
			followAssertSame(t, cached, direct, true)
			mismatch := followJSON(t, followClear(cached, false)) != followJSON(t, followClear(direct, false))
			if mismatch != (name == "normal-4096") {
				t.Fatal("known managedFields mismatch calibration failed")
			}
			if name == "omitted" && (direct.Spec.RestartPolicy != corev1.RestartPolicyAlways || direct.Spec.DNSPolicy != corev1.DNSClusterFirst || direct.Spec.SchedulerName != corev1.DefaultSchedulerName) {
				t.Fatal("stored omitted-field default oracle failed")
			}
			// Cacher.Get returns a shallow struct containing cache-owned nested fields.
			source := &core.Pod{}
			if e = f.cache.Get(context.Background(), "/pods/ns/"+name, apistorage.GetOptions{ResourceVersion: "0"}, source); e != nil {
				t.Fatal(e)
			}
			before := followJSON(t, source)
			probe := source.DeepCopy()
			probe.Spec.Containers[0].Image = "changed-evaluator-control"
			if followJSON(t, probe) == before {
				t.Fatal("source-change evaluator false green")
			}
			direct.Labels["fixture"] = "changed-return"
			direct.Spec.Containers[0].Image = "changed-return"
			if len(direct.ManagedFields) > 0 {
				direct.ManagedFields[0].FieldsV1.Raw[0] = 'X'
			}
			if followJSON(t, source) != before {
				t.Fatal("real cache source changed through returned object")
			}
			again, e := f.lister.Pods("ns").Get(name)
			if e != nil {
				t.Fatal(e)
			}
			followAssertSame(t, httpPod, again, false)
			followReceipt(t, "N1", map[string]any{"fixture": name, "uid": httpPod.UID, "rv": httpPod.ResourceVersion, "raw_managed_fields_mismatch": mismatch, "default_restart": again.Spec.RestartPolicy, "default_dns": again.Spec.DNSPolicy, "default_scheduler": again.Spec.SchedulerName, "source_unchanged": true, "cache_get_backend_calls": 0, "http": httpPod, "cached": cached, "direct": again})
		})
	}
	for _, selector := range []labels.Selector{labels.Everything(), labels.SelectorFromSet(labels.Set{"fixture": "followup"}), labels.Nothing()} {
		before := f.backend.lists.Load()
		got, e := f.lister.Pods("ns").List(selector)
		if e != nil {
			t.Fatal(e)
		}
		cached, e := corelisters.NewPodLister(f.informer.GetIndexer()).Pods("ns").List(selector)
		if e != nil {
			t.Fatal(e)
		}
		if len(got) != len(cached) {
			t.Fatal("list count differs")
		}
		byName := map[string]*corev1.Pod{}
		for _, p := range cached {
			byName[p.Name] = p
		}
		for _, p := range got {
			followAssertSame(t, byName[p.Name], p, true)
		}
		if f.backend.lists.Load() != before {
			t.Fatal("direct warm List delegated to backend")
		}
		followReceipt(t, "N1-list", map[string]any{"selector": selector.String(), "matches_nothing": labels.MatchesNothing(selector), "count": len(got), "backend_calls": 0})
	}
}

type followTrimLister struct{ corelisters.PodNamespaceLister }

func (l followTrimLister) Get(name string) (*corev1.Pod, error) {
	p, e := l.PodNamespaceLister.Get(name)
	if e == nil {
		p.ManagedFields = nil
	}
	return p, e
}
func (l followTrimLister) List(s labels.Selector) ([]*corev1.Pod, error) {
	p, e := l.PodNamespaceLister.List(s)
	if e == nil {
		for _, o := range p {
			o.ManagedFields = nil
		}
	}
	return p, e
}

func followCostArms(t testing.TB, count, size int) (readCostFixture, map[string]corelisters.PodNamespaceLister) {
	f := newReadCostFixture(t, count, size)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for i := range f.storage.listObject.(*core.PodList).Items {
		p := &f.storage.listObject.(*core.PodList).Items[i]
		o, e := legacyscheme.Scheme.ConvertToVersion(p, corev1.SchemeGroupVersion)
		if e != nil {
			t.Fatal(e)
		}
		o.(*corev1.Pod).TypeMeta = metav1.TypeMeta{}
		if e = indexer.Add(o); e != nil {
			t.Fatal(e)
		}
	}
	return f, map[string]corelisters.PodNamespaceLister{"cached_trimmed": f.cached, "direct_trimmed": followTrimLister{f.direct}, "cached_retained": corelisters.NewPodLister(indexer).Pods("ns"), "direct_retained": f.direct}
}

func TestFollowupN2EqualOutputGate(t *testing.T) {
	for _, n := range []int{1, 100} {
		for _, size := range []int{0, 4096, 32768} {
			t.Run(fmt.Sprintf("n=%d/mf=%d", n, size), func(t *testing.T) {
				f, arms := followCostArms(t, n, size)
				before := followJSON(t, f.storage.listObject)
				for _, shape := range []string{"trimmed", "retained"} {
					a, e := arms["cached_"+shape].Get("pod-0000")
					if e != nil {
						t.Fatal(e)
					}
					b, e := arms["direct_"+shape].Get("pod-0000")
					if e != nil {
						t.Fatal(e)
					}
					followAssertSame(t, a, b, false)
					aa, e := arms["cached_"+shape].List(labels.Everything())
					if e != nil {
						t.Fatal(e)
					}
					bb, e := arms["direct_"+shape].List(labels.Everything())
					if e != nil {
						t.Fatal(e)
					}
					m := map[string]*corev1.Pod{}
					for _, p := range aa {
						m[p.Name] = p
					}
					if len(bb) != n {
						t.Fatal("unexpected count")
					}
					for _, p := range bb {
						followAssertSame(t, m[p.Name], p, false)
					}
				}
				if followJSON(t, f.storage.listObject) != before {
					t.Fatal("output trimming mutated storage source")
				}
				followReceipt(t, "N2", map[string]any{"n": n, "managed_fields_bytes": f.managedFieldsBytes, "trimmed_equal": true, "retained_equal": true, "source_unchanged": true})
			})
		}
	}
	// Real-cache output parity qualifies promotion, independent of the synthetic cost probe.
	f := newFollowFixture(t, true, true, false)
	p := f.create("real-cost", 4096)
	f.startInformer()
	f.barrier(p.Name, p.ResourceVersion)
	a, _ := corelisters.NewPodLister(f.informer.GetIndexer()).Pods("ns").Get(p.Name)
	b, e := followTrimLister{f.lister.Pods("ns")}.Get(p.Name)
	if e != nil {
		t.Fatal(e)
	}
	followAssertSame(t, a, b, false)
	followReceipt(t, "N2-real-cache", map[string]any{"equal_output": true, "rv": p.ResourceVersion})
}

func BenchmarkFollowupN2EqualOutput(b *testing.B) {
	for _, op := range []string{"get", "list"} {
		for _, n := range []int{1, 100} {
			for _, size := range []int{0, 4096, 32768} {
				f, arms := followCostArms(b, n, size)
				for _, name := range []string{"cached_trimmed", "direct_trimmed", "cached_retained", "direct_retained"} {
					b.Run(fmt.Sprintf("op=%s/n=%d/mf=%d/path=%s", op, n, f.managedFieldsBytes, name), func(b *testing.B) {
						l := arms[name]
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if op == "get" {
								p, e := l.Get("pod-0000")
								if e != nil {
									b.Fatal(e)
								}
								readCostPodSink = p
							} else {
								p, e := l.List(labels.Everything())
								if e != nil {
									b.Fatal(e)
								}
								readCostPodsSink = p
							}
						}
					})
				}
			}
		}
	}
}

func TestFollowupN3ProductionConsumerWiring(t *testing.T) {
	clientfeaturetesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.DRAExtendedResource, true)
	for _, directMode := range []bool{false, true} {
		for _, graph := range []bool{false, true} {
			t.Run(fmt.Sprintf("direct=%t/graph=%t", directMode, graph), func(t *testing.T) {
				// Production setters register their real prerequisites on one shared factory.
				client := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns", UID: "wire-fixture", ResourceVersion: "1"}, Spec: corev1.PodSpec{NodeName: "fixture-node"}})
				var mu sync.Mutex
				lists := map[string]int{}
				watches := map[string]int{}
				client.PrependReactor("list", "*", func(a clienttesting.Action) (bool, runtime.Object, error) {
					mu.Lock()
					lists[a.GetResource().Resource]++
					mu.Unlock()
					return false, nil, nil
				})
				client.PrependWatchReactor("*", func(a clienttesting.Action) (bool, watch.Interface, error) {
					mu.Lock()
					watches[a.GetResource().Resource]++
					mu.Unlock()
					return false, nil, nil
				})
				original := informers.NewSharedInformerFactory(client, 0)
				var factory informers.SharedInformerFactory = original
				storage := &followAtomicREST{pod: &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns", UID: "wire-fixture", ResourceVersion: "1"}, Spec: core.PodSpec{NodeName: "fixture-node"}}}
				if directMode {
					factory = NewSharedInformerFactory(original)
					SetStorage(factory, corev1.Resource("pods"), storage)
				}
				nr := noderestriction.NewPlugin(nodeidentifier.NewDefaultNodeIdentifier())
				nr.SetExternalKubeInformerFactory(factory)
				ps := &podsecurity.Plugin{Handler: admission.NewHandler(admission.Create, admission.Update)}
				ps.SetExternalKubeInformerFactory(factory)
				if _, e := quota.NewEvaluators(nil, factory, func(g schema.GroupVersionResource) bool { return g.Resource == "resourceclaims" }); e != nil {
					t.Fatal(e)
				}
				getter := tokengetter.NewGetterFromClient(client, nil, nil, factory.Core().V1().Pods().Lister(), nil, nil, nil)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if graph {
					nodegraph.AddGraphEventHandlers(ctx, nodegraph.NewGraph(), factory.Core().V1().Nodes(), factory.Core().V1().Pods(), factory.Core().V1().PersistentVolumes(), factory.Storage().V1().VolumeAttachments(), factory.Resource().V1().ResourceSlices(), nil)
				}
				stop := ctx.Done()
				factory.Start(stop)
				defer original.Shutdown()
				defer cancel()
				for typ, ready := range factory.WaitForCacheSync(stop) {
					if !ready {
						t.Fatalf("prerequisite %v not ready", typ)
					}
				}
				p, e := getter.GetPod(ctx, "ns", "pod")
				if e != nil || p.UID != "wire-fixture" {
					t.Fatalf("getter outcome p=%v err=%v", p, e)
				}
				adapter := psa.PodListerFromInformer(factory.Core().V1().Pods().Lister())
				cancelled, cc := context.WithCancel(ctx)
				cc()
				if _, e = adapter.ListPods(cancelled, "ns"); e != nil {
					t.Fatal(e)
				}
				mu.Lock()
				podLists, podWatches := lists["pods"], watches["pods"]
				mu.Unlock()
				expectPod := !directMode || graph
				if (podLists > 0) != expectPod || (podWatches > 0) != expectPod {
					t.Fatalf("unexpected Pod lifecycle lists=%d watches=%d", podLists, podWatches)
				}
				if graph && directMode {
					followPoll(t, func() (bool, error) { return storage.gets.Load() >= 2, nil })
				}
				mu.Lock()
				followReceipt(t, "N3", map[string]any{"direct": directMode, "node_graph": graph, "list_counts": lists, "watch_counts": watches, "direct_gets": storage.gets.Load(), "direct_lists": storage.lists.Load(), "psa_cancelled_adapter_completed": true, "prerequisites_synced": true})
				mu.Unlock()
			})
		}
	}
}

type followAtomicREST struct {
	pod         *core.Pod
	gets, lists atomic.Int64
	err         error
}

func (s *followAtomicREST) Get(context.Context, string, *metav1.GetOptions) (runtime.Object, error) {
	s.gets.Add(1)
	return s.pod, s.err
}
func (s *followAtomicREST) List(context.Context, *metainternalversion.ListOptions) (runtime.Object, error) {
	s.lists.Add(1)
	return &core.PodList{Items: []core.Pod{*s.pod}}, s.err
}

func TestFollowupN6StartupAndFallback(t *testing.T) {
	// Disable background inconsistency checks so held initial List has one owner.
	featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, apifeatures.DetectCacheInconsistency, false)
	f := newFollowFixture(t, true, true, true)
	select {
	case <-f.backend.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("initial backend list gate not reached")
	}
	p := f.create("startup", 0)
	other := NewSharedInformerFactory(f.original)
	unbound := other.Core().V1().Pods().Lister()
	if _, e := unbound.Pods("ns").Get(p.Name); e == nil {
		t.Fatal("unregistered storage unexpectedly read")
	}
	before := f.backend.gets.Load()
	early, e := f.lister.Pods("ns").Get(p.Name)
	if e != nil {
		t.Fatal(e)
	}
	if f.cache.Ready() || f.backend.gets.Load() != before+1 || early.UID != p.UID {
		t.Fatal("unready Get delegation receipt failed")
	}
	_, listErr := f.lister.Pods("ns").List(labels.Everything())
	if !apierrors.IsTooManyRequests(listErr) {
		t.Fatalf("unready list outcome=%v", listErr)
	}
	followReceipt(t, "N6-startup", map[string]any{"cache_ready": false, "get_backend_calls": 1, "list_too_many_requests": true, "uid": early.UID, "rv": early.ResourceVersion, "unregistered_error": true})
	close(f.backend.gate)
	f.waitReady()
	f.startInformer()
	f.barrier(p.Name, p.ResourceVersion)
	before = f.backend.gets.Load()
	after, e := f.lister.Pods("ns").Get(p.Name)
	if e != nil || after.UID != p.UID || f.backend.gets.Load() != before {
		t.Fatal("ready Get did not use cache")
	}
	p = f.update(p, 1)
	f.barrier(p.Name, p.ResourceVersion)
	after, e = f.lister.Pods("ns").Get(p.Name)
	if e != nil || after.ResourceVersion != p.ResourceVersion || after.Annotations["sequence"] != "1" {
		t.Fatal("update barrier outcome differs")
	}
	// Immediate deletion makes the ordinary UID transition unambiguous.
	key := request.WithNamespace(context.Background(), "ns")
	_, _, e = f.rest.Delete(key, p.Name, registryrest.ValidateAllObjectFunc, &metav1.DeleteOptions{GracePeriodSeconds: ptr.To[int64](0)})
	if e != nil {
		t.Fatal(e)
	}
	followPoll(t, func() (bool, error) { _, e := f.lister.Pods("ns").Get(p.Name); return apierrors.IsNotFound(e), nil })
	replacement := f.create(p.Name, 0)
	f.barrier(replacement.Name, replacement.ResourceVersion)
	if replacement.UID == p.UID {
		t.Fatal("recreation identity control failed")
	}
	after, e = f.lister.Pods("ns").Get(p.Name)
	if e != nil || after.UID != replacement.UID {
		t.Fatal("recreated identity mismatch")
	}
	for _, state := range []string{"active", "cancelled", "expired"} {
		ctx := context.Background()
		var cancel context.CancelFunc
		if state == "expired" {
			ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
		} else {
			ctx, cancel = context.WithCancel(ctx)
			if state == "cancelled" {
				cancel()
			}
		}
		begin := time.Now()
		pods, err := f.lister.(interface {
			ListPods(context.Context, string) ([]*corev1.Pod, error)
		}).ListPods(ctx, "ns")
		cancel()
		followReceipt(t, "N6-context", map[string]any{"state": state, "error": fmt.Sprint(err), "returned": len(pods), "elapsed_ns": time.Since(begin).Nanoseconds()})
	}
	for _, kind := range []string{"success", "not_found", "unready", "ordinary_error"} {
		client := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns", UID: "fallback"}})
		var fallback atomic.Int64
		client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) { fallback.Add(1); return false, nil, nil })
		storage := &followAtomicREST{pod: &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns", UID: "lister"}}}
		switch kind {
		case "not_found":
			storage.err = apierrors.NewNotFound(core.Resource("pods"), "pod")
		case "unready":
			storage.err = apierrors.NewTooManyRequests("bounded fixture", 1)
		case "ordinary_error":
			storage.err = fmt.Errorf("bounded fixture read error")
		}
		factory := NewSharedInformerFactory(informers.NewSharedInformerFactory(client, 0))
		SetStorage(factory, corev1.Resource("pods"), storage)
		getter := tokengetter.NewGetterFromClient(client, nil, nil, factory.Core().V1().Pods().Lister(), nil, nil, nil)
		got, e := getter.GetPod(context.Background(), "ns", "pod")
		if e != nil {
			t.Fatal(e)
		}
		expected := int64(1)
		uid := "fallback"
		if kind == "success" {
			expected = 0
			uid = "lister"
		}
		if fallback.Load() != expected || string(got.UID) != uid || storage.gets.Load() != 1 {
			t.Fatal("fallback route outcome differs")
		}
		followReceipt(t, "N6-fallback", map[string]any{"kind": kind, "lister_calls": 1, "fallback_calls": fallback.Load(), "uid": got.UID})
	}
	followReceipt(t, "N6-version", map[string]any{"updated_rv": p.ResourceVersion, "old_uid": p.UID, "replacement_uid": replacement.UID, "replacement_rv": replacement.ResourceVersion, "ready_get_backend_calls": 0})
}
