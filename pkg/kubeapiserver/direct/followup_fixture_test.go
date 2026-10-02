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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/handlers"
	request "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	registryrest "k8s.io/apiserver/pkg/registry/rest"
	apistorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher"
	etcdtesting "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	storagefactory "k8s.io/apiserver/pkg/storage/storagebackend/factory"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturetesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	clientrest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	core "k8s.io/kubernetes/pkg/apis/core"
	podstorage "k8s.io/kubernetes/pkg/registry/core/pod/storage"
	"k8s.io/utils/clock"
)

// The counters surround the real interfaces; they do not replace reads with fixtures.
type followStorage struct {
	apistorage.Interface
	gets, lists, watches atomic.Int64
	gate                 chan struct{}
	entered              chan struct{}
	enteredOnce          sync.Once
}

func (s *followStorage) Get(ctx context.Context, key string, opts apistorage.GetOptions, out runtime.Object) error {
	s.gets.Add(1)
	return s.Interface.Get(ctx, key, opts, out)
}
func (s *followStorage) GetList(ctx context.Context, key string, opts apistorage.ListOptions, out runtime.Object) error {
	s.lists.Add(1)
	if s.gate != nil {
		s.enteredOnce.Do(func() { close(s.entered) })
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Interface.GetList(ctx, key, opts, out)
}
func (s *followStorage) Watch(ctx context.Context, key string, opts apistorage.ListOptions) (watch.Interface, error) {
	s.watches.Add(1)
	return s.Interface.Watch(ctx, key, opts)
}

type followFixture struct {
	t                                             testing.TB
	rest                                          *podstorage.REST
	backend, front                                *followStorage
	etcd                                          *etcdtesting.EtcdTestServer
	cache                                         *cacher.Cacher
	delegator                                     *cacher.CacheDelegator
	client                                        kubernetes.Interface
	server                                        *httptest.Server
	factory                                       informers.SharedInformerFactory
	original                                      informers.SharedInformerFactory
	lister                                        corelisters.PodLister
	informer                                      cache.SharedIndexInformer
	stop                                          chan struct{}
	started                                       bool
	httpLists, httpWatches, httpGets, httpUpdates atomic.Int64
	httpBytes                                     atomic.Int64
	closeWatch                                    atomic.Bool
	expireWatch                                   atomic.Bool
	watchInterrupted                              atomic.Int64
	traceMu                                       sync.Mutex
	watchRVs                                      []string
	watchCancel                                   context.CancelFunc
}

type followCountingWriter struct {
	http.ResponseWriter
	bytes *atomic.Int64
}

func (w followCountingWriter) Write(b []byte) (int, error) {
	n, e := w.ResponseWriter.Write(b)
	w.bytes.Add(int64(n))
	return n, e
}
func (w followCountingWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func newFollowFixture(t testing.TB, directMode, trim, hold bool) *followFixture {
	t.Helper()
	clientfeaturetesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	f := &followFixture{t: t, stop: make(chan struct{})}
	etcd, config := etcdtesting.NewUnsecuredEtcd3TestClientServer(t)
	f.etcd = etcd
	config.Prefix = "/followup"
	config.Codec = legacyscheme.Codecs.LegacyCodec(corev1.SchemeGroupVersion)
	config.EncodeVersioner = corev1.SchemeGroupVersion
	config.CompactionInterval = 0
	config.DBMetricPollInterval = 0
	config.CountMetricPollPeriod = 0
	config.LeaseManagerConfig = storagebackend.NewDefaultConfig("", config.Codec).LeaseManagerConfig
	opts := generic.RESTOptions{StorageConfig: config.ForResource(core.Resource("pods")), ResourcePrefix: "pods", DeleteCollectionWorkers: 1}
	opts.Decorator = func(c *storagebackend.ConfigForResource, prefix string, key func(runtime.Object) (string, error), newFn, newListFn func() runtime.Object, attrs apistorage.AttrFunc, trigger apistorage.IndexerFuncs, indexers *cache.Indexers) (apistorage.Interface, storagefactory.DestroyFunc, error) {
		raw, destroy, err := generic.NewRawStorage(c, newFn, newListFn, prefix)
		if err != nil {
			return nil, nil, err
		}
		f.backend = &followStorage{Interface: raw}
		if hold {
			f.backend.gate = make(chan struct{})
			f.backend.entered = make(chan struct{})
		}
		f.cache, err = cacher.NewCacherFromConfig(cacher.Config{Storage: f.backend, Versioner: apistorage.APIObjectVersioner{}, GroupResource: core.Resource("pods"), EventsHistoryWindow: cacher.DefaultEventFreshDuration, ResourcePrefix: prefix, KeyFunc: key, GetAttrsFunc: attrs, NewFunc: newFn, NewListFunc: newListFn, IndexerFuncs: trigger, Indexers: indexers, Codec: c.Codec, Clock: clock.RealClock{}})
		if err != nil {
			destroy()
			return nil, nil, err
		}
		f.delegator = cacher.NewCacheDelegator(f.cache, f.backend)
		f.front = &followStorage{Interface: f.delegator}
		return f.front, func() {
			if hold {
				select {
				case <-f.backend.gate:
				default:
					close(f.backend.gate)
				}
			}
			f.delegator.Stop()
			f.cache.Stop()
			destroy()
		}, nil
	}
	stores, err := podstorage.NewStorage(opts, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.rest = stores.Pod
	t.Cleanup(f.rest.Destroy)
	if !hold {
		f.waitReady()
	}
	scope := &handlers.RequestScope{Namer: handlers.ContextBasedNaming{Namer: meta.NewAccessor()}, Serializer: legacyscheme.Codecs, ParameterCodec: legacyscheme.ParameterCodec, Creater: legacyscheme.Scheme, Convertor: legacyscheme.Scheme, UnsafeConvertor: runtime.UnsafeObjectConvertor(legacyscheme.Scheme), Defaulter: legacyscheme.Scheme, Typer: legacyscheme.Scheme, Resource: corev1.SchemeGroupVersion.WithResource("pods"), Kind: corev1.SchemeGroupVersion.WithKind("Pod"), MetaGroupVersion: metav1.SchemeGroupVersion, HubGroupVersion: core.SchemeGroupVersion, TableConvertor: f.rest.TableConvertor, MaxRequestBodyBytes: 1 << 20}
	scope.FieldManager, err = managedfields.NewDefaultFieldManager(managedfields.NewDeducedTypeConverter(), legacyscheme.Scheme, legacyscheme.Scheme, legacyscheme.Scheme, scope.Kind, core.SchemeGroupVersion, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	getHandler := handlers.GetResource(f.rest, scope)
	listHandler := handlers.ListResource(f.rest, f.rest, scope, false, 30*time.Second)
	updateHandler := handlers.UpdateResource(f.rest, scope, nil)
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		info := &request.RequestInfo{IsResourceRequest: true, APIVersion: "v1", Resource: "pods", Verb: "list"}
		if len(parts) >= 5 && parts[2] == "namespaces" {
			info.Namespace = parts[3]
			if len(parts) > 5 {
				info.Name = parts[5]
			}
		}
		if info.Name != "" {
			info.Verb = "get"
		}
		if r.Method == http.MethodPut {
			info.Verb = "update"
		}
		if r.URL.Query().Get("watch") == "true" {
			info.Verb = "watch"
			f.httpWatches.Add(1)
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(ctx)
			f.traceMu.Lock()
			f.watchRVs = append(f.watchRVs, r.URL.Query().Get("resourceVersion"))
			f.watchCancel = cancel
			f.traceMu.Unlock()
			if f.expireWatch.CompareAndSwap(true, false) {
				f.watchInterrupted.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				_ = json.NewEncoder(w).Encode(map[string]any{"type": "ERROR", "object": metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonExpired, Code: 410, Message: "registered bounded test watch expiry"}})
				return
			}
			if f.closeWatch.CompareAndSwap(true, false) {
				f.watchInterrupted.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				if fl, ok := w.(http.Flusher); ok {
					fl.Flush()
				}
				return
			}
		} else if info.Verb == "list" {
			f.httpLists.Add(1)
		} else if info.Verb == "get" {
			f.httpGets.Add(1)
		} else if info.Verb == "update" {
			f.httpUpdates.Add(1)
		}
		r = r.WithContext(request.WithRequestInfo(request.WithNamespace(r.Context(), info.Namespace), info))
		out := followCountingWriter{ResponseWriter: w, bytes: &f.httpBytes}
		switch info.Verb {
		case "get":
			getHandler(out, r)
		case "update":
			updateHandler(out, r)
		default:
			listHandler(out, r)
		}
	}))
	t.Cleanup(f.server.Close)
	f.client, err = kubernetes.NewForConfig(&clientrest.Config{Host: f.server.URL, QPS: 10000, Burst: 10000, ContentConfig: clientrest.ContentConfig{ContentType: "application/json"}})
	if err != nil {
		t.Fatal(err)
	}
	options := []informers.SharedInformerOption{}
	if trim {
		options = append(options, informers.WithTransform(func(obj any) (any, error) { obj.(metav1.Object).SetManagedFields(nil); return obj, nil }))
	}
	f.original = informers.NewSharedInformerFactoryWithOptions(f.client, 0, options...)
	f.factory = f.original
	if directMode {
		f.factory = NewSharedInformerFactory(f.original)
		SetStorage(f.factory, corev1.Resource("pods"), f.rest)
	}
	f.lister = f.factory.Core().V1().Pods().Lister()
	t.Cleanup(func() {
		close(f.stop)
		if f.started {
			f.original.Shutdown()
		}
	})
	return f
}

func (f *followFixture) waitReady() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e := f.cache.Wait(ctx); e != nil {
		f.t.Fatal(e)
	}
}
func followPoll(t testing.TB, fn func() (bool, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e := wait.PollUntilContextCancel(ctx, 5*time.Millisecond, true, func(context.Context) (bool, error) { return fn() }); e != nil {
		t.Fatal(e)
	}
}
func (f *followFixture) startInformer() {
	f.informer = f.factory.Core().V1().Pods().Informer()
	f.factory.Start(f.stop)
	f.started = true
	if !cache.WaitForCacheSync(f.stop, f.informer.HasSynced) {
		f.t.Fatal("Pod informer did not synchronize")
	}
}
func (f *followFixture) barrier(name, rv string) {
	followPoll(f.t, func() (bool, error) {
		p := &core.Pod{}
		e := f.cache.Get(context.Background(), "/pods/ns/"+name, apistorage.GetOptions{ResourceVersion: rv}, p)
		return e == nil && p.ResourceVersion == rv, nil
	})
	if f.informer != nil {
		followPoll(f.t, func() (bool, error) {
			o, ok, e := f.informer.GetStore().GetByKey("ns/" + name)
			return e == nil && ok && o.(*corev1.Pod).ResourceVersion == rv, nil
		})
	}
}
func (f *followFixture) create(name string, mf int) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{"fixture": "followup"}, Annotations: map[string]string{"sequence": "0"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "container", Image: "registry.k8s.io/pause:3.10"}}}}
	legacyscheme.Scheme.Default(p)
	if raw := readCostManagedFields(mf); len(raw) > 0 {
		p.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "bounded-fixture", Operation: metav1.ManagedFieldsOperationApply, APIVersion: "v1", FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: raw}}}
	}
	obj, e := legacyscheme.Scheme.ConvertToVersion(p, core.SchemeGroupVersion)
	if e != nil {
		f.t.Fatal(e)
	}
	created, e := f.rest.Create(request.WithNamespace(context.Background(), "ns"), obj, registryrest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if e != nil {
		f.t.Fatal(e)
	}
	external, e := legacyscheme.Scheme.ConvertToVersion(created, corev1.SchemeGroupVersion)
	if e != nil {
		f.t.Fatal(e)
	}
	return external.(*corev1.Pod)
}
func (f *followFixture) update(p *corev1.Pod, sequence int) *corev1.Pod {
	p = p.DeepCopy()
	p.Annotations["sequence"] = fmt.Sprint(sequence)
	out, e := f.client.CoreV1().Pods("ns").Update(context.Background(), p, metav1.UpdateOptions{FieldManager: "followup"})
	if e != nil {
		f.t.Fatal(e)
	}
	return out
}
func followJSON(t testing.TB, v any) string {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}
func followReceipt(t testing.TB, id string, data any) {
	t.Helper()
	t.Log("FOLLOWUP_RECEIPT " + id + " " + followJSON(t, data))
}
func followClear(p *corev1.Pod, trim bool) *corev1.Pod {
	p = p.DeepCopy()
	p.TypeMeta = metav1.TypeMeta{}
	if trim {
		p.ManagedFields = nil
	}
	return p
}
func followAssertSame(t testing.TB, a, b *corev1.Pod, trim bool) {
	t.Helper()
	if followJSON(t, followClear(a, trim)) != followJSON(t, followClear(b, trim)) {
		t.Fatalf("Pod parity differs: a=%s b=%s", followJSON(t, a), followJSON(t, b))
	}
}
