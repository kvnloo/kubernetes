/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package direct

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	core "k8s.io/kubernetes/pkg/apis/core"
)

var conversionBenchmarkSink runtime.Object

func BenchmarkPodConversionCopyBoundary(b *testing.B) {
	gv := corev1.SchemeGroupVersion
	pod := benchmarkPod(24)

	b.Run("current-safe-ConvertToVersion", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := legacyscheme.Scheme.ConvertToVersion(pod, gv)
			if err != nil {
				b.Fatal(err)
			}
			out.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
			conversionBenchmarkSink = out
		}
	})

	b.Run("safe-DeepCopy-plus-UnsafeConvertToVersion", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := legacyscheme.Scheme.UnsafeConvertToVersion(pod.DeepCopyObject(), gv)
			if err != nil {
				b.Fatal(err)
			}
			out.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
			conversionBenchmarkSink = out
		}
	})

	b.Run("unsafe-no-copy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := legacyscheme.Scheme.UnsafeConvertToVersion(pod, gv)
			if err != nil {
				b.Fatal(err)
			}
			out.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
			conversionBenchmarkSink = out
		}
	})
}

func BenchmarkPodListConversionCopyBoundary(b *testing.B) {
	gv := corev1.SchemeGroupVersion
	list := &core.PodList{Items: make([]core.Pod, 100)}
	for i := range list.Items {
		list.Items[i] = *benchmarkPod(8)
		list.Items[i].Name = fmt.Sprintf("pod-%d", i)
	}

	b.Run("current-safe-ConvertToVersion", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := legacyscheme.Scheme.ConvertToVersion(list, gv)
			if err != nil {
				b.Fatal(err)
			}
			out.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
			conversionBenchmarkSink = out
		}
	})

	b.Run("safe-DeepCopy-plus-UnsafeConvertToVersion", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := legacyscheme.Scheme.UnsafeConvertToVersion(list.DeepCopyObject(), gv)
			if err != nil {
				b.Fatal(err)
			}
			out.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
			conversionBenchmarkSink = out
		}
	})

	b.Run("unsafe-no-copy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := legacyscheme.Scheme.UnsafeConvertToVersion(list, gv)
			if err != nil {
				b.Fatal(err)
			}
			out.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
			conversionBenchmarkSink = out
		}
	})
}

func benchmarkPod(envCount int) *core.Pod {
	p := mutationTestPod()
	p.Spec.Containers[0].Env = make([]core.EnvVar, envCount)
	for i := range p.Spec.Containers[0].Env {
		p.Spec.Containers[0].Env[i] = core.EnvVar{
			Name:  fmt.Sprintf("KEY_%d", i),
			Value: fmt.Sprintf("VALUE_%d", i),
		}
	}
	return p
}
