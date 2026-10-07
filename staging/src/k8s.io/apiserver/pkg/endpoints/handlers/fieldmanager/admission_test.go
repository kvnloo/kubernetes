/*
Copyright 2021 The Kubernetes Authors.

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

package fieldmanager_test

import (
	"context"
	_ "embed"
	"reflect"
	"testing"

	"sigs.k8s.io/structured-merge-diff/v7/fieldpath"
	"sigs.k8s.io/yaml"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"k8s.io/apiserver/pkg/warning"
)

//go:embed testdata/exemplar_pod.yaml
var exemplarPodYAML []byte

func TestAdmission(t *testing.T) {
	wrap := &mockAdmissionController{}
	ac := fieldmanager.NewManagedFieldsValidatingAdmissionController(wrap)
	now := metav1.Now()

	validFieldsV1 := metav1.FieldsV1{}
	raw, err := fieldpath.NewSet(fieldpath.MakePathOrDie("metadata", "labels", "test-label")).ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	validFieldsV1.SetRawBytes(raw)
	validManagedFieldsEntry := metav1.ManagedFieldsEntry{
		APIVersion: "v1",
		Operation:  metav1.ManagedFieldsOperationApply,
		Time:       &now,
		Manager:    "test",
		FieldsType: "FieldsV1",
		FieldsV1:   &validFieldsV1,
	}

	managedFieldsMutators := map[string]func(in metav1.ManagedFieldsEntry) (out metav1.ManagedFieldsEntry, shouldReset bool){
		"invalid APIVersion": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.APIVersion = ""
			return managedFields, true
		},
		"invalid Operation": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.Operation = "invalid operation"
			return managedFields, true
		},
		"invalid fieldsType": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.FieldsType = "invalid fieldsType"
			return managedFields, true
		},
		"invalid fieldsV1": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.FieldsV1 = metav1.NewFieldsV1("{invalid}")
			return managedFields, true
		},
		"invalid fieldsV1 SetRawBytes": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.FieldsV1.SetRawBytes([]byte("{invalid}"))
			return managedFields, true
		},
		"invalid manager": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.Manager = ""
			return managedFields, false
		},
	}

	mutationStyles := []struct {
		name      string
		admitWith func(metav1.ManagedFieldsEntry) admitFunc
	}{
		{name: "replaceSlice", admitWith: replaceManagedFields},
		{name: "overwriteElement", admitWith: overwriteManagedFieldsElement},
	}

	for name, mutate := range managedFieldsMutators {
		for _, style := range mutationStyles {
			t.Run(name+"/"+style.name, func(t *testing.T) {
				initial := *validManagedFieldsEntry.DeepCopy()
				validEntries := []metav1.ManagedFieldsEntry{*validManagedFieldsEntry.DeepCopy()}

				obj := &v1.ConfigMap{}
				obj.SetManagedFields([]metav1.ManagedFieldsEntry{initial})

				var mutated metav1.ManagedFieldsEntry
				var shouldReset bool
				wrap.admit = func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
					objectMeta, err := meta.Accessor(a.GetObject())
					if err != nil {
						return err
					}
					mutated, shouldReset = mutate(objectMeta.GetManagedFields()[0])
					return style.admitWith(mutated)(ctx, a, o)
				}

				attrs := admission.NewAttributesRecord(obj, obj, schema.GroupVersionKind{}, "default", "", schema.GroupVersionResource{}, "", admission.Update, nil, false, nil)
				if err := ac.(admission.MutationInterface).Admit(context.TODO(), attrs, nil); err != nil {
					t.Fatal(err)
				}

				if shouldReset && !reflect.DeepEqual(obj.GetManagedFields(), validEntries) {
					t.Fatalf("expected: \n%v\ngot:\n%v", validEntries, obj.GetManagedFields())
				}
				if !shouldReset && reflect.DeepEqual(obj.GetManagedFields(), validEntries) {
					t.Fatalf("expected: \n%v\ngot:\n%v", []metav1.ManagedFieldsEntry{mutated}, obj.GetManagedFields())
				}
			})
		}
	}
}

func TestAdmissionSkipsValidationWhenUnchanged(t *testing.T) {
	wrap := &mockAdmissionController{admit: func(context.Context, admission.Attributes, admission.ObjectInterfaces) error { return nil }}
	ac := fieldmanager.NewManagedFieldsValidatingAdmissionController(wrap)

	obj := &v1.ConfigMap{}
	obj.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "test", Operation: "invalid operation"}})

	rec := &warningRecorder{}
	ctx := warning.WithWarningRecorder(context.TODO(), rec)
	attrs := admission.NewAttributesRecord(obj, obj, schema.GroupVersionKind{}, "default", "", schema.GroupVersionResource{}, "", admission.Update, nil, false, nil)
	if err := ac.(admission.MutationInterface).Admit(ctx, attrs, nil); err != nil {
		t.Fatal(err)
	}
	if len(rec.warnings) != 0 {
		t.Errorf("managedFields were revalidated although admission did not change them: %v", rec.warnings)
	}
}

// Research fixture for PR #142320: equivalent managedFields represented with fresh nested pointers.
func TestAdmissionSkipsValidationForEquivalentManagedFieldsRepresentations(t *testing.T) {
	fields := metav1.NewFieldsV1(`{"f:metadata":{"f:labels":{"f:test":{}}}}`)
	entry := metav1.ManagedFieldsEntry{
		APIVersion: "v1",
		Operation:  "invalid operation",
		Manager:    "test",
		FieldsType: "FieldsV1",
		FieldsV1:   fields,
	}

	newUnstructured := func() *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name": "test",
			},
		}}
	}
	for _, tc := range []struct {
		name      string
		newObject func() runtime.Object
	}{
		{name: "typed", newObject: func() runtime.Object { return &v1.ConfigMap{} }},
		{name: "unstructured", newObject: func() runtime.Object { return newUnstructured() }},
	} {
		for _, reconstruct := range []bool{false, true} {
			style := "unchanged"
			if reconstruct {
				style = "reconstructed"
			}
			t.Run(tc.name+"/"+style, func(t *testing.T) {
				obj := tc.newObject()
				objectMeta, err := meta.Accessor(obj)
				if err != nil {
					t.Fatal(err)
				}
				objectMeta.SetManagedFields([]metav1.ManagedFieldsEntry{*entry.DeepCopy()})
				wrap := &mockAdmissionController{admit: func(_ context.Context, a admission.Attributes, _ admission.ObjectInterfaces) error {
					if reconstruct {
						copiedMeta, err := meta.Accessor(a.GetObject().DeepCopyObject())
						if err != nil {
							return err
						}
						objectMeta.SetManagedFields(copiedMeta.GetManagedFields())
					}
					return nil
				}}
				ac := fieldmanager.NewManagedFieldsValidatingAdmissionController(wrap)
				rec := &warningRecorder{}
				ctx := warning.WithWarningRecorder(context.TODO(), rec)
				attrs := admission.NewAttributesRecord(obj, obj, schema.GroupVersionKind{}, "default", "", schema.GroupVersionResource{}, "", admission.Update, nil, false, nil)
				if err := ac.(admission.MutationInterface).Admit(ctx, attrs, nil); err != nil {
					t.Fatal(err)
				}
				if len(rec.warnings) != 0 {
					t.Fatalf("expected equivalent managedFields to skip validation, got warnings: %v", rec.warnings)
				}
			})
		}
	}

	t.Run("changed unstructured managedFields are still validated", func(t *testing.T) {
		validEntry := *entry.DeepCopy()
		validEntry.Operation = metav1.ManagedFieldsOperationApply
		obj := newUnstructured()
		obj.SetManagedFields([]metav1.ManagedFieldsEntry{validEntry})

		wrap := &mockAdmissionController{admit: replaceManagedFields(entry)}
		ac := fieldmanager.NewManagedFieldsValidatingAdmissionController(wrap)
		rec := &warningRecorder{}
		ctx := warning.WithWarningRecorder(context.TODO(), rec)
		attrs := admission.NewAttributesRecord(obj, obj, schema.GroupVersionKind{}, "default", "", schema.GroupVersionResource{}, "", admission.Update, nil, false, nil)
		if err := ac.(admission.MutationInterface).Admit(ctx, attrs, nil); err != nil {
			t.Fatal(err)
		}
		if len(rec.warnings) != 1 {
			t.Fatalf("expected changed invalid managedFields to be validated, got %d warning(s)", len(rec.warnings))
		}
		got := obj.GetManagedFields()
		if len(got) != 1 || got[0].Operation != metav1.ManagedFieldsOperationApply {
			t.Fatalf("expected invalid managedFields mutation to be restored, got %#v", got)
		}
	})
}

func BenchmarkAdmission(b *testing.B) {
	pod := &v1.Pod{}
	if err := yaml.Unmarshal(exemplarPodYAML, pod); err != nil {
		b.Fatal(err)
	}
	entries := pod.ManagedFields
	// Same content, distinct pointers. Keep this separate from genuinely changed values.
	copied := pod.DeepCopy().ManagedFields
	changedA := pod.DeepCopy().ManagedFields
	changedA[0].Manager += "-a"
	changedB := pod.DeepCopy().ManagedFields
	changedB[0].Manager += "-b"

	for _, tc := range []struct {
		name  string
		admit admitFunc
	}{
		{
			name:  "unchanged",
			admit: func(context.Context, admission.Attributes, admission.ObjectInterfaces) error { return nil },
		},
		{
			name: "replaced",
			admit: func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
				objectMeta, err := meta.Accessor(a.GetObject())
				if err != nil {
					return err
				}
				if &objectMeta.GetManagedFields()[0] == &entries[0] {
					objectMeta.SetManagedFields(copied)
				} else {
					objectMeta.SetManagedFields(entries)
				}
				return nil
			},
		},
		{
			name: "changed",
			admit: func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
				objectMeta, err := meta.Accessor(a.GetObject())
				if err != nil {
					return err
				}
				if objectMeta.GetManagedFields()[0].Manager == changedA[0].Manager {
					objectMeta.SetManagedFields(changedB)
				} else {
					objectMeta.SetManagedFields(changedA)
				}
				return nil
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ac := fieldmanager.NewManagedFieldsValidatingAdmissionController(&mockAdmissionController{admit: tc.admit})
			obj := pod.DeepCopy()
			obj.SetManagedFields(entries)
			attrs := admission.NewAttributesRecord(obj, obj, schema.GroupVersionKind{}, "default", "", schema.GroupVersionResource{}, "", admission.Update, nil, false, nil)
			b.ReportAllocs()
			for b.Loop() {
				if err := ac.(admission.MutationInterface).Admit(context.TODO(), attrs, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type admitFunc = func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error

type warningRecorder struct {
	warnings []string
}

func (r *warningRecorder) AddWarning(_, text string) {
	r.warnings = append(r.warnings, text)
}

func overwriteManagedFieldsElement(to metav1.ManagedFieldsEntry) admitFunc {
	return func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
		objectMeta, err := meta.Accessor(a.GetObject())
		if err != nil {
			return err
		}
		objectMeta.GetManagedFields()[0] = to
		return nil
	}
}

func replaceManagedFields(with metav1.ManagedFieldsEntry) admitFunc {
	return func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
		objectMeta, err := meta.Accessor(a.GetObject())
		if err != nil {
			return err
		}
		objectMeta.SetManagedFields([]metav1.ManagedFieldsEntry{with})
		return nil
	}
}

type mockAdmissionController struct {
	admit admitFunc
}

func (c *mockAdmissionController) Handles(operation admission.Operation) bool {
	return true
}

func (c *mockAdmissionController) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	return c.admit(ctx, a, o)
}
