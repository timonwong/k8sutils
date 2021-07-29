package fakeutil

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

// NewSimpleDynamicClient creates fake dynamic client which addresses kubernetes/client-go#914
// Ported from this PR: https://github.com/kubernetes/kubernetes/pull/102928
func NewSimpleDynamicClient(scheme *runtime.Scheme, objects ...runtime.Object) *fake.FakeDynamicClient {
	unstructuredScheme := runtime.NewScheme()
	for gvk := range scheme.AllKnownTypes() {
		if unstructuredScheme.Recognizes(gvk) {
			continue
		}
		if strings.HasSuffix(gvk.Kind, "List") {
			unstructuredScheme.AddKnownTypeWithName(gvk, &unstructured.UnstructuredList{})
			continue
		}
		unstructuredScheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	}

	objects, err := convertObjectsToUnstructured(scheme, objects)
	if err != nil {
		panic(err)
	}

	for _, obj := range objects {
		gvk := obj.GetObjectKind().GroupVersionKind()
		if !unstructuredScheme.Recognizes(gvk) {
			unstructuredScheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		}
		gvk.Kind += "List"
		if !unstructuredScheme.Recognizes(gvk) {
			unstructuredScheme.AddKnownTypeWithName(gvk, &unstructured.UnstructuredList{})
		}
	}

	return fake.NewSimpleDynamicClientWithCustomListKinds(unstructuredScheme, nil, objects...)
}

func convertObjectsToUnstructured(s *runtime.Scheme, objs []runtime.Object) ([]runtime.Object, error) {
	ul := make([]runtime.Object, 0, len(objs))
	for _, obj := range objs {
		u, err := convertToUnstructured(s, obj)
		if err != nil {
			return nil, err
		}

		ul = append(ul, u)
	}

	return ul, nil
}

func convertToUnstructured(s *runtime.Scheme, obj runtime.Object) (runtime.Object, error) {
	var (
		err error
		u   unstructured.Unstructured
	)

	u.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to unstructured: %w", err)
	}

	gvk := u.GroupVersionKind()
	if gvk.Group == "" || gvk.Kind == "" {
		gvks, _, err := s.ObjectKinds(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to convert to unstructured - unable to get GVK %w", err)
		}

		u.SetGroupVersionKind(gvks[0])
	}
	return &u, nil
}
