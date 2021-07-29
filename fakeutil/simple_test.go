package fakeutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	testGroup      = "testgroup"
	testVersion    = "testversion"
	testResource   = "testkinds"
	testNamespace  = "testns"
	testName       = "testname"
	testKind       = "TestKind"
	testAPIVersion = "testgroup/testversion"
)

// This test ensures list works when the fake dynamic client is seeded with a typed scheme and
// unstructured type fixtures
func TestListWithUnstructuredObjectsAndTypedScheme(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}
	gvk := gvr.GroupVersion().WithKind(testKind)

	listGVK := gvk
	listGVK.Kind += "List"

	u := unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetName("name")
	u.SetNamespace("namespace")

	typedScheme := runtime.NewScheme()
	typedScheme.AddKnownTypeWithName(gvk, &mockResource{})
	typedScheme.AddKnownTypeWithName(listGVK, &mockResourceList{})

	client := NewSimpleDynamicClient(typedScheme, &u)
	list, err := client.Resource(gvr).Namespace("namespace").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err, "error listing")

	expectedList := &unstructured.UnstructuredList{}
	expectedList.SetGroupVersionKind(listGVK)
	expectedList.SetResourceVersion("") // by product of the fake setting resource version
	expectedList.Items = append(expectedList.Items, u)

	assert.Equal(t, expectedList, list)
}

func TestListWithNoFixturesAndTypedScheme(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}
	gvk := gvr.GroupVersion().WithKind(testKind)

	listGVK := gvk
	listGVK.Kind += "List"

	typedScheme := runtime.NewScheme()
	typedScheme.AddKnownTypeWithName(gvk, &mockResource{})
	typedScheme.AddKnownTypeWithName(listGVK, &mockResourceList{})

	client := NewSimpleDynamicClient(typedScheme)
	list, err := client.Resource(gvr).Namespace("namespace").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err, "error listing")

	expectedList := &unstructured.UnstructuredList{}
	expectedList.SetGroupVersionKind(listGVK)
	expectedList.SetResourceVersion("") // by product of the fake setting resource version

	assert.Equal(t, expectedList, list)
}

// This test ensures list works when the dynamic client is seeded with an empty scheme and
// unstructured typed fixtures
func TestListWithNoScheme(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}
	gvk := gvr.GroupVersion().WithKind(testKind)

	listGVK := gvk
	listGVK.Kind += "List"

	u := unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetName("name")
	u.SetNamespace("namespace")

	emptyScheme := runtime.NewScheme()

	client := NewSimpleDynamicClient(emptyScheme, &u)
	list, err := client.Resource(gvr).Namespace("namespace").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err, "error listing")

	expectedList := &unstructured.UnstructuredList{}
	expectedList.SetGroupVersionKind(listGVK)
	expectedList.SetResourceVersion("") // by product of the fake setting resource version
	expectedList.Items = append(expectedList.Items, u)

	assert.Equal(t, expectedList, list)
}

// This test ensures list works when the dynamic client is seeded with an empty scheme and
// unstructured typed fixtures
func TestListWithTypedFixtures(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}
	gvk := gvr.GroupVersion().WithKind(testKind)

	listGVK := gvk
	listGVK.Kind += "List"

	r := mockResource{}
	r.SetGroupVersionKind(gvk)
	r.SetName("name")
	r.SetNamespace("namespace")

	u := unstructured.Unstructured{}
	u.SetGroupVersionKind(r.GetObjectKind().GroupVersionKind())
	u.SetName(r.GetName())
	u.SetNamespace(r.GetNamespace())
	// Needed see: https://github.com/kubernetes/kubernetes/issues/67610
	unstructured.SetNestedField(u.Object, nil, "metadata", "creationTimestamp")

	typedScheme := runtime.NewScheme()
	typedScheme.AddKnownTypeWithName(gvk, &mockResource{})
	typedScheme.AddKnownTypeWithName(listGVK, &mockResourceList{})

	client := NewSimpleDynamicClient(typedScheme, &r)
	list, err := client.Resource(gvr).Namespace("namespace").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err, "error listing")

	expectedList := &unstructured.UnstructuredList{}
	expectedList.SetGroupVersionKind(listGVK)
	expectedList.SetResourceVersion("") // by product of the fake setting resource version
	expectedList.Items = []unstructured.Unstructured{u}

	assert.Equal(t, expectedList, list)
}

type (
	mockResource struct {
		metav1.TypeMeta   `json:",inline"`
		metav1.ObjectMeta `json:"metadata"`
	}
	mockResourceList struct {
		metav1.TypeMeta `json:",inline"`
		metav1.ListMeta `json:"metadata"`

		Items []mockResource
	}
)

func (l *mockResourceList) DeepCopyObject() runtime.Object {
	o := *l
	return &o
}

func (r *mockResource) DeepCopyObject() runtime.Object {
	o := *r
	return &o
}

var _ runtime.Object = (*mockResource)(nil)
var _ runtime.Object = (*mockResourceList)(nil)
