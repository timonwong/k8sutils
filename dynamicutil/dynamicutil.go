// Package dynamicutil contains utility functions, modified from controller-runtime
package dynamicutil

import (
	"context"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

var (
	unstructuredConverter = runtime.DefaultUnstructuredConverter
)

// OperationResult is the action result of a CreateOrUpdate call
type OperationResult string

const ( // They should complete the sentence "Deployment default/foo has been ..."
	// OperationResultNone means that the resource has not been changed
	OperationResultNone OperationResult = "unchanged"
	// OperationResultCreated means that a new resource is created
	OperationResultCreated OperationResult = "created"
	// OperationResultUpdated means that an existing resource is updated
	OperationResultUpdated OperationResult = "updated"
)

type Object interface {
	metav1.Object
	runtime.Object
}

func namespacedNameFromObject(obj metav1.Object) types.NamespacedName {
	return types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}
}

// mutate wraps a MutateFn and applies validation to its result
func mutate(f MutateFn, key types.NamespacedName, obj metav1.Object) error {
	if err := f(); err != nil {
		return err
	}
	newKey := namespacedNameFromObject(obj)
	if key != newKey {
		return fmt.Errorf("MutateFn cannot mutate object name and/or object namespace")
	}
	return nil
}

// MutateFn is a function which mutates the existing object into it's desired state.
type MutateFn func() error

// CreateOrUpdate creates or updates the given object in the Kubernetes
// cluster. The object's desired state must be reconciled with the existing
// state inside the passed in callback MutateFn.
//
// The MutateFn is called regardless of creating or updating an object.
//
// It returns the executed operation and an error.
func CreateOrUpdate(ctx context.Context, c dynamic.NamespaceableResourceInterface, obj Object, f MutateFn) (OperationResult, error) {
	var (
		err       error
		objUns    *unstructured.Unstructured
		resultUns *unstructured.Unstructured
	)

	key := namespacedNameFromObject(obj)
	cli := c.Namespace(key.Namespace)

	resultUns, err = cli.Get(ctx, key.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return OperationResultNone, err
		}

		if err = mutate(f, key, obj); err != nil {
			return OperationResultNone, err
		}

		if objUns, err = unstructuredFromObject(obj); err != nil {
			return OperationResultNone, err
		}
		if resultUns, err = cli.Create(ctx, objUns, metav1.CreateOptions{}); err != nil {
			return OperationResultNone, err
		}
		if err = objectFromUnstructured(resultUns, obj); err != nil {
			return OperationResultNone, err
		}
		return OperationResultCreated, nil
	}

	existing, err := newObject(obj)
	if err != nil {
		return OperationResultNone, err
	}
	if err = objectFromUnstructured(resultUns, existing); err != nil {
		return OperationResultNone, err
	}

	if err = mutate(f, key, obj); err != nil {
		return OperationResultNone, err
	}
	if equality.Semantic.DeepEqual(existing, obj) {
		return OperationResultNone, nil
	}

	if objUns, err = unstructuredFromObject(obj); err != nil {
		return OperationResultNone, err
	}
	if resultUns, err = cli.Update(ctx, objUns, metav1.UpdateOptions{}); err != nil {
		return OperationResultNone, err
	}
	if err = objectFromUnstructured(resultUns, obj); err != nil {
		return OperationResultNone, err
	}
	return OperationResultUpdated, nil
}

func newObject(obj runtime.Object) (runtime.Object, error) {
	t := reflect.TypeOf(obj)
	if t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("newObject requires a pointer to an object, got %v", t)
	}

	newObj := reflect.New(t.Elem()).Interface()
	return newObj.(runtime.Object), nil
}

func unstructuredFromObject(obj runtime.Object) (*unstructured.Unstructured, error) {
	uns, ok := obj.(*unstructured.Unstructured)
	if ok {
		return uns, nil
	}

	m, err := unstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}

	return &unstructured.Unstructured{Object: m}, nil
}

func objectFromUnstructured(uns *unstructured.Unstructured, obj runtime.Object) error {
	objUns, ok := obj.(*unstructured.Unstructured)
	if ok {
		objUns.Object = uns.Object
		return nil
	}

	return unstructuredConverter.FromUnstructured(uns.Object, obj)
}
