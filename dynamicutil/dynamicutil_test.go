package dynamicutil

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/banzaicloud/k8s-objectmatcher/patch"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

var (
	deploymentGVK = schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}
	deploymentGVR = schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "deployments",
	}
)

var _ = Describe("Dynamicutil", func() {
	Describe("CreateOrUpdate", func() {
		var deploymentCli dynamic.NamespaceableResourceInterface
		var deploy *appsv1.Deployment
		var deployUns *unstructured.Unstructured
		var deplSpec appsv1.DeploymentSpec
		var deplKey types.NamespacedName
		var specrActual MutateFn
		var specrUns MutateFn

		BeforeEach(func() {
			deploymentCli = dynClient.Resource(deploymentGVR)

			deploy = &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("deploy-%d", rand.Int31()),
					Namespace: "default",
				},
			}

			deployUns = &unstructured.Unstructured{}
			deployUns.SetName(deploy.Name)
			deployUns.SetNamespace(deploy.Namespace)
			deployUns.SetGroupVersionKind(deploymentGVK)

			deplSpec = appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"foo": "bar"},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"foo": "bar",
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "busybox",
								Image: "busybox",
							},
						},
					},
				},
			}

			deplKey = types.NamespacedName{
				Name:      deployUns.GetName(),
				Namespace: deployUns.GetNamespace(),
			}

			specrActual = deploymentSpecr(deploy, deplSpec)
			specrUns = deploymentSpecr(deployUns, deplSpec)
		})

		It("creates a new object if one doesn't exists (actual object)", func() {
			op, err := CreateOrUpdate(context.TODO(), deploymentCli, deploy, CreateOrUpdateOptions{
				MutateFunc: specrActual,
			})

			By("returning no error")
			Expect(err).NotTo(HaveOccurred())

			By("returning OperationResultCreated")
			Expect(op).To(BeEquivalentTo(OperationResultCreated))

			By("actually having the deployment created")
			fetchedUns, err := deploymentCli.Namespace(deplKey.Namespace).Get(context.TODO(), deplKey.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("being mutated by MutateFn")
			fetched := &appsv1.Deployment{}
			err = unstructuredConverter.FromUnstructured(fetchedUns.Object, fetched)
			Expect(err).NotTo(HaveOccurred())
			Expect(fetched.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(fetched.Spec.Template.Spec.Containers[0].Name).To(Equal(deplSpec.Template.Spec.Containers[0].Name))
			Expect(fetched.Spec.Template.Spec.Containers[0].Image).To(Equal(deplSpec.Template.Spec.Containers[0].Image))

			By("returned should equal to fetched")
			Expect(fetched).To(Equal(deploy))
		})

		It("creates a new object if one doesn't exists (unstructured)", func() {
			op, err := CreateOrUpdate(context.TODO(), deploymentCli, deployUns, CreateOrUpdateOptions{
				MutateFunc: specrUns,
			})

			By("returning no error")
			Expect(err).NotTo(HaveOccurred())

			By("returning OperationResultCreated")
			Expect(op).To(BeEquivalentTo(OperationResultCreated))

			By("actually having the deployment created")
			fetchedUns, err := deploymentCli.Namespace(deplKey.Namespace).Get(context.TODO(), deplKey.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())

			By("being mutated by MutateFn")
			fetched := &appsv1.Deployment{}
			err = unstructuredConverter.FromUnstructured(fetchedUns.Object, fetched)
			Expect(err).NotTo(HaveOccurred())
			Expect(fetched.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(fetched.Spec.Template.Spec.Containers[0].Name).To(Equal(deplSpec.Template.Spec.Containers[0].Name))
			Expect(fetched.Spec.Template.Spec.Containers[0].Image).To(Equal(deplSpec.Template.Spec.Containers[0].Image))

			By("returned should equal to fetched")
			m, err := unstructuredConverter.ToUnstructured(fetched)
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(deployUns.Object))
		})

		It("updates existing object (actual object)", func() {
			var scale int32 = 2
			op, err := CreateOrUpdate(context.TODO(), deploymentCli, deploy, CreateOrUpdateOptions{
				MutateFunc: specrActual,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(op).To(BeEquivalentTo(OperationResultCreated))

			op, err = CreateOrUpdate(context.TODO(), deploymentCli, deploy, CreateOrUpdateOptions{
				MutateFunc: deploymentScaler(deploy, scale),
			})
			By("returning no error")
			Expect(err).NotTo(HaveOccurred())

			By("returning OperationResultUpdated")
			Expect(op).To(BeEquivalentTo(OperationResultUpdated))

			By("actually having the deployment scaled")
			fetchedUns, err := deploymentCli.Namespace(deplKey.Namespace).Get(context.TODO(), deplKey.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())

			fetched := &appsv1.Deployment{}
			err = unstructuredConverter.FromUnstructured(fetchedUns.Object, fetched)
			Expect(err).NotTo(HaveOccurred())
			Expect(*fetched.Spec.Replicas).To(Equal(scale))

			By("returned should equal to fetched")
			Expect(fetched).To(Equal(deploy))
		})

		It("updates existing object (unstructured)", func() {
			var scale int32 = 2
			op, err := CreateOrUpdate(context.TODO(), deploymentCli, deployUns, CreateOrUpdateOptions{
				MutateFunc: specrUns,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(op).To(BeEquivalentTo(OperationResultCreated))

			op, err = CreateOrUpdate(context.TODO(), deploymentCli, deployUns, CreateOrUpdateOptions{
				MutateFunc: deploymentScalerUnstructured(deployUns, scale),
			})
			By("returning no error")
			Expect(err).NotTo(HaveOccurred())

			By("returning OperationResultUpdated")
			Expect(op).To(BeEquivalentTo(OperationResultUpdated))

			By("actually having the deployment scaled")
			fetchedUns, err := deploymentCli.Namespace(deplKey.Namespace).Get(context.TODO(), deplKey.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())

			fetched := &appsv1.Deployment{}
			err = unstructuredConverter.FromUnstructured(fetchedUns.Object, fetched)
			Expect(err).NotTo(HaveOccurred())
			Expect(*fetched.Spec.Replicas).To(Equal(scale))

			By("returned should equal to fetched")
			m, err := unstructuredConverter.ToUnstructured(fetched)
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(deployUns.Object))
		})

		It("updates only changed objects", func() {
			deployClone := deploy.DeepCopy()
			op, err := CreateOrUpdate(context.TODO(), deploymentCli, deploy, CreateOrUpdateOptions{
				MutateFunc: specrActual,
			})
			Expect(op).To(BeEquivalentTo(OperationResultCreated))
			Expect(err).NotTo(HaveOccurred())

			op, err = CreateOrUpdate(context.TODO(), deploymentCli, deployClone, CreateOrUpdateOptions{
				MutateFunc: deploymentSpecr(deployClone, deplSpec),
			})
			By("returning no error")
			Expect(err).NotTo(HaveOccurred())

			By("returning OperationResultNone")
			Expect(op).To(BeEquivalentTo(OperationResultNone))
		})

		It("won't update for excluded fields", func() {
			op, err := CreateOrUpdate(context.TODO(), deploymentCli, deploy, CreateOrUpdateOptions{
				MutateFunc: specrActual,
			})
			Expect(op).To(BeEquivalentTo(OperationResultCreated))
			Expect(err).NotTo(HaveOccurred())

			op, err = CreateOrUpdate(context.TODO(), deploymentCli, deploy, CreateOrUpdateOptions{
				MutateFunc: func() error {
					deploy.Status.UnavailableReplicas = 32
					return nil
				},
				PatchCalculateOptions: []patch.CalculateOption{
					patch.IgnoreStatusFields(),
				},
			})
			By("returning no error")
			Expect(err).NotTo(HaveOccurred())

			By("returning OperationResultNone")
			Expect(op).To(BeEquivalentTo(OperationResultNone))
		})

		It("errors when MutateFn changes object name on creation", func() {
			op, err := CreateOrUpdate(context.TODO(), deploymentCli, deployUns, CreateOrUpdateOptions{
				MutateFunc: func() error {
					Expect(specrUns()).To(Succeed())
					return deploymentRenamer(deployUns)()
				},
			})

			By("returning error")
			Expect(err).To(HaveOccurred())

			By("returning OperationResultNone")
			Expect(op).To(BeEquivalentTo(OperationResultNone))
		})

		It("errors when MutateFn renames an object", func() {
			op, err := CreateOrUpdate(context.TODO(), deploymentCli, deployUns, CreateOrUpdateOptions{
				MutateFunc: specrUns,
			})

			Expect(op).To(BeEquivalentTo(OperationResultCreated))
			Expect(err).NotTo(HaveOccurred())

			op, err = CreateOrUpdate(context.TODO(), deploymentCli, deployUns, CreateOrUpdateOptions{
				MutateFunc: deploymentRenamer(deployUns),
			})

			By("returning error")
			Expect(err).To(HaveOccurred())

			By("returning OperationResultNone")
			Expect(op).To(BeEquivalentTo(OperationResultNone))
		})

		It("errors when object namespace changes", func() {
			op, err := CreateOrUpdate(context.TODO(), deploymentCli, deployUns, CreateOrUpdateOptions{
				MutateFunc: specrUns,
			})

			Expect(op).To(BeEquivalentTo(OperationResultCreated))
			Expect(err).NotTo(HaveOccurred())

			op, err = CreateOrUpdate(context.TODO(), deploymentCli, deployUns, CreateOrUpdateOptions{
				MutateFunc: deploymentNamespaceChanger(deployUns),
			})

			By("returning error")
			Expect(err).To(HaveOccurred())

			By("returning OperationResultNone")
			Expect(op).To(BeEquivalentTo(OperationResultNone))
		})

		It("aborts immediately if there was an error initially retrieving the object", func() {
			op, err := CreateOrUpdate(context.TODO(),
				namespaceableErrorReader{deploymentCli},
				deployUns,
				CreateOrUpdateOptions{
					MutateFunc: func() error {
						Fail("Mutation method should not run")
						return nil
					},
				})

			Expect(op).To(BeEquivalentTo(OperationResultNone))
			Expect(err).To(HaveOccurred())
		})
	})
})

func deploymentRenamer(deploy *unstructured.Unstructured) MutateFn {
	return func() error {
		deploy.SetName(fmt.Sprintf("%s-1", deploy.GetName()))
		return nil
	}
}

func deploymentNamespaceChanger(deploy *unstructured.Unstructured) MutateFn {
	return func() error {
		deploy.SetNamespace(fmt.Sprintf("%s-1", deploy.GetNamespace()))
		return nil
	}
}

func deploymentSpecr(deploy runtime.Object, spec appsv1.DeploymentSpec) MutateFn {
	deployObj, ok := deploy.(*appsv1.Deployment)
	if ok {
		return func() error {
			deployObj.Spec = spec
			return nil
		}
	}

	deployUns := deploy.(*unstructured.Unstructured)
	return func() error {
		m, err := unstructuredConverter.ToUnstructured(&spec)
		if err != nil {
			return err
		}
		return unstructured.SetNestedMap(deployUns.Object, m, "spec")
	}
}

func deploymentScaler(deploy runtime.Object, replicas int32) MutateFn {
	deployObj, ok := deploy.(*appsv1.Deployment)
	if ok {
		return func() error {
			deployObj.Spec.Replicas = &replicas
			return nil
		}
	}

	deployUns := deploy.(*unstructured.Unstructured)
	return func() error {
		intReplicas := int64(replicas)
		return unstructured.SetNestedField(deployUns.Object, intReplicas, "spec", "replicas")
	}
}

func deploymentScalerUnstructured(deploy *unstructured.Unstructured, replicas int32) MutateFn {
	return func() error {
		intReplicas := int64(replicas)
		return unstructured.SetNestedField(deploy.Object, intReplicas, "spec", "replicas")
	}
}

type namespaceableErrorReader struct {
	dynamic.NamespaceableResourceInterface
}

func (e namespaceableErrorReader) Namespace(string) dynamic.ResourceInterface {
	return &errorReader{e}
}

type errorReader struct {
	dynamic.ResourceInterface
}

func (e errorReader) Get(ctx context.Context, name string, options metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return nil, fmt.Errorf("unexpected error")
}
