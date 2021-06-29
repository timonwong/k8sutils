package dynamicutil

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/dynamic"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestDynamicUtil(t *testing.T) {
	RegisterFailHandler(Fail)
	suiteName := "Dynamicutil Suite"
	RunSpecs(t, suiteName)
}

var t *envtest.Environment
var cfg *rest.Config
var dynClient dynamic.Interface

var _ = BeforeSuite(func() {
	var err error

	t = &envtest.Environment{}

	cfg, err = t.Start()
	Expect(err).NotTo(HaveOccurred())

	dynClient, err = dynamic.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	Expect(t.Stop()).To(Succeed())
})
