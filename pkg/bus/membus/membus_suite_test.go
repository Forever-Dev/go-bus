package membus_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMembus(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Membus Suite")
}
