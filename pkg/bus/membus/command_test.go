package membus_test

import (
	"context"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/forever-dev/go-bus/pkg/bus"
	"github.com/forever-dev/go-bus/pkg/bus/membus"
)

var _ = Describe("Bus.Message", func() {
	var (
		messageBus bus.MessageBus
		msg        bus.Message
	)

	BeforeEach(func() {
		msg = bus.NewBasicMessage(uuid.New().String(), []byte(faker.Word()))
		Expect(msg).NotTo(BeNil())

		messageBus = membus.New(membus.NewConfig())
		Expect(messageBus).NotTo(BeNil())
	})

	Describe("Send", func() {
		It("sends a message to the bus", func(ctx context.Context) {
			Expect(func() {
				messageBus.Send(ctx, msg)
			}).NotTo(Panic())
		})
	})

})
