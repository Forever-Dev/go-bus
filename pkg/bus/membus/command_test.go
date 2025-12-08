package membus_test

import (
	"context"
	"sync"

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

	BeforeEach(func(ctx context.Context) {
		msg = bus.NewBasicMessage(uuid.New().String(), []byte(faker.Word()))
		Expect(msg).NotTo(BeNil())

		messageBus = membus.New(ctx, membus.NewDefaultConfig())
		Expect(messageBus).NotTo(BeNil())
	})

	Describe("Send", func() {
		It("returns an error when no handler is registered", func(ctx context.Context) {
			err := messageBus.Send(ctx, msg)
			Expect(err).To(MatchError(bus.ErrNoHandler))
		})

		It("sends a message to the registered handler", func(ctx context.Context) {
			handled := false

			err := messageBus.Handle(msg, func(ctx context.Context, m bus.Message) error {
				handled = true
				Expect(m).To(Equal(msg))
				return nil
			})
			Expect(err).To(Succeed())

			err = messageBus.Send(ctx, msg)
			Expect(err).To(Succeed())
			Expect(handled).To(BeTrue())
		})

		It("prioritizes group handlers over individual handlers", func(ctx context.Context) {
			handledByGroup := false
			handledByIndividual := false

			groupId := faker.Word()
			handlerId := faker.Word()

			err := messageBus.GroupHandle(handlerId, groupId, msg, func(ctx context.Context, m bus.Message) error {
				handledByGroup = true
				return nil
			})
			Expect(err).To(Succeed())

			err = messageBus.Handle(msg, func(ctx context.Context, m bus.Message) error {
				handledByIndividual = true
				return nil
			})
			Expect(err).To(Succeed())

			err = messageBus.Send(ctx, msg)
			Expect(err).To(Succeed())
			Expect(handledByGroup).To(BeTrue())
			Expect(handledByIndividual).To(BeFalse())
		})

		It("warns on no available group members", func(ctx context.Context) {
			groupId := faker.Word()
			handlerId := faker.Word()

			err := messageBus.GroupHandle(handlerId, groupId, msg, func(ctx context.Context, m bus.Message) error {
				return nil
			})
			Expect(err).To(Succeed())

			// Remove the only member to simulate no available members
			err = messageBus.GroupRemoveHandler(handlerId, groupId, msg)
			Expect(err).To(Succeed())

			err = messageBus.Send(ctx, msg)
			Expect(err).NotTo(Succeed())
		})
	})

	Describe("Handle", func() {
		It("registers an individual message handler", func() {
			err := messageBus.Handle(msg, func(ctx context.Context, m bus.Message) error {
				return nil
			})
			Expect(err).To(Succeed())
		})

		It("overwrites an existing handler when registering a new one", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			handler1Called := false
			handler2Called := false
			err := messageBus.Handle(msg, func(ctx context.Context, m bus.Message) error {
				wg.Done()
				handler1Called = true
				return nil
			})
			Expect(err).To(Succeed())

			err = messageBus.Handle(msg, func(ctx context.Context, m bus.Message) error {
				handler2Called = true
				wg.Done()
				return nil
			})
			Expect(err).To(Succeed())

			messageBus.Send(context.Background(), msg)
			wg.Wait()
			Expect(handler1Called).To(BeFalse())
			Expect(handler2Called).To(BeTrue())
		})
	})

	Describe("GroupHandle", func() {
		It("registers a group message handler", func() {
			groupId := faker.Word()
			handlerId := faker.Word()

			err := messageBus.GroupHandle(handlerId, groupId, msg, func(ctx context.Context, m bus.Message) error {
				return nil
			})
			Expect(err).To(Succeed())
		})

		It("returns an error when adding a duplicate group member", func() {
			groupId := faker.Word()
			handlerId := faker.Word()

			err := messageBus.GroupHandle(handlerId, groupId, msg, func(ctx context.Context, m bus.Message) error {
				return nil
			})
			Expect(err).To(Succeed())

			err = messageBus.GroupHandle(handlerId, groupId, msg, func(ctx context.Context, m bus.Message) error {
				return nil
			})
			Expect(err).ToNot(Succeed())
		})
	})

	Describe("GroupRemoveHandler", func() {
		It("removes a group message handler", func() {
			groupId := faker.Word()
			handlerId := faker.Word()

			err := messageBus.GroupHandle(handlerId, groupId, msg, func(ctx context.Context, m bus.Message) error {
				return nil
			})
			Expect(err).To(Succeed())

			err = messageBus.GroupRemoveHandler(handlerId, groupId, msg)
			Expect(err).To(Succeed())

			// Now sending the message should result in no handler found
			err = messageBus.Send(context.Background(), msg)
			Expect(err).To(MatchError(bus.ErrNoHandler))
		})

		It("returns an error when removing a non-existent group member", func() {
			groupId := faker.Word()
			handlerId := faker.Word()

			err := messageBus.GroupRemoveHandler(handlerId, groupId, msg)
			Expect(err).ToNot(Succeed())
		})
	})
})
