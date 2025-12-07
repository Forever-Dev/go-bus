package membus_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/forever-dev/go-bus/pkg/bus"
	"github.com/forever-dev/go-bus/pkg/bus/membus"
)

var _ = Describe("Bus.Event", func() {
	var (
		messageBus bus.MessageBus
		msg        bus.Message

		eventType string
	)

	BeforeEach(func() {
		eventType = faker.Word()

		msg = bus.NewBasicMessage(uuid.New().String(), []byte(faker.Word()))
		Expect(msg).NotTo(BeNil())

		messageBus = membus.New(membus.NewConfig())
		Expect(messageBus).NotTo(BeNil())

	})

	Describe("Publish", func() {
		It("publishes an event to the bus", func(ctx context.Context) {
			Expect(func() {
				messageBus.Publish(ctx, eventType, msg)
			}).NotTo(Panic())
		})
	})

	Describe("Subscribe", func() {
		It("subscribes to an event type", func(ctx context.Context) {
			handler := func(ctx context.Context, msg bus.Message) error {
				return nil
			}

			Expect(func() {
				messageBus.Subscribe(eventType, uuid.New().String(), handler)
			}).NotTo(Panic())
		})

		It("should work concurrently", func(ctx context.Context) {
			numSubscribers := 100
			errs := make(chan error, numSubscribers)

			for range numSubscribers {
				go func() {
					handler := func(ctx context.Context, msg bus.Message) error {
						return nil
					}

					err := messageBus.Subscribe(eventType, uuid.New().String(), handler)
					errs <- err
				}()
			}

			halfway := numSubscribers / 2
			for range halfway {
				go func() {
					err := <-errs
					Expect(err).To(Succeed())
				}()
			}
			for i := halfway; i < numSubscribers; i++ {
				err := <-errs
				Expect(err).To(Succeed())
			}
		})

		It("should error when subscribing with duplicate handler ID", func(ctx context.Context) {
			handler := func(ctx context.Context, msg bus.Message) error {
				return nil
			}

			handlerId := uuid.New().String()
			err := messageBus.Subscribe(eventType, handlerId, handler)
			Expect(err).To(Succeed())

			err = messageBus.Subscribe(eventType, handlerId, handler)
			Expect(err).ToNot(Succeed())
		})
	})

	Context("Publish with subscribers", func() {
		It("invokes subscribed handlers when an event is published", func(ctx context.Context) {
			numSend := 100
			invoked := make(chan string, numSend)

			handler1 := func(ctx context.Context, msg bus.Message) error {
				invoked <- "handler1"
				return nil
			}

			handler2 := func(ctx context.Context, msg bus.Message) error {
				invoked <- "handler2"
				return nil
			}

			Expect(messageBus.Subscribe(eventType, "handler1", handler1)).To(Succeed())
			Expect(messageBus.Subscribe(eventType, "handler2", handler2)).To(Succeed())

			for range numSend {
				go messageBus.Publish(ctx, eventType, msg)
			}

			receivedHandlers := make(map[string]uint)
			for range numSend * 2 {
				h := <-invoked
				receivedHandlers[h]++
			}

			Expect(receivedHandlers).To(HaveKey("handler1"))
			Expect(receivedHandlers).To(HaveKey("handler2"))
			Expect(receivedHandlers["handler1"]).To(Equal(uint(numSend)))
			Expect(receivedHandlers["handler2"]).To(Equal(uint(numSend)))
		})

		It("invokes subscribed group handlers when an event is published", func(ctx context.Context) {
			numSend := 100
			numMembers := 5
			invoked := make(chan string, numSend)

			groupId := uuid.New().String()

			for i := range numMembers {
				memberId := "member-" + strconv.Itoa(i)
				handler := func(ctx context.Context, msg bus.Message) error {
					invoked <- memberId
					return nil
				}

				Expect(messageBus.GroupSubscribe(eventType, memberId, groupId, handler)).To(Succeed())
			}

			for range numSend {
				go messageBus.Publish(ctx, eventType, msg)
			}

			receivedMembers := make(map[string]uint)
			for range numSend {
				member := <-invoked
				receivedMembers[member]++
			}

			Expect(len(receivedMembers)).To(Equal(numMembers))
			totalInvocations := uint(0)
			for _, count := range receivedMembers {
				totalInvocations += count
			}
			Expect(totalInvocations).To(Equal(uint(numSend)))
		})

		It("invokes subscribed group handles and handlers", func(ctx context.Context) {
			numSend := 100
			numMembers := 5
			invoked := make(chan string, numSend)

			groupId := uuid.New().String()

			for i := range numMembers {
				memberId := "member-" + strconv.Itoa(i)
				handler := func(ctx context.Context, msg bus.Message) error {
					invoked <- memberId
					return nil
				}

				Expect(messageBus.GroupSubscribe(eventType, memberId, groupId, handler)).To(Succeed())
			}

			handler1 := func(ctx context.Context, msg bus.Message) error {
				invoked <- "handler1"
				return nil
			}

			handler2 := func(ctx context.Context, msg bus.Message) error {
				invoked <- "handler2"
				return nil
			}

			Expect(messageBus.Subscribe(eventType, "handler1", handler1)).To(Succeed())
			Expect(messageBus.Subscribe(eventType, "handler2", handler2)).To(Succeed())

			for range numSend {
				go messageBus.Publish(ctx, eventType, msg)
			}
			receivedMembers := make(map[string]uint)
			for range numSend * 2 {
				member := <-invoked
				receivedMembers[member]++
			}

			Expect(len(receivedMembers)).To(Equal(numMembers + 2))
			totalInvocations := uint(0)
			for _, count := range receivedMembers {
				totalInvocations += count
			}

			Expect(totalInvocations).To(Equal(uint(numSend * 2)))
		})
	})
})

type fmtPrintfLogger struct{}

// NewFmtPrintfLogger creates a new instance of the basic logger.
func NewFmtPrintfLogger() bus.Logger {
	return &fmtPrintfLogger{}
}

// formatFields attempts to format the variadic fields into a simple string.
// It iterates through the fields two at a time (key, value) and formats them.
func formatFields(fields []any) string {
	if len(fields) == 0 {
		return ""
	}

	var parts []string
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			// Skip if the key is not a string
			continue
		}

		// Ensure there is a corresponding value
		value := ""
		if i+1 < len(fields) {
			value = fmt.Sprintf("%v", fields[i+1])
		}

		parts = append(parts, fmt.Sprintf("%s=%s", key, value))
	}

	return " | " + strings.Join(parts, ", ")
}

func (l *fmtPrintfLogger) log(level string, msg string, fields ...any) {
	formattedFields := formatFields(fields)
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	fmt.Printf("[%s] %s: %s%s\n", timestamp, level, msg, formattedFields)
}

func (l *fmtPrintfLogger) Debug(msg string, fields ...any) {
	l.log("DEBUG", msg, fields...)
}

func (l *fmtPrintfLogger) Info(msg string, fields ...any) {
	l.log("INFO", msg, fields...)
}

func (l *fmtPrintfLogger) Warn(msg string, fields ...any) {
	l.log("WARN", msg, fields...)
}

func (l *fmtPrintfLogger) Error(msg string, fields ...any) {
	l.log("ERROR", msg, fields...)
}
