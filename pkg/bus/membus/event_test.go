package membus_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
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

		runCtx context.Context
	)

	defaultCfg := membus.NewDefaultConfig()
	workerCfg := membus.NewDefaultConfig()
	workerCfg.WorkerPoolConfig = membus.NewWorkerPoolConfig(10, 100)

	cfgs := map[string]*membus.Config{
		"without workers": defaultCfg,
		"with workers":    workerCfg,
	}

	for name, cfg := range cfgs {
		Context(name, func() {
			BeforeEach(func() {
				runCtx = context.Background()

				msg = bus.NewBasicMessage(uuid.New().String(), []byte(faker.Word()))
				Expect(msg).NotTo(BeNil())

				messageBus = membus.New(runCtx, cfg)
				Expect(messageBus).NotTo(BeNil())
			})

			Describe("Publish", func() {
				It("publishes an event to the bus", func(ctx context.Context) {
					Expect(func() {
						messageBus.Publish(ctx, msg)
					}).NotTo(Panic())
				})
			})

			Describe("Subscribe", func() {
				It("subscribes to an event type", func(ctx context.Context) {
					handler := func(ctx context.Context, msg bus.Message) error {
						return nil
					}

					Expect(func() {
						messageBus.Subscribe(msg.TypeKey(), uuid.New().String(), handler)
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

							err := messageBus.Subscribe(msg.TypeKey(), uuid.New().String(), handler)
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
					err := messageBus.Subscribe(msg.TypeKey(), handlerId, handler)
					Expect(err).To(Succeed())

					err = messageBus.Subscribe(msg.TypeKey(), handlerId, handler)
					Expect(err).ToNot(Succeed())
				})
			})

			Describe("Unsubscribe", func() {
				It("unsubscribes from an event type", func(ctx context.Context) {
					handler := func(ctx context.Context, msg bus.Message) error {
						return nil
					}

					handlerId := uuid.New().String()
					err := messageBus.Subscribe(msg.TypeKey(), handlerId, handler)
					Expect(err).To(Succeed())

					err = messageBus.Unsubscribe(msg.TypeKey(), handlerId)
					Expect(err).To(Succeed())
				})

				It("should error when unsubscribing with unknown handler ID", func(ctx context.Context) {
					handlerId := uuid.New().String()
					err := messageBus.Unsubscribe(msg.TypeKey(), handlerId)
					Expect(err).ToNot(Succeed())
				})
			})

			Describe("GroupUnsubscribe", func() {
				It("unsubscribes a group member from an event type", func(ctx context.Context) {
					handler := func(ctx context.Context, msg bus.Message) error {
						return nil
					}

					groupId := uuid.New().String()
					handlerId := uuid.New().String()
					err := messageBus.GroupSubscribe(msg.TypeKey(), handlerId, groupId, handler)
					Expect(err).To(Succeed())

					err = messageBus.GroupUnsubscribe(msg.TypeKey(), handlerId, groupId)
					Expect(err).To(Succeed())
				})

				It("should error when unsubscribing with unknown group member ID", func(ctx context.Context) {
					groupId := uuid.New().String()
					handlerId := uuid.New().String()
					err := messageBus.GroupUnsubscribe(msg.TypeKey(), handlerId, groupId)
					Expect(err).ToNot(Succeed())
				})

				It("should error when unsubscribing with unknown group ID", func(ctx context.Context) {
					handler := func(ctx context.Context, msg bus.Message) error {
						return nil
					}

					groupId := uuid.New().String()
					handlerId := uuid.New().String()
					err := messageBus.GroupSubscribe(msg.TypeKey(), handlerId, groupId, handler)
					Expect(err).To(Succeed())

					unknownGroupId := uuid.New().String()
					err = messageBus.GroupUnsubscribe(msg.TypeKey(), handlerId, unknownGroupId)
					Expect(err).ToNot(Succeed())
				})
			})

			Describe("GroupSubscribe", func() {
				It("subscribes a group member to an event type", func(ctx context.Context) {
					handler := func(ctx context.Context, msg bus.Message) error {
						return nil
					}

					groupId := uuid.New().String()
					handlerId := uuid.New().String()
					err := messageBus.GroupSubscribe(msg.TypeKey(), handlerId, groupId, handler)
					Expect(err).To(Succeed())
				})

				It("should work concurrently", func(ctx context.Context) {
					numSubscribers := 100
					errs := make(chan error, numSubscribers)

					for range numSubscribers {
						go func() {
							handler := func(ctx context.Context, msg bus.Message) error {
								return nil
							}

							groupId := uuid.New().String()
							handlerId := uuid.New().String()
							err := messageBus.GroupSubscribe(msg.TypeKey(), handlerId, groupId, handler)
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

				It("should error when subscribing with duplicate group member ID", func(ctx context.Context) {
					handler := func(ctx context.Context, msg bus.Message) error {
						return nil
					}

					groupId := uuid.New().String()
					handlerId := uuid.New().String()
					err := messageBus.GroupSubscribe(msg.TypeKey(), handlerId, groupId, handler)
					Expect(err).To(Succeed())

					err = messageBus.GroupSubscribe(msg.TypeKey(), handlerId, groupId, handler)
					Expect(err).ToNot(Succeed())
				})
			})

			Describe("Publish with no subscribers", func() {
				It("should not error", func(ctx context.Context) {
					err := messageBus.Publish(ctx, msg)
					Expect(err).To(Succeed())
				})
			})

			Context("Publish with subscribers", func() {
				It("should not error when all handlers are removed", func(ctx context.Context) {
					var wg sync.WaitGroup
					wg.Add(1)
					count := 0
					handler := func(ctx context.Context, msg bus.Message) error {
						count++
						wg.Done()
						return nil
					}

					handlerId := uuid.New().String()
					err := messageBus.Subscribe(msg.TypeKey(), handlerId, handler)
					Expect(err).To(Succeed())

					err = messageBus.Publish(ctx, msg)
					Expect(err).To(Succeed())
					wg.Wait()
					Expect(count).To(Equal(1))

					err = messageBus.Unsubscribe(msg.TypeKey(), handlerId)
					Expect(err).To(Succeed())

					err = messageBus.Publish(ctx, msg)
					Expect(err).To(Succeed())
					time.Sleep(50 * time.Millisecond)
					Consistently(count).MustPassRepeatedly(10).To(Equal(1))
				})

				It("should not error when all group handlers are removed", func(ctx context.Context) {
					var wg sync.WaitGroup
					wg.Add(1)
					count := 0
					handler := func(ctx context.Context, msg bus.Message) error {
						count++
						wg.Done()
						return nil
					}

					groupId := uuid.New().String()
					handlerId := uuid.New().String()
					err := messageBus.GroupSubscribe(msg.TypeKey(), handlerId, groupId, handler)
					Expect(err).To(Succeed())

					err = messageBus.Publish(ctx, msg)
					Expect(err).To(Succeed())
					wg.Wait()
					Expect(count).To(Equal(1))

					err = messageBus.GroupUnsubscribe(msg.TypeKey(), handlerId, groupId)
					Expect(err).To(Succeed())

					err = messageBus.Publish(ctx, msg)
					Expect(err).To(Succeed())
					time.Sleep(50 * time.Millisecond)
					Consistently(count).MustPassRepeatedly(10).To(Equal(1))
				})

				type runSettings struct {
					numSend     int
					numHandlers int
					numMembers  int
				}

				var (
					possibleSettings = map[string]runSettings{
						"only handlers":      {numSend: 50, numHandlers: 3, numMembers: 0},
						"only group members": {numSend: 50, numHandlers: 0, numMembers: 3},
						// "handlers and group members": {numSend: 50, numHandlers: 3, numMembers: 3},
					}
				)

				for name, settings := range possibleSettings {
					Context(name, func() {
						var (
							invoked = make(chan string, 5)
							wg      sync.WaitGroup

							expectedInvocations uint
						)

						BeforeEach(func() {
							invoked = make(chan string, settings.numSend)
							wg = sync.WaitGroup{}
							expectedInvocations = uint(settings.numSend * (settings.numHandlers))
							if settings.numMembers > 0 {
								expectedInvocations += uint(settings.numSend)
							}
							wg.Add(int(expectedInvocations))
						})

						Context("without context timeout", func() {
							AfterEach(func(ctx context.Context) {
								for range settings.numSend {
									go messageBus.Publish(ctx, msg)
								}
								go func() {
									wg.Wait()
									close(invoked)
								}()

								receivedHandlers := make(map[string]uint)
								for h := range invoked {
									receivedHandlers[h]++
								}

								for i := range settings.numHandlers {
									handlerId := "handler-" + strconv.Itoa(i)
									Expect(receivedHandlers).To(HaveKey(handlerId))
									Expect(receivedHandlers[handlerId]).To(Equal(uint(settings.numSend)))
								}

								memberTotal := uint(0)
								for i := range settings.numMembers {
									memberId := "member-" + strconv.Itoa(i)
									Expect(receivedHandlers).To(HaveKey(memberId))
									count := receivedHandlers[memberId]
									memberTotal += count
									Expect(count).To(BeNumerically("<", uint(settings.numSend)))
								}

								if settings.numMembers > 0 {
									Expect(memberTotal).To(Equal(uint(settings.numSend)))
								}
							})

							It("invokes subscribed handlers when an event is published", func(ctx context.Context) {
								newHandler := func(id string) bus.MessageHandler {
									return func(ctx context.Context, msg bus.Message) error {
										invoked <- id
										wg.Done()
										return nil
									}
								}
								for i := range settings.numHandlers {
									handlerId := "handler-" + strconv.Itoa(i)
									handler := newHandler(handlerId)
									Expect(messageBus.Subscribe(msg.TypeKey(), handlerId, handler)).To(Succeed())
								}

								if settings.numMembers > 0 {
									groupId := uuid.New().String()

									for i := range settings.numMembers {
										memberId := "member-" + strconv.Itoa(i)
										handler := newHandler(memberId)
										Expect(messageBus.GroupSubscribe(msg.TypeKey(), memberId, groupId, handler)).To(Succeed())
									}
								}
							})

							It("invokes generic subscribed handlers when an event is published", func(ctx context.Context) {
								newHandler := func(id string) bus.GenericMessageHandler[*bus.BasicMessage] {
									return func(ctx context.Context, msg *bus.BasicMessage) error {
										invoked <- id
										wg.Done()
										return nil
									}
								}
								for i := range settings.numHandlers {
									handlerId := "handler-" + strconv.Itoa(i)
									handler := newHandler(handlerId)
									Expect(bus.Subscribe(messageBus, handlerId, handler)).To(Succeed())
								}

								if settings.numMembers > 0 {
									groupId := uuid.New().String()

									for i := range settings.numMembers {
										memberId := "member-" + strconv.Itoa(i)
										handler := newHandler(memberId)
										Expect(bus.GroupSubscribe(messageBus, memberId, groupId, handler)).To(Succeed())
									}
								}
							})
						})

						It("should pass timeout ctx", func(ctx context.Context) {
							ctxDoneInvoked := make(chan string, 5)
							errInvoked := make(chan string, 5)
							newHander := func(id string) bus.MessageHandler {
								return func(ctx context.Context, msg bus.Message) error {
									defer wg.Done()

									timer := time.NewTimer(time.Duration(rand.Int32N(2)) * time.Millisecond)
									defer timer.Stop()
									select {
									case <-ctx.Done():
										ctxDoneInvoked <- id
									case <-timer.C:
										invoked <- id
									}

									if rand.IntN(3) == 1 {
										return fmt.Errorf("random error")
									}
									return nil
								}
							}

							for i := range settings.numHandlers {
								handlerId := "handler-" + strconv.Itoa(i)
								handler := newHander(handlerId)
								Expect(messageBus.Subscribe(msg.TypeKey(), handlerId, handler)).To(Succeed())
							}

							for i := range settings.numMembers {
								memberId := "member-" + strconv.Itoa(i)
								handler := newHander(memberId)
								Expect(messageBus.GroupSubscribe(msg.TypeKey(), memberId, "group-1", handler)).To(Succeed())
							}

							for range settings.numSend {
								go func() {
									pubCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
									defer cancel()
									if err := messageBus.Publish(pubCtx, msg); err != nil {
										errInvoked <- "publish-error"
										for range settings.numHandlers {
											wg.Done()
										}
									}
								}()
							}

							go func() {
								wg.Wait()
								close(invoked)
								close(ctxDoneInvoked)
								close(errInvoked)
							}()

							receivedHandlers := make(map[string]int)
							for {
								select {
								case h, ok := <-invoked:
									if !ok {
										invoked = nil
									} else {
										receivedHandlers[h]++
									}
								case h, ok := <-ctxDoneInvoked:
									if !ok {
										ctxDoneInvoked = nil
									} else {
										receivedHandlers[h]--
									}
								case h, ok := <-errInvoked:
									if !ok {
										errInvoked = nil
									} else {
										receivedHandlers[h]--
									}
								}

								if invoked == nil && ctxDoneInvoked == nil && errInvoked == nil {
									break
								}
							}

							for i := range settings.numHandlers {
								handlerId := "handler-" + strconv.Itoa(i)
								Expect(receivedHandlers).To(HaveKey(handlerId))
								Expect(receivedHandlers[handlerId]).To(BeNumerically("<", uint(settings.numSend)))
								Expect(receivedHandlers[handlerId]).To(BeNumerically(">", -settings.numSend))
							}
						})
					})
				}

			})
		})
	}
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
