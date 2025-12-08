package main

import (
	"context"
	"fmt"
	"time"

	"github.com/forever-dev/go-bus/pkg/bus"
)

func commandHandler(ctx context.Context, msg bus.Message) error {
	// Zero-copy access is preferred in membus
	cmd := msg.(*ProcessImageCommand)
	fmt.Printf(" [CMD] [SingleHandler] Processing image: %s (ID: %s)\n", cmd.FilePath, cmd.ID)
	return nil
}

// eventSubscriberHandler simulates a listener subscribed to ALL UserRegisteredEvent messages.
func eventSubscriberHandler(ctx context.Context, msg *UserRegisteredEvent) error {
	// Demonstrate generics usage (no manual type assertion)
	fmt.Printf(" [EVT] [Subscriber] Sending welcome email to: %s\n", msg.Email)
	return nil
}

// groupCommandHandler simulates a worker in a load-balancing command group.
func groupCommandHandler(workerID string) bus.MessageHandler {
	return func(ctx context.Context, msg bus.Message) error {
		cmd := msg.(*ProcessImageCommand)
		fmt.Printf(" [CMD] [Worker:%s] Starting processing file: %s\n", workerID, cmd.FilePath)
		time.Sleep(10 * time.Millisecond) // Simulate work
		fmt.Printf(" [CMD] [Worker:%s] Finished processing file: %s\n", workerID, cmd.FilePath)
		return nil
	}
}

// groupEventConsumer simulates a consumer in an event consumer group.
func groupEventConsumer(consumerID string) bus.GenericMessageHandler[*UserRegisteredEvent] {
	return func(ctx context.Context, msg *UserRegisteredEvent) error {
		fmt.Printf(" [EVT] [Consumer:%s] Logging user registration for: %s\n", consumerID, msg.Email)
		time.Sleep(5 * time.Millisecond) // Simulate persistence work
		return nil
	}
}
