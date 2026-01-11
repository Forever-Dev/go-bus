package main

import (
	"context"
	"fmt"
	"time"

	"github.com/forever-dev/go-bus/pkg/bus"
)

func commandHandler(ctx context.Context, msg bus.Message) error {
	// Standard CommandBus still uses the Message interface in this example,
	// but we could wrap it similarly if desired.
	cmd := msg.(*ProcessImageCommand)
	fmt.Printf(" [CMD] [SingleHandler] Processing image: %s (ID: %s)\n", cmd.FilePath, cmd.ID)
	return nil
}

// eventSubscriberHandler is now strongly typed.
// No more type assertions or manual unmarshalling inside the function.
func eventSubscriberHandler(ctx context.Context, msg *UserRegisteredEvent) error {
	fmt.Printf(" [EVT] [Subscriber] Sending welcome email to: %s\n", msg.Email)
	return nil
}

func groupCommandHandler(workerID string) bus.MessageHandler {
	return func(ctx context.Context, msg bus.Message) error {
		cmd := msg.(*ProcessImageCommand)
		fmt.Printf(" [CMD] [Worker:%s] Starting processing file: %s\n", workerID, cmd.FilePath)
		time.Sleep(10 * time.Millisecond)
		fmt.Printf(" [CMD] [Worker:%s] Finished processing file: %s\n", workerID, cmd.FilePath)
		return nil
	}
}

// groupEventConsumer is now strongly typed.
func groupEventConsumer(consumerID string) bus.TypedMessageHandler[UserRegisteredEvent] {
	return func(ctx context.Context, msg *UserRegisteredEvent) error {
		fmt.Printf(" [EVT] [Consumer:%s] Logging user registration for: %s\n", consumerID, msg.Email)
		time.Sleep(5 * time.Millisecond)
		return nil
	}
}
