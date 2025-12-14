package main

import (
	"context"
	"fmt"
	"time"

	"github.com/forever-dev/go-bus/pkg/bus"
)

// --- Event Handlers (Subscribers/Consumers) ---

// inventorySubscriber consumes OrderCreatedEvent (Subscribed via bus.Subscribe)
// This handler demonstrates accessing the decoded type directly via generics.
func inventorySubscriber(ctx context.Context, msg *OrderCreatedEvent) error {
	fmt.Printf(" [EVT] [Subscriber:Inventory] Received Order %s for Product %s. Updating stock.\n", msg.ID, msg.ProductID)
	return nil
}

// auditConsumer is a member of an event consumer group (Subscribed via bus.GroupSubscribe)
// Only one consumer in this group will receive a given event.
func auditConsumer(consumerID string) bus.GenericMessageHandler[*OrderCreatedEvent] {
	return func(ctx context.Context, msg *OrderCreatedEvent) error {
		fmt.Printf(" [EVT] [Consumer:Audit-%s] Logging new order: ID=%s, Qty=%d\n", consumerID, msg.ID, msg.Quantity)
		time.Sleep(10 * time.Millisecond) // Simulate work
		return nil
	}
}
