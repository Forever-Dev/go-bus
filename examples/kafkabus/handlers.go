package main

import (
	"context"
	"fmt"
	"time"

	"github.com/forever-dev/go-bus/pkg/bus"
)

// inventorySubscriber is a TypedMessageHandler[OrderCreatedEvent]
func inventorySubscriber(ctx context.Context, msg *OrderCreatedEvent) error {
	fmt.Printf(" [EVT] [Subscriber:Inventory] Received Order %s for Product %s. Updating stock.\n", msg.ID, msg.ProductID)
	return nil
}

// auditConsumer returns a TypedMessageHandler[OrderCreatedEvent]
func auditConsumer(consumerID string) bus.TypedMessageHandler[OrderCreatedEvent] {
	return func(ctx context.Context, msg *OrderCreatedEvent) error {
		fmt.Printf(" [EVT] [Consumer:Audit-%s] Logging new order: ID=%s, Qty=%d\n", consumerID, msg.ID, msg.Quantity)
		time.Sleep(10 * time.Millisecond)
		return nil
	}
}
