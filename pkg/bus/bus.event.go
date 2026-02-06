package bus

import "context"

// EventBus defines the interface for a event bus (Pub/Sub) system.
type EventBus interface {
	// Publish sends a message to the bus.
	Publish(ctx context.Context, msg Message) error

	// Subscribe registers a handler for incoming messages.
	Subscribe(eventType, listenerId string, handler MessageHandler) error

	// Unsubscribe removes a handler for incoming messages.
	Unsubscribe(eventType, listenerId string) error

	// GroupSubscribe registers a handler for incoming messages for a specific group.
	// Only one listener will receive a given message within a group.
	GroupSubscribe(eventType, listenerId, groupId string, handler MessageHandler) error

	// GroupUnsubscribe removes a handler for incoming messages for a specific group.
	GroupUnsubscribe(eventType, listenerId, groupId string) error

	// Shutdown gracefully shuts down the event bus
	Shutdown(ctx context.Context) error

	// Ping checks the health of the event bus
	Ping(ctx context.Context) error
}

type EventBusError struct {
	*BusError
}
