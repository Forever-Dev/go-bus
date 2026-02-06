package bus

import "context"

// CommandBus defines the interface for a message bus (point-to-point) system.
type CommandBus interface {
	// Send sends a message to the bus.
	Send(ctx context.Context, msg Message) error

	// Handle registers a handler for incoming messages.
	// To unregister, pass a nil handler.
	Handle(msg Message, handler MessageHandler) error

	// GroupHandle registers a handler for incoming messages for a specific group.
	// Only one listener will receive a given message within a group.
	GroupHandle(handlerId, groupId string, msg Message, handler MessageHandler) error

	// GroupRemoveHandler removes a handler for incoming messages for a specific group.
	GroupRemoveHandler(handlerId, groupId string, msg Message) error

	// Shutdown gracefully shuts down the event bus
	Shutdown(ctx context.Context) error

	// Ping checks the health of the command bus
	Ping(ctx context.Context) error
}

type MessageBusError struct {
	*BusError
}

var (
	ErrAlreadySubscribed = &MessageBusError{&BusError{"already subscribed"}}
	ErrNoHandler         = &MessageBusError{&BusError{"no handler registered"}}
)
