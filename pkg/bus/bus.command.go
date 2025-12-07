package bus

import "context"

// CommandBus defines the interface for a message bus (point-to-point) system.
type CommandBus interface {
	// Send sends a message to the bus.
	Send(ctx context.Context, msg Message)

	// Handle registers a handler for incoming messages.
	// To unregister, pass a nil handler.
	Handle(msg Message, handler MessageHandler) error

	// GroupHandle registers a handler for incoming messages for a specific group.
	// Only one listener will receive a given message within a group.
	GroupHandle(handlerId, groupId string, msg Message, handler MessageHandler) error

	// GroupRemoveHandler removes a handler for incoming messages for a specific group.
	GroupRemoveHandler(handlerId, groupId string, msg Message) error
}

type MessageBusError struct {
	*BusError
}

var (
	ErrAlreadySubscribed = &MessageBusError{&BusError{"already subscribed"}}
)
