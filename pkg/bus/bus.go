package bus

import "context"

type MessageBus interface {
	CommandBus
	EventBus
}

type MessageHandler func(ctx context.Context, msg Message) error
