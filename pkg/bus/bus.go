package bus

import "context"

type MessageBus interface {
	CommandBus
	EventBus
}

type GenericMessageHandler[T Message] func(ctx context.Context, msg T) error

type MessageHandler GenericMessageHandler[Message]
