package kafkabus

import "github.com/forever-dev/go-bus/pkg/bus"

type listener struct {
	group string
	topic string
	id    string

	handler bus.MessageHandler
}

func newListener(topic, id string, handler bus.MessageHandler) *listener {
	return &listener{
		topic:   topic,
		id:      id,
		handler: handler,
	}
}

func newGroupListener(topic, id, group string, handler bus.MessageHandler) *listener {
	return &listener{
		topic:   topic,
		id:      id,
		group:   group,
		handler: handler,
	}
}

func newMainClient(topic string) *listener {
	return &listener{
		topic: topic,
	}
}
