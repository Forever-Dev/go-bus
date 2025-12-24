package kafkabus

import (
	"context"

	"github.com/forever-dev/go-bus/pkg/bus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaBus struct {
	ctx context.Context

	client       *connection
	groupClients map[string]*connection

	config Config
}

type Config struct {
	Seeds []string

	Logger bus.Logger

	LogLevel kgo.LogLevel

	Acks kgo.Acks

	Opts []kgo.Opt

	// StrictConsumeOrdering ensures that messages consumed from the same topic
	// are processed in the order they were sent. This may impact performance.
	StrictConsumeOrdering bool
}

func NewKafkaBus(
	ctx context.Context,
	cfg Config,
) (bus.EventBus, error) {
	return &kafkaBus{
		config:       cfg,
		groupClients: make(map[string]*connection),
		ctx:          ctx,
	}, nil
}
