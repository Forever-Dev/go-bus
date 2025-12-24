package kafkabus

import (
	"context"
	"errors"

	"github.com/forever-dev/go-bus/pkg/bus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// connection represents a Kafka connection with a client and a set of listeners.
type connection struct {
	client *kgo.Client

	listeners     map[string]*listener
	topicHandlers map[string]map[string]bus.MessageHandler

	strictProcessingOrder bool

	keepAlive bool

	logger bus.Logger

	exitCh chan struct{}
}

// Close closes the Kafka connection.
func (c *connection) Close() error {
	if c.client == nil {
		return nil
	}

	c.client.Close()

	<-c.exitCh

	c.client = nil
	c.listeners = nil
	c.topicHandlers = nil

	return nil
}

// NewConnection creates a new Kafka connection with the given seeds, group ID, and listener ID.
func (k *kafkaBus) NewConnection(l *listener, strictOrderProcessing bool) (*connection, error) {
	if len(k.config.Seeds) == 0 {
		return nil, ErrNoSeedsProvided
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(k.config.Seeds...),
		kgo.ConsumeTopics(l.topic),
		kgo.RequiredAcks(k.config.Acks),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.WithLogger(NewKafkaLoggerAdapter(k.config.Logger, k.config.LogLevel)),
	}
	opts = append(opts, k.config.Opts...)
	if l.group != "" {
		k.config.Logger.Debug("setting consumer group for listener",
			"group_id", l.group,
			"listener_id", l.id,
			"topic", l.topic,
		)
		opts = append(opts, kgo.ConsumerGroup(l.group))
	}

	if l.id == "" {
		k.config.Logger.Debug("creating main kafka client without listener",
			"topic", l.topic,
		)
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, ErrConnectionFailed.wrap(err)
	}
	if client == nil {
		return nil, ErrConnectionFailed
	}

	conn := &connection{
		client:        client,
		listeners:     make(map[string]*listener),
		topicHandlers: make(map[string]map[string]bus.MessageHandler),
		logger:        k.config.Logger,
		exitCh:        make(chan struct{}),

		strictProcessingOrder: strictOrderProcessing,
	}
	if l.id != "" {
		k.config.Logger.Debug("adding listener to new kafka connection",
			"group_id", l.group,
			"listener_id", l.id,
			"topic", l.topic,
		)
		conn.listeners[l.id] = l
		conn.topicHandlers[l.topic] = map[string]bus.MessageHandler{
			l.id: l.handler,
		}
	} else {
		k.config.Logger.Debug("creating main kafka connection without listener",
			"group_id", l.group,
			"topic", l.topic,
		)
		conn.keepAlive = true
	}

	go conn.Listen(k.ctx)
	return conn, nil
}

// AddListener adds a listener to the connection.
func (c *connection) AddListener(l *listener) {
	c.listeners[l.id] = l
	handlers, exists := c.topicHandlers[l.topic]

	if !exists {
		c.logger.Debug("adding new topic to kafka connection",
			"listener_id", l.id,
			"topic", l.topic,
		)
		c.topicHandlers[l.topic] = make(map[string]bus.MessageHandler)
		c.topicHandlers[l.topic][l.id] = l.handler
		c.client.AddConsumeTopics(l.topic)
	} else {
		c.logger.Debug("adding listener to existing topic in kafka connection",
			"listener_id", l.id,
			"topic", l.topic,
		)
		handlers[l.id] = l.handler
	}
}

// RemoveListener removes a listener from the connection.
// If there are no more listeners, it closes the connection and returns false.
// Otherwise, it returns true.
func (c *connection) RemoveListener(id, topic string) (bool, error) {
	delete(c.listeners, id)
	if len(c.listeners) == 0 && !c.keepAlive {
		c.logger.Debug("no more listeners in kafka connection, closing connection",
			"listener_id", id,
			"topic", topic,
		)
		c.client.Close()
		c.client = nil
		return false, nil
	}

	delete(c.topicHandlers[topic], id)
	if len(c.topicHandlers[topic]) == 0 {
		c.logger.Debug("no more listeners for topic in kafka connection, removing topic",
			"listener_id", id,
			"topic", topic,
		)
		delete(c.topicHandlers, topic)
		c.client.PurgeTopicsFromConsuming(topic)
	}

	return true, nil
}

func (c *connection) Listen(ctx context.Context) {
	defer close(c.exitCh)

	c.logger.Info("kafka connection started listening for messages")
	for {

		select {
		case <-ctx.Done():
			c.logger.Debug("application context done, listener exiting")
			return
		default:
		}

		fetches := c.client.PollFetches(ctx)

		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, kgo.ErrClientClosed) {
				c.logger.Info("kafka connection closed, stopping listener")
				return
			}
			c.logger.Error("error fetching messages from kafka", "error", err)
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			c.logger.Debug("kafka connection received message",
				"topic", r.Topic,
				"partition", r.Partition,
				"offset", r.Offset,
			)
			handlers, exists := c.topicHandlers[r.Topic]
			if !exists {
				c.logger.Debug("no handlers for topic in kafka connection, skipping message",
					"topic", r.Topic,
				)
				return
			}

			rawMessage := &bus.RawMessage{
				Topic:   r.Topic,
				Id:      string(r.Key),
				Payload: r.Value,
			}

			for _, handler := range handlers {
				if c.strictProcessingOrder {
					if err := handler(context.Background(), rawMessage); err != nil {
						c.logger.Error("error handling message in kafka connection",
							"error", err,
							"topic", rawMessage.TypeKey(),
							"message_id", rawMessage.GetId(),
						)
					}
				} else {
					go func(h bus.MessageHandler, msg *bus.RawMessage) {
						if err := h(context.Background(), msg); err != nil {
							c.logger.Error("error handling message in kafka connection",
								"error", err,
								"topic", msg.TypeKey(),
								"message_id", msg.GetId(),
							)
						}
					}(handler, rawMessage)
				}
			}
		})

		if err := c.client.CommitRecords(ctx, fetches.Records()...); err != nil {
			c.logger.Error("error committing records in kafka connection", "error", err)
		}
	}
}
