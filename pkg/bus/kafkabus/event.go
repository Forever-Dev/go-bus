package kafkabus

import (
	"context"
	"fmt"

	"github.com/forever-dev/go-bus/pkg/bus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// GroupSubscribe implements bus.EventBus.
func (k *kafkaBus) GroupSubscribe(eventType string, listenerId string, groupId string, handler bus.MessageHandler) error {
	l := newGroupListener(eventType, listenerId, groupId, handler)

	client, exists := k.groupClients[l.group]
	if !exists {
		k.config.Logger.Debug("creating new kafka client for group",
			"group_id", l.group,
			"listener_id", l.id,
			"topic", l.topic,
		)
		connection, err := k.NewConnection(l)
		if err != nil {
			return fmt.Errorf("failed to create new connection for group %s listener %s for %s: %w", l.group, l.id, l.topic, err)
		}
		k.groupClients[l.group] = connection
		return nil
	}

	k.config.Logger.Debug("adding listener to existing kafka client for group",
		"group_id", l.group,
		"listener_id", l.id,
		"topic", l.topic,
	)
	client.AddListener(l)
	return nil
}

// GroupUnsubscribe implements bus.EventBus.
func (k *kafkaBus) GroupUnsubscribe(eventType string, listenerId string, groupId string) error {
	client, exists := k.groupClients[groupId]
	if !exists {
		k.config.Logger.Debug("no kafka client found for group",
			"group_id", groupId,
			"listener_id", listenerId,
			"topic", eventType,
		)
		return nil
	}

	hasListeners, err := client.RemoveListener(listenerId, eventType)
	if err != nil {
		return fmt.Errorf("failed to remove group %s listener %s from %s: %w", groupId, listenerId, eventType, err)
	}
	if !hasListeners {
		k.config.Logger.Debug("group has no more listeners",
			"group_id", groupId,
		)
		delete(k.groupClients, groupId)
	}
	return nil
}

// Publish implements bus.EventBus.
func (k *kafkaBus) Publish(ctx context.Context, msg bus.Message) error {
	if k.client == nil {
		k.config.Logger.Debug("creating main kafka client")
		var err error
		listener := newMainClient(msg.TypeKey())
		k.client, err = k.NewConnection(listener)
		if err != nil {
			return fmt.Errorf("failed to create new connection: %w", err)
		}
	}

	payload, err := msg.Bytes()
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	k.config.Logger.Debug("producing message to kafka",
		"key", msg.GetId(),
		"value", string(payload),
		"topic", msg.TypeKey(),
	)
	k.client.client.Produce(
		ctx,
		&kgo.Record{
			Key:     []byte(msg.GetId()),
			Value:   payload,
			Topic:   msg.TypeKey(),
			Context: ctx,
		},
		func(r *kgo.Record, err error) {
			if err != nil {
				k.config.Logger.Error("failed to publish message to kafka",
					"error", err.Error(),
				)
				return
			}
			k.config.Logger.Debug("message published to kafka",
				"topic", r.Topic,
				"partition", r.Partition,
				"offset", r.Offset,
			)
		})

	return nil
}

// Shutdown implements bus.EventBus.
func (k *kafkaBus) Shutdown(ctx context.Context) error {
	if err := k.Flush(ctx); err != nil {
		k.config.Logger.Warn("Producer flush failed during shutdown", "error", err)
	}

	for _, client := range k.groupClients {
		if err := client.Close(); err != nil {
			return err
		}
	}

	if k.client != nil {
		if err := k.client.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Subscribe implements bus.EventBus.
func (k *kafkaBus) Subscribe(eventType string, listenerId string, handler bus.MessageHandler) error {
	l := newListener(eventType, listenerId, handler)

	if k.client == nil {
		k.config.Logger.Debug("creating new main kafka client",
			"listener_id", l.id,
			"topic", l.topic,
		)
		connection, err := k.NewConnection(l)
		connection.keepAlive = true
		if err != nil {
			return fmt.Errorf("failed to create new connection: %w", err)
		}
		k.client = connection
		return nil
	}

	k.config.Logger.Debug("adding listener to existing main kafka client",
		"listener_id", l.id,
		"topic", l.topic,
	)
	k.client.AddListener(l)
	return nil
}

// Unsubscribe implements bus.EventBus.
func (k *kafkaBus) Unsubscribe(eventType string, listenerId string) error {
	if k.client == nil {
		k.config.Logger.Debug("no main kafka client to unsubscribe from",
			"listener_id", listenerId,
			"topic", eventType,
		)
		return nil
	}

	hasListener, err := k.client.RemoveListener(listenerId, eventType)
	if err != nil {
		return fmt.Errorf("failed to remove listener %s from %s: %w", listenerId, eventType, err)
	}
	if !hasListener {
		k.config.Logger.Warn("internal error: hasListener returned false when keepAlive is true", nil)
	}
	return nil
}
