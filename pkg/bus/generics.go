package bus

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// UnmarshalFunc defines the signature for a function that decodes bytes into a value.
// e.g., json.Unmarshal or protojson.Unmarshal
type UnmarshalFunc func(data []byte, v any) error

// TypedMessageHandler is a strongly typed handler for a specific message type.
type TypedMessageHandler[T any] func(ctx context.Context, msg *T) error

// SubscribeTyped registers a handler for a specific type T.
// It uses the provided unmarshaller to decode the message payload if necessary.
func SubscribeTyped[T any](
	b EventBus,
	topic string,
	listenerId string,
	handler TypedMessageHandler[T],
	unmarshaller UnmarshalFunc,
) error {
	return b.Subscribe(
		topic,
		listenerId,
		shimHandler(handler, unmarshaller),
	)
}

// GroupSubscribeTyped registers a group handler for a specific type T.
func GroupSubscribeTyped[T any](
	b EventBus,
	topic string,
	listenerId string,
	groupId string,
	handler TypedMessageHandler[T],
	unmarshaller UnmarshalFunc,
) error {
	return b.GroupSubscribe(
		topic,
		listenerId,
		groupId,
		shimHandler(handler, unmarshaller),
	)
}

// shimHandler creates a wrapper that handles type assertion (for local bus)
// or unmarshalling (for network bus).
func shimHandler[T any](handler TypedMessageHandler[T], unmarshaller UnmarshalFunc) MessageHandler {
	return func(ctx context.Context, m Message) error {
		// 1. Fast Path: In-Memory (Membus)
		// If the message is already the correct pointer type, use it directly.
		if typed, ok := m.(T); ok {
			return handler(ctx, &typed)
		}

		// 2. Slow Path: Serialized (Kafka/Network)
		// If we have raw bytes, unmarshal them into T.
		data, err := m.Bytes()
		if err != nil {
			return fmt.Errorf("failed to get message bytes: %w", err)
		}

		var payload T
		if err := unmarshaller(data, &payload); err != nil {
			return &EventBusError{&BusError{fmt.Sprintf("failed to unmarshal to %T: %v", payload, err)}}
		}

		return handler(ctx, &payload)
	}
}

func SubscribeJSON[T any](b EventBus, topic, lid string, handler TypedMessageHandler[T]) error {
	return SubscribeTyped(b, topic, lid, handler, json.Unmarshal)
}

func SubscribeJSONGroup[T any](b EventBus, topic, lid, gid string, handler TypedMessageHandler[T]) error {
	return GroupSubscribeTyped(b, topic, lid, gid, handler, json.Unmarshal)
}

// ProtoUnmarshalAdapter allows protojson.Unmarshal to be used where UnmarshalFunc is expected.
func ProtoUnmarshalAdapter(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("target type %T does not implement proto.Message", v)
	}
	return protojson.Unmarshal(data, msg)
}

func SubscribeProto[T any](b EventBus, topic, lid string, handler TypedMessageHandler[T]) error {
	return SubscribeTyped(b, topic, lid, handler, ProtoUnmarshalAdapter)
}

func SubscribeProtoGroup[T any](b EventBus, topic, lid, gid string, handler TypedMessageHandler[T]) error {
	return GroupSubscribeTyped(b, topic, lid, gid, handler, ProtoUnmarshalAdapter)
}
