package bus

import (
	"context"
	"fmt"
	"reflect"
)

func Subscribe[T Message](
	b EventBus,
	listenerId string,
	handler GenericMessageHandler[T],
) error {
	if err := validateMessageType[T](); err != nil {
		return err
	}

	return b.Subscribe(
		getTopic[T](),
		listenerId,
		shimHandler(handler),
	)
}

func GroupSubscribe[T Message](
	b EventBus,
	listenerId string,
	groupId string,
	handler GenericMessageHandler[T],
) error {
	if err := validateMessageType[T](); err != nil {
		return err
	}

	return b.GroupSubscribe(
		getTopic[T](),
		listenerId,
		groupId,
		shimHandler(handler),
	)
}

func shimHandler[T Message](handler GenericMessageHandler[T]) MessageHandler {
	return func(ctx context.Context, m Message) error {
		// Membus
		if typedMsg, ok := m.(T); ok {
			return handler(ctx, typedMsg)
		}

		// Network bus (e.g., MQTT, Kafka)
		if raw, ok := m.(*RawMessage); ok {
			var z T
			typedMsgPtr := reflect.New(reflect.TypeOf(z).Elem()).Interface().(T)
			if err := raw.Unmarshal(raw.Payload); err != nil {
				return &EventBusError{&BusError{fmt.Sprintf("failed to unmarshal raw message to type %T: %v", typedMsgPtr, err)}}
			}
			return handler(ctx, typedMsgPtr)
		}

		return &EventBusError{&BusError{fmt.Sprintf("unexpected message type %T", m)}}
	}
}

func validateMessageType[T Message]() error {
	var z T

	if reflect.TypeOf(z) == nil {
		return &EventBusError{&BusError{"cannot use nil message type"}}
	}

	if reflect.TypeOf(z).Kind() != reflect.Ptr {
		return &EventBusError{&BusError{fmt.Sprintf("message type %T must be a pointer", z)}}
	}

	return nil
}

func getTopic[T Message]() string {
	var z T

	if reflect.ValueOf(z).IsNil() {
		return reflect.New(reflect.TypeOf(z).Elem()).Interface().(T).TypeKey()
	}
	return z.TypeKey()
}
