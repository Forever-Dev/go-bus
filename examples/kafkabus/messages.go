package main

import (
	"encoding/json"
	"fmt"
)

// OrderCreatedEvent is an Event intended to be published via Kafka.
type OrderCreatedEvent struct {
	ID        string `json:"id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

func (e *OrderCreatedEvent) GetId() string   { return e.ID }
func (e *OrderCreatedEvent) TypeKey() string { return "event.order.created" }
func (e *OrderCreatedEvent) Raw() any        { return e }

// Bytes implements bus.Message, serializing the struct to JSON.
func (e *OrderCreatedEvent) Bytes() ([]byte, error) {
	return json.Marshal(e)
}

// Unmarshal implements bus.Message, deserializing the JSON bytes into the struct.
func (e *OrderCreatedEvent) Unmarshal(data []byte) error {
	// This method is called by the generic shim handler to decode the payload.
	if err := json.Unmarshal(data, &e); err != nil {
		return fmt.Errorf("failed to unmarshal OrderCreatedEvent: %w", err)
	}
	return nil
}
