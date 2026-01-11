package main

import (
	"encoding/json"
)

const TopicOrderCreated = "event.order.created"

type OrderCreatedEvent struct {
	ID        string `json:"id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

func (e *OrderCreatedEvent) GetId() string   { return e.ID }
func (e *OrderCreatedEvent) TypeKey() string { return TopicOrderCreated }
func (e *OrderCreatedEvent) Raw() any        { return e }

// Bytes serializes the event for the Publisher (Kafka Producer).
func (e *OrderCreatedEvent) Bytes() ([]byte, error) {
	return json.Marshal(e)
}
