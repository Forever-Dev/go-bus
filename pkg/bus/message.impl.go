package bus

// BasicMessage is a simple implementation of the Message interface.
type BasicMessage struct {
	// id is a unique identifier for the message
	id string

	// payload contains the actual data of the message
	payload []byte
}

// Unmarshal implements Message.
func (b *BasicMessage) Unmarshal(data []byte) error {
	b.payload = data
	return nil
}

// Raw implements Message.
func (b *BasicMessage) Raw() any {
	return b
}

// TypeKey implements Message.
func (b *BasicMessage) TypeKey() string {
	return "basic"
}

// GetId implements Message.
func (b *BasicMessage) GetId() string {
	return b.id
}

// Bytes implements Message.
func (b *BasicMessage) Bytes() ([]byte, error) {
	return b.payload, nil
}

// NewBasicMessage creates a new basicMessage.
func NewBasicMessage(id string, payload []byte) Message {
	return &BasicMessage{
		id:      id,
		payload: payload,
	}
}
