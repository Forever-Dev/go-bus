package bus

// basicMessage is a simple implementation of the Message interface.
type basicMessage struct {
	// id is a unique identifier for the message
	id string

	// payload contains the actual data of the message
	payload []byte
}

// Equals implements Message.
func (b *basicMessage) Equals(other Message) bool {
	if otherMsg, ok := other.(*basicMessage); ok {
		return b.id == otherMsg.id &&
			string(b.payload) == string(otherMsg.payload)
	}
	return false
}

// GetId implements Message.
func (b *basicMessage) GetId() string {
	return b.id
}

// GetPayload implements Message.
func (b *basicMessage) GetPayload() []byte {
	return b.payload
}

// NewBasicMessage creates a new basicMessage.
func NewBasicMessage(id string, payload []byte) Message {
	return &basicMessage{
		id:      id,
		payload: payload,
	}
}
