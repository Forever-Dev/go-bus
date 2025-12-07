package bus

// Message defines the interface for a message in the message bus system.
type Message interface {
	// GetId returns the unique identifier of the message.
	GetId() string

	// GetPayload returns the payload of the message.
	GetPayload() []byte

	// Equals compares this message with another message for equality.
	Equals(other Message) bool
}
