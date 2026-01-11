package bus

// Message defines the interface for a message in the message bus system.
type Message interface {
	// GetId returns the unique identifier of the message.
	GetId() string

	// TypeKey returns the type key of the message.
	TypeKey() string

	// Bytes returns the payload of the message.
	Bytes() ([]byte, error)
}
