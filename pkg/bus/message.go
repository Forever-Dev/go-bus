package bus

// Message defines the interface for a message in the message bus system.
type Message interface {
	// GetId returns the unique identifier of the message.
	GetId() string

	// TypeKey returns the type key of the message.
	TypeKey() string

	// Bytes returns the payload of the message.
	Bytes() ([]byte, error)

	// Unmarshal populates the message from the given byte slice.
	Unmarshal(data []byte) error

	// Raw returns the raw representation of the message.
	Raw() any
}
