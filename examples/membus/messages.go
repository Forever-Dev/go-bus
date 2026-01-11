package main

const (
	TopicProcessImage   = "command.image.process"
	TopicUserRegistered = "event.user.registered"
)

// ProcessImageCommand
type ProcessImageCommand struct {
	ID       string
	FilePath string
}

func (c *ProcessImageCommand) GetId() string          { return c.ID }
func (c *ProcessImageCommand) TypeKey() string        { return TopicProcessImage }
func (c *ProcessImageCommand) Bytes() ([]byte, error) { return []byte{}, nil }
func (c *ProcessImageCommand) Raw() any               { return c }

// UserRegisteredEvent
type UserRegisteredEvent struct {
	ID    string
	Email string
}

func (e *UserRegisteredEvent) GetId() string          { return e.ID }
func (e *UserRegisteredEvent) TypeKey() string        { return TopicUserRegistered }
func (e *UserRegisteredEvent) Bytes() ([]byte, error) { return []byte{}, nil }
func (e *UserRegisteredEvent) Raw() any               { return e }
