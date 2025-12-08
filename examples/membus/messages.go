package main

// ProcessImageCommand is a Command (point-to-point)
type ProcessImageCommand struct {
	ID       string
	FilePath string
}

func (c *ProcessImageCommand) GetId() string          { return c.ID }
func (c *ProcessImageCommand) TypeKey() string        { return "command.image.process" }
func (c *ProcessImageCommand) Bytes() ([]byte, error) { return []byte{}, nil }
func (c *ProcessImageCommand) Unmarshal(data []byte) error {
	c.FilePath = string(data)
	return nil
}
func (c *ProcessImageCommand) Raw() any { return c }

// UserRegisteredEvent is an Event (pub/sub)
type UserRegisteredEvent struct {
	ID    string
	Email string
}

func (e *UserRegisteredEvent) GetId() string          { return e.ID }
func (e *UserRegisteredEvent) TypeKey() string        { return "event.user.registered" }
func (e *UserRegisteredEvent) Bytes() ([]byte, error) { return []byte{}, nil }
func (e *UserRegisteredEvent) Unmarshal(data []byte) error {
	e.Email = string(data)
	return nil
}
func (e *UserRegisteredEvent) Raw() any { return e }
