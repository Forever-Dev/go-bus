package bus

type BusError struct {
	Message string
}

func (e *BusError) Error() string {
	return e.Message
}

var (
	ErrHandlerExists = &BusError{"already listening"}

	ErrHandlerEmpty = &BusError{"handler is empty"}

	ErrHandlerNotFound = &BusError{"handler not found"}
)
