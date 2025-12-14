package kafkabus

import (
	"fmt"

	"github.com/forever-dev/go-bus/pkg/bus"
)

type BusError struct {
	*bus.BusError
}

var (
	ErrNoSeedsProvided = fmt.Errorf("no seeds provided")

	ErrConnectionFailed = &BusError{&bus.BusError{Message: "connection to kafka cluster failed"}}
)

func (e *BusError) wrap(err error) error {
	return fmt.Errorf("%w: %w", e, err)
}
