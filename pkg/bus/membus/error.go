package membus

import "github.com/forever-dev/go-bus/pkg/bus"

type GroupError struct {
	*bus.BusError
}

var (
	ErrGroupNotFound        = &GroupError{&bus.BusError{Message: "member already exists"}}
	ErrMemberExists         = &GroupError{&bus.BusError{Message: "member already exists"}}
	ErrMemberNotProccessing = &GroupError{&bus.BusError{Message: "member not processing"}}
)
