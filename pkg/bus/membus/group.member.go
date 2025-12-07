package membus

import (
	"sync"

	"github.com/forever-dev/go-bus/pkg/bus"
)

type GroupMember struct {
	Id string

	activeMessages    uint
	processedMessages uint

	handler bus.MessageHandler

	mu sync.Mutex
}

func (gm *GroupMember) startProcessing() {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.activeMessages++
}

// FinishProcessing marks a message as processed for the member
// and decrements the active message count. An error is returned
// if there are no active messages to finish processing.
func (gm *GroupMember) FinishProcessing() error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if gm.activeMessages > 0 {
		gm.activeMessages--
		gm.processedMessages++
		return nil
	}

	return ErrMemberNotProccessing
}

// lessActiveThan compares two GroupMember instances
// and returns true if the current instance has fewer active messages
// or the same number of active messages but fewer processed messages.
func (g *GroupMember) lessActiveThan(other *GroupMember) bool {
	if g.activeMessages < other.activeMessages {
		return true
	}

	if g.activeMessages == other.activeMessages &&
		g.processedMessages < other.processedMessages {
		return true
	}

	return false
}
