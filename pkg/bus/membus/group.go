package membus

import (
	"sync"

	"github.com/forever-dev/go-bus/pkg/bus"
)

type Group struct {
	// Id is the name of the group
	Id string

	// Members holds statistics for each member in the group
	// Key: Member identifier, Value: GroupMember
	Members map[string]*GroupMember

	// mu protects access to the Members map
	mu sync.Mutex
}

func NewGroups() map[string]*Group {
	return make(map[string]*Group)
}

func NewGroup(id string) *Group {
	return &Group{
		Id:      id,
		Members: make(map[string]*GroupMember),
	}
}

// AddMember adds a new member to the group
func (g *Group) AddMember(memberID string, handler bus.MessageHandler) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.Members[memberID]; exists {
		return ErrMemberExists
	}

	g.Members[memberID] = &GroupMember{
		Id:      memberID,
		handler: handler,
	}

	return nil
}

// RemoveMember removes a member from the group
func (g *Group) RemoveMember(memberID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.Members, memberID)
}

// ChooseMember selects the member with the fewest active messages.
// If there is a tie, it selects the member with the fewest processed messages.
// The chosen member's active message count is incremented before returning.
func (g *Group) ChooseMember() *GroupMember {
	g.mu.Lock()
	defer g.mu.Unlock()

	var chosenMember *GroupMember

	for _, member := range g.Members {
		if chosenMember == nil || member.lessActiveThan(chosenMember) {
			chosenMember = member
		}
	}

	if chosenMember == nil {
		return nil
	}

	chosenMember.startProcessing()
	return chosenMember
}
