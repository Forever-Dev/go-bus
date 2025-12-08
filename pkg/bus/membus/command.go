package membus

import (
	"context"

	"github.com/forever-dev/go-bus/pkg/bus"
)

// Publish implements bus.Bus.
func (m *memBus) Send(ctx context.Context, msg bus.Message) error {
	m.msgMutex.RLock()
	defer m.msgMutex.RUnlock()

	// First priotity is group handlers
	if group, ok := m.msgGroups[msg.TypeKey()]; ok {
		member := group.ChooseMember()
		if member != nil {
			m.cfg.Logger.Debug("sending message group member handler",
				"messageType", msg.TypeKey(),
				"member", member.Id,
				"group", group.Id,
			)
			err := member.handler(ctx, msg)

			if err := member.FinishProcessing(); err != nil {
				m.cfg.Logger.Error("error finishing processing for member",
					"member", member.Id,
					"group", group.Id,
					"messageType", msg.TypeKey(),
					"error", err,
				)
			}

			return err
		} else {
			m.cfg.Logger.Warn("no available members in message group",
				"messageType", msg.TypeKey(),
				"group", group.Id,
			)
		}
	}

	// Next priority is individual handlers
	if handler, ok := m.msgHandlers[msg.TypeKey()]; ok {
		m.cfg.Logger.Debug("sending message handler",
			"messageType", msg.TypeKey(),
		)
		return handler(ctx, msg)
	}

	m.cfg.Logger.Warn("no message handlers for message type",
		"messageType", msg.TypeKey(),
	)
	return bus.ErrNoHandler
}

// Subscribe implements bus.Bus.
func (m *memBus) Handle(msg bus.Message, handler bus.MessageHandler) error {
	m.msgMutex.Lock()
	defer m.msgMutex.Unlock()

	key := msg.TypeKey()
	m.msgHandlers[key] = handler
	return nil
}

// GroupSubscribe implements bus.Bus.
func (m *memBus) GroupHandle(handlerId, groupId string, msg bus.Message, handler bus.MessageHandler) error {
	m.msgMutex.Lock()
	defer m.msgMutex.Unlock()

	group, exists := m.msgGroups[msg.TypeKey()]
	if !exists {
		group = NewGroup(groupId)
		m.msgGroups[msg.TypeKey()] = group
	}

	m.cfg.Logger.Info("adding message group member",
		"messageType", msg.TypeKey(),
		"handler", handlerId,
		"group", groupId,
	)
	if err := group.AddMember(handlerId, handler); err != nil {
		m.cfg.Logger.Error("adding message group member error",
			"error", err,
			"messageType", msg.TypeKey(),
			"handler", handlerId,
			"group", groupId,
		)
		return err
	}

	return nil
}

func (m *memBus) GroupRemoveHandler(handlerId, groupId string, msg bus.Message) error {
	m.msgMutex.Lock()
	defer m.msgMutex.Unlock()

	group, exists := m.msgGroups[msg.TypeKey()]
	if !exists {
		m.cfg.Logger.Warn("no message group for message type",
			"messageType", msg.TypeKey(),
			"group", groupId,
		)
		return ErrGroupNotFound
	}

	m.cfg.Logger.Info("removing message group member",
		"messageType", msg.TypeKey(),
		"handler", handlerId,
		"group", groupId,
	)
	group.RemoveMember(handlerId)
	return nil
}
