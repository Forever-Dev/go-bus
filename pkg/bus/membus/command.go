package membus

import (
	"context"
	"reflect"

	"github.com/forever-dev/go-bus/pkg/bus"
)

// Publish implements bus.Bus.
func (m *memBus) Send(ctx context.Context, msg bus.Message) {
	m.msgMutex.RLock()
	defer m.msgMutex.RUnlock()

	msgKey := getMessageKey(msg)

	// First priotity is group handlers
	if group, ok := m.msgGroups[msgKey]; ok {
		member := group.ChooseMember()
		if member != nil {
			go func(member *GroupMember) {
				handlerCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
				defer cancel()

				m.cfg.Logger.Debug("sending message group member handler",
					"messageType", msgKey,
					"member", member.Id,
					"group", group.Id,
				)
				if err := member.handler(handlerCtx, msg); err != nil {
					m.cfg.Logger.Error("message group member handler error",
						"error", err,
						"messageType", msgKey,
						"member", member.Id,
						"group", group.Id,
					)
				}
				if err := member.handler(handlerCtx, msg); err != nil {
					m.cfg.Logger.Error("message group member handler error",
						"error", err,
						"messageType", msgKey,
						"member", member.Id,
						"group", group.Id,
					)
				}
			}(member)
			return
		}
	}

	// Next priority is individual handlers
	if handler, ok := m.msgHandlers[msgKey]; ok {
		go func(handler bus.MessageHandler) {
			handlerCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
			defer cancel()

			m.cfg.Logger.Debug("sending message handler",
				"messageType", msgKey,
			)
			if err := handler(handlerCtx, msg); err != nil {
				m.cfg.Logger.Error("message handler error",
					"error", err,
					"messageType", msgKey,
				)
			}
		}(handler)
		return
	}

	m.cfg.Logger.Warn("no message handlers for message type",
		"messageType", msgKey,
	)
}

// Subscribe implements bus.Bus.
func (m *memBus) Handle(msg bus.Message, handler bus.MessageHandler) error {
	m.msgMutex.Lock()
	defer m.msgMutex.Unlock()

	key := getMessageKey(msg)
	m.msgHandlers[key] = handler
	return nil
}

// GroupSubscribe implements bus.Bus.
func (m *memBus) GroupHandle(handlerId, groupId string, msg bus.Message, handler bus.MessageHandler) error {
	m.msgMutex.Lock()
	defer m.msgMutex.Unlock()

	key := getMessageKey(msg)
	group, exists := m.msgGroups[key]
	if !exists {
		group = NewGroup(groupId)
		m.msgGroups[key] = group
	}

	m.cfg.Logger.Info("adding message group member",
		"messageType", key,
		"handler", handlerId,
		"group", groupId,
	)
	if err := group.AddMember(handlerId, handler); err != nil {
		m.cfg.Logger.Error("adding message group member error",
			"error", err,
			"messageType", key,
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

	key := getMessageKey(msg)
	group, exists := m.msgGroups[key]
	if !exists {
		m.cfg.Logger.Warn("no message group for message type",
			"messageType", key,
			"group", groupId,
		)
		return ErrGroupNotFound
	}

	m.cfg.Logger.Info("removing message group member",
		"messageType", key,
		"handler", handlerId,
		"group", groupId,
	)
	group.RemoveMember(handlerId)
	return nil
}

func getMessageKey(msg bus.Message) string {
	return reflect.TypeOf(msg).String()
}
