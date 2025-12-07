package membus

import (
	"context"

	"github.com/forever-dev/go-bus/pkg/bus"
)

// Publish implements bus.EventBus.
func (m *memBus) Publish(ctx context.Context, eventType string, event bus.Message) {
	m.eventMutex.RLock()
	defer m.eventMutex.RUnlock()

	if handlers, ok := m.eventHandlers[eventType]; ok {
		for id, handler := range handlers {
			if handler == nil {
				m.cfg.Logger.Warn("nil event handler",
					"eventType", eventType,
					"handler", id,
				)
				continue
			}

			go func(handler bus.MessageHandler, id string) {
				handlerCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
				defer cancel()

				m.cfg.Logger.Debug("invoking event handler",
					"eventType", eventType,
					"handler", id,
				)
				err := handler(handlerCtx, event)
				if err != nil {
					m.cfg.Logger.Error("event handler error",
						"error", err,
						"eventType", eventType,
						"handler", id,
					)
				}
			}(handler, id)
		}
	} else {
		m.cfg.Logger.Debug("no event handlers for event type",
			"eventType", eventType,
		)
	}

	if groups, ok := m.eventGroups[eventType]; ok {
		for _, group := range groups {
			if len(group.Members) == 0 {
				m.cfg.Logger.Error("event group has no members",
					"eventType", eventType,
					"group", group.Id,
				)
				continue
			}

			go func(group *Group) {
				handlerCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
				defer cancel()

				member := group.ChooseMember()

				m.cfg.Logger.Debug("invoking event handler",
					"eventType", eventType,
					"handler", member.Id,
					"group", group.Id,
				)
				if err := member.handler(handlerCtx, event); err != nil {
					m.cfg.Logger.Error("event handler error",
						"error", err,
						"eventType", eventType,
						"handler", member.Id,
						"group", group.Id,
					)
				}
				if err := member.FinishProcessing(); err != nil {
					m.cfg.Logger.Error("event handler finished processing error",
						"error", err,
						"eventType", eventType,
						"handler", member.Id,
						"group", group.Id,
					)
				}
			}(group)
		}
	} else {
		m.cfg.Logger.Debug("no event groups for event type",
			"eventType", eventType,
		)
	}
}

// Subscribe implements bus.EventBus.
func (m *memBus) Subscribe(eventType, listenerId string, handler bus.MessageHandler) error {
	m.eventMutex.Lock()
	defer m.eventMutex.Unlock()

	handlers, exists := m.eventHandlers[eventType]
	if !exists {
		m.cfg.Logger.Debug("creating new event handlers",
			"eventType", eventType,
		)
		handlers = newEventHandlers()
		m.eventHandlers[eventType] = handlers
	}

	m.cfg.Logger.Info("registering event handler",
		"handler", listenerId,
		"eventType", eventType,
	)
	if _, exists := handlers[listenerId]; exists {
		m.cfg.Logger.Error("event handler already exists",
			"handler", listenerId,
			"eventType", eventType,
		)
		return bus.ErrHandlerExists
	}

	handlers[listenerId] = handler
	return nil
}

// GroupSubscribe implements bus.EventBus.
func (m *memBus) GroupSubscribe(eventType, listenerId, groupId string, handler bus.MessageHandler) error {
	m.eventMutex.Lock()
	defer m.eventMutex.Unlock()

	groups, exists := m.eventGroups[eventType]
	if !exists {
		m.cfg.Logger.Debug("creating new event groups",
			"eventType", eventType,
		)
		groups = NewGroups()
		m.eventGroups[eventType] = groups
	}

	group, exists := groups[groupId]
	if !exists {
		m.cfg.Logger.Debug("creating new event group",
			"eventType", eventType,
			"group", groupId,
		)
		group = NewGroup(groupId)
		groups[groupId] = group
	}

	m.cfg.Logger.Info("registering event group handler",
		"handler", listenerId,
		"eventType", eventType,
		"group", groupId,
	)
	if err := group.AddMember(listenerId, handler); err != nil {
		m.cfg.Logger.Error("failed to add member to event group",
			"error", err,
			"handler", listenerId,
			"eventType", eventType,
			"group", groupId,
		)
		return err
	}

	return nil
}

// GroupUnsubscribe implements bus.EventBus.
func (m *memBus) GroupUnsubscribe(eventType string, listenerId string, groupId string) error {
	m.eventMutex.Lock()
	defer m.eventMutex.Unlock()

	groups, exists := m.eventGroups[eventType]
	if !exists {
		m.cfg.Logger.Warn("no event groups for event type",
			"eventType", eventType,
		)
		return ErrGroupNotFound
	}

	group, exists := groups[groupId]
	if !exists {
		m.cfg.Logger.Warn("no event group for event type and group ID",
			"eventType", eventType,
			"group", groupId,
		)
		return ErrGroupNotFound
	}

	m.cfg.Logger.Info("removing event group member",
		"handler", listenerId,
		"eventType", eventType,
		"group", groupId,
	)
	group.RemoveMember(listenerId)

	return nil
}

// Unsubscribe implements bus.EventBus.
func (m *memBus) Unsubscribe(eventType string, listenerId string) error {
	m.eventMutex.Lock()
	defer m.eventMutex.Unlock()

	handlers, exists := m.eventHandlers[eventType]
	if !exists {
		m.cfg.Logger.Warn("no event handlers for event type",
			"eventType", eventType,
		)
		return bus.ErrHandlerNotFound
	}

	m.cfg.Logger.Info("removing event handler",
		"handler", listenerId,
		"eventType", eventType,
	)
	delete(handlers, listenerId)

	return nil
}

func newEventHandlers() map[string]bus.MessageHandler {
	return make(map[string]bus.MessageHandler)
}
