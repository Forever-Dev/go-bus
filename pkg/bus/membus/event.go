package membus

import (
	"context"

	"github.com/forever-dev/go-bus/pkg/bus"
)

// Publish implements bus.EventBus.
func (m *memBus) Publish(ctx context.Context, event bus.Message) error {
	m.eventMutex.RLock()
	defer m.eventMutex.RUnlock()

	if handlers, ok := m.eventHandlers[event.TypeKey()]; ok {
		for id, handler := range handlers {
			if handler == nil {
				m.cfg.Logger.Warn("nil event handler",
					"eventType", event.TypeKey(),
					"handler", id,
				)
				continue
			}

			if m.usingWorkerPool {
				select {
				case m.jobCh <- Job{
					Ctx:     ctx,
					Message: event,
					Handler: handler,
					Type:    event.TypeKey(),
				}:
				case <-ctx.Done():
					m.cfg.Logger.Error("context done before event handler could be queued",
						"eventType", event.TypeKey(),
						"handler", id,
						"error", ctx.Err(),
					)
					return ctx.Err()
				}
			} else {
				go func(handler bus.MessageHandler, id string) {
					m.cfg.Logger.Debug("invoking event handler",
						"eventType", event.TypeKey(),
						"handler", id,
					)
					err := handler(ctx, event)
					if err != nil {
						m.cfg.Logger.Error("event handler error",
							"error", err,
							"eventType", event.TypeKey(),
							"handler", id,
						)
					}
				}(handler, id)
			}
		}
	} else {
		m.cfg.Logger.Debug("no event handlers for event type",
			"eventType", event.TypeKey(),
		)
	}

	if groups, ok := m.eventGroups[event.TypeKey()]; ok {
		for _, group := range groups {
			if len(group.Members) == 0 {
				m.cfg.Logger.Error("event group has no members",
					"eventType", event.TypeKey(),
					"group", group.Id,
				)
				continue
			}

			if m.usingWorkerPool {
				member := group.ChooseMember()
				select {
				case m.jobCh <- Job{
					Ctx:     ctx,
					Message: event,
					Handler: member.handler,
					Member:  member,
					Type:    event.TypeKey(),
				}:
				case <-ctx.Done():
					m.cfg.Logger.Error("context done before event group handler could be queued",
						"eventType", event.TypeKey(),
						"handler", member.Id,
						"group", group.Id,
						"error", ctx.Err(),
					)
					return ctx.Err()
				}
			} else {
				go func(group *Group) {
					member := group.ChooseMember()
					if member == nil {
						m.cfg.Logger.Error("no available group member to handle event",
							"eventType", event.TypeKey(),
							"group", group.Id,
						)
						return
					}

					m.cfg.Logger.Debug("invoking event handler",
						"eventType", event.TypeKey(),
						"handler", member.Id,
						"group", group.Id,
					)
					if err := member.handler(ctx, event); err != nil {
						m.cfg.Logger.Error("event handler error",
							"error", err,
							"eventType", event.TypeKey(),
							"handler", member.Id,
							"group", group.Id,
						)
					}
					if err := member.FinishProcessing(); err != nil {
						m.cfg.Logger.Error("event handler finished processing error",
							"error", err,
							"eventType", event.TypeKey(),
							"handler", member.Id,
							"group", group.Id,
						)
					}
				}(group)
			}
		}
	} else {
		m.cfg.Logger.Debug("no event groups for event type",
			"eventType", event.TypeKey(),
		)
	}

	return nil
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
