package membus

import (
	"sync"
	"time"

	"github.com/forever-dev/go-bus/pkg/bus"
)

// EventMap is a map of event handlers grouped by event type and listener ID
type EventMap map[string]map[string]bus.MessageHandler
type EventGroupMap map[string]map[string]*Group

type memBus struct {
	// msgHandlers hold the registered handlers for messages
	msgHandlers map[string]bus.MessageHandler

	// msgGroups hold the group subscriptions for messages
	msgGroups map[string]*Group

	// eventHandlers hold the registered handlers for events
	eventHandlers EventMap
	// eventGroups hold the group subscriptions for events
	eventGroups EventGroupMap

	// msgMutx is used to synchronize access to message handlers and groups
	msgMutex sync.RWMutex
	// eventMutx is used to synchronize access to event handlers and groups
	eventMutex sync.RWMutex

	cfg *Config
}

type Config struct {
	Timeout time.Duration
	Logger  bus.Logger
}

func NewConfig() *Config {
	return &Config{
		Timeout: 15 * time.Second,
		Logger:  bus.NewNoOpLogger(),
	}
}

func New(
	cfg *Config,
) bus.MessageBus {
	return &memBus{
		msgHandlers:   make(map[string]bus.MessageHandler),
		msgGroups:     make(map[string]*Group),
		eventHandlers: make(EventMap),
		eventGroups:   make(EventGroupMap),
		cfg:           cfg,
	}
}
