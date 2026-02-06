package membus

import (
	"context"
	"sync"

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

	jobCh           chan Job
	workers         []*Worker
	usingWorkerPool bool
	workerWg        sync.WaitGroup

	cfg *Config
}

type Config struct {
	Logger bus.Logger

	// WorkerPoolConfig holds the configuration for the worker pool.
	// If nil, no worker pool is used and event handlers are
	// executed in a separate goroutine for each event. Otherwise,
	// a worker pool is used to limit the number of concurrent event handlers
	// and goroutines.
	WorkerPoolConfig *WorkerPoolConfig
}

func NewDefaultConfig() *Config {
	return &Config{
		Logger: bus.NewNoOpLogger(),
	}
}

func New(
	ctx context.Context,
	cfg *Config,
) bus.MessageBus {
	b := &memBus{
		msgHandlers:   make(map[string]bus.MessageHandler),
		msgGroups:     make(map[string]*Group),
		eventHandlers: make(EventMap),
		eventGroups:   make(EventGroupMap),
		cfg:           cfg,
	}

	if b.cfg.WorkerPoolConfig != nil {
		if b.cfg.WorkerPoolConfig.NumWorkers <= 0 {
			b.cfg.WorkerPoolConfig.NumWorkers = 10
		}
		if b.cfg.WorkerPoolConfig.QueueSize <= 0 {
			b.cfg.WorkerPoolConfig.QueueSize = 100
		}

		b.workers = make([]*Worker, b.cfg.WorkerPoolConfig.NumWorkers)
		b.jobCh = make(chan Job, b.cfg.WorkerPoolConfig.QueueSize)
		b.workerWg.Add(len(b.workers))
		for idx := range b.workers {
			worker := &Worker{
				Id:     idx + 1,
				Logger: b.cfg.Logger,
			}
			b.workers[idx] = worker
			worker.Start(ctx, b.jobCh, &b.workerWg)
		}

		b.usingWorkerPool = true
	}

	return b
}

func (b *memBus) Shutdown(ctx context.Context) error {
	close(b.jobCh)
	b.workerWg.Wait()
	return nil
}

func (b *memBus) Ping(ctx context.Context) error {
	return nil
}
