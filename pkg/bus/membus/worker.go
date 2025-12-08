package membus

import (
	"context"
	"sync"

	"github.com/forever-dev/go-bus/pkg/bus"
)

type WorkerPoolConfig struct {
	// NumWorkers is the number of workers in the pool,
	// and therefore the number of goroutines that will
	// be used to process events.
	NumWorkers int

	// QueueSize is the size of the event queue.
	// If the queue is full, adding new events will block
	// until there is space in the queue or the timeout is reached.
	QueueSize int
}

type Job struct {
	Ctx     context.Context
	Message bus.Message
	Handler bus.MessageHandler

	Member *GroupMember

	Type string
}

type Worker struct {
	Id     int
	Logger bus.Logger
}

func NewWorkerPoolConfig(numWorkers, queueSize int) *WorkerPoolConfig {
	return &WorkerPoolConfig{
		NumWorkers: numWorkers,
		QueueSize:  queueSize,
	}
}

func (w *Worker) Start(
	ctx context.Context,
	workerPoolCh chan Job,
	wg *sync.WaitGroup,
) {
	go func() {
		w.Logger.Debug("worker started", "worker", w.Id)
		defer wg.Done()

		for {
			select {
			case job, ok := <-workerPoolCh:
				if !ok {
					w.Logger.Debug("worker pool channel closed, stopping worker", "worker", w.Id)
					return
				}

				w.Logger.Debug("worker processing job", "worker", w.Id, "type", job.Type)

				err := job.Handler(job.Ctx, job.Message)
				if err != nil {
					w.Logger.Error("error processing job",
						"worker", w.Id,
						"type", job.Type,
						"error", err,
					)
				} else {
					w.Logger.Debug("worker finished job successfully", "worker", w.Id, "type", job.Type)
				}

				if job.Member != nil {
					w.Logger.Debug("worker finishing job member", "worker", w.Id, "type", job.Type)
					if err := job.Member.FinishProcessing(); err != nil {
						w.Logger.Error("error finishing processing for member",
							"worker", w.Id,
							"member", job.Member.Id,
							"type", job.Type,
							"error", err,
						)
					}
				}

			case <-ctx.Done():
				w.Logger.Debug("worker context done, stopping", "worker", w.Id)
				return
			}
		}
	}()
}
