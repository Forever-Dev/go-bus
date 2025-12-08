# go-bus
go-bus is a general-purpose, high-performance message bus library for Go. It is designed to abstract away implementation details, allowing your application to communicate via loosely coupled components using a unified interface(s).

# Features

* **Interface-First Design**: Code against the bus.MessageBus interface, not the specific implementation.

* Dual Patterns:

    * **Command Bus (Point-to-Point)**: Send commands to specific handlers or load-balance across a group.

    * **Event Bus (Pub/Sub)**: Broadcast events to multiple listeners and consumer groups.

* High Performance In-Memory Implementation:

    * **Zero-Copy**: Passes pointers and interfaces directly within memory, avoiding expensive serialization/deserialization overhead.

    * **Concurrency Control**: Optional Worker Pool to limit goroutines and manage system load.

    * **Load Balancing**: Smart member selection within groups based on active processing counts.

    * **Generics Support**: Type-safe helper functions (`Subscribe[T]`, `GroupSubscribe[T]`) to avoid manual type assertions.

# Installation

```bash
go get github.com/forever-dev/go-bus
```


# Core Concepts

## The Message Interface

All data sent over the bus must satisfy the bus.Message interface. This ensures the bus can identify, route, and potentially serialize the data.

```go
// Message defines the interface for a message in the message bus system.
type Message interface {
	// GetId returns the unique identifier of the message.
	GetId() string

	// TypeKey returns the type key of the message.
	TypeKey() string

	// Bytes returns the payload of the message.
	Bytes() ([]byte, error)

	// Unmarshal populates the message from the given byte slice.
	Unmarshal(data []byte) error

	// Raw returns the raw representation of the message.
	Raw() any
}
```


## Modes of Communication

### Commands (Send / Handle) (Point-to-Point)

Used when an action needs to be performed exactly once. If you register a handler via Handle, it receives the message. If you register a group via GroupHandle, the bus selects one member of that group to process the command.
```go
// CommandBus defines the interface for a message bus (point-to-point) system.
type CommandBus interface {
	// Send sends a message to the bus.
	Send(ctx context.Context, msg Message) error

	// Handle registers a handler for incoming messages.
	// To unregister, pass a nil handler.
	Handle(msg Message, handler MessageHandler) error

	// GroupHandle registers a handler for incoming messages for a specific group.
	// Only one listener will receive a given message within a group.
	GroupHandle(handlerId, groupId string, msg Message, handler MessageHandler) error

	// GroupRemoveHandler removes a handler for incoming messages for a specific group.
	GroupRemoveHandler(handlerId, groupId string, msg Message) error

	// Shutdown gracefully shuts down the event bus
	Shutdown(ctx context.Context) error
}
```

### Events (Publish / Subscribe) (Pub/Sub)
Used for notifications where multiple components may be interested. Publish sends the message to all individual subscribers registered via Subscribe. If Consumer Groups are used (GroupSubscribe), the message is also delivered to one member of each group.
```go
type EventBus interface {
	// Publish sends a message to the bus.
	Publish(ctx context.Context, msg Message) error

	// Subscribe registers a handler for incoming messages.
	Subscribe(eventType, listenerId string, handler MessageHandler) error

	// Unsubscribe removes a handler for incoming messages.
	Unsubscribe(eventType, listenerId string) error

	// GroupSubscribe registers a handler for incoming messages for a specific group.
	// Only one listener will receive a given message within a group.
	GroupSubscribe(eventType, listenerId, groupId string, handler MessageHandler) error

	// GroupUnsubscribe removes a handler for incoming messages for a specific group.
	GroupUnsubscribe(eventType, listenerId, groupId string) error

	// Shutdown gracefully shuts down the event bus
	Shutdown(ctx context.Context) error
}
```
## Usage Examples

### `membus`: In-Memory Message Bus
A full example application is available [here](examples/membus/main.go).

1. Initialization (In-Memory Bus)

You initialize the membus with an optional configuration, which includes worker pool settings.

```go
package main

import (
    "context"
    "github.com/forever-dev/go-bus/pkg/bus/membus"
)

func main() {
    ctx := context.Background()

    // 1. Simple Setup (Default, No Worker Pool)
    // Events are executed in separate goroutines (unbounded concurrency).
    simpleBus := membus.New(ctx, membus.NewDefaultConfig())

    // 2. Performance Setup (Worker Pool)
    // Limits event concurrency to 50 workers with a buffer of 1000 jobs.
    cfg := membus.NewDefaultConfig()
    cfg.WorkerPoolConfig = membus.NewWorkerPoolConfig(50, 1000) // 50 workers, 1000 queue size
    
    workerBus := membus.New(ctx, cfg)
    
    // Always call Shutdown for graceful termination
    defer workerBus.Shutdown(ctx)
}
```

2. Defining a Message

You can use the built-in `BasicMessage` or implement the Message interface on your own structs.

```go
// Example implementation of bus.Message
type UserCreatedEvent struct {
    ID    string
    Email string
}

func (*UserCreatedEvent) TypeKey() string { return "event.user.created" }
func (e *UserCreatedEvent) GetId() string { return e.ID }
// ... implement Raw(), Bytes(), Unmarshal() ...
```


3. Pub/Sub with Generics

Use the type-safe generic helpers for subscribing.

```go
import "github.com/forever-dev/go-bus/pkg/bus"

func main() {
    b := membus.New(context.Background(), membus.NewDefaultConfig())

    // Subscribe using the generic helper
    handlerId := "email-service-listener"
    bus.Subscribe(b, handlerId, func(ctx context.Context, msg *UserCreatedEvent) error {
        // 'msg' is automatically cast and type-safe (*UserCreatedEvent)
        fmt.Printf("Sending welcome email to: %s\n", msg.Email)
        return nil
    })

    // Publish
    evt := &UserCreatedEvent{ID: "uuid-123", Email: "test@example.com"}
    b.Publish(context.Background(), evt)
}
```


4. Command Groups (Load Balancing)

Use GroupHandle to distribute commands among group members. The `membus` implementation balances load by choosing the member with the fewest active messages.

```go
type ResizeCommand struct { FilePath string }
func (*ResizeCommand) TypeKey() string { return "command.image.resize" }

func main() {
    b := membus.New(ctx, cfg)
    
    groupId := "image-processors"
    
    // Register 3 workers under the "image-processors" group
    for i := 0; i < 3; i++ {
        workerID := fmt.Sprintf("worker-%d", i)
        b.GroupHandle(workerID, groupId, &ResizeCommand{}, func(ctx context.Context, msg bus.Message) error {
            // Processing logic here
            fmt.Printf("%s processing command for %s...\n", workerID, msg.TypeKey())
            time.Sleep(10 * time.Millisecond) // Simulate work
            return nil
        })
    }

    // Send commands - they will be load-balanced across the 3 workers
    cmd1 := &ResizeCommand{FilePath: "image1.png"}
    cmd2 := &ResizeCommand{FilePath: "image2.png"}
    b.Send(ctx, cmd1) // Worker 1 gets it
    b.Send(ctx, cmd2) // Worker 2 gets it
}
```


### Configuration

#### Worker Pool Configuration

The `membus` implementation uses a worker pool for events (Publish) to control concurrency and resource utilization.

| Setting | Description |
|---|---|
| `NumWorkers` | The number of goroutines that will be used to process events concurrently. |
| `QueueSize` | The size of the buffered channel (event queue). If the queue is full, Publish will block or return an error if the context times out. |

* To **Enable**: Set the WorkerPoolConfig field on your bus configuration object.

* To **Disable**: Set WorkerPoolConfig to nil (default behavior is to spawn a new goroutine per event).
