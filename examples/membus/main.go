package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/forever-dev/go-bus/pkg/bus"
	"github.com/forever-dev/go-bus/pkg/bus/membus"
)

func main() {
	fmt.Println("--- Starting go-bus Example ---")
	ctx := context.Background()

	// Initialize bus with a Worker Pool for concurrent event processing
	cfg := membus.NewDefaultConfig()
	cfg.Logger = NewSimpleConsoleLogger()
	cfg.WorkerPoolConfig = membus.NewWorkerPoolConfig(4, 20) // 4 workers, 20 job queue size

	b := membus.New(ctx, cfg)
	defer b.Shutdown(ctx) // Ensure graceful shutdown

	// Give time for workers to start
	time.Sleep(100 * time.Millisecond)

	// --- SETUP: COMMAND BUS (Point-to-Point) ---

	// 1. Register a simple single handler for ProcessImageCommand
	fmt.Println("\n--- Setting up Command Bus Handlers ---")
	b.Handle(&ProcessImageCommand{}, commandHandler)
	cfg.Logger.Info("Registered single handler for ProcessImageCommand")

	// 2. Register 3 workers for a load-balanced Command Group
	groupId := "image-processor-group"
	b.GroupHandle("cmd-worker-1", groupId, &ProcessImageCommand{}, groupCommandHandler("cmd-worker-1"))
	b.GroupHandle("cmd-worker-2", groupId, &ProcessImageCommand{}, groupCommandHandler("cmd-worker-2"))
	b.GroupHandle("cmd-worker-3", groupId, &ProcessImageCommand{}, groupCommandHandler("cmd-worker-3"))
	cfg.Logger.Info("Registered 3 workers for image processing command group", "group", groupId)

	// --- EXECUTE: COMMAND BUS ---

	fmt.Println("\n--- Sending ProcessImageCommand ---")
	// The message should be handled by the group members (higher priority)
	// OR the single handler if the group is removed/empty.
	// Since the group is registered, it will receive the command and load-balance it.

	for i := 1; i <= 6; i++ {
		cmd := &ProcessImageCommand{ID: fmt.Sprintf("cmd-%d", i), FilePath: fmt.Sprintf("/path/to/img_%d.jpg", i)}
		if err := b.Send(ctx, cmd); err != nil {
			cfg.Logger.Error("Error sending command", "error", err)
			os.Exit(1)
		}
	}

	time.Sleep(200 * time.Millisecond) // Wait for command processing

	// --- SETUP: EVENT BUS (Pub/Sub) ---

	fmt.Println("\n--- Setting up Event Bus Subscribers ---")
	// 1. Register two independent subscribers (both will receive the event)
	bus.Subscribe(b, "email-service", eventSubscriberHandler)
	bus.Subscribe(b, "notification-service", eventSubscriberHandler)
	cfg.Logger.Info("Registered 2 independent subscribers for UserRegisteredEvent")

	// 2. Register a load-balanced consumer group (only one member gets the event)
	eventGroupId := "analytics-consumer-group"
	bus.GroupSubscribe(b, "analytics-consumer-a", eventGroupId, groupEventConsumer("A"))
	bus.GroupSubscribe(b, "analytics-consumer-b", eventGroupId, groupEventConsumer("B"))
	cfg.Logger.Info("Registered 2 consumers for analytics event group", "group", eventGroupId)

	// --- EXECUTE: EVENT BUS ---

	fmt.Println("\n--- Publishing UserRegisteredEvent ---")
	// The event will be delivered to:
	// - email-service (Subscriber)
	// - notification-service (Subscriber)
	// - analytics-consumer-a OR analytics-consumer-b (Group Consumer - load balanced)

	for i := 1; i <= 4; i++ {
		evt := &UserRegisteredEvent{ID: fmt.Sprintf("evt-%d", i), Email: fmt.Sprintf("user_%d@example.com", i)}
		if err := b.Publish(ctx, evt); err != nil {
			cfg.Logger.Error("Error publishing event", "error", err)
			os.Exit(1)
		}
	}

	fmt.Println("\n--- Waiting for all background events to complete (Worker Pool active) ---")
	time.Sleep(500 * time.Millisecond) // Wait for worker pool to finish async event jobs

	fmt.Println("\n--- Example Finished. Shutting Down Bus ---")
}
