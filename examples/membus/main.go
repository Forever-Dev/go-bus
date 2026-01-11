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

	cfg := membus.NewDefaultConfig()
	cfg.Logger = NewSimpleConsoleLogger()
	cfg.WorkerPoolConfig = membus.NewWorkerPoolConfig(4, 20)

	b := membus.New(ctx, cfg)
	defer b.Shutdown(ctx)

	time.Sleep(100 * time.Millisecond)

	// --- SETUP: COMMAND BUS ---
	fmt.Println("\n--- Setting up Command Bus Handlers ---")
	b.Handle(&ProcessImageCommand{}, commandHandler)

	groupId := "image-processor-group"
	b.GroupHandle("cmd-worker-1", groupId, &ProcessImageCommand{}, groupCommandHandler("cmd-worker-1"))
	b.GroupHandle("cmd-worker-2", groupId, &ProcessImageCommand{}, groupCommandHandler("cmd-worker-2"))
	b.GroupHandle("cmd-worker-3", groupId, &ProcessImageCommand{}, groupCommandHandler("cmd-worker-3"))

	// --- EXECUTE: COMMAND BUS ---
	fmt.Println("\n--- Sending ProcessImageCommand ---")
	for i := 1; i <= 6; i++ {
		cmd := &ProcessImageCommand{ID: fmt.Sprintf("cmd-%d", i), FilePath: fmt.Sprintf("/path/to/img_%d.jpg", i)}
		if err := b.Send(ctx, cmd); err != nil {
			cfg.Logger.Error("Error sending command", "error", err)
			os.Exit(1)
		}
	}
	time.Sleep(200 * time.Millisecond)

	// --- SETUP: EVENT BUS (Typed) ---

	fmt.Println("\n--- Setting up Event Bus Subscribers ---")

	// 1. SubscribeTyped: We pass nil for the unmarshaller because membus
	//    passes the object pointer directly. The shim will assert the type.
	bus.SubscribeTyped(b, TopicUserRegistered, "email-service", eventSubscriberHandler, nil)
	bus.SubscribeTyped(b, TopicUserRegistered, "notification-service", eventSubscriberHandler, nil)

	cfg.Logger.Info("Registered 2 independent subscribers for UserRegisteredEvent")

	// 2. GroupSubscribeTyped
	eventGroupId := "analytics-consumer-group"
	bus.GroupSubscribeTyped(b, TopicUserRegistered, "analytics-consumer-a", eventGroupId, groupEventConsumer("A"), nil)
	bus.GroupSubscribeTyped(b, TopicUserRegistered, "analytics-consumer-b", eventGroupId, groupEventConsumer("B"), nil)

	cfg.Logger.Info("Registered 2 consumers for analytics event group", "group", eventGroupId)

	// --- EXECUTE: EVENT BUS ---
	fmt.Println("\n--- Publishing UserRegisteredEvent ---")

	for i := 1; i <= 4; i++ {
		evt := &UserRegisteredEvent{ID: fmt.Sprintf("evt-%d", i), Email: fmt.Sprintf("user_%d@example.com", i)}
		if err := b.Publish(ctx, evt); err != nil {
			cfg.Logger.Error("Error publishing event", "error", err)
			os.Exit(1)
		}
	}

	fmt.Println("\n--- Waiting for all background events to complete ---")
	time.Sleep(500 * time.Millisecond)
	fmt.Println("\n--- Example Finished. Shutting Down Bus ---")
}
