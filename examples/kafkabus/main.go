package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/forever-dev/go-bus/pkg/bus"
	"github.com/forever-dev/go-bus/pkg/bus/kafkabus"
	"github.com/twmb/franz-go/pkg/kgo"
)

const KafkaBroker = "localhost:19092"
const MaxWaitTime = 30 * time.Second

func main() {
	fmt.Println("--- Starting go-bus Kafka Example ---")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize Bus
	logger := NewSimpleConsoleLogger()
	cfg := kafkabus.Config{
		Seeds:  []string{KafkaBroker},
		Logger: logger,
	}

	if err := waitForKafka(ctx, logger, cfg.Seeds); err != nil {
		logger.Error("Failed to connect to Kafka", "error", err)
		os.Exit(1)
	}

	b, err := kafkabus.NewKafkaBus(ctx, cfg)
	if err != nil {
		logger.Error("Failed to initialize Kafka Bus.", "error", err)
		os.Exit(1)
	}
	defer b.Shutdown(ctx)
	logger.Info("Kafka Bus initialized successfully")

	// --- SETUP: EVENT BUS (Typed) ---
	var wg sync.WaitGroup
	wg.Add(1)

	// 2. SubscribeTyped: We pass json.Unmarshal.
	// The library will fetch bytes from Kafka, call Unmarshal, and pass *OrderCreatedEvent to handlers.

	bus.SubscribeTyped(b, TopicOrderCreated, "inventory-service", inventorySubscriber, json.Unmarshal)

	// Inline handler example
	bus.SubscribeTyped(b, TopicOrderCreated, "reporting-service", func(ctx context.Context, msg *OrderCreatedEvent) error {
		fmt.Printf(" [EVT] [Subscriber:Reporting] Received Order %s. Generating report.\n", msg.ID)
		return nil
	}, json.Unmarshal)

	logger.Info("Registered 2 independent subscribers for OrderCreatedEvent")

	// 3. GroupSubscribeTyped
	eventGroupId := "order-audit-group"
	bus.GroupSubscribeTyped(b, TopicOrderCreated, "audit-consumer-1", eventGroupId, auditConsumer("A"), json.Unmarshal)
	bus.GroupSubscribeTyped(b, TopicOrderCreated, "audit-consumer-2", eventGroupId, auditConsumer("B"), json.Unmarshal)

	logger.Info("Registered 2 consumers for audit event group", "group", eventGroupId)

	go waitForTermination(logger, cancel, &wg)

	logger.Info("Waiting for Kafka consumers to connect and stabilize...")
	time.Sleep(3 * time.Second)

	// --- PUBLISH ---

	logger.Info("\n--- Publishing 5 OrderCreatedEvent messages ---")
	for i := 1; i <= 5; i++ {
		evt := &OrderCreatedEvent{
			ID:        fmt.Sprintf("ORD-%03d", i),
			ProductID: fmt.Sprintf("PROD-%d", i%3),
			Quantity:  i * 10,
		}
		if err := b.Publish(ctx, evt); err != nil {
			logger.Error("Failed to publish event", "error", err)
		}
	}

	wg.Wait()
	logger.Info("Application shutting down.")
}

func waitForTermination(logger bus.Logger, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer wg.Done()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signalChan:
		logger.Info("Termination signal received.")
	case <-time.After(MaxWaitTime):
		logger.Info("Exiting after max wait time.")
	}
	cancel()
}

func waitForKafka(ctx context.Context, logger bus.Logger, seeds []string) error {
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.WithLogger(kafkabus.NewKafkaLoggerAdapter(logger, kgo.LogLevelDebug)),
	)
	if err != nil {
		return fmt.Errorf("failed to create client for health check: %w", err)
	}
	defer client.Close()

	if err := client.Ping(checkCtx); err != nil {
		return fmt.Errorf("failed to ping Kafka brokers: %w", err)
	}
	return nil
}
