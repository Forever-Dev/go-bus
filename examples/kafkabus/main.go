package main

import (
	"context"
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

	// 1. Initialize Bus and Logger
	logger := NewSimpleConsoleLogger()
	cfg := kafkabus.Config{
		Seeds:  []string{KafkaBroker},
		Logger: logger,
		Acks:   kgo.AllISRAcks(),
	}

	// Wait up to 5 seconds for Kafka to be available (useful for Docker setup)
	if err := waitForKafka(ctx, logger, cfg.Seeds); err != nil {
		logger.Error("Failed to connect to Kafka after waiting", "error", err)
		os.Exit(1)
	}

	b, err := kafkabus.NewKafkaBus(ctx, cfg)
	if err != nil {
		logger.Error("Failed to initialize Kafka Bus.", "error", err)
		os.Exit(1)
	}
	defer b.Shutdown(ctx)
	logger.Info("Kafka Bus initialized successfully", "brokers", KafkaBroker)

	// --- SETUP: EVENT BUS (Subscribers & Consumers) ---
	var wg sync.WaitGroup
	wg.Add(1) // Keeps the main goroutine alive until cancelled

	// 2. Register independent subscribers (both get all messages)
	bus.Subscribe(b, "inventory-service", inventorySubscriber)
	bus.Subscribe(b, "reporting-service", func(ctx context.Context, msg *OrderCreatedEvent) error {
		fmt.Printf(" [EVT] [Subscriber:Reporting] Received Order %s. Generating report.\n", msg.ID)
		return nil
	})
	logger.Info("Registered 2 independent subscribers for OrderCreatedEvent")

	// 3. Register a load-balanced consumer group (only one member gets a given message)
	eventGroupId := "order-audit-group"
	bus.GroupSubscribe(b, "audit-consumer-1", eventGroupId, auditConsumer("A"))
	bus.GroupSubscribe(b, "audit-consumer-2", eventGroupId, auditConsumer("B"))
	logger.Info("Registered 2 consumers for audit event group", "group", eventGroupId)

	// Start the main loop to listen for termination signals
	go waitForTermination(logger, cancel, &wg)

	// Give consumers time to connect and partition to balance
	logger.Info("Waiting for Kafka consumers to connect and stabilize...")
	time.Sleep(3 * time.Second)

	// --- PUBLISH & SEND ---

	logger.Info("\n--- Publishing 5 OrderCreatedEvent messages (Async Pub/Sub) ---")
	for i := 1; i <= 5; i++ {
		evt := &OrderCreatedEvent{
			ID:        fmt.Sprintf("ORD-%03d", i),
			ProductID: fmt.Sprintf("PROD-%d", i%3),
			Quantity:  i * 10,
		}
		if err := b.Publish(ctx, evt); err != nil {
			logger.Error("Failed to publish event", "error", err, "messageId", evt.ID)
		}
	}

	// Keep the main goroutine alive until cancelled by signal
	wg.Wait()
	logger.Info("Application shutting down.")
}

// waitForTermination blocks until a termination signal is received or MaxWaitTime is reached.
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

// waitForKafka attempts to connect to Kafka with a timeout.
func waitForKafka(ctx context.Context, logger bus.Logger, seeds []string) error {
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Use kgo.NewClient with ConsumeTopics set to nil to just check connectivity
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
