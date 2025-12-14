package kafkabus

import (
	"context"
	"time"
)

// Flush ensures all outstanding production messages are delivered.
func (k *kafkaBus) Flush(ctx context.Context) error {
	if k.client == nil {
		return nil
	}
	// Use a 5 second timeout for flushing producers
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	k.config.Logger.Info("Flushing producer buffer...")
	if err := k.client.client.Flush(flushCtx); err != nil {
		k.config.Logger.Error("Failed to flush producer buffer", "error", err)
		return err
	}
	k.config.Logger.Info("Producer buffer successfully flushed.")
	return nil
}
