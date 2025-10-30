package kafka

// ============================================================================
// Kafka Analytics Module for Prebid Server
// ============================================================================
//
// This module streams raw OpenRTB requests/responses to Apache Kafka.
// All field extraction and transformation happens in Flink.
//
// ARCHITECTURE:
//   Prebid Server → Kafka (raw events) → Flink (transform) → ClickHouse (store)
//
// FILE ORGANIZATION:
// - kafka_module.go: Main module logic (this file)
//   - Kafka connection and producer configuration
//   - Event filtering (event types, accounts, sample rate)
//   - Optional privacy controls (IP anonymization, user ID masking)
//   - Message sending to Kafka
//
// - transform.go: Simple transformation to Kafka event
//   - Extracts metadata (event_type, timestamp, account_id, request_id)
//   - Includes raw OpenRTB request/response as JSON
//
// - model.go: Event data structure (OpenRTBEvent)
//
// CONFIGURATION:
// See pbs.yaml for configuration options
//
// ============================================================================

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"log"
	"math/big"
	"time"

	"github.com/prebid/prebid-server/v3/analytics"
	"github.com/prebid/prebid-server/v3/config"
	"github.com/segmentio/kafka-go"
)

// KafkaWriter implements the Prebid Server analytics interface
// and streams OpenRTB events to Kafka for processing with Flink and ClickHouse
type KafkaWriter struct {
	writer *kafka.Writer
	config *config.KafkaLogs
}

// NewKafkaWriter creates a new Kafka analytics adapter with configuration
func NewKafkaWriter(cfg *config.KafkaLogs) (*KafkaWriter, error) {
	// Apply defaults
	batchSize := 100
	if cfg.Producer.BatchSize > 0 {
		batchSize = cfg.Producer.BatchSize
	}

	batchTimeout := 10 * time.Millisecond
	if cfg.Producer.BatchTimeout != "" {
		if duration, err := time.ParseDuration(cfg.Producer.BatchTimeout); err == nil {
			batchTimeout = duration
		}
	}

	requiredAcks := kafka.RequireOne
	if cfg.Producer.RequiredAcks != 0 {
		requiredAcks = kafka.RequiredAcks(cfg.Producer.RequiredAcks)
	}

	writer := &kafka.Writer{
		Addr:            kafka.TCP(cfg.Brokers...),
		Topic:           cfg.Topic,
		Balancer:        &kafka.LeastBytes{},
		BatchSize:       batchSize,
		BatchTimeout:    batchTimeout,
		Async:           cfg.Producer.Async,
		RequiredAcks:    requiredAcks,
		MaxAttempts:     cfg.Retry.MaxRetries,
		WriteBackoffMin: 100 * time.Millisecond,
		WriteBackoffMax: 1 * time.Second,
	}

	// Apply retry backoff if configured
	if cfg.Retry.RetryBackoff != "" {
		if duration, err := time.ParseDuration(cfg.Retry.RetryBackoff); err == nil {
			writer.WriteBackoffMin = duration
			writer.WriteBackoffMax = duration * 10
		}
	}

	return &KafkaWriter{
		writer: writer,
		config: cfg,
	}, nil
}

// LogAuctionObject processes auction events and sends to Kafka
func (a *KafkaWriter) LogAuctionObject(ao *analytics.AuctionObject) {
	if !a.shouldLogEvent("auction", ao.Account) {
		return
	}
	if !a.shouldSample() {
		return
	}
	event := a.transformAuctionToEvent(ao)
	a.sendToKafka(event, "auction")
}

// LogVideoObject processes video events
func (a *KafkaWriter) LogVideoObject(vo *analytics.VideoObject) {
	if !a.shouldLogEvent("video", nil) {
		return
	}
	if !a.shouldSample() {
		return
	}
	event := a.transformVideoToEvent(vo)
	a.sendToKafka(event, "video")
}

// LogCookieSyncObject processes cookie sync events
func (a *KafkaWriter) LogCookieSyncObject(cso *analytics.CookieSyncObject) {
	if !a.shouldLogEvent("cookie_sync", nil) {
		return
	}
	if !a.shouldSample() {
		return
	}
	event := a.transformCookieSyncToEvent(cso)
	a.sendToKafka(event, "cookie_sync")
}

// LogSetUIDObject processes setuid events
func (a *KafkaWriter) LogSetUIDObject(so *analytics.SetUIDObject) {
	if !a.shouldLogEvent("setuid", nil) {
		return
	}
	if !a.shouldSample() {
		return
	}
	event := a.transformSetUIDToEvent(so)
	a.sendToKafka(event, "setuid")
}

// LogAmpObject processes AMP events
func (a *KafkaWriter) LogAmpObject(ao *analytics.AmpObject) {
	if !a.shouldLogEvent("amp", nil) {
		return
	}
	if !a.shouldSample() {
		return
	}
	event := a.transformAmpToEvent(ao)
	a.sendToKafka(event, "amp")
}

// LogNotificationEventObject processes notification events
func (a *KafkaWriter) LogNotificationEventObject(neo *analytics.NotificationEvent) {
	if !a.shouldLogEvent("notification", neo.Account) {
		return
	}
	if !a.shouldSample() {
		return
	}
	event := a.transformNotificationToEvent(neo)
	a.sendToKafka(event, "notification")
}

// Shutdown closes the Kafka writer
func (a *KafkaWriter) Shutdown() {
	if a.writer != nil {
		if err := a.writer.Close(); err != nil {
			log.Printf("[KafkaWriter] Error closing Kafka writer: %v", err)
		}
	}
}

// shouldLogEvent checks if the event type should be logged based on filters
func (a *KafkaWriter) shouldLogEvent(eventType string, account *config.Account) bool {
	// Check event type filter
	if len(a.config.Filters.EventTypes) > 0 {
		found := false
		for _, allowedType := range a.config.Filters.EventTypes {
			if allowedType == eventType {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check account filter
	if len(a.config.Filters.Accounts) > 0 && account != nil {
		found := false
		for _, allowedAccount := range a.config.Filters.Accounts {
			if allowedAccount == account.ID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// shouldSample determines if this event should be sampled based on sample rate
func (a *KafkaWriter) shouldSample() bool {
	sampleRate := a.config.Filters.SampleRate
	if sampleRate <= 0 {
		sampleRate = 1.0 // Default to 100%
	}
	if sampleRate >= 1.0 {
		return true
	}

	// Generate random number between 0 and 1
	randomBig, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return true // On error, allow the event
	}
	random := float64(randomBig.Int64()) / 1000000.0
	return random < sampleRate
}

// ============================================================================
// KAFKA MESSAGE SENDING
// ============================================================================
// All OpenRTB transformation logic has been moved to transform.go
// This keeps kafka_module.go focused on Kafka operations and configuration

// sendToKafka sends the event to Kafka
func (a *KafkaWriter) sendToKafka(event *OpenRTBEvent, eventType string) {
	if event == nil {
		return
	}

	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("[KafkaWriter] Failed to marshal %s event: %v", eventType, err)
		return
	}

	msg := kafka.Message{
		Key:   []byte(event.RequestID), // Use request ID as partition key
		Value: data,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "account_id", Value: []byte(event.AccountID)},
		},
		Time: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.writer.WriteMessages(ctx, msg); err != nil {
		log.Printf("[KafkaWriter] Failed to write %s message to Kafka: %v", eventType, err)
	}
}
