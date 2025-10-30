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
	"crypto/rand"
	"encoding/json"
	"log"
	"math/big"
	"strings"

	kafkalib "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prebid/prebid-server/v3/analytics"
	"github.com/prebid/prebid-server/v3/config"
)

// KafkaWriter implements the Prebid Server analytics interface
// and streams OpenRTB events to Kafka for processing with Flink and ClickHouse
type KafkaWriter struct {
	producer *kafkalib.Producer
	config   *config.KafkaLogs
}

// NewKafkaWriter creates a new Kafka analytics adapter with configuration
func NewKafkaWriter(cfg *config.KafkaLogs) (*KafkaWriter, error) {
	// Build producer config from YAML settings
	producerConfig := &kafkalib.ConfigMap{
		"bootstrap.servers":   strings.Join(cfg.Brokers, ","),
		"go.delivery.reports": false, // Disable delivery reports for fire-and-forget mode
	}

	// Apply producer settings from config (with defaults)
	if cfg.Producer.LingerMs > 0 {
		producerConfig.SetKey("linger.ms", cfg.Producer.LingerMs)
	}
	if cfg.Producer.BatchSize > 0 {
		producerConfig.SetKey("batch.size", cfg.Producer.BatchSize)
	}
	if cfg.Producer.CompressionType != "" {
		producerConfig.SetKey("compression.type", cfg.Producer.CompressionType)
	}
	if cfg.Producer.Acks >= -1 {
		producerConfig.SetKey("acks", cfg.Producer.Acks)
	}
	if cfg.Producer.MaxInFlight > 0 {
		producerConfig.SetKey("max.in.flight", cfg.Producer.MaxInFlight)
	}
	if cfg.Producer.QueueBufferingMaxMessages > 0 {
		producerConfig.SetKey("queue.buffering.max.messages", cfg.Producer.QueueBufferingMaxMessages)
	}

	// Initialize the producer
	producer, err := kafkalib.NewProducer(producerConfig)
	if err != nil {
		log.Printf("[KafkaWriter] Failed to create Kafka producer: %v", err)
		return nil, err
	}

	log.Printf("[KafkaWriter] Successfully initialized (brokers: %v, topic: %s, compression: %s)",
		cfg.Brokers, cfg.Topic, cfg.Producer.CompressionType)

	return &KafkaWriter{
		producer: producer,
		config:   cfg,
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

// Shutdown closes the Kafka producer
func (a *KafkaWriter) Shutdown() {
	if a.producer != nil {
		a.producer.Flush(5000) // Wait up to 5 seconds for messages to be delivered
		a.producer.Close()
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

// sendToKafka sends the event to Kafka asynchronously
// Produce() is non-blocking and returns immediately after queuing the message
// The message will be sent by a background thread
func (a *KafkaWriter) sendToKafka(event *OpenRTBEvent, eventType string) {
	if event == nil {
		return
	}

	jsonData, err := json.Marshal(event)
	if err != nil {
		log.Printf("[KafkaWriter] Failed to marshal %s event: %v", eventType, err)
		return
	}

	msg := &kafkalib.Message{
		TopicPartition: kafkalib.TopicPartition{
			Topic:     &a.config.Topic,
			Partition: kafkalib.PartitionAny,
		},
		Value: jsonData,
	}

	// Produce() is async - queues message and returns immediately without blocking
	// nil delivery channel = fire-and-forget mode (no delivery confirmation)
	if err := a.producer.Produce(msg, nil); err != nil {
		// Only fails if queue is full or producer is closed
		log.Printf("[KafkaWriter] Failed to queue %s message (queue full?): %v", eventType, err)
	}
}
