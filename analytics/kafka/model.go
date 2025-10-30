package kafka

import "encoding/json"

// OpenRTBEvent represents the event structure sent to Kafka
// Contains raw OpenRTB request/response for processing in Flink
type OpenRTBEvent struct {
	// Event metadata
	EventType string `json:"event_type"`
	Timestamp int64  `json:"timestamp"` // Unix milliseconds
	AccountID string `json:"account_id"`
	RequestID string `json:"request_id"`

	// Raw OpenRTB objects - all field extraction happens in Flink
	RawBidRequest  json.RawMessage `json:"raw_bid_request,omitempty"`
	RawBidResponse json.RawMessage `json:"raw_bid_response,omitempty"`
}
