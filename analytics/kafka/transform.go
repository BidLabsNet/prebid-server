package kafka

import (
	"encoding/json"
	"time"

	"github.com/prebid/prebid-server/v3/analytics"
)

// transformAuctionToEvent converts auction data to a Kafka-ready event
func (a *KafkaWriter) transformAuctionToEvent(ao *analytics.AuctionObject) *OpenRTBEvent {
	if ao == nil {
		return nil
	}

	event := &OpenRTBEvent{
		EventType: "auction",
		Timestamp: time.Now().UnixMilli(),
	}

	// Extract account ID
	if ao.Account != nil {
		event.AccountID = ao.Account.ID
	}

	// Extract request ID
	if ao.RequestWrapper != nil && ao.RequestWrapper.BidRequest != nil {
		event.RequestID = ao.RequestWrapper.BidRequest.ID

		// Include complete OpenRTB request
		if rawReq, err := json.Marshal(ao.RequestWrapper.BidRequest); err == nil {
			event.RawBidRequest = rawReq
		}
	}

	// Include complete OpenRTB response
	if ao.Response != nil {
		if rawResp, err := json.Marshal(ao.Response); err == nil {
			event.RawBidResponse = rawResp
		}
	}

	return event
}

// transformVideoToEvent converts video data to a Kafka-ready event
func (a *KafkaWriter) transformVideoToEvent(vo *analytics.VideoObject) *OpenRTBEvent {
	if vo == nil {
		return nil
	}

	event := &OpenRTBEvent{
		EventType: "video",
		Timestamp: time.Now().UnixMilli(),
	}

	// Include complete OpenRTB request/response if available
	if vo.RequestWrapper != nil && vo.RequestWrapper.BidRequest != nil {
		event.RequestID = vo.RequestWrapper.BidRequest.ID
		if rawReq, err := json.Marshal(vo.RequestWrapper.BidRequest); err == nil {
			event.RawBidRequest = rawReq
		}
	}

	if vo.Response != nil {
		if rawResp, err := json.Marshal(vo.Response); err == nil {
			event.RawBidResponse = rawResp
		}
	}

	return event
}

// transformCookieSyncToEvent converts cookie sync data to a Kafka-ready event
func (a *KafkaWriter) transformCookieSyncToEvent(cso *analytics.CookieSyncObject) *OpenRTBEvent {
	if cso == nil {
		return nil
	}

	return &OpenRTBEvent{
		EventType: "cookie_sync",
		Timestamp: time.Now().UnixMilli(),
		// Cookie sync doesn't have OpenRTB request/response
	}
}

// transformSetUIDToEvent converts setuid data to a Kafka-ready event
func (a *KafkaWriter) transformSetUIDToEvent(so *analytics.SetUIDObject) *OpenRTBEvent {
	if so == nil {
		return nil
	}

	return &OpenRTBEvent{
		EventType: "setuid",
		Timestamp: time.Now().UnixMilli(),
		AccountID: so.Bidder,
	}
}

// transformAmpToEvent converts AMP data to a Kafka-ready event
func (a *KafkaWriter) transformAmpToEvent(ao *analytics.AmpObject) *OpenRTBEvent {
	if ao == nil {
		return nil
	}

	event := &OpenRTBEvent{
		EventType: "amp",
		Timestamp: time.Now().UnixMilli(),
	}

	// Include complete OpenRTB request/response
	if ao.RequestWrapper != nil && ao.RequestWrapper.BidRequest != nil {
		event.RequestID = ao.RequestWrapper.BidRequest.ID
		if rawReq, err := json.Marshal(ao.RequestWrapper.BidRequest); err == nil {
			event.RawBidRequest = rawReq
		}
	}

	if ao.AuctionResponse != nil {
		if rawResp, err := json.Marshal(ao.AuctionResponse); err == nil {
			event.RawBidResponse = rawResp
		}
	}

	return event
}

// transformNotificationToEvent converts notification data to a Kafka-ready event
func (a *KafkaWriter) transformNotificationToEvent(neo *analytics.NotificationEvent) *OpenRTBEvent {
	if neo == nil {
		return nil
	}

	event := &OpenRTBEvent{
		EventType: "notification",
		Timestamp: time.Now().UnixMilli(),
	}

	if neo.Account != nil {
		event.AccountID = neo.Account.ID
	}

	return event
}
