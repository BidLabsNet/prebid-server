package openrtb_ext

// ExtImpCarbonatix defines the contract for bidrequest.imp[i].ext.prebid.bidder.carbonatix
type ExtImpCarbonatix struct {
	// Endpoint is the custom endpoint URL for this impression (optional)
	// If not provided, the default endpoint from bidder-info will be used
	Endpoint string `json:"endpoint,omitempty"`
}
