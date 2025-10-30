package carbonatix

import (
	"encoding/json"
	"testing"

	"github.com/prebid/prebid-server/v3/openrtb_ext"
)

func TestValidParams(t *testing.T) {
	validator, err := openrtb_ext.NewBidderParamsValidator("../../static/bidder-params")
	if err != nil {
		t.Fatalf("Failed to fetch the json-schemas. %v", err)
	}

	for _, validParam := range validParams {
		if err := validator.Validate(openrtb_ext.BidderCarbonatix, json.RawMessage(validParam)); err != nil {
			t.Errorf("Schema rejected carbonatix params: %s", validParam)
		}
	}
}

func TestInvalidParams(t *testing.T) {
	validator, err := openrtb_ext.NewBidderParamsValidator("../../static/bidder-params")
	if err != nil {
		t.Fatalf("Failed to fetch the json-schemas. %v", err)
	}

	for _, invalidParam := range invalidParams {
		if err := validator.Validate(openrtb_ext.BidderCarbonatix, json.RawMessage(invalidParam)); err == nil {
			t.Errorf("Schema allowed unexpected params: %s", invalidParam)
		}
	}
}

// validParams - carbonatix accepts empty object or optional endpoint
var validParams = []string{
	`{}`,
	`{"endpoint": "https://custom-endpoint.carbonatix.com/openrtb2"}`,
	`{"endpoint": "https://region-us.carbonatix.com/bid"}`,
}

// invalidParams - null, non-objects, empty strings, or unexpected params
var invalidParams = []string{
	`null`,
	`nil`,
	``,
	`[]`,
	`true`,
	`{"endpoint": ""}`,
	`{"endpoint": 123}`,
	`{"unexpected": "param"}`,
	`{"endpoint": "https://valid.com", "unexpected": "param"}`,
}
