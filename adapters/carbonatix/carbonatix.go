package carbonatix

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/prebid/openrtb/v20/openrtb2"
	"github.com/prebid/prebid-server/v3/adapters"
	"github.com/prebid/prebid-server/v3/config"
	"github.com/prebid/prebid-server/v3/errortypes"
	"github.com/prebid/prebid-server/v3/openrtb_ext"
	"github.com/prebid/prebid-server/v3/util/jsonutil"
)

// adapter represents the Carbonatix bidder adapter
type adapter struct {
	endpoint string
}

// Builder builds a new instance of the Carbonatix adapter for the given bidder with the given config.
func Builder(bidderName openrtb_ext.BidderName, config config.Adapter, server config.Server) (adapters.Bidder, error) {
	bidder := &adapter{
		endpoint: config.Endpoint,
	}
	return bidder, nil
}

// MakeRequests creates the HTTP requests for the Carbonatix bidder
func (a *adapter) MakeRequests(request *openrtb2.BidRequest, reqInfo *adapters.ExtraRequestInfo) ([]*adapters.RequestData, []error) {
	var errs []error

	if len(request.Imp) == 0 {
		return nil, []error{&errortypes.BadInput{
			Message: "No Imps in Bid Request",
		}}
	}

	// Group impressions by endpoint
	// Most requests will use the default endpoint, but we support per-impression override
	endpointImps := make(map[string][]openrtb2.Imp)

	for _, imp := range request.Imp {
		impExt, err := parseImpExt(&imp)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Determine which endpoint to use for this impression
		endpoint := a.endpoint
		if impExt.Endpoint != "" {
			endpoint = impExt.Endpoint
		}

		endpointImps[endpoint] = append(endpointImps[endpoint], imp)
	}

	// If all impressions had errors, return
	if len(endpointImps) == 0 {
		return nil, errs
	}

	// Create a request for each endpoint
	var requests []*adapters.RequestData
	for endpoint, imps := range endpointImps {
		reqCopy := *request
		reqCopy.Imp = imps

		reqJSON, err := json.Marshal(&reqCopy)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		headers := http.Header{}
		headers.Add("Content-Type", "application/json;charset=utf-8")
		headers.Add("Accept", "application/json")

		requests = append(requests, &adapters.RequestData{
			Method:  "POST",
			Uri:     endpoint,
			Body:    reqJSON,
			Headers: headers,
			ImpIDs:  openrtb_ext.GetImpIDs(imps),
		})
	}

	return requests, errs
}

// parseImpExt parses the impression extension to extract Carbonatix-specific params
func parseImpExt(imp *openrtb2.Imp) (*openrtb_ext.ExtImpCarbonatix, error) {
	var bidderExt adapters.ExtImpBidder
	if err := jsonutil.Unmarshal(imp.Ext, &bidderExt); err != nil {
		return nil, &errortypes.BadInput{
			Message: fmt.Sprintf("Error parsing imp.ext for imp.id %s: %v", imp.ID, err),
		}
	}

	var carbonatixExt openrtb_ext.ExtImpCarbonatix
	if err := jsonutil.Unmarshal(bidderExt.Bidder, &carbonatixExt); err != nil {
		return nil, &errortypes.BadInput{
			Message: fmt.Sprintf("Error parsing Carbonatix params for imp.id %s: %v", imp.ID, err),
		}
	}

	return &carbonatixExt, nil
}

// MakeBids processes the bid response from Carbonatix
func (a *adapter) MakeBids(internalRequest *openrtb2.BidRequest, externalRequest *adapters.RequestData, response *adapters.ResponseData) (*adapters.BidderResponse, []error) {
	if response.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if response.StatusCode == http.StatusBadRequest {
		return nil, []error{&errortypes.BadInput{
			Message: fmt.Sprintf("Unexpected status code: %d. Run with request.debug = 1 for more info", response.StatusCode),
		}}
	}

	if response.StatusCode != http.StatusOK {
		return nil, []error{&errortypes.BadServerResponse{
			Message: fmt.Sprintf("Unexpected status code: %d. Run with request.debug = 1 for more info", response.StatusCode),
		}}
	}

	var bidResp openrtb2.BidResponse

	if err := jsonutil.Unmarshal(response.Body, &bidResp); err != nil {
		return nil, []error{err}
	}

	count := getBidCount(bidResp)
	bidResponse := adapters.NewBidderResponseWithBidsCapacity(count)

	var errs []error

	for _, sb := range bidResp.SeatBid {
		for i := range sb.Bid {
			bidType, err := getMediaTypeForImp(sb.Bid[i].ImpID, internalRequest.Imp)
			if err != nil {
				errs = append(errs, err)
			} else {
				b := &adapters.TypedBid{
					Bid:     &sb.Bid[i],
					BidType: bidType,
				}
				bidResponse.Bids = append(bidResponse.Bids, b)
			}
		}
	}
	return bidResponse, errs
}

func getBidCount(bidResponse openrtb2.BidResponse) int {
	c := 0
	for _, sb := range bidResponse.SeatBid {
		c = c + len(sb.Bid)
	}
	return c
}

func getMediaTypeForImp(impID string, imps []openrtb2.Imp) (openrtb_ext.BidType, error) {
	for _, imp := range imps {
		if imp.ID == impID {
			// Determine media type based on what's present in the impression
			if imp.Banner != nil {
				return openrtb_ext.BidTypeBanner, nil
			}
			if imp.Video != nil {
				return openrtb_ext.BidTypeVideo, nil
			}
			if imp.Audio != nil {
				return openrtb_ext.BidTypeAudio, nil
			}
		}
	}

	// This shouldn't happen. Handle it just in case by returning an error.
	return "", &errortypes.BadInput{
		Message: fmt.Sprintf("Failed to find impression \"%s\"", impID),
	}
}
