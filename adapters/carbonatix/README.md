# Carbonatix Bidder Adapter

## Overview

The Carbonatix adapter allows Prebid Server to request bids from the Carbonatix exchange. It supports all standard OpenRTB media types: banner, video, audio.

## Configuration

### Default Endpoint

The default endpoint is configured in `static/bidder-info/carbonatix.yaml`:

```yaml
endpoint: "https://your-carbonatix-endpoint.com/openrtb2"
```

### Dynamic Endpoint (Per-Impression Override)

The Carbonatix adapter supports **dynamic endpoint selection** on a per-impression basis. This allows you to:

- Route different impressions to different regional endpoints
- A/B test different bidding endpoints
- Support multiple environments (staging, production) in a single request

## Usage Examples

### Example 1: Using Default Endpoint

```json
{
  "id": "auction-123",
  "imp": [
    {
      "id": "imp-1",
      "banner": {
        "w": 300,
        "h": 250
      },
      "ext": {
        "carbonatix": {}
      }
    }
  ],
  "site": {
    "page": "https://example.com"
  }
}
```

This will use the default endpoint from `carbonatix.yaml`.

### Example 2: Using Custom Endpoint

```json
{
  "id": "auction-456",
  "imp": [
    {
      "id": "imp-1",
      "banner": {
        "w": 728,
        "h": 90
      },
      "ext": {
        "carbonatix": {
          "endpoint": "https://us-east.carbonatix.com/openrtb2"
        }
      }
    }
  ],
  "site": {
    "page": "https://example.com"
  }
}
```

This impression will be sent to the custom endpoint `https://us-east.carbonatix.com/openrtb2`.

### Example 3: Mixed Endpoints (Multi-Region)

```json
{
  "id": "auction-789",
  "imp": [
    {
      "id": "imp-us",
      "banner": {
        "w": 300,
        "h": 250
      },
      "ext": {
        "carbonatix": {
          "endpoint": "https://us-east.carbonatix.com/openrtb2"
        }
      }
    },
    {
      "id": "imp-eu",
      "banner": {
        "w": 300,
        "h": 250
      },
      "ext": {
        "carbonatix": {
          "endpoint": "https://eu-west.carbonatix.com/openrtb2"
        }
      }
    },
    {
      "id": "imp-default",
      "video": {
        "mimes": ["video/mp4"],
        "w": 640,
        "h": 480
      },
      "ext": {
        "carbonatix": {}
      }
    }
  ],
  "site": {
    "page": "https://example.com"
  }
}
```

This creates **3 separate bid requests**:

1. `imp-us` → `https://us-east.carbonatix.com/openrtb2`
2. `imp-eu` → `https://eu-west.carbonatix.com/openrtb2`
3. `imp-default` → default endpoint from config

## Parameters

| Parameter  | Type   | Required | Description                                                                                                |
| ---------- | ------ | -------- | ---------------------------------------------------------------------------------------------------------- |
| `endpoint` | string | No       | Custom endpoint URL for this impression. If not provided, uses the default endpoint from `carbonatix.yaml` |

## Supported Media Types

- ✅ Banner
- ✅ Video
- ✅ Audio

## Implementation Details

### Request Grouping

The adapter intelligently groups impressions by endpoint to minimize the number of HTTP requests:

- All impressions with the same endpoint are sent in a single bid request
- If impressions have different endpoints, separate requests are made for each endpoint
- This optimizes performance while supporting multi-region deployments

### Error Handling

- **204 No Content**: Returns empty response (no bids)
- **400 Bad Request**: Returns BadInput error
- **500+ Server Error**: Returns BadServerResponse error
- **Invalid Impression Extension**: Skips the impression and continues with others

## File Structure

```
adapters/carbonatix/
├── carbonatix.go              # Main adapter implementation
├── carbonatix_test.go         # Adapter test suite
├── params_test.go             # Parameter validation tests
├── README.md                  # This file
└── carbonatixtest/
    ├── exemplary/
    │   ├── simple-banner.json     # Basic banner test
    │   ├── simple-video.json      # Video test
    │   └── custom-endpoint.json   # Custom endpoint test
    └── supplemental/
        ├── no-content-response.json      # 204 response test
        └── bad-server-response.json      # 500 error test
```

## Testing

Run the test suite:

```bash
go test ./adapters/carbonatix/... -v
```

Expected output:

```
=== RUN   TestJsonSamples
--- PASS: TestJsonSamples
=== RUN   TestValidParams
--- PASS: TestValidParams
=== RUN   TestInvalidParams
--- PASS: TestInvalidParams
PASS
```

## Integration with Kafka Analytics

All auction events involving the Carbonatix bidder are automatically logged to your Kafka analytics pipeline (if enabled). The events include:

- Full OpenRTB bid request (with endpoint selection)
- Full OpenRTB bid response
- Account and request metadata

Example Kafka event:

```json
{
  "event_type": "auction",
  "timestamp": 1698765432000,
  "account_id": "publisher-123",
  "request_id": "auction-456",
  "raw_bid_request": {
    "imp": [{
      "ext": {
        "carbonatix": {
          "endpoint": "https://us-east.carbonatix.com/openrtb2"
        }
      }
    }]
  },
  "raw_bid_response": { ... }
}
```

## Comparison with AppNexus Adapter

### Similarities

- Clean, idiomatic Go code structure
- Proper error handling and validation
- Support for multiple impressions
- Type-safe implementation

### Key Difference

- **Carbonatix**: Supports dynamic per-impression endpoint override
- **AppNexus**: Uses single endpoint with URL modifications based on member ID

## Production Checklist

- [ ] Update `static/bidder-info/carbonatix.yaml` with production endpoint
- [ ] Test with sample bid requests
- [ ] Verify Kafka analytics events are being logged
- [ ] Configure regional endpoints if using multi-region deployment
- [ ] Monitor bid response rates and latency

## Support

For issues or questions about the Carbonatix adapter:

- Email: prebid-maintainer@carbonatix.com
- GVL Vendor ID: 1376
