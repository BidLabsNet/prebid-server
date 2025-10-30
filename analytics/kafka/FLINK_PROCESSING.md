# Processing Raw OpenRTB in Kafka + Flink

This guide explains how to log complete OpenRTB bid requests/responses to Kafka and process them in Flink.

## ✅ What's Included

Your Kafka events now contain:

1. **Structured fields** (for simple queries):

   - `event_type`, `timestamp`, `request_id`, `account_id`
   - Basic fields like `device`, `site`, `impressions`, `bids`

2. **Raw OpenRTB objects** (for flexible processing):
   - `raw_bid_request`: Complete OpenRTB 2.x BidRequest as JSON
   - `raw_bid_response`: Complete OpenRTB 2.x BidResponse as JSON

## Event Structure

```json
{
  "event_type": "auction",
  "timestamp": 1698765432123,
  "request_id": "req-123",
  "auction_id": "auction-456",
  "account_id": "acc-789",

  // Structured fields (for quick access)
  "device": {
    "ua": "Mozilla/5.0...",
    "ip": "192.168.1.100"
  },
  "site": {
    "domain": "example.com",
    "page": "https://example.com/page"
  },
  "bids": [
    {"bid_id": "bid1", "price": 2.5}
  ],

  // Raw OpenRTB (for flexible field extraction in Flink)
  "raw_bid_request": {
    "id": "req-123",
    "imp": [...],
    "device": {...},
    "site": {...},
    "user": {...},
    "ext": {...}
    // ... complete OpenRTB 2.x structure
  },
  "raw_bid_response": {
    "id": "req-123",
    "seatbid": [...],
    "bidid": "...",
    "cur": "USD",
    "ext": {...}
    // ... complete OpenRTB 2.x structure
  }
}
```

## Flink Processing Examples

### Example 1: Extract Specific Fields in Flink

```java
// Flink DataStream API example
DataStream<String> kafkaStream = env
    .addSource(new FlinkKafkaConsumer<>("openrtb-events", ...));

DataStream<DeviceAnalytics> deviceAnalytics = kafkaStream
    .map(new MapFunction<String, DeviceAnalytics>() {
        @Override
        public DeviceAnalytics map(String value) {
            JsonNode event = objectMapper.readTree(value);

            // Extract from raw_bid_request
            JsonNode rawRequest = event.get("raw_bid_request");
            JsonNode device = rawRequest.get("device");
            JsonNode geo = device.get("geo");

            return new DeviceAnalytics(
                event.get("timestamp").asLong(),
                event.get("account_id").asText(),
                device.get("devicetype").asInt(),
                device.get("os").asText(),
                geo != null ? geo.get("country").asText() : null,
                // Extract any fields you need
                rawRequest.get("user").get("ext").get("custom_field").asText()
            );
        }
    });
```

### Example 2: Flink SQL for Ad-Hoc Queries

```sql
-- Register Kafka topic as Flink SQL table
CREATE TABLE openrtb_events (
    event_type STRING,
    timestamp BIGINT,
    account_id STRING,
    raw_bid_request STRING,  -- JSON string
    raw_bid_response STRING  -- JSON string
) WITH (
    'connector' = 'kafka',
    'topic' = 'openrtb-events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- Extract any field using JSON functions
SELECT
    account_id,
    -- Extract device type from raw JSON
    JSON_VALUE(raw_bid_request, '$.device.devicetype') as device_type,
    -- Extract country from geo
    JSON_VALUE(raw_bid_request, '$.device.geo.country') as country,
    -- Extract bid prices
    JSON_VALUE(raw_bid_response, '$.seatbid[0].bid[0].price') as price,
    COUNT(*) as request_count,
    AVG(CAST(JSON_VALUE(raw_bid_response, '$.seatbid[0].bid[0].price') AS DOUBLE)) as avg_price
FROM openrtb_events
WHERE event_type = 'auction'
GROUP BY
    TUMBLE(TO_TIMESTAMP(FROM_UNIXTIME(timestamp/1000)), INTERVAL '1' MINUTE),
    account_id,
    JSON_VALUE(raw_bid_request, '$.device.devicetype'),
    JSON_VALUE(raw_bid_request, '$.device.geo.country');
```

### Example 3: Multiple Outputs from One Stream

```java
// Process the same events for different purposes
SingleOutputStreamOperator<JsonNode> events = kafkaStream
    .map(json -> objectMapper.readTree(json));

// Output 1: Revenue analytics to ClickHouse
DataStream<RevenueRecord> revenueStream = events
    .map(event -> {
        JsonNode response = event.get("raw_bid_response");
        return extractRevenue(event.get("timestamp"),
                             event.get("account_id"),
                             response);
    })
    .addSink(clickHouseSink);

// Output 2: User behavior to another Kafka topic
events
    .filter(event -> event.has("raw_bid_request"))
    .map(event -> {
        JsonNode request = event.get("raw_bid_request");
        return extractUserBehavior(request);
    })
    .addSink(new FlinkKafkaProducer<>("user-behavior", ...));

// Output 3: Real-time alerts
events
    .filter(event -> {
        JsonNode response = event.get("raw_bid_response");
        double price = response.get("seatbid").get(0)
                              .get("bid").get(0).get("price").asDouble();
        return price > 100.0; // Alert on high-value bids
    })
    .addSink(alertingSink);
```

### Example 4: Field Selection Based on Privacy Rules

```java
// Different field extraction based on privacy requirements
public class PrivacyAwareMapper implements MapFunction<String, OutputRecord> {

    @Override
    public OutputRecord map(String value) {
        JsonNode event = objectMapper.readTree(value);
        JsonNode rawRequest = event.get("raw_bid_request");

        String accountId = event.get("account_id").asText();

        // Different fields for different accounts
        if (isGDPRAccount(accountId)) {
            // For GDPR accounts: exclude PII
            return new OutputRecord(
                accountId,
                // Don't extract IP or user IDs
                null,  // no IP
                null,  // no user ID
                rawRequest.get("device").get("devicetype").asInt(),
                rawRequest.get("site").get("domain").asText()
            );
        } else {
            // For non-GDPR accounts: include all fields
            return new OutputRecord(
                accountId,
                rawRequest.get("device").get("ip").asText(),
                rawRequest.get("user").get("id").asText(),
                rawRequest.get("device").get("devicetype").asInt(),
                rawRequest.get("site").get("domain").asText()
            );
        }
    }
}
```

## ClickHouse Schema

If you're storing in ClickHouse, you can keep the raw JSON:

```sql
CREATE TABLE openrtb_events
(
    event_type String,
    timestamp DateTime64(3),
    account_id String,

    -- Store raw JSON for flexible querying
    raw_bid_request String,
    raw_bid_response String,

    -- Or parse in ClickHouse with JSON functions
    device_type Int8 MATERIALIZED JSONExtractInt(raw_bid_request, 'device', 'devicetype'),
    country String MATERIALIZED JSONExtractString(raw_bid_request, 'device', 'geo', 'country'),
    bid_price Float64 MATERIALIZED JSONExtractFloat(raw_bid_response, 'seatbid', 1, 'bid', 1, 'price')
) ENGINE = MergeTree()
ORDER BY (timestamp, account_id);

-- Query with JSONExtract functions
SELECT
    account_id,
    JSONExtractString(raw_bid_request, 'device', 'os') as os,
    JSONExtractString(raw_bid_request, 'site', 'domain') as domain,
    COUNT(*) as events
FROM openrtb_events
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY account_id, os, domain;
```

## Configuration

### In `pbs.yaml`:

```yaml
analytics:
  kafka:
    enabled: true
    brokers: ["localhost:9092"]
    topic: "openrtb-events"

    # The structured fields are always included
    # Raw OpenRTB is always included
    # Process/filter in Flink as needed

    producer:
      batch_size: 100
      compression: "snappy" # Recommended for large JSON

    filters:
      # Still useful for volume control
      sample_rate: 1.0 # 100% of events
      event_types: ["auction"]
```

## Best Practices

### 1. Use Compression

Raw JSON is verbose. Use Snappy or LZ4 compression:

```yaml
producer:
  compression: "snappy"
```

### 2. Combine Structured + Raw

- Use **structured fields** for simple aggregations (faster)
- Use **raw fields** for complex analysis (more flexible)

```java
// Fast path: use structured field
String accountId = event.get("account_id").asText();

// Flexible path: use raw field for custom logic
JsonNode customExt = event.get("raw_bid_request")
                          .get("ext")
                          .get("your_custom_field");
```

### 3. Cache JSON Parsing

Parse once, use multiple times:

```java
.map(new RichMapFunction<String, OutputRecord>() {
    private ObjectMapper mapper;

    @Override
    public void open(Configuration config) {
        mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    }

    @Override
    public OutputRecord map(String value) {
        JsonNode parsed = mapper.readTree(value);  // Parse once
        // Extract multiple fields from parsed JSON
        return extractMultipleFields(parsed);
    }
});
```

### 4. Use Schema Registry (Optional)

For production, consider Avro with Schema Registry:

- Smaller message size
- Schema evolution
- Better performance

## Performance Comparison

| Approach               | Message Size | Flexibility  | Performance    |
| ---------------------- | ------------ | ------------ | -------------- |
| **Structured only**    | ~500 bytes   | Limited      | Fastest        |
| **Raw only**           | ~3-5 KB      | Maximum      | Slower parsing |
| **Both** (recommended) | ~3.5-5.5 KB  | Best of both | Balanced       |

With Snappy compression: ~70-80% size reduction
