#!/usr/bin/env bash
# =============================================================================
# Ranger Dev: Post-start seed script for Elasticsearch & DynamoDB Local
# Run AFTER docker compose up -d is healthy:
#   bash docker/seed-services.sh
# =============================================================================
set -euo pipefail

ES_HOST="${ELASTICSEARCH_HOST:-http://localhost:9200}"
DYNAMO_ENDPOINT="${DYNAMODB_ENDPOINT:-http://localhost:8000}"
AWS_REGION="${AWS_REGION:-us-east-1}"

echo "🔍 Seeding Elasticsearch at $ES_HOST ..."

# -- Create 'products' index with mapping ------------------------------------
curl -sf -X PUT "$ES_HOST/products" \
  -H 'Content-Type: application/json' \
  -d '{
    "mappings": {
      "properties": {
        "sku":        { "type": "keyword" },
        "name":       { "type": "text", "fields": { "raw": { "type": "keyword" } } },
        "category":   { "type": "keyword" },
        "price":      { "type": "float" },
        "in_stock":   { "type": "boolean" },
        "tags":       { "type": "keyword" },
        "created_at": { "type": "date" }
      }
    }
  }' && echo " ✓ index created"

# Bulk-index documents
curl -sf -X POST "$ES_HOST/_bulk" \
  -H 'Content-Type: application/x-ndjson' \
  -d '
{"index":{"_index":"products","_id":"1"}}
{"sku":"WA-001","name":"Widget A","category":"widgets","price":19.99,"in_stock":true,"tags":["bestseller","small"],"created_at":"2025-01-15"}
{"index":{"_index":"products","_id":"2"}}
{"sku":"WB-002","name":"Widget B","category":"widgets","price":49.99,"in_stock":true,"tags":["premium"],"created_at":"2025-02-01"}
{"index":{"_index":"products","_id":"3"}}
{"sku":"GX-010","name":"Gadget X","category":"gadgets","price":12.50,"in_stock":true,"tags":["value"],"created_at":"2025-03-10"}
{"index":{"_index":"products","_id":"4"}}
{"sku":"GY-011","name":"Gadget Y","category":"gadgets","price":149.00,"in_stock":false,"tags":["premium","fragile"],"created_at":"2025-04-22"}
{"index":{"_index":"products","_id":"5"}}
{"sku":"GZ-012","name":"Gadget Z","category":"gadgets","price":34.95,"in_stock":true,"tags":[],"created_at":"2025-06-05"}
' && echo " ✓ products indexed"

echo ""
echo "📦 Seeding DynamoDB Local at $DYNAMO_ENDPOINT ..."

# -- Create 'ranger_events' table -------------------------------------------
aws dynamodb create-table \
  --endpoint-url "$DYNAMO_ENDPOINT" \
  --region "$AWS_REGION" \
  --table-name ranger_events \
  --attribute-definitions \
    AttributeName=event_id,AttributeType=S \
    AttributeName=event_type,AttributeType=S \
  --key-schema \
    AttributeName=event_id,KeyType=HASH \
    AttributeName=event_type,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  2>/dev/null && echo " ✓ table created" || echo " (table may already exist)"

# -- Insert seed items -------------------------------------------------------
for i in 1 2 3 4 5; do
  aws dynamodb put-item \
    --endpoint-url "$DYNAMO_ENDPOINT" \
    --region "$AWS_REGION" \
    --table-name ranger_events \
    --item "{
      \"event_id\":   {\"S\": \"evt-$i\"},
      \"event_type\": {\"S\": \"test_event\"},
      \"user_id\":    {\"S\": \"u-${i}00\"},
      \"payload\":    {\"S\": \"{\\\"action\\\":\\\"click\\\",\\\"page\\\":\\\"/demo/$i\\\"}\"},
      \"timestamp\":  {\"N\": \"$(date +%s)\"}
    }" 2>/dev/null
done
echo " ✓ 5 events inserted"

echo ""
echo "✅ All services seeded successfully."
