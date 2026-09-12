#!/bin/bash
set -e

# List of topics to create
TOPICS=(
  "telemetry.authorized.v1"
  "telemetry.rejected.v1"
  "telemetry.batched.v1"
  "ipfs.published.v1"
  "telemetry.dlq.v1"
)

PARTITIONS=${KAFKA_NUM_PARTITIONS:-3}
REPLICATION_FACTOR=${KAFKA_REPLICATION_FACTOR:-1}

echo "Creating Kafka topics (partitions=$PARTITIONS, replication-factor=$REPLICATION_FACTOR)..."

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  /opt/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --if-not-exists \
    --config retention.ms=604800000 || true
done

echo "Topics initialized successfully"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
