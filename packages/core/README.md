# `@scp/core`

Core primitives for the sensors telemetry pipeline: utilities, runtime processing logic, and shared protobuf schemas.

### Processing Rule with Retry/DLQ

```ts
import {
  runConsumerProcessingRule,
  createRetryDlqPublisher,
  InMemoryRetryCounterStore,
} from '@scp/core';

const retryDlqPublisher = createRetryDlqPublisher({
  producer: kafkaProducer,
  source: 'my-service',
  getKey: (event) => event.eventId,
  serializeEvent: (event) => toBinary(EnvelopeSchema, event),
});

await runConsumerProcessingRule(event, {
  retryPolicy: {
    maxAttempts: 3,
    getEventId: (event) => event.eventId,
    store: new InMemoryRetryCounterStore(),
  },
  performExternalAction: async (event) => {
    // Your processing logic
  },
  waitForConfirmation: async () => {
    // Wait for external system confirmation
  },
  emitResultEvent: async (event) => ({
    /* result event */
  }),
  publishResultEvent: async (result) => {
    // Publish result to Kafka
  },
  commitOffset: async () => {
    // Commit Kafka offset
  },
  retryDlqPublisher,
});
```

## Protobuf Schemas

```ts
import {
  EnvelopeSchema,
  TelemetryAuthorizedPayloadSchema,
  TelemetryRejectedPayloadSchema,
  TelemetryBatchedPayloadSchema,
  TelemetryPubsubResultPayloadSchema,
  TelemetryBlockchainResultPayloadSchema,
} from '@scp/core';
```

## Architecture

- **producer.ts**: Generic event producer with retry logic
- **consumer.ts**: Consumer processing runtime with deduplication, retry/DLQ, and idempotency
- **utils.ts**: Sensor ID formatting and signature utilities
- **topics.ts**: Kafka topic constants
- **rejection-codes.ts**: Telemetry rejection reason codes
- **sensor-auth.ts**: Sensor authorization interface
