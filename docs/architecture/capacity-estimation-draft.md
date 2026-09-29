# Capacity Estimation Draft

## Status
Draft / pre-implementation estimate. **No service code exists yet** — the numbers
below are derived from the architecture in `project-architecture.md` and
`integration-guide.md`, not from measurements. This document will be
**updated with real benchmarks after implementation**.

## Question
How many sensors can a single "average" server handle if each sensor sends
telemetry via `POST /v1/telemetry` **once per minute**?

## Assumptions
- **Hardware (average node):** 4 vCPU, 8–16 GB RAM, standard SSD.
- **Send interval:** 1 request per sensor per minute.
- **Hot path:** schema validation, canonical SHA-256 hash, Ed25519 signature
  verification, anti-replay (nonce) check against an in-memory store
  (no blockchain RPC on the hot path), then produce to Kafka. `202 Accepted`
  is returned only after Kafka ACK.
- **Downstream** (PubSub, IPFS, Blockchain) is decoupled behind Kafka and does
  **not** gate the ingest RPS.

## Per-request cost (why CPU is not the bottleneck)
| Operation | Rough throughput (single core) |
|---|---|
| Ed25519 verify | ~30k–70k ops/sec |
| SHA-256 + JSON canonicalization | tens–hundreds of thousands ops/sec |
| Kafka produce (batched) | tens of thousands msgs/sec |

The real limit is **whole-request handling RPS** (HTTP parse + validation +
network round-trip to Kafka waiting for ACK), not the crypto itself.
Realistic sustained ingest for this kind of service on 4 vCPU:
**~2,000–5,000 processed requests/sec**.

## Sensors from RPS
With a 1-minute send interval, the average request rate is:

```
RPS = N / 60
```

| Sustained RPS | Sensors (perfectly uniform) | With ×3 headroom for burstiness |
|---|---|---|
| 1,000 | 60,000 | ~20,000 |
| 2,000 | 120,000 | ~40,000 |
| 5,000 | 300,000 | ~100,000 |

**Practical answer:** on a single average node (4 vCPU), on the order of
**50,000–100,000 sensors** at a 1-minute interval, assuming send times are
reasonably spread across the minute.

## Practical limiting factors (not CPU)
1. **Send burstiness.** If sensors are not jittered across the minute (e.g. all
   fire at `:00`), peak RPS can be 10–60× the average. This is the main risk —
   sensors should apply send jitter.
2. **Kafka ACK on the hot path.** `202` is returned only after Kafka ACK, so
   Kafka latency and `acks=all` directly cap per-connection throughput.
   Requires connection pooling / async producers.
3. **Anti-replay (nonce dedup).** The in-memory nonce store grows with N
   (bounded with oldest-first eviction in `none` mode) and is per instance, so
   horizontal scaling needs a shared, TTL-based store (e.g. Redis) or sharding.
4. **Downstream (IPFS + Blockchain).** A much slower, asynchronous path behind
   Kafka. It does not limit sensor count, but batching in IPFS/anchoring is
   mandatory or queues will grow.

## Conclusion
Order of magnitude on an average node: **tens of thousands of sensors per node**
(realistic range ~50k–100k at a 1-minute interval), scaling roughly linearly
horizontally thanks to Kafka as the central bus. The real ceiling is driven by
**peak send burstiness and Kafka-ACK latency**, not the number of sensors itself.

## To update after implementation
- [ ] Replace estimated RPS with measured p50/p95/p99 under load.
- [ ] Benchmark Ed25519 verify + hash on the target hardware.
- [ ] Measure Kafka produce latency with the chosen `acks` setting.
- [ ] Measure nonce/dedup store throughput and memory footprint.
- [ ] Validate horizontal scaling assumption end-to-end.

## Related documents
- Project architecture: `docs/architecture/project-architecture.md`
- Integration guide: `docs/architecture/integration-guide.md`
