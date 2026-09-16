# Cloud Run Deployment Runbook

This runbook describes how to build, deploy, operate, and roll back the
connectivity-cloud services on Google Cloud Run. It complements the
correctness work tracked in issue #32 ("Harden Cloud Run deployment
correctness and document deployment") and should be re-validated whenever a
service's shutdown, idempotency, or partitioning behavior changes.

## 1. Services and Cloud Run resource types

| Service              | Cloud Run resource | Initial replicas | Notes |
|----------------------|---------------------|:-----------------:|-------|
| `endpoint`           | Service (HTTP)      | 2+ (autoscaled)   | Public ingress; stateless; reads `PORT` |
| `heartbeat-tracker`  | Worker Pool         | 1                 | Observability only, no DLQ |
| `batcher`            | Worker Pool         | 1                 | Must stay 1 while ordering is per-consumer-group (see §7) |
| `ipfs-publisher`     | Worker Pool         | 1                 | See §6 for the at-least-once/idempotency contract |
| `pubsub-broadcaster` | Worker Pool         | 1+ (autoscale later) | Stateless relay; safe to scale once peer connectivity is verified per-replica |
| `blockchain-anchor`  | Worker Pool         | **1 (singleton)** | Must remain a singleton until partition-level ordering is guaranteed (see §7) |
| `registry-sync`      | Worker Pool         | **1 (singleton)** | Single writer of the Redis projection; do not scale out |

Do not enable autoscaling for any worker pool until the validation scenarios
in §9 have passed against the target environment.

## 2. Prerequisites

- A GCP project with Cloud Run, Artifact Registry, Secret Manager, and (if
  using managed Redis) Memorystore enabled.
- Reachable Kafka brokers, Redis, an IPFS Kubo RPC endpoint (or Pinata
  credentials), and a Robonomics Substrate RPC endpoint from the Cloud Run
  environment (via Serverless VPC Access / Direct VPC egress if these run
  inside a VPC).
- A **Buf Schema Registry (BSR) token** with read access to
  `buf.build/airalab/connectivity-protocol`. This is required at *build
  time* (not runtime): `packages/core` generates protobuf bindings via `buf
  generate`, and `@buf/airalab_connectivity-protocol.bufbuild_es` is
  installed from the BSR npm registry. Store it as `BUF_TOKEN`.
- `gcloud`, `docker` (or another BuildKit-compatible builder), and `pnpm`
  installed locally, or an equivalent CI runner (see `.github/workflows/ci.yml`
  for the token wiring already used for tests).

## 3. Build and push images (Artifact Registry)

Each service has its own multi-stage `Dockerfile` under `services/<name>/`.
They must be built with the **monorepo root as the build context** since
pnpm workspace packages (`@scp/core`, `@scp/registry-sync`, `@scp/whitelist`)
are resolved from sibling directories.

```bash
export PROJECT_ID=my-gcp-project
export REGION=europe-west1
export REPO=connectivity          # Artifact Registry repository name
export TAG=$(git rev-parse --short HEAD)

gcloud artifacts repositories create "$REPO" \
  --repository-format=docker --location="$REGION" || true

gcloud auth configure-docker "${REGION}-docker.pkg.dev"

# One-time: BSR auth for the codegen step, mounted as a build secret so it
# never lands in an image layer.
cat > /tmp/npmrc <<EOF
@buf:registry=https://buf.build/gen/npm/v1/
//buf.build/gen/npm/v1/:_authToken=${BUF_TOKEN}
EOF

for service in endpoint heartbeat-tracker batcher ipfs-publisher \
               pubsub-broadcaster blockchain-anchor registry-sync; do
  docker build \
    -f "services/${service}/Dockerfile" \
    --secret id=npmrc,src=/tmp/npmrc \
    -t "${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${service}:${TAG}" \
    .
  docker push "${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${service}:${TAG}"
done

rm -f /tmp/npmrc
```

Tag images with the Git SHA (`$TAG` above), never `:latest`, so a specific
image digest can always be redeployed during rollback (§8).

## 4. Service accounts and IAM

Create one dedicated service account per service (least privilege) rather
than reusing the default compute service account:

```bash
for sa in endpoint heartbeat-tracker batcher ipfs-publisher \
          pubsub-broadcaster blockchain-anchor registry-sync; do
  gcloud iam service-accounts create "scp-${sa}" \
    --display-name "connectivity-cloud ${sa}"
done
```

Minimum roles per service account:

| Service account        | Roles |
|-------------------------|-------|
| all                     | `roles/secretmanager.secretAccessor` (scoped to the secrets it actually reads, via per-secret IAM bindings, not project-wide) |
| `scp-blockchain-anchor` | above, plus access to the `BLOCKCHAIN_ANCHOR_SURI` secret only |
| `scp-endpoint`          | above; if Cloud Run pushes logs/traces, also `roles/cloudtrace.agent`, `roles/logging.logWriter` (usually already granted to the runtime) |

Bind secrets narrowly, e.g.:

```bash
gcloud secrets add-iam-policy-binding blockchain-anchor-suri \
  --member="serviceAccount:scp-blockchain-anchor@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

## 5. Secret Manager integration

Never bake credentials into images or plain Cloud Run environment variables.
Store at least the following in Secret Manager and mount them as env vars
via `--set-secrets` on `gcloud run deploy` / `gcloud run worker-pools
create`:

| Secret                     | Used by              | Env var |
|----------------------------|-----------------------|---------|
| `blockchain-anchor-suri`   | `blockchain-anchor`   | `BLOCKCHAIN_ANCHOR_SURI` |
| `pinata-jwt`               | `ipfs-publisher` (if using the Pinata provider) | `PINATA_JWT` |
| `redis-url`                | `endpoint`, `heartbeat-tracker`, `registry-sync` | `REDIS_URL` |
| `kafka-brokers`            | all Kafka-consuming/producing services | `KAFKA_BROKERS` |
| `substrate-ws-url`         | `registry-sync`, `blockchain-anchor` | `SUBSTRATE_WS_URL` |
| `pubsub-libp2p-key-seed`   | `pubsub-broadcaster` | `PUBSUB_LIBP2P_PRIVATE_KEY_SEED_HEX` |

Example:

```bash
printf '%s' "$SURI" | gcloud secrets create blockchain-anchor-suri --data-file=-

gcloud run worker-pools deploy blockchain-anchor \
  --image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/blockchain-anchor:${TAG}" \
  --region="$REGION" \
  --service-account="scp-blockchain-anchor@${PROJECT_ID}.iam.gserviceaccount.com" \
  --set-secrets="BLOCKCHAIN_ANCHOR_SURI=blockchain-anchor-suri:latest,KAFKA_BROKERS=kafka-brokers:latest,SUBSTRATE_WS_URL=substrate-ws-url:latest" \
  --set-env-vars="BLOCKCHAIN_ANCHOR_NODE_ID=<node-id>" \
  --min-instances=1 --max-instances=1
```

## 6. Connectivity: Kafka, Redis, IPFS, blockchain

- **Kafka**: set `KAFKA_BROKERS` (comma-separated) for every service that
  consumes/produces telemetry topics. Confirm topics exist before traffic
  starts: `telemetry.authorized.v1`, `telemetry.rejected.v1`,
  `telemetry.batched.v1`, `ipfs.published.v1`,
  `telemetry.blockchain.result.v1`, `telemetry.retry.v1`,
  `telemetry.dlq.v1` (see `tools/init-kafka-topics.mjs`).
- **Redis**: `REDIS_URL` for `endpoint`/`registry-sync` (authorization
  projection + nonce store) and `heartbeat-tracker` (liveness state).
- **IPFS**: `IPFS_API_URL` for the Kubo RPC provider, or `PINATA_API_URL` +
  `PINATA_JWT` for the Pinata provider. `IPFS_PROVIDERS` controls which
  providers `ipfs-publisher` uses and in what order (first is primary).
- **Blockchain**: `SUBSTRATE_WS_URL` for `registry-sync` and
  `blockchain-anchor`; `BLOCKCHAIN_ANCHOR_SURI` (secret) and
  `BLOCKCHAIN_ANCHOR_NODE_ID` for signing/anchoring.

### `ipfs.published.v1` delivery contract

`ipfs-publisher` treats `ipfs.published.v1` as an **at-least-once,
idempotent-by-`event_id`** stream: the envelope's `event_id` is derived
deterministically from the durable `batch_id` (not a random UUID), so a
batch redelivered after a crash between the IPFS upload and the Kafka offset
commit re-emits an event with the *same* `event_id` rather than a distinct
one. Downstream consumers must not assume exactly-once delivery; they should
either dedupe by `event_id` or, like `blockchain-anchor`, derive
idempotency from authoritative external state (the on-chain payload for a
CID).

## 7. Ordering requirements and singleton services

Cloud Run Worker Pools can run multiple replicas of the same consumer group.
That is safe only when nothing downstream depends on the *relative order* of
messages across partitions/replicas. Two services in this system currently
do:

- **`registry-sync`** is the single writer of the Redis authorization
  projection driven from a strictly ordered finalized-block chain event
  stream. Running more than one replica risks interleaved/out-of-order
  writes to the same projection keys. Keep `min-instances=1,
  max-instances=1` until a partitioned/leader-election design is
  implemented.
- **`blockchain-anchor`** submits `cps.setPayload` extrinsics for a single,
  fixed `BLOCKCHAIN_ANCHOR_NODE_ID` per deployment. The on-chain payload
  check protects against re-anchoring an *identical* CID twice, but it does
  **not** protect against two replicas concurrently submitting two
  *different* CIDs for the same node out of order (e.g. an older batch's CID
  landing after a newer one). Until `ipfs.published.v1` messages for a given
  node are guaranteed to land on the same Kafka partition (e.g. by keying
  produced messages on `nodeId` and asserting `blockchain-anchor`'s
  consumer group has exactly one consumer per node-partition), keep
  `blockchain-anchor` a **singleton**: `min-instances=1, max-instances=1`
  for a given `BLOCKCHAIN_ANCHOR_NODE_ID`.

Both worker pools should be deployed with `--min-instances=1
--max-instances=1` explicitly, rather than relying on the default, and this
constraint should be re-verified (see §9) before ever raising `max-instances`
above 1 for either.

## 8. Deploying `endpoint` (HTTP Cloud Run Service)

```bash
gcloud run deploy endpoint \
  --image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/endpoint:${TAG}" \
  --region="$REGION" \
  --service-account="scp-endpoint@${PROJECT_ID}.iam.gserviceaccount.com" \
  --set-secrets="KAFKA_BROKERS=kafka-brokers:latest,REDIS_URL=redis-url:latest" \
  --set-env-vars="SENSOR_AUTH_STRATEGY=registry-sync" \
  --min-instances=1 --max-instances=10 \
  --allow-unauthenticated
```

`endpoint` reads Cloud Run's injected `PORT` environment variable
(`env.PORT ?? env.ENDPOINT_PORT`, defaulting to `3000` if neither is set), so
no port configuration is required for Cloud Run itself; `ENDPOINT_PORT`
remains available for local/non-Cloud-Run runs.

## 9. Deploying worker-pool services

Worker pools have no public HTTP endpoint from Cloud Run's perspective;
their `/health` and `/metrics` HTTP servers are for internal
scraping/liveness only (bind them to a private network path or a sidecar
scraper as appropriate for your observability stack).

```bash
gcloud run worker-pools deploy ipfs-publisher \
  --image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/ipfs-publisher:${TAG}" \
  --region="$REGION" \
  --service-account="scp-ipfs-publisher@${PROJECT_ID}.iam.gserviceaccount.com" \
  --set-secrets="KAFKA_BROKERS=kafka-brokers:latest,PINATA_JWT=pinata-jwt:latest" \
  --set-env-vars="IPFS_PROVIDERS=kubo,IPFS_API_URL=http://ipfs.internal:5001" \
  --min-instances=1 --max-instances=1
```

Repeat for `heartbeat-tracker`, `batcher`, `pubsub-broadcaster`,
`registry-sync`, and `blockchain-anchor`, substituting the appropriate
secrets/env vars from §5–§6 and the replica bounds from §1/§7.

## 10. Graceful shutdown

Cloud Run sends `SIGTERM` before terminating an instance (scale-in,
redeploy, or rollback), followed by `SIGKILL` after the configured
termination grace period. Every long-running service in this repository
wires `SIGTERM`/`SIGINT` (via the shared `installShutdownHandler` helper in
`@scp/core`) to its own `stop()` method, which:

1. Stops consuming new Kafka messages (closes the consumer / stops accepting
   new HTTP connections for `endpoint`).
2. Lets in-flight processing complete — e.g. `batcher` flushes any partially
   filled batch, and `blockchain-anchor` waits for an in-flight extrinsic to
   finalize before disconnecting the chain API.
3. Commits Kafka offsets only after the corresponding durable side effect
   (Kafka produce, IPFS publish, on-chain finalization, etc.) has completed.
4. Disconnects external clients (Kafka producer/consumer, Redis, the
   Substrate API) only after the above has completed.
5. Exits with code `0` on clean shutdown, or `1` if the shutdown sequence
   itself throws (surfaced in logs for alerting).

Set Cloud Run's termination grace period generously enough for the slowest
in-flight operation to finish (e.g. blockchain finalization can take several
seconds): `--timeout` for Services controls request timeout, and worker
pools should be given enough time via their own internal flush/commit
timeouts, which are all bounded by the retry/backoff configuration of each
service (see each service's `config.ts`).

## 11. Rollback procedure

1. Identify the last known-good image tag (Git SHA) from Artifact Registry
   or your deployment history: `gcloud run services describe endpoint
   --region="$REGION" --format='value(spec.template.spec.containers[0].image)'`.
2. Redeploy that exact image digest:
   ```bash
   gcloud run deploy endpoint \
     --image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/endpoint:${PREVIOUS_TAG}" \
     --region="$REGION"
   ```
   For worker pools, use `gcloud run worker-pools deploy <name> --image=...`
   the same way.
3. Because Kafka offsets are only committed after durable side effects
   (§10), rolling back to a previous image is safe with respect to
   in-flight messages: nothing is acknowledged/committed until it is
   durable, so a rolled-back instance simply resumes from the last
   committed offset.
4. For `registry-sync`/`blockchain-anchor` rollbacks, confirm the previous
   image version is compatible with the current on-chain pallet/storage
   layout before redeploying.

## 12. Smoke tests

After each deploy, run:

```bash
curl -sf "https://<endpoint-url>/health"
curl -sf "https://<endpoint-url>/metrics"
pnpm fake-sensor -- \
  --endpoint "https://<endpoint-url>/v1/telemetry" \
  --signer-seed-hex 0x0101010101010101010101010101010101010101010101010101010101010101 \
  --count 1
```

Then verify, within a few seconds:

- The synthetic event appears in `telemetry.authorized.v1`.
- `heartbeat-tracker`'s `/metrics` shows the synthetic sensor as online.
- `pubsub-broadcaster` forwards it to the GossipSub topic (check a
  subscribed peer, or its `/metrics` `publishSuccess` counter).
- The batch containing it reaches `telemetry.batched.v1`, is published to
  IPFS (`ipfs.published.v1`), and is anchored on-chain
  (`blockchain-anchor`'s `/metrics` `anchored` counter increments).

## 13. Validation scenarios (correctness regression suite)

Run these against a staging environment before enabling autoscaling or
promoting a change that touches shutdown/idempotency/ordering logic. Each
maps to an acceptance criterion in issue #32:

1. **Mid-flight kill and safe replay**: kill each consumer (`SIGKILL`, not
   `SIGTERM`, to bypass graceful shutdown) immediately after its side effect
   succeeds but before the Kafka offset commit; restart it and confirm no
   duplicate *logical* side effect occurs (batcher: no duplicate
   `telemetry.batched.v1`; ipfs-publisher: same `event_id` re-emitted;
   blockchain-anchor: on-chain state unchanged, `skippedDuplicate` metric
   increments).
2. **Out-of-order heartbeats**: send an older heartbeat after a newer one
   for the same sensor and confirm `heartbeat-tracker`'s Redis state does
   not regress (see `HEARTBEAT_CAS_SCRIPT` in
   `services/heartbeat-tracker/src/index.ts`).
3. **Partial batch preserved on stop**: send fewer than `BATCHER_BATCH_SIZE`
   authorized events, then send `SIGTERM` to `batcher`, and confirm the
   partial batch is flushed to `telemetry.batched.v1` rather than dropped.
4. **`ipfs-publisher` restart in the produce-before-commit window**: kill
   the process after IPFS publish succeeds but before the Kafka commit;
   restart and confirm the redelivered batch produces an
   `ipfs.published.v1` event with the same `event_id` as the original
   attempt.
5. **Sequential CIDs for one node**: publish two CIDs in quick succession
   for the same node and confirm `blockchain-anchor` anchors them in the
   order submitted (not reordered by concurrent replicas — validates the
   singleton constraint in §7).
6. **`SIGTERM` during an in-flight blockchain transaction**: send `SIGTERM`
   to `blockchain-anchor` while an extrinsic is being finalized; confirm the
   process waits for finalization (or a bounded failure) before
   disconnecting the chain API, and that the transaction is neither lost
   nor double-submitted on restart.
7. **Synthetic backlog + scale-out**: build up a Kafka backlog on a
   partition-safe consumer (e.g. `pubsub-broadcaster`), increase worker
   instances, trigger a consumer group rebalance, and confirm no missing
   side effects and no duplicate published messages beyond the documented
   at-least-once contract.

Document the date and result of each run (staging environment, image tag)
alongside the deployment record so regressions can be traced to the change
that introduced them.
