# Deploying on Google Cloud Run

This runbook covers running the telemetry pipeline on **Cloud Run** with autoscaling, **Memorystore for Redis** (shared replay protection and heartbeat state) and **Google Cloud Managed Service for Apache Kafka**. It does not create the infrastructure; it lists what the services expect.

```text
sensor -> endpoint (Cloud Run, 1..5 instances)
            |-- Redis TLS + AUTH --> Memorystore (nonces, heartbeat state, anchor observation)
            |-- Kafka TLS + SASL  --> Managed Kafka
            |-- OIDC/HTTPS -------> private Cloud Run services (status collection)
            `-- WSS --------------> Robonomics RPC (CPS auth / blockchain-anchor)
```

## Services and scaling

| Setting | Recommendation |
| --- | --- |
| Instances | `--min-instances=1 --max-instances=5` (scale to zero is not required) |
| Concurrency | Start at 80 per instance; the endpoint is I/O bound (Redis + Kafka ACK) |
| CPU / memory | Start at 1 vCPU / 512 MiB; raise if Ed25519 verification saturates CPU |
| Request timeout | 30 s (must exceed the Kafka ACK + Redis timeouts) |
| Container port | `8080`. Every service listens on Cloud Run's `PORT` (the image sets `PORT=8080`), which takes precedence over the service-specific `*_PORT` / `*_HEALTH_PORT` variables |
| Startup probe | `GET /ready` on port 8080 (endpoint: fails until Redis and Kafka are reachable and authenticated; consumers: only answers once the Kafka consumer is running; pubsub-broadcaster: also requires `PUBSUB_MIN_CONNECTED_PEERS`) |
| Liveness probe | `GET /health` on port 8080 (does not check dependencies) |
| CPU allocation | Always allocated for consumers (batcher, blockchain-anchor, heartbeat-tracker, pubsub-broadcaster) |

All services share the same image; choose the service with `--build-arg SERVICE=<name>` (see the `Dockerfile`). The image sets `NODE_ENV=production`, so `SCP_ALLOW_INSECURE_LOCAL=true` makes every service fail at startup.

Cloud Run does not configure HTTP probes by default (it only checks that the container port accepts a TCP connection), so the probes must be set explicitly, as shown in the deploy command below.

## Networking

- Use **Direct VPC egress** on the VPC that has Memorystore and Managed Kafka. Verify DNS, firewall rules, routes, and access to every advertised Kafka broker (TCP 9092) and Redis (TCP 6378) from each service subnet.
- Do not assume `--vpc-egress=private-ranges-only` makes private Cloud Run service URLs reachable. With internal ingress, configure the endpoint's VPC egress/network path for Cloud Run-to-Cloud Run traffic (for example, Direct VPC egress with all traffic routed through the VPC and the required Google API/private routing). Verify that the configured URL resolves and that an authenticated request succeeds from the deployed endpoint revision. Test after any ingress, VPC, DNS, or egress change.
- All-traffic VPC egress also changes outbound RPC routing. Provide Cloud NAT for public Robonomics RPC access where required, and Private Google Access for Google APIs as appropriate to the subnet. Validate Redis, Managed Kafka, Cloud Run IAM token acquisition, and Robonomics RPC independently; do not make a public service URL or unauthenticated fallback just to work around routing.
- Only `endpoint` is public. Set the other four services to internal ingress and require IAM authentication. Grant the endpoint runtime identity `roles/run.invoker` on each private service. Configure `PUBSUB_BROADCASTER_URL`, `HEARTBEAT_TRACKER_URL`, `BATCHER_URL`, and `BLOCKCHAIN_ANCHOR_URL` on the endpoint with each service's HTTPS URL; these URLs are server-side configuration and are never exposed to the browser. The endpoint sends Google-signed OIDC ID tokens with the target URL as audience using ADC (no service-account key). Tokens are only sent to origins allowed by `STATUS_ALLOWED_UPSTREAM_HOSTS` (exact hostnames or `*.suffix`), or to Cloud Run `*.run.app` hostnames when it is unset; set it to pin the exact service hostnames.
- CPS RPC is independent of private Redis/Kafka credentials. For `SENSOR_AUTH_STRATEGY=cps` set `SUBSTRATE_WS_URL=wss://polkadot.rpc.robonomics.network` and `CPS_ROOT_ADDRESS=<identity account of the sensors.social root>`.

## Identity and secrets

Use one runtime service account per deployment with least privilege:

- Kafka: `roles/managedkafka.client` on the project. Authentication uses the Cloud Run service-account identity via Application Default Credentials; **do not** ship a service-account JSON key (`GOOGLE_APPLICATION_CREDENTIALS` is rejected at startup). Restrict topics/consumer groups with Kafka ACLs where supported.
- The blockchain-anchor status Admin client needs read-only metadata, end-offset, and consumer-group offset permissions for `telemetry.batched.v1` and `BLOCKCHAIN_ANCHOR_GROUP_ID`. It does not consume records or commit offsets.
- Secret Manager: `roles/secretmanager.secretAccessor` only on the Redis AUTH secret and the Redis CA secret.
- Never put the Redis AUTH string or certificates in `.env`, the Dockerfile, Cloud Build substitutions or build arguments.

```bash
gcloud beta run deploy scp-endpoint \
  --image REGION-docker.pkg.dev/PROJECT/scp/endpoint \
  --service-account scp-runtime@PROJECT.iam.gserviceaccount.com \
  --min-instances 1 --max-instances 5 --port 8080 \
  --startup-probe=httpGet.path=/ready,httpGet.port=8080,periodSeconds=5,timeoutSeconds=3,failureThreshold=24 \
  --liveness-probe=httpGet.path=/health,httpGet.port=8080,periodSeconds=30,timeoutSeconds=3,failureThreshold=3 \
  --ingress all \
  --network VPC --subnet SUBNET --vpc-egress all \
  --set-env-vars "SENSOR_AUTH_STRATEGY=cps,SUBSTRATE_WS_URL=wss://polkadot.rpc.robonomics.network,CPS_ROOT_ADDRESS=<root>" \
  --set-env-vars "PUBSUB_BROADCASTER_URL=https://<pubsub-service-url>,HEARTBEAT_TRACKER_URL=https://<heartbeat-service-url>,BATCHER_URL=https://<batcher-service-url>,BLOCKCHAIN_ANCHOR_URL=https://<anchor-service-url>" \
  --set-env-vars "STATUS_CACHE_TTL_MS=30000,STATUS_PROBE_TIMEOUT_MS=5000,STATUS_FAILURE_THRESHOLD=3" \
  --set-env-vars "KAFKA_BROKERS=<managed-kafka-bootstrap>:9092,KAFKA_SECURITY_PROTOCOL=SASL_SSL,KAFKA_SASL_MECHANISM=OAUTHBEARER,KAFKA_AUTH_PROVIDER=google-adc" \
  --set-env-vars "REDIS_URL=rediss://<memorystore-ip>:6378,REDIS_TLS_CA_PATH=/var/run/secrets/redis/ca.pem,NONCE_TTL_SECONDS=900" \
  --set-secrets "REDIS_AUTH_PASSWORD=redis-auth:latest,/var/run/secrets/redis/ca.pem=redis-ca:latest"
```

The command is an example, not a production-ready network recipe: validate VPC routing, Cloud NAT/Private Google Access, and Cloud Run-to-Cloud Run reachability in the target project. The endpoint is the only service deployed with `--ingress all`; deploy `pubsub-broadcaster`, `heartbeat-tracker`, `batcher`, and `blockchain-anchor` with `--ingress internal --no-allow-unauthenticated`, the same health probes, and their service-specific least-privilege accounts. Grant the endpoint account `roles/run.invoker` on those four services. Set each private service URL on the endpoint as described above. Cloud Run has no separate readiness probe: `/ready` is a startup check, not a global dashboard. Do not set `SCP_ALLOW_INSECURE_LOCAL` in Cloud Run: it is rejected when `NODE_ENV=production` (set by the image).

## Memorystore for Redis

- Create the instance **with AUTH and in-transit encryption**: `--enable-auth --transit-encryption-mode=SERVER_AUTHENTICATION`. TLS cannot be enabled in place later.
- Clients connect over TLS (port `6378`) with the AUTH string and verify the server certificate against the **instance CA** (download it from the instance's server CA certs and store it in Secret Manager). Certificate verification is never disabled.
- Hostname verification: Memorystore addresses are IPs. If the certificate does not list the IP in its SAN, set `REDIS_TLS_SERVERNAME` to a name that does appear in the certificate instead of turning verification off. Confirm this against the current Memorystore connection guidance.
- The endpoint (`nonce:v1:*`), heartbeat-tracker (`heartbeat-tracker:v1:*`), and blockchain-anchor status observation (`blockchain-anchor:v1:last-finalized`) share the instance with separate keys. Enable TLS + AUTH on the instance and roll out every service that uses it with the same `REDIS_*` settings. The anchor's observation write is best-effort and does not participate in transaction or Kafka commit semantics.

### Replay-protection and anchor observations

- Claim = `SET nonce:v1:<sha256> <token> NX EX NONCE_TTL_SECONDS`, made **before** publishing to Kafka.
- `NONCE_TTL_SECONDS` must be at least `2 x ENDPOINT_TIMESTAMP_SKEW_SECONDS + 60`; the service refuses to start otherwise.
- Redis errors return `503 nonce_store_unavailable` (nothing is published). Kafka failures return `503 kafka_unavailable` and **keep** the claim, because the broker may have accepted the event.
- A crash between the claim and the Kafka publish suppresses retries of that envelope until the TTL expires. This is a deliberate conservative trade-off; exactly-once delivery across Redis and Kafka would need an outbox and is out of scope.
- Blockchain-anchor's last finalized batch uses a separate Redis hash and best-effort monotonic writes. The write runs outside Kafka commit/transaction success handling; Redis failures are visible as an unavailable observation, not an anchor failure.

## Managed Service for Apache Kafka

- Brokers require TLS; `KAFKA_SECURITY_PROTOCOL=SASL_SSL`. Use `KAFKA_SASL_MECHANISM=OAUTHBEARER` with `KAFKA_AUTH_PROVIDER=google-adc` (the token is Google's `GOOG_OAUTH2_TOKEN` envelope built from the ADC access token, requested again on every broker connection, so reconnects and token expiry recover automatically).
- Fallback: `KAFKA_SASL_MECHANISM=PLAIN` with `KAFKA_AUTH_PROVIDER=google-adc` sends the principal e-mail and a short-lived access token over TLS. Long-lived service-account keys are not supported in production.
- If the cluster's broker certificates do not chain to the system trust store, mount the CA and set `KAFKA_TLS_CA_PATH`. Use `KAFKA_TLS_SERVERNAME` only to correct SNI/hostname verification.
- Create the topics from `docker/kafka-init-topics.sh` before the first deployment.
- Known limit: the Platformatic client authenticates when a connection is opened and does not re-authenticate an existing connection. When the broker closes a connection at token expiry the client reconnects with a fresh token; check this behaviour in the managed-cluster smoke test below.

## Rotation

- **Redis AUTH**: add the new password to Secret Manager, set the Memorystore AUTH string to it (Memorystore briefly accepts both while updating, verify for your version), deploy a new revision so every instance reconnects with the new secret, then disable the old version. `/ready` fails while a revision cannot authenticate, which blocks traffic to it.
- **Redis CA**: put the old and new CA certificates in the same PEM (`REDIS_TLS_CA_PATH` accepts a bundle), deploy, rotate the instance certificate, then remove the old CA and deploy again.
- **Kafka**: credentials come from the service account and refresh automatically; to change identity, grant the new account `roles/managedkafka.client`, deploy with the new `--service-account`, then remove the old binding.

## Observability

- `/ready` returns only the names of failing dependencies; details are logged without credentials. Connection strings are logged with credentials redacted and passwords are never logged.
- `/metrics` includes `duplicateNonces`, `nonceStoreErrors` and `kafkaErrors`. Alert on a sustained `nonceStoreErrors` increase (Redis outage or credential problem).
- The status collector caches snapshots in memory per endpoint instance; it is not a globally consistent history or a global aggregation of process-local counters.

## Local development

Local Docker Compose stays plaintext and is accepted only with the explicit opt-in, and never with `NODE_ENV=production`:

```bash
SCP_ALLOW_INSECURE_LOCAL=true
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```

The `secure` Compose profile (`docker compose --profile secure up -d redis-secure kafka-secure`) provides TLS + AUTH Redis and TLS + SASL/PLAIN Kafka with throw-away certificates from `docker/generate-test-certs.sh` for integration tests.

## Smoke tests (run against real infrastructure)

These cannot run in CI without a Google Cloud project; run them before the first production rollout and after changing credentials:

1. Deploy one instance of each service, confirm `/ready` returns 200 and that logs show no credentials.
2. Managed Kafka: from the endpoint, `GET /ready` must pass (authenticated metadata request) and a produced event must be consumed by the batcher and heartbeat-tracker. Keep an instance running past the access-token lifetime (about one hour) and confirm a new connection is established after a forced broker disconnect.
3. `sensor -> CPS WSS -> Redis claim -> authenticated Kafka publish -> 202` using `pnpm fake-sensor`.
4. Send the identical envelope again (also via a different instance): expect `409 duplicate_nonce` and exactly one event on `telemetry.authorized.v1`.
5. Break Redis AUTH on a test revision: expect `/ready` 503 and startup failure, not an insecure fallback.
6. Status dashboard (staging, with all four private services on internal ingress and the endpoint on all-traffic Direct VPC egress):
   - From outside the VPC, `curl` a private service URL directly: expect `403`/`404` (ingress blocks it), not a response body.
   - `curl -H "Authorization: Bearer $(gcloud auth print-identity-token --audiences=<service-url>)" <service-url>/ready` from a VM in the VPC: expect `200`; without the header: `401`/`403`.
   - Without `roles/run.invoker` for the endpoint identity, `GET /api/status` must show the service as `unknown` (not `operational`); after granting it, `operational`.
   - Set `HEARTBEAT_TRACKER_URL` to a hostname outside `STATUS_ALLOWED_UPSTREAM_HOSTS`: the service must show `unknown` and the target must receive no request.
   - `GET /api/status/sensors?limit=50` returns `available: true` with at most 50 sensors; stop the heartbeat-tracker revision and confirm the next refresh reports `stale: true` (then `available: false` after `STATUS_STALE_MAX_AGE_MS`) and that the dashboard shows the stale banner.
   - Anchor two batches that finalize in the same block and confirm `/api/status/network` shows the later batch ID.
