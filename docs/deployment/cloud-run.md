# Deploying on Google Cloud Run

This runbook covers running the telemetry pipeline on **Cloud Run** with autoscaling, **Memorystore for Redis** (shared replay protection and heartbeat state) and **Google Cloud Managed Service for Apache Kafka**. It does not create the infrastructure; it lists what the services expect.

```text
sensor -> endpoint (Cloud Run, 1..5 instances)
            |-- Redis TLS + AUTH --> Memorystore  (nonce:v1:*, heartbeat-tracker:v1:*)
            |-- Kafka TLS + SASL  --> Managed Kafka (every producer and consumer)
            `-- WSS --------------> wss://polkadot.rpc.robonomics.network   (CPS auth only)
```

## Services and scaling

| Setting | Recommendation |
| --- | --- |
| Instances | `--min-instances=1 --max-instances=5` (scale to zero is not required) |
| Concurrency | Start at 80 per instance; the endpoint is I/O bound (Redis + Kafka ACK) |
| CPU / memory | Start at 1 vCPU / 512 MiB; raise if Ed25519 verification saturates CPU |
| Request timeout | 30 s (must exceed the Kafka ACK + Redis timeouts) |
| Startup probe | `GET /ready` (fails until Redis and Kafka are reachable and authenticated) |
| Liveness probe | `GET /health` (does not check dependencies) |
| CPU allocation | Always allocated for consumers (batcher, blockchain-anchor, heartbeat-tracker, pubsub-broadcaster) |

All services share the same image; choose the service with `--build-arg SERVICE=<name>` (see the `Dockerfile`).

## Networking

- Use **Direct VPC egress** on the VPC that has the Memorystore instance and the Managed Kafka cluster, with `--vpc-egress=private-ranges-only` so the public CPS RPC (`wss://polkadot.rpc.robonomics.network`) still goes out over the internet.
- Verify before go-live: DNS resolves the Kafka bootstrap and **advertised broker** names from the Cloud Run subnet, firewall rules allow TCP 9092 to the brokers and TCP 6378 to Redis, and routes exist for the Memorystore range.
- CPS RPC is independent of the private Redis/Kafka credentials. For `SENSOR_AUTH_STRATEGY=cps` set `SUBSTRATE_WS_URL=wss://polkadot.rpc.robonomics.network` and `CPS_ROOT_ADDRESS=<identity account of the sensors.social root>`.

## Identity and secrets

Use one runtime service account per deployment with least privilege:

- Kafka: `roles/managedkafka.client` on the project. Authentication uses the Cloud Run service-account identity via Application Default Credentials; **do not** ship a service-account JSON key (`GOOGLE_APPLICATION_CREDENTIALS` is rejected at startup). Restrict topics/consumer groups with Kafka ACLs where supported.
- Secret Manager: `roles/secretmanager.secretAccessor` only on the Redis AUTH secret and the Redis CA secret.
- Never put the Redis AUTH string or certificates in `.env`, the Dockerfile, Cloud Build substitutions or build arguments.

```bash
gcloud run deploy scp-endpoint \
  --image REGION-docker.pkg.dev/PROJECT/scp/endpoint \
  --service-account scp-runtime@PROJECT.iam.gserviceaccount.com \
  --min-instances 1 --max-instances 5 \
  --network VPC --subnet SUBNET --vpc-egress private-ranges-only \
  --set-env-vars "SENSOR_AUTH_STRATEGY=cps,SUBSTRATE_WS_URL=wss://polkadot.rpc.robonomics.network,CPS_ROOT_ADDRESS=<root>" \
  --set-env-vars "KAFKA_BROKERS=<managed-kafka-bootstrap>:9092,KAFKA_SECURITY_PROTOCOL=SASL_SSL,KAFKA_SASL_MECHANISM=OAUTHBEARER,KAFKA_AUTH_PROVIDER=google-adc" \
  --set-env-vars "REDIS_URL=rediss://<memorystore-ip>:6378,REDIS_TLS_CA_PATH=/var/run/secrets/redis/ca.pem,NONCE_TTL_SECONDS=900" \
  --set-secrets "REDIS_AUTH_PASSWORD=redis-auth:latest,/var/run/secrets/redis/ca.pem=redis-ca:latest"
```

Do not set `SCP_ALLOW_INSECURE_LOCAL` in Cloud Run.

## Memorystore for Redis

- Create the instance **with AUTH and in-transit encryption**: `--enable-auth --transit-encryption-mode=SERVER_AUTHENTICATION`. TLS cannot be enabled in place later.
- Clients connect over TLS (port `6378`) with the AUTH string and verify the server certificate against the **instance CA** (download it from the instance's server CA certs and store it in Secret Manager). Certificate verification is never disabled.
- Hostname verification: Memorystore addresses are IPs. If the certificate does not list the IP in its SAN, set `REDIS_TLS_SERVERNAME` to a name that does appear in the certificate instead of turning verification off. Confirm this against the current Memorystore connection guidance.
- The endpoint (`nonce:v1:*`) and the heartbeat-tracker (`heartbeat-tracker:v1:*`) share the instance with separate prefixes. Enable TLS + AUTH on the instance *and* roll out the heartbeat-tracker with the same `REDIS_*` settings at the same time, otherwise the running tracker loses its connection.

### Replay-protection semantics (read before tuning)

- Claim = `SET nonce:v1:<sha256> <token> NX EX NONCE_TTL_SECONDS`, made **before** publishing to Kafka.
- `NONCE_TTL_SECONDS` must be at least `2 x ENDPOINT_TIMESTAMP_SKEW_SECONDS + 60`; the service refuses to start otherwise.
- Redis errors return `503 nonce_store_unavailable` (nothing is published). Kafka failures return `503 kafka_unavailable` and **keep** the claim, because the broker may have accepted the event.
- A crash between the claim and the Kafka publish suppresses retries of that envelope until the TTL expires. This is a deliberate conservative trade-off; exactly-once delivery across Redis and Kafka would need an outbox and is out of scope.

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

## Local development

Local Docker Compose stays plaintext and is accepted only with the explicit opt-in:

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
