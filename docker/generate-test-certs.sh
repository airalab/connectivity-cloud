#!/usr/bin/env bash
# Generates throw-away TLS material for the secure local integration profile:
#   ca.pem / ca-key.pem          trusted CA
#   other-ca.pem                 unrelated CA (for "untrusted CA" and CA-rotation tests)
#   server.pem / server-key.pem  server certificate for localhost / 127.0.0.1
#   server-keystore.pem          key + certificate (PEM keystore for Kafka)
# Output goes to docker/certs (git-ignored). Never use these in production.
set -euo pipefail

OUT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
mkdir -p "$OUT"

SCRIPT='
set -eu
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -keyout ca-key.pem -out ca.pem -subj "/CN=scp-test-ca"
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -keyout other-ca-key.pem -out other-ca.pem -subj "/CN=scp-other-ca"
openssl req -newkey rsa:2048 -nodes \
  -keyout server-key.pem -out server.csr -subj "/CN=localhost"
printf "subjectAltName=DNS:localhost,DNS:redis-secure,DNS:kafka-secure,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n" > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -out server.pem -days 30 -extfile server.ext
cat server-key.pem server.pem > server-keystore.pem
rm -f server.csr server.ext ./*.srl
# Redis/Kafka containers run as non-root users and only read these files.
chmod 644 ./*.pem
'

if command -v openssl >/dev/null 2>&1; then
  (cd "$OUT" && sh -c "$SCRIPT")
else
  docker run --rm -v "$OUT:/work:Z" -w /work --entrypoint sh \
    docker.io/alpine/openssl -c "$SCRIPT"
fi
echo "Test certificates written to $OUT"
