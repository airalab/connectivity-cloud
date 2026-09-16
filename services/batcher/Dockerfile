# syntax=docker/dockerfile:1.7
#
# Multi-stage Cloud Run image for @scp/batcher.
#
# Build from the monorepo root (not this directory), e.g.:
#
#   docker build -f services/batcher/Dockerfile \
#     --secret id=npmrc,src=$HOME/.npmrc \
#     -t "${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/batcher:${TAG}" .
#
# The `npmrc` secret must authenticate the private Buf Schema Registry (BSR)
# npm scope used by `@buf/airalab_connectivity-protocol.bufbuild_es` and by
# `buf generate` (see docs/deployment/cloud-run.md for the exact contents).
FROM node:22-alpine AS base
RUN corepack enable

FROM base AS deps
WORKDIR /repo
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages ./packages
COPY services ./services
COPY tools ./tools
COPY proto ./proto
COPY buf.yaml buf.gen.yaml ./
RUN --mount=type=secret,id=npmrc,target=/root/.npmrc \
    pnpm install --frozen-lockfile

FROM deps AS build
# pnpm's recursive filter runs in topological order, so @scp/core (and any
# other workspace dependency of this service) is built before the service
# itself without needing a separate step.
RUN --mount=type=secret,id=npmrc,target=/root/.npmrc \
    pnpm --filter "...@scp/batcher" build

FROM node:22-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /repo /app
USER node
# Cloud Run injects the listening port via $PORT; the endpoint HTTP service
# reads it (see docs/deployment/cloud-run.md). Health/metrics ports for
# worker-pool services are not publicly exposed by Cloud Run.
CMD ["node", "services/batcher/dist/index.mjs"]
