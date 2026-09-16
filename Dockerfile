FROM node:24-alpine AS base
RUN npm install -g pnpm
WORKDIR /app

FROM base AS builder
COPY . .

RUN npm config set @buf:registry https://buf.build/gen/npm/v1/
RUN pnpm install --frozen-lockfile
RUN pnpm run build

# Each Cloud Run service is built from its own image: pick which @scp/*
# workspace gets deployed via --build-arg SERVICE=<name>, e.g.:
#   gcloud builds submit --config cloudbuild.yaml --substitutions=_SERVICE=batcher
#   docker build --build-arg SERVICE=batcher -t scp-batcher .
FROM builder AS deployer
ARG SERVICE=endpoint
RUN pnpm --filter=@scp/${SERVICE} --prod --legacy deploy prod

FROM node:24-alpine AS runner
WORKDIR /app

COPY --from=deployer /app/prod .

# Cloud Run injects the listening port via `PORT` (defaults to 8080) and
# passes all other configuration as regular env vars (--set-env-vars /
# --env-vars-file), which process.env picks up directly — nothing else to
# bake into the image. See .env.example for the full variable list.
ENV PORT=8080
EXPOSE ${PORT}
CMD ["node", "dist/index.mjs"]
