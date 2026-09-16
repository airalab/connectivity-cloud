FROM node:24-alpine AS base
RUN npm install -g pnpm
WORKDIR /app

FROM base AS builder
COPY . .

RUN npm config set @buf:registry https://buf.build/gen/npm/v1/
RUN pnpm install --frozen-lockfile
RUN pnpm run build

FROM builder AS deployer
RUN pnpm --filter=@scp/endpoint --prod --legacy deploy prod

FROM node:24-alpine AS runner
WORKDIR /app

COPY --from=deployer /app/prod .

EXPOSE 3000
CMD ["node", "dist/index.mjs"]
