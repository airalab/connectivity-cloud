FROM node:24-alpine AS base
RUN npm install -g pnpm
WORKDIR /app

FROM base AS builder
COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm run build

FROM builder AS deployer
RUN pnpm --filter=@cps/entrypoint deploy /prod/app 

FROM node:24-alpine AS runner
WORKDIR /app

COPY --from=deployer /prod/app .

EXPOSE 3000
CMD ["node", "dist/index.js"]
