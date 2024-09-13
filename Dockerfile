# use the official Bun image
# see all versions at https://hub.docker.com/r/oven/bun/tags
FROM oven/bun:1 AS base
WORKDIR /app
COPY . .
RUN apt update && apt install -y build-essential
RUN bun install --frozen-lockfile --production
CMD ["bun", "run", "start"]
