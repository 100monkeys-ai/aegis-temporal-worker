# SPDX-License-Identifier: AGPL-3.0-only
# Copyright 2026 100monkeys.ai

# Dockerfile for AEGIS Temporal Worker
# Using Debian-based (slim) image for glibc compatibility with Temporal native addons.
# Temporal's SDK uses @temporalio/worker which ships native binaries that require glibc;
# Alpine (musl) is incompatible and will fail at runtime.

# Node 24, the long-term-support line, in both stages. The version is
# declared once, in .nvmrc; src/node-version.test.ts fails if this file
# disagrees with it, and CI's Image job fails if the built image runs
# another Node. The digest is the multi-platform index of node:24-slim as
# read from Docker Hub on 2026-09-28: Node 24.21.0.
FROM node:24-slim@sha256:0e0ff40c39bc087845bfb27465a0df4ea419520094bc35842ff83dd8cbe6f9b6 AS builder

WORKDIR /app

# Copy package files and install ALL dependencies (including devDeps for build)
COPY package*.json ./
COPY tsconfig.json ./
RUN npm ci

# Copy source and proto
COPY src ./src
COPY aegis-proto ./aegis-proto

# Build TypeScript → dist/
RUN npm run build

# ─── Production image ────────────────────────────────────────────────────────

FROM node:24-slim@sha256:0e0ff40c39bc087845bfb27465a0df4ea419520094bc35842ff83dd8cbe6f9b6

WORKDIR /app

# Install production dependencies only
COPY package*.json ./
RUN npm ci --omit=dev

# Copy compiled output from builder
COPY --from=builder /app/dist ./dist

# Copy aegis-proto submodule (needed at runtime by gRPC client)
COPY aegis-proto ./aegis-proto

# Create data directory
RUN mkdir -p /app/data

# Proto path is fixed in the container — no need for PROTO_PATH env override
ENV PROTO_PATH=/app/aegis-proto/proto/aegis_runtime.proto
ENV NODE_ENV=production

# HTTP registration API
EXPOSE 3000

# Healthcheck: call the /health endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health', (r) => { process.exit(r.statusCode === 200 ? 0 : 1) }).on('error', () => process.exit(1))"

CMD ["node", "dist/index.js"]
