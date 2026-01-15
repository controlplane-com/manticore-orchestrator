# Build stage
FROM node:20-alpine AS builder

WORKDIR /app

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

# Copy package files and install dependencies
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

# Copy source and build
COPY . .
RUN pnpm run build

# Runtime stage - Express server
FROM node:20-alpine

WORKDIR /app

# Copy built client assets and server
COPY --from=builder /app/dist ./dist

# Install production dependencies only
COPY --from=builder /app/package.json ./
RUN npm install --omit=dev express axios compression

ENV PORT=3000
ENV NODE_ENV=production

EXPOSE 3000

CMD ["node", "dist/server.js"]
