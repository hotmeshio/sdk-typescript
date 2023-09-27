# Base stage
FROM node:19.8.1 AS base
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .

# Development stage
FROM base AS development
ENV NODE_ENV=development
CMD ["tail", "-f", "/dev/null"]

# Build stage
FROM base AS builder
ENV NODE_ENV=production
RUN npm run build

# Production stage
FROM node:19.8.1-alpine AS production
ENV NODE_ENV=production
WORKDIR /app
COPY --from=builder /app/package*.json ./
RUN npm ci --only=production
COPY --from=builder /app/build ./build

CMD ["npm", "run", "test"]
