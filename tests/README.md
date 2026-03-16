# Running Tests

## Quick Start

**Always use `POSTGRES_HOST=localhost` when running tests locally:**

```bash
# Check Docker is running
docker ps

# Run any test suite
POSTGRES_HOST=localhost npm run test:durable:exporter
POSTGRES_HOST=localhost npm run test:durable:goodbye
POSTGRES_HOST=localhost npm run test:durable:hello
```

## Why localhost?

The default config (`tests/$setup/config/index.ts:15`) uses `POSTGRES_HOST=postgres` for docker-compose networking. Local development needs `localhost`.

## Common Commands

```bash
# All durable tests
POSTGRES_HOST=localhost npm run test:durable:postgres

# With debug logging
POSTGRES_HOST=localhost HMSH_LOGLEVEL=debug npm run test:durable:goodbye

# Tail output
POSTGRES_HOST=localhost npm run test:durable:exporter 2>&1 | tail -100
```

## Test Structure

```
tests/durable/[name]/
├── src/
│   ├── workflows.ts
│   └── activities.ts
└── postgres.test.ts
```

Import from `../../$setup/postgres` for `postgres_options` and `dropTables()`.
