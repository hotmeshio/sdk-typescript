# HotMesh
![beta release](https://img.shields.io/badge/release-beta-blue.svg)

HotMesh is a drop-in Temporal.io alternative that runs natively on your Postgres database. It provides the same durable execution capabilities - automatic retries, timeouts, and failure recovery - without requiring additional infrastructure.

## Features

- **Drop-in Temporal Alternative**: Use familiar Temporal.io patterns and concepts
- **Postgres Native**: Runs directly on your existing Postgres/Supabase database
- **Durable Execution**: Automatic retry, timeout handling, and failure recovery
- **Fault Tolerant**: Workflow state persists across system restarts
- **Process Analytics**: Built-in observability with OpenTelemetry

## Install

```sh
npm install @hotmeshio/hotmesh
```

## Quick Start

1. Connect to your database:

```typescript
import { HotMesh } from '@hotmeshio/hotmesh';
import { Client as Postgres } from 'pg';

const hotMesh = await HotMesh.init({
  appId: 'my-app',
  engine: {
    connection: {
      class: Postgres,
      options: {
        connectionString: 'postgresql://user:pass@localhost:5432/db'
      }
    }
  }
});
```

2. Define your workflow:

```typescript
// Define activities
export async function validateOrder(order: any): Promise<boolean> {
  // Your validation logic
  return true;
}

export async function processPayment(order: any): Promise<string> {
  // Your payment logic
  return 'payment-id';
}

// Define workflow
import { workflow } from '@hotmeshio/hotmesh';
import * as activities from './activities';

const { validateOrder, processPayment } = workflow
  .proxyActivities<typeof activities>({
    activities
  });

export async function orderWorkflow(order: any): Promise<string> {
  // Activities are automatically retried on failure
  const isValid = await validateOrder(order);
  if (!isValid) {
    throw new Error('Invalid order');
  }
  
  return await processPayment(order);
}
```

3. Run your workflow:

```typescript
import { Client } from '@hotmeshio/hotmesh';
import { Client as Postgres } from 'pg';

const client = new Client({
  connection: {
    class: Postgres,
    options: {
      connectionString: 'postgresql://user:pass@localhost:5432/db'
    }
  }
});

// Start workflow
const handle = await client.workflow.start({
  args: [{ orderId: '123', amount: 99.99 }],
  taskQueue: 'default',
  workflowName: 'orderWorkflow'
});

// Wait for result
const result = await handle.result();
```

## Key Concepts

- **Durable Execution**: Activities are automatically retried on failure with exponential backoff
- **State Management**: Workflow state is persisted in Postgres, surviving crashes and restarts
- **Observability**: Built-in OpenTelemetry support for monitoring and debugging
- **Type Safety**: Full TypeScript support with type inference

## Learn More

- [📄 SDK Documentation](https://hotmeshio.github.io/sdk-typescript/)
- [💼 Examples](https://github.com/hotmeshio/samples-typescript)
- [🏠 Website](https://hotmesh.io/)

## License

See [LICENSE](./LICENSE)
