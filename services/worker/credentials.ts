/**
 * High-level worker credential lifecycle management.
 *
 * Creates an admin client from a connection config, delegates to the
 * low-level Postgres credential functions, and cleans up the client.
 * This is the canonical API — `HotMesh` and `Durable` re-export these
 * as static convenience methods.
 */

import {
  provisionWorkerRole as _provisionWorkerRole,
  rotateWorkerPassword as _rotateWorkerPassword,
  revokeWorkerRole as _revokeWorkerRole,
  listWorkerRoles as _listWorkerRoles,
} from '../stream/providers/postgres/credentials';
import type {
  WorkerCredential,
  WorkerCredentialInfo,
} from '../stream/providers/postgres/credentials';
import { ProviderConfig, ProvidersConfig } from '../../types/provider';

/**
 * Create a raw Postgres client from a connection config for admin
 * operations (credential provisioning, rotation, revocation).
 */
async function createAdminClient(
  connection: ProviderConfig | ProvidersConfig,
): Promise<any> {
  const config =
    'options' in connection
      ? connection
      : (connection as any).store ?? connection;
  const ClientClass = config.class;
  const options = config.options;
  const client = new ClientClass(options);
  await client.connect();
  return client;
}

/**
 * Provision a scoped Postgres role for a worker router.
 *
 * The role can only dequeue/ack/respond on the specified stream
 * names via SECURITY DEFINER stored procedures — it has **zero
 * direct table access**.
 */
export async function provisionWorkerRole(config: {
  connection: ProviderConfig | ProvidersConfig;
  namespace?: string;
  streamNames: string[];
  password?: string;
}): Promise<WorkerCredential> {
  const namespace = config.namespace ?? 'durable';
  const pgClient = await createAdminClient(config.connection);
  try {
    return await _provisionWorkerRole(
      pgClient,
      namespace,
      config.streamNames,
      config.password,
    );
  } finally {
    await pgClient.end?.();
  }
}

/**
 * Rotate the password for an existing worker role.
 */
export async function rotateWorkerPassword(config: {
  connection: ProviderConfig | ProvidersConfig;
  namespace?: string;
  roleName: string;
  newPassword?: string;
}): Promise<{ password: string }> {
  const namespace = config.namespace ?? 'durable';
  const pgClient = await createAdminClient(config.connection);
  try {
    return await _rotateWorkerPassword(
      pgClient,
      namespace,
      config.roleName,
      config.newPassword,
    );
  } finally {
    await pgClient.end?.();
  }
}

/**
 * Revoke a worker role by disabling login. The role is not dropped,
 * preserving the audit trail in the `worker_credentials` table.
 */
export async function revokeWorkerRole(config: {
  connection: ProviderConfig | ProvidersConfig;
  namespace?: string;
  roleName: string;
}): Promise<void> {
  const namespace = config.namespace ?? 'durable';
  const pgClient = await createAdminClient(config.connection);
  try {
    await _revokeWorkerRole(pgClient, namespace, config.roleName);
  } finally {
    await pgClient.end?.();
  }
}

/**
 * List all provisioned worker roles for a namespace.
 */
export async function listWorkerRoles(config: {
  connection: ProviderConfig | ProvidersConfig;
  namespace?: string;
}): Promise<WorkerCredentialInfo[]> {
  const namespace = config.namespace ?? 'durable';
  const pgClient = await createAdminClient(config.connection);
  try {
    return await _listWorkerRoles(pgClient, namespace);
  } finally {
    await pgClient.end?.();
  }
}

export type { WorkerCredential, WorkerCredentialInfo };
