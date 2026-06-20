import { guid, uuid } from '../../modules/utils';
import { EscalationClientService } from './client';

/**
 * Escalation queue — the canonical signal-pause surface for HotMesh workflows.
 *
 * `public.hmsh_escalations` is a global Postgres table written by both the
 * YAML DAG hook activity and `Durable.workflow.condition()`. Any consumer
 * can interact with the queue using just a Postgres connection — no Durable
 * dependency required.
 *
 * ## Quick Start
 *
 * ```typescript
 * import { Escalations } from '@hotmeshio/hotmesh';
 * import { Client as Postgres } from 'pg';
 *
 * const client = new Escalations.Client({
 *   connection: {
 *     class: Postgres,
 *     options: { connectionString: 'postgresql://usr:pwd@localhost:5432/db' },
 *   },
 * });
 *
 * // List all available approvals for the 'manager' role
 * const pending = await client.list({ role: 'manager', available: true });
 *
 * // Claim one atomically
 * const result = await client.claimByMetadata({
 *   key: 'orderId', value: 'order-123',
 *   assignee: 'alice@company.com',
 *   roles: ['manager', 'senior-manager'],
 * });
 *
 * // Resolve and resume the waiting workflow in one round-trip
 * if (result.ok) {
 *   await client.resolve({ id: result.entry.id, resolverPayload: { approved: true } });
 * }
 * ```
 */
class EscalationsClass {
  /** @private */
  constructor() {}

  /**
   * Creates an escalation queue client. Pass a `connection` for standalone
   * use, or inject a `getHotMeshClient` function to share an existing
   * engine pool (e.g. from a `Durable.Client`).
   */
  static Client: typeof EscalationClientService = EscalationClientService;

  /**
   * Generate a compact HotMesh identifier (not RFC 4122).
   * Use `Escalations.uuid()` for DB primary keys.
   */
  static guid = guid;

  /**
   * Generate a standard RFC 4122 v4 UUID — required for the `id` field
   * when calling `client.migrate()` or any direct UUID column insert.
   */
  static uuid = uuid;

  /**
   * Gracefully stop all escalation engine instances.
   * Call from your process signal handlers (`SIGTERM`, `SIGINT`).
   */
  static async shutdown(): Promise<void> {
    await EscalationClientService.shutdown();
  }
}

export { EscalationsClass as Escalations };
export { EscalationClientService };
