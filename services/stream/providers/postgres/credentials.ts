/**
 * Worker role credential lifecycle management.
 *
 * These are the low-level functions that create, rotate, and revoke
 * Postgres roles for worker routers. Each role is scoped to specific
 * stream_name(s) via the `app.allowed_streams` session variable.
 *
 * **Prefer the high-level API**: Call `HotMesh.provisionWorkerRole()`,
 * `HotMesh.rotateWorkerPassword()`, etc. — those methods handle
 * connection creation and cleanup automatically.
 */

import { randomUUID } from 'crypto';

import { PostgresClientType } from '../../../../types/postgres';

export interface WorkerCredential {
  roleName: string;
  password: string;
}

export interface WorkerCredentialInfo {
  id: number;
  roleName: string;
  streamNames: string[];
  createdAt: Date;
  revokedAt: Date | null;
  lastRotatedAt: Date;
}

/**
 * Sanitize a stream name for use in a Postgres role name.
 * Replaces non-alphanumeric characters with underscores.
 */
function sanitizeForRoleName(name: string): string {
  return name.replace(/[^a-zA-Z0-9]/g, '_');
}

/**
 * Provision a new Postgres role scoped to specific stream names.
 *
 * The role:
 * - Can LOGIN with the returned password
 * - Has USAGE on the schema
 * - Has EXECUTE on the 5 worker stored procedures
 * - Has `app.allowed_streams` set to the comma-separated stream names
 * - Has NO direct table access
 *
 * @param adminClient - A Postgres client connected as the admin/owner
 * @param schema - The appId schema name (e.g., 'durable')
 * @param streamNames - Stream names this role can access (e.g., ['payment-activity'])
 * @param password - Optional password (generated if not provided)
 * @returns The created role name and password
 */
export async function provisionWorkerRole(
  adminClient: PostgresClientType,
  schema: string,
  streamNames: string[],
  password?: string,
): Promise<WorkerCredential> {
  const safeSchema = schema.replace(/[^a-zA-Z0-9_]/g, '_');
  const streamSuffix = streamNames.map(sanitizeForRoleName).join('__');
  const roleName = `hmsh_wrk_${safeSchema}_${streamSuffix}`.substring(0, 63);
  const rolePassword = password || randomUUID();
  const allowedStreams = streamNames.join(',');

  // Create the role (idempotent — drop if exists first for credential reset)
  await adminClient.query(`
    DO $$
    BEGIN
      IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${roleName}') THEN
        EXECUTE format('ALTER ROLE %I LOGIN PASSWORD %L', '${roleName}', '${rolePassword}');
      ELSE
        EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '${roleName}', '${rolePassword}');
      END IF;
    END
    $$;
  `);

  // Grant schema usage (required to call functions in the schema)
  await adminClient.query(
    `GRANT USAGE ON SCHEMA ${safeSchema} TO ${roleName};`,
  );

  // Grant EXECUTE on the 5 worker procedures
  const procedures = [
    `${safeSchema}.worker_dequeue(TEXT, INT, TEXT, INT)`,
    `${safeSchema}.worker_ack(TEXT, BIGINT[])`,
    `${safeSchema}.worker_dead_letter(TEXT, BIGINT[])`,
    `${safeSchema}.worker_respond(TEXT, TEXT, INT, NUMERIC, INT, TIMESTAMPTZ, INT)`,
    `${safeSchema}.worker_listen(TEXT)`,
    `${safeSchema}.worker_unlisten(TEXT)`,
  ];
  for (const proc of procedures) {
    await adminClient.query(
      `GRANT EXECUTE ON FUNCTION ${proc} TO ${roleName};`,
    );
  }

  // Set the allowed streams as a session default for this role
  await adminClient.query(
    `ALTER ROLE ${roleName} SET app.allowed_streams = '${allowedStreams}';`,
  );

  // Explicitly revoke direct table access (defense in depth)
  await adminClient.query(`
    REVOKE ALL ON ALL TABLES IN SCHEMA ${safeSchema} FROM ${roleName};
  `);

  // Record in worker_credentials table
  await adminClient.query(
    `INSERT INTO ${safeSchema}.worker_credentials (role_name, stream_names)
     VALUES ($1, $2)
     ON CONFLICT (role_name) DO UPDATE
       SET stream_names = $2,
           revoked_at = NULL,
           last_rotated_at = NOW()`,
    [roleName, streamNames],
  );

  return { roleName, password: rolePassword };
}

/**
 * Rotate the password for an existing worker role.
 */
export async function rotateWorkerPassword(
  adminClient: PostgresClientType,
  schema: string,
  roleName: string,
  newPassword?: string,
): Promise<{ password: string }> {
  const safeSchema = schema.replace(/[^a-zA-Z0-9_]/g, '_');
  const password = newPassword || randomUUID();

  await adminClient.query(
    `ALTER ROLE ${roleName} PASSWORD '${password}';`,
  );

  await adminClient.query(
    `UPDATE ${safeSchema}.worker_credentials
     SET last_rotated_at = NOW()
     WHERE role_name = $1`,
    [roleName],
  );

  return { password };
}

/**
 * Revoke a worker role by disabling login.
 * The role is not dropped — this preserves the audit trail.
 */
export async function revokeWorkerRole(
  adminClient: PostgresClientType,
  schema: string,
  roleName: string,
): Promise<void> {
  const safeSchema = schema.replace(/[^a-zA-Z0-9_]/g, '_');

  await adminClient.query(`ALTER ROLE ${roleName} NOLOGIN;`);

  await adminClient.query(
    `UPDATE ${safeSchema}.worker_credentials
     SET revoked_at = NOW()
     WHERE role_name = $1`,
    [roleName],
  );
}

/**
 * List all worker roles for a schema.
 */
export async function listWorkerRoles(
  adminClient: PostgresClientType,
  schema: string,
): Promise<WorkerCredentialInfo[]> {
  const safeSchema = schema.replace(/[^a-zA-Z0-9_]/g, '_');

  const result = await adminClient.query(
    `SELECT id, role_name, stream_names, created_at, revoked_at, last_rotated_at
     FROM ${safeSchema}.worker_credentials
     ORDER BY created_at`,
  );

  return result.rows.map((row: any) => ({
    id: row.id,
    roleName: row.role_name,
    streamNames: row.stream_names,
    createdAt: row.created_at,
    revokedAt: row.revoked_at,
    lastRotatedAt: row.last_rotated_at,
  }));
}
