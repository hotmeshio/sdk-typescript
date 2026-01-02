import { ILogger } from '../../../logger';
import { KeyType } from '../../../../modules/key';
import { sleepFor, formatISODate } from '../../../../modules/utils';
import { KeyStoreParams, ScoutType } from '../../../../types';
import { PostgresClientType } from '../../../../types/postgres';
import { ProviderClient } from '../../../../types/provider';
import {
  HMSH_ROUTER_SCOUT_INTERVAL_MS,
  HMSH_ROUTER_SCOUT_INTERVAL_SECONDS,
} from '../../../../modules/enums';

/**
 * Scout state manager for coordinating polling across multiple instances.
 * Only one instance at a time should be the active scout.
 */
export class ScoutManager {
  private isScout = false;
  private shouldStopScout = false;

  constructor(
    private client: PostgresClientType & ProviderClient,
    private appId: string,
    private getTableName: () => string,
    private mintKey: (type: KeyType, params: KeyStoreParams) => string,
    private logger: ILogger,
  ) {}

  /**
   * Start the router scout polling loop.
   * Winner polls frequently for visible messages, losers retry acquiring role less frequently.
   */
  startRouterScoutPoller(): void {
    this.shouldStopScout = false;
    this.pollForVisibleMessagesLoop().catch((error) => {
      this.logger.error('postgres-stream-router-scout-start-error', { error });
    });

    this.logger.info('postgres-stream-router-scout-started', {
      appId: this.appId,
      pollInterval: this.getRouterScoutInterval(),
      scoutInterval: HMSH_ROUTER_SCOUT_INTERVAL_SECONDS,
    });
  }

  /**
   * Stop the router scout polling loop and release the role.
   */
  async stopRouterScoutPoller(): Promise<void> {
    this.shouldStopScout = true;
    
    if (this.isScout) {
      await this.releaseScoutRole('router');
      this.isScout = false;
    }
  }

  /**
   * Check if this instance should act as the router scout.
   */
  private async shouldScout(): Promise<boolean> {
    const wasScout = this.isScout;
    const isScout =
      wasScout || (this.isScout = await this.reserveRouterScoutRole());
    
    if (isScout) {
      if (!wasScout) {
        // First time becoming scout - set timeout to reset after interval
        setTimeout(() => {
          this.isScout = false;
        }, HMSH_ROUTER_SCOUT_INTERVAL_SECONDS * 1_000);
      }
      return true;
    }
    return false;
  }

  /**
   * Main polling loop for the router scout.
   */
  private async pollForVisibleMessagesLoop(): Promise<void> {
    while (!this.shouldStopScout) {
      try {
        if (await this.shouldScout()) {
          // We're the scout - poll for visible messages frequently
          await this.pollForVisibleMessages();
          
          // Sleep for the short polling interval
          await sleepFor(this.getRouterScoutInterval());
        } else {
          // Not the scout - sleep longer before trying to acquire role again
          await sleepFor(HMSH_ROUTER_SCOUT_INTERVAL_SECONDS * 1_000);
        }
      } catch (error) {
        this.logger.error('postgres-stream-router-scout-loop-error', { error });
        await sleepFor(1000); // Brief pause on error
      }
    }
  }

  /**
   * Poll for visible messages and trigger notifications for any found.
   */
  private async pollForVisibleMessages(): Promise<void> {
    try {
      const tableName = this.getTableName();
      const schemaName = tableName.split('.')[0];
      
      const result = await this.client.query(
        `SELECT ${schemaName}.notify_visible_messages() as count`
      );
      
      const notificationCount = result.rows[0]?.count || 0;
      
      if (notificationCount > 0) {
        this.logger.debug('postgres-stream-router-scout-notifications', {
          count: notificationCount,
        });
      }
    } catch (error) {
      // Log but don't throw - this is a background task
      this.logger.debug('postgres-stream-router-scout-poll-error', {
        error: error.message,
      });
    }
  }

  /**
   * Reserve the router scout role using direct SQL.
   */
  private async reserveRouterScoutRole(): Promise<boolean> {
    try {
      return await this.reserveScoutRole('router', HMSH_ROUTER_SCOUT_INTERVAL_SECONDS);
    } catch (error) {
      this.logger.error('postgres-stream-router-scout-reserve-error', { error });
      return false;
    }
  }

  /**
   * Reserve a scout role for the specified type.
   * Uses SET NX (set if not exists) with expiration to ensure only one instance holds the role.
   */
  private async reserveScoutRole(
    scoutType: ScoutType,
    delay = HMSH_ROUTER_SCOUT_INTERVAL_SECONDS,
  ): Promise<boolean> {
    try {
      const key = this.mintKey(KeyType.WORK_ITEMS, {
        appId: this.appId,
        scoutType,
      });
      const value = `${scoutType}:${formatISODate(new Date())}`;
      const expirySeconds = delay - 1;
      const tableName = this.getTableName().split('.')[0] + '.roles';

      // Use INSERT ... ON CONFLICT to implement SET NX with expiration
      // Only succeeds if key doesn't exist OR if existing entry has expired
      const result = await this.client.query(
        `INSERT INTO ${tableName} (key, value, expiry)
         VALUES ($1, $2, NOW() + INTERVAL '${expirySeconds} seconds')
         ON CONFLICT (key) 
         DO UPDATE SET 
           value = EXCLUDED.value,
           expiry = EXCLUDED.expiry
         WHERE ${tableName}.expiry IS NULL OR ${tableName}.expiry <= NOW()
         RETURNING key`,
        [key, value]
      );

      return result.rows.length > 0;
    } catch (error) {
      this.logger.error('postgres-stream-reserve-scout-error', { 
        scoutType, 
        error: error.message 
      });
      return false;
    }
  }

  /**
   * Release a scout role for the specified type.
   */
  private async releaseScoutRole(
    scoutType: ScoutType,
  ): Promise<boolean> {
    try {
      const key = this.mintKey(KeyType.WORK_ITEMS, {
        appId: this.appId,
        scoutType,
      });
      const tableName = this.getTableName().split('.')[0] + '.roles';

      const result = await this.client.query(
        `DELETE FROM ${tableName} WHERE key = $1`,
        [key]
      );

      return result.rowCount > 0;
    } catch (error) {
      this.logger.error('postgres-stream-release-scout-error', { 
        scoutType, 
        error: error.message 
      });
      return false;
    }
  }

  /**
   * Get the router scout polling interval in milliseconds.
   */
  private getRouterScoutInterval(): number {
    return HMSH_ROUTER_SCOUT_INTERVAL_MS;
  }

  /**
   * Check if this instance is currently the scout.
   */
  isCurrentlyScout(): boolean {
    return this.isScout;
  }
}

