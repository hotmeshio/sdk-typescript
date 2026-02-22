import { HotMesh } from '../hotmesh';
import { SearchService } from '../search';
import { KeyService, KeyType } from '../../modules/key';
import { asyncLocalStorage } from '../../modules/storage';

/**
 * The Entity module provides methods for reading and writing
 * JSONB data to a workflow's entity. The instance methods
 * exposed by this class are available for use from within
 * a running workflow.
 *
 * @example
 * ```typescript
 * //entityWorkflow.ts
 * import { workflow } from '@hotmeshio/hotmesh';
 *
 * export async function entityExample(): Promise<void> {
 *   const entity = await workflow.entity();
 *   await entity.set({ user: { id: 123 } });
 *   await entity.merge({ user: { name: "John" } });
 *   const user = await entity.get("user");
 *   // user = { id: 123, name: "John" }
 * }
 * ```
 */
export class Entity {
  /**
   * @private
   */
  jobId: string;
  /**
   * @private
   */
  searchSessionId: string;
  /**
   * @private
   */
  searchSessionIndex = 0;
  /**
   * @private
   */
  hotMeshClient: HotMesh;
  /**
   * @private
   */
  search: SearchService<any> | null;
  /**
   * @private
   */
  workflowDimension: string;

  /**
   * @private
   */
  constructor(
    workflowId: string,
    hotMeshClient: HotMesh,
    searchSessionId: string,
  ) {
    const keyParams = {
      appId: hotMeshClient.appId,
      jobId: workflowId,
    };
    this.jobId = KeyService.mintKey(
      hotMeshClient.namespace,
      KeyType.JOB_STATE,
      keyParams,
    );
    this.searchSessionId = searchSessionId;
    this.hotMeshClient = hotMeshClient;
    this.search = hotMeshClient.engine.search;

    // Get workflow dimension from async local storage
    const store = asyncLocalStorage.getStore();
    this.workflowDimension = store?.get('workflowDimension') ?? '';
  }

  /**
   * increments the index to return a unique search session guid when
   * calling any method that produces side effects (changes the value)
   * @private
   */
  getSearchSessionGuid(): string {
    return `${this.searchSessionId}-${this.searchSessionIndex++}-`;
  }

  /**
   * Sets the entire entity object. This replaces any existing entity.
   *
   * @example
   * const entity = await workflow.entity();
   * await entity.set({ user: { id: 123, name: "John" } });
   */
  async set<T>(value: T): Promise<T> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      return JSON.parse(replay[ssGuid]);
    }

    // Use single transactional call to update entity and store replay value
    const result = await this.search.updateContext(this.jobId, {
      '@context': JSON.stringify(value),
      [ssGuid]: '', // Pass replay ID to hash module for transactional replay storage
    });

    return result as T;
  }

  /**
   * Deep merges the provided object with the existing entity
   *
   * @example
   * const entity = await workflow.entity();
   * await entity.merge({ user: { email: "john@example.com" } });
   */
  async merge<T>(value: T): Promise<T> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      return JSON.parse(replay[ssGuid]);
    }

    // Use server-side JSONB merge operation with replay storage
    const newContext = await this.search.updateContext(this.jobId, {
      '@context:merge': JSON.stringify(value),
      [ssGuid]: '', // Pass replay ID to hash module
    });

    return newContext as T;
  }

  /**
   * Gets a value from the entity by path
   *
   * @example
   * const entity = await workflow.entity();
   * const user = await entity.get("user");
   * const email = await entity.get("user.email");
   */
  async get<T>(path?: string): Promise<T> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      // Replay cache stores the already-extracted value, not full entity
      return JSON.parse(replay[ssGuid]);
    }

    let value: any;

    if (!path) {
      // No path - fetch entire entity with replay storage
      const result = await this.search.updateContext(this.jobId, {
        '@context:get': '',
        [ssGuid]: '', // Pass replay ID to hash module
      });
      // setFields returns the actual entity value for @context:get operations
      value = result || {};
    } else {
      // Use PostgreSQL JSONB path extraction for specific paths with replay storage
      const result = await this.search.updateContext(this.jobId, {
        '@context:get': path,
        [ssGuid]: '', // Pass replay ID to hash module
      });
      // setFields returns the actual path value for @context:get operations
      value = result;
    }

    return value as T;
  }

  /**
   * Deletes a value from the entity by path
   *
   * @example
   * const entity = await workflow.entity();
   * await entity.delete("user.email");
   */
  async delete(path: string): Promise<any> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      return JSON.parse(replay[ssGuid]);
    }

    // Use server-side JSONB delete operation with replay storage
    const newContext = await this.search.updateContext(this.jobId, {
      '@context:delete': path,
      [ssGuid]: '', // Pass replay ID to hash module
    });

    return newContext as any;
  }

  /**
   * Appends a value to an array at the specified path
   *
   * @example
   * const entity = await workflow.entity();
   * await entity.append("items", { id: 1, name: "New Item" });
   */
  async append(path: string, value: any): Promise<any[]> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      return JSON.parse(replay[ssGuid]);
    }

    // Use server-side JSONB array append operation with replay storage
    const newArray = await this.search.updateContext(this.jobId, {
      '@context:append': JSON.stringify({ path, value }),
      [ssGuid]: '', // Pass replay ID to hash module
    });

    return newArray as unknown as any[];
  }

  /**
   * Prepends a value to an array at the specified path
   *
   * @example
   * const entity = await workflow.entity();
   * await entity.prepend("items", { id: 0, name: "First Item" });
   */
  async prepend(path: string, value: any): Promise<any[]> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      return JSON.parse(replay[ssGuid]);
    }

    // Use server-side JSONB array prepend operation with replay storage
    const newArray = await this.search.updateContext(this.jobId, {
      '@context:prepend': JSON.stringify({ path, value }),
      [ssGuid]: '', // Pass replay ID to hash module
    });

    return newArray as unknown as any[];
  }

  /**
   * Removes an item from an array at the specified path and index
   *
   * @example
   * const entity = await workflow.entity();
   * await entity.remove("items", 0); // Remove first item
   */
  async remove(path: string, index: number): Promise<any[]> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      return JSON.parse(replay[ssGuid]);
    }

    // Use server-side JSONB array remove operation with replay storage
    const newArray = await this.search.updateContext(this.jobId, {
      '@context:remove': JSON.stringify({ path, index }),
      [ssGuid]: '', // Pass replay ID to hash module
    });

    return newArray as unknown as any[];
  }

  /**
   * Increments a numeric value at the specified path
   *
   * @example
   * const entity = await workflow.entity();
   * await entity.increment("counter", 5);
   */
  async increment(path: string, value = 1): Promise<number> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      return JSON.parse(replay[ssGuid]);
    }

    // Use server-side JSONB increment operation with replay storage
    const newValue = await this.search.updateContext(this.jobId, {
      '@context:increment': JSON.stringify({ path, value }),
      [ssGuid]: '', // Pass replay ID to hash module
    });

    return Number(newValue);
  }

  /**
   * Toggles a boolean value at the specified path
   *
   * @example
   * const entity = await workflow.entity();
   * await entity.toggle("settings.enabled");
   */
  async toggle(path: string): Promise<boolean> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      return JSON.parse(replay[ssGuid]);
    }

    // Use server-side JSONB toggle operation with replay storage
    const newValue = await this.search.updateContext(this.jobId, {
      '@context:toggle': path,
      [ssGuid]: '', // Pass replay ID to hash module
    });

    return Boolean(newValue);
  }

  /**
   * Sets a value at the specified path only if it doesn't already exist
   *
   * @example
   * const entity = await workflow.entity();
   * await entity.setIfNotExists("user.id", 123);
   */
  async setIfNotExists(path: string, value: any): Promise<any> {
    const ssGuid = this.getSearchSessionGuid();
    const store = asyncLocalStorage.getStore();
    const replay = store?.get('replay') ?? {};

    if (ssGuid in replay) {
      return JSON.parse(replay[ssGuid]);
    }

    // Use server-side JSONB conditional set operation with replay storage
    const newValue = await this.search.updateContext(this.jobId, {
      '@context:setIfNotExists': JSON.stringify({ path, value }),
      [ssGuid]: '', // Pass replay ID to hash module
    });

    return newValue;
  }

  // Static readonly find methods for cross-entity querying (not tied to specific workflow)

  /**
   * Finds entity records matching complex conditions using JSONB/SQL queries.
   * This is a readonly operation that queries across all entities of a given type.
   *
   * @example
   * ```typescript
   * // Basic find with simple conditions
   * const activeUsers = await Entity.find(
   *   'user',
   *   { status: 'active', country: 'US' },
   *   hotMeshClient
   * );
   *
   * // Complex query with comparison operators
   * const seniorUsers = await Entity.find(
   *   'user',
   *   {
   *     age: { $gte: 65 },
   *     status: 'active',
   *     'preferences.notifications': true
   *   },
   *   hotMeshClient,
   *   { limit: 10, offset: 0 }
   * );
   *
   * // Query with multiple conditions and nested objects
   * const premiumUsers = await Entity.find(
   *   'user',
   *   {
   *     'subscription.type': 'premium',
   *     'subscription.status': 'active',
   *     'billing.amount': { $gt: 100 },
   *     'profile.verified': true
   *   },
   *   hotMeshClient,
   *   { limit: 20 }
   * );
   *
   * // Array conditions
   * const taggedPosts = await Entity.find(
   *   'post',
   *   {
   *     'tags': { $in: ['typescript', 'javascript'] },
   *     'status': 'published',
   *     'views': { $gte: 1000 }
   *   },
   *   hotMeshClient
   * );
   * ```
   */
  static async find(
    entity: string,
    conditions: Record<string, any>,
    hotMeshClient: HotMesh,
    options?: { limit?: number; offset?: number },
  ): Promise<any[]> {
    // Use SearchService for JSONB/SQL querying
    const searchClient = hotMeshClient.engine.search;
    return await searchClient.findEntities(entity, conditions, options);
  }

  /**
   * Finds a specific entity record by its ID using direct JSONB/SQL queries.
   * This is the most efficient method for retrieving a single entity record.
   *
   * @example
   * ```typescript
   * // Basic findById usage
   * const user = await Entity.findById('user', 'user123', hotMeshClient);
   *
   * // Example with type checking
   * interface User {
   *   id: string;
   *   name: string;
   *   email: string;
   *   preferences: {
   *     theme: 'light' | 'dark';
   *     notifications: boolean;
   *   };
   * }
   *
   * const typedUser = await Entity.findById<User>('user', 'user456', hotMeshClient);
   * console.log(typedUser.preferences.theme); // 'light' | 'dark'
   *
   * // Error handling example
   * try {
   *   const order = await Entity.findById('order', 'order789', hotMeshClient);
   *   if (!order) {
   *     console.log('Order not found');
   *     return;
   *   }
   *   console.log('Order details:', order);
   * } catch (error) {
   *   console.error('Error fetching order:', error);
   * }
   * ```
   */
  static async findById(
    entity: string,
    id: string,
    hotMeshClient: HotMesh,
  ): Promise<any> {
    // Use SearchService for JSONB/SQL querying
    const searchClient = hotMeshClient.engine.search;
    return await searchClient.findEntityById(entity, id);
  }

  /**
   * Finds entity records matching a specific field condition using JSONB/SQL queries.
   * Supports various operators for flexible querying across all entities of a type.
   *
   * @example
   * ```typescript
   * // Basic equality search
   * const activeUsers = await Entity.findByCondition(
   *   'user',
   *   'status',
   *   'active',
   *   '=',
   *   hotMeshClient,
   *   { limit: 20 }
   * );
   *
   * // Numeric comparison
   * const highValueOrders = await Entity.findByCondition(
   *   'order',
   *   'total_amount',
   *   1000,
   *   '>=',
   *   hotMeshClient
   * );
   *
   * // Pattern matching with LIKE
   * const gmailUsers = await Entity.findByCondition(
   *   'user',
   *   'email',
   *   '%@gmail.com',
   *   'LIKE',
   *   hotMeshClient
   * );
   *
   * // IN operator for multiple values
   * const specificProducts = await Entity.findByCondition(
   *   'product',
   *   'category',
   *   ['electronics', 'accessories'],
   *   'IN',
   *   hotMeshClient
   * );
   *
   * // Not equals operator
   * const nonPremiumUsers = await Entity.findByCondition(
   *   'user',
   *   'subscription_type',
   *   'premium',
   *   '!=',
   *   hotMeshClient
   * );
   *
   * // Date comparison
   * const recentOrders = await Entity.findByCondition(
   *   'order',
   *   'created_at',
   *   new Date('2024-01-01'),
   *   '>',
   *   hotMeshClient,
   *   { limit: 50 }
   * );
   * ```
   */
  static async findByCondition(
    entity: string,
    field: string,
    value: any,
    operator: '=' | '!=' | '>' | '<' | '>=' | '<=' | 'LIKE' | 'IN' = '=',
    hotMeshClient: HotMesh,
    options?: { limit?: number; offset?: number },
  ): Promise<any[]> {
    // Use SearchService for JSONB/SQL querying
    const searchClient = hotMeshClient.engine.search;
    return await searchClient.findEntitiesByCondition(
      entity,
      field,
      value,
      operator,
      options,
    );
  }

  /**
   * Creates an efficient GIN index for a specific entity field to optimize queries.
   *
   * @example
   * ```typescript
   * await Entity.createIndex('user', 'email', hotMeshClient);
   * await Entity.createIndex('user', 'status', hotMeshClient);
   * ```
   */
  static async createIndex(
    entity: string,
    field: string,
    hotMeshClient: HotMesh,
    indexType: 'gin' = 'gin',
  ): Promise<void> {
    // Use SearchService for index creation
    const searchClient = hotMeshClient.engine.search;
    return await searchClient.createEntityIndex(entity, field, indexType);
  }
}
