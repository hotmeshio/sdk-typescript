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
  async set(value: any): Promise<any> {
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

    return result || value;
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
  async get(path?: string): Promise<any> {
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

    return value;
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

    return newContext;
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
  async increment(path: string, value: number = 1): Promise<number> {
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

  /**
   * @private
   */
  private deepMerge(target: any, source: any): any {
    if (!source) return target;
    
    const output = { ...target };
    
    Object.keys(source).forEach(key => {
      if (source[key] instanceof Object && key in target) {
        output[key] = this.deepMerge(target[key], source[key]);
      } else {
        output[key] = source[key];
      }
    });
    
    return output;
  }
} 