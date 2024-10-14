import * as Redis from 'redis';

import { MeshData } from '../meshdata/index';
import { arrayToHash, guid } from '../../modules/utils';
import * as Types from '../../types';

/**
 * MeshOS is an abstract base class for schema-driven entity management within the Mesh network.
 * It provides a foundation for defining custom entities that interact with the Redis-backed mesh data store.
 * By subclassing MeshOS, you can create entities with specific schemas and behaviors, enabling
 * structured data storage, retrieval, and transactional workflows.
 *
 * ### Subclassing MeshOS
 *
 * To create a custom entity, subclass MeshOS and implement the required methods. At a minimum, you should implement:
 *
 * - `getEntity()`: Returns the name of the entity.
 * - `getSearchOptions()`: Returns indexing and schema options for the entity.
 *
 * You can also implement other methods as needed for your entity's functionality.
 *
 * @example
 * ```typescript
 * import { MeshOS } from '@hotmeshio/hotmesh';
 * import { Types } from '@hotmeshio/hotmesh';
 * import { schema } from './schema'; // Import your schema
 * import * as workflows from './workflows';
 *
 * class Widget extends MeshOS {
 *   
 *   //Return the version
 *   getTaskQueue(): string {
 *     return 'v1';
 *   }
 * 
 *   // Return the entity name
 *   getEntity(): string {
 *     return 'widget';
 *   }
 *
 *   // Return the search options including the schema
 *   getSearchOptions(): Types.WorkflowSearchOptions {
 *     return {
 *       index: `${this.getNamespace()}-${this.getEntity()}`,
 *       prefix: [this.getEntity()],
 *       schema,
 *     };
 *   }

 *   //Subclass the `connect` method to connect workers/hooks
 *   //upon server startup
 *   async connect() {
 *     await this.meshData.connect({
 *       entity: 'widget',
 *       target: workflows.createWidget,
 *       options: {
 *         namespace: this.getNamespace(),
 *         taskQueue: this.getTaskQueue(),
 *       },
 *     });
 *   }
 *
 *   // subclass the `create` method to start a transactional workflow
 *   // use the options/search field to set record data
 *   async create(input: Record<string, string>): Promise<void> {
 *     await meshData.exec<string>({
 *       entity: this.getEntity(),
 *       args: [{ ...input }],
 *       options: {
 *         taskQueue: this.getTaskQueue(),
 *         search: { data: { ...input }},
 *        },
 *     });
 *   }
 * }
 * ```
 *
 * ### Defining the Schema
 *
 * The schema defines the data model for your entity and is used for indexing and searching within the mesh network.
 * Each field in the schema specifies the data type, whether it's required, and other indexing options.
 *
 * Here's an example of a schema (`schema.ts`):
 *
 * ```typescript
 * import { Types } from '@hotmeshio/hotmesh';
 *
 * export const schema: Types.WorkflowSearchSchema = {
 *   /**
 *    * Unique identifier for the widget, including the entity prefix.
 *    *\/
 *   id: {
 *     type: 'TAG',
 *     primitive: 'string',
 *     required: true,
 *     examples: ['widget-H56789'],
 *   },
 *   /**
 *    * Description of the widget.
 *    *\/
 *   widget: {
 *     type: 'TEXT',
 *     primitive: 'string',
 *     required: true,
 *     examples: ['Bake a cake.'],
 *   },
 *   /**
 *    * Field indicating whether the widget is active ('y') or pruned ('n').
 *    *\/
 *   active: {
 *     type: 'TAG',
 *     primitive: 'string',
 *     required: true,
 *     examples: ['y', 'n'],
 *   },
 *   // ... other fields as needed
 * };
 * ```
 *
 * In your entity class (`Widget`), you use this schema in the `getSearchOptions` method to define how your entity's data
 * is indexed and searched within the mesh network.
 */
abstract class MeshOS {
  meshData: MeshData;
  connected = false;
  namespace: string;
  namespaceType: string;

  // Static properties
  static databases: Record<string, any> = {};
  static namespaces: Types.Namespaces = {};
  static entities: Record<string, any> = {};
  static schemas: Record<string, Types.WorkflowSearchSchema> = {};
  static profiles: Types.Profiles = {};
  static classes: Record<string, typeof MeshOS> = {};

  constructor(
    namespace: string,
    namespaceType: string,
    config: Types.DBConfig,
  ) {
    this.namespace = namespace; // e.g., 's'
    this.namespaceType = namespaceType; // e.g., 'fuzzy' (friendly name in case namespace is abbreviated)
    this.meshData = this.initializeMeshData(config);
  }

  // Abstract methods to be implemented by child classes
  protected abstract getEntity(): string;
  abstract getSearchOptions(): Types.WorkflowSearchOptions;
  protected abstract getTaskQueue(): string;

  /**
   * Initialize MeshData instance
   */
  private initializeMeshData(dbConfig: Types.DBConfig): MeshData {
    return new MeshData(
      Redis,
      this.getRedisUrl(dbConfig),
      this.getSearchOptions(),
    );
  }

  /**
   * Default target function
   */
  protected async defaultTargetFn(): Promise<string> {
    return 'OK';
  }

  /**
   * Get namespace
   */
  getNamespace(): string {
    return this.namespace;
  }

  getRedisUrl = (config: Types.DBConfig) => {
    return {
      url: `redis${config.REDIS_USE_TLS ? 's' : ''}://${config.REDIS_USERNAME ?? ''}:${config.REDIS_PASSWORD}@${config.REDIS_HOST}:${config.REDIS_PORT}`,
    };
  };

  /**
   * Connect to the database
   */
  async connect(): Promise<void> {
    this.connected = await this.meshData.connect({
      entity: this.getEntity(),
      target: this.defaultTargetFn,
      options: {
        namespace: this.getNamespace(),
        taskQueue: this.getTaskQueue(),
      },
    });
  }

  /**
   * Create the search index
   */
  async index(): Promise<void> {
    await this.meshData.createSearchIndex(
      this.getEntity(),
      { namespace: this.getNamespace() },
      this.getSearchOptions(),
    );
  }

  // On-container shutdown commands

  /**
   * Shutdown the connection
   */
  async shutdown(): Promise<void> {
    await MeshData.shutdown();
  }

  /**
   * Get index name
   */
  getIndexName(): string {
    return this.getSearchOptions().index;
  }
/**
   * Create entity
   * NOTE: subclasses can override this method with any signature.
   */
  async create(...args: any[]): Promise<any> {
    const body = args[0] || {};
    return this._create(body);
  }

  /**
   * @private
   */
  async _create(body: Record<string, any>): Promise<Types.StringStringType> {
    const id = body.id || guid();
    await this.meshData.set(this.getEntity(), id, {
      search: { data: body },
      namespace: this.getNamespace(),
    });
    return this.retrieve(id);
  }

  /**
   * Retrieve Entity
   */
  async retrieve(id: string, sparse = false): Promise<Types.StringStringType> {
    const opts = this.getSearchOptions();
    const fields = sparse ? ['id'] : Object.keys(opts?.schema || {});

    const result = await this.meshData.get(this.getEntity(), id, {
      fields,
      namespace: this.getNamespace(),
    });
    if (!result?.id) throw new Error(`${this.getEntity()} not found`);
    return result;
  }

  /**
   * Update entity
   */
  async update(
    id: string,
    body: Record<string, any>,
  ): Promise<Types.StringStringType> {
    await this.retrieve(id);
    await this.meshData.set(this.getEntity(), id, {
      search: { data: body },
      namespace: this.getNamespace(),
    });
    return this.retrieve(id);
  }

  /**
   * Delete entity
   */
  async delete(id: string): Promise<boolean> {
    await this.retrieve(id);
    await this.meshData.flush(this.getEntity(), id, this.getNamespace());
    return true;
  }

  /**
   * Find matching entities
   */
  async find(
    query: {
      field: string;
      is: '=' | '[]' | '>=' | '<=';
      value: string;
    }[] = [],
    start = 0,
    size = 100,
  ): Promise<{ count: number; query: string; data: Types.StringStringType[] }> {
    const opts = this.getSearchOptions();
    return this.meshData.findWhere(this.getEntity(), {
      query,
      return: Object.keys(opts?.schema || {}),
      limit: { start, size },
      options: { namespace: this.getNamespace() },
    }) as Promise<{
      count: number;
      query: string;
      data: Types.StringStringType[];
    }>;
  }

  /**
   * Count matching entities
   */
  async count(
    query: { field: string; is: '=' | '[]' | '>=' | '<='; value: string }[],
  ): Promise<number> {
    return this.meshData.findWhere(this.getEntity(), {
      query,
      count: true,
      options: { namespace: this.getNamespace() },
    }) as Promise<number>;
  }

  /**
   * Aggregate matching entities
   */
  async aggregate(
    filter: {
      field: string;
      is: '=' | '[]' | '>=' | '<=';
      value: string;
    }[] = [],
    apply: { expression: string; as: string }[] = [],
    rows: string[] = [],
    columns: string[] = [],
    reduce: { operation: string; as: string; property?: string }[] = [],
    sort: { field: string; order: 'ASC' | 'DESC' }[] = [],
    start = 0,
    size = 100,
  ): Promise<{ count: number; query: string; data: Types.StringStringType[] }> {
    const command = this.buildAggregateCommand(
      filter,
      apply,
      rows,
      columns,
      reduce,
      sort,
    );

    try {
      const results = await this.meshData.find(
        this.getEntity(),
        {
          index: this.getIndexName(),
          namespace: this.getNamespace(),
          taskQueue: this.getTaskQueue(),
          search: this.getSearchOptions(),
        },
        ...command,
      );

      return {
        count: results[0] as number,
        query: command.join(' '),
        data: arrayToHash(results as [number, ...(string | string[])[]]),
      };
    } catch (e) {
      console.error({ query: command.join(' '), error: e.message });
      throw e;
    }
  }

  /**
   * Build aggregate command
   */
  private buildAggregateCommand(
    filter: { field: string; is: '=' | '[]' | '>=' | '<='; value: string }[],
    apply: { expression: string; as: string }[],
    rows: string[],
    columns: string[],
    reduce: { operation: string; as: string; property?: string }[],
    sort: { field: string; order: 'ASC' | 'DESC' }[],
  ): string[] {
    const command = ['FT.AGGREGATE', this.getIndexName() || 'default'];
    const opts = this.getSearchOptions();

    // Add filter
    command.push(this.buildFilterCommand(filter));

    // Add apply
    apply.forEach((a) => command.push('APPLY', a.expression, 'AS', a.as));

    // Add groupBy
    const groupBy = rows.concat(columns);
    if (groupBy.length > 0) {
      command.push(
        'GROUPBY',
        `${groupBy.length}`,
        ...groupBy.map((g) => opts?.schema?.[g] ? `@_${g}` : `@${g}`),
      );
    }

    // Add reduce
    reduce.forEach((r) => {
      const op = r.operation.toUpperCase();
      if (op === 'COUNT') {
        command.push('REDUCE', op, '0', 'AS', r.as ?? 'count');
      } else if (
        [
          'COUNT_DISTINCT',
          'COUNT_DISTINCTISH',
          'SUM',
          'AVG',
          'MIN',
          'MAX',
          'STDDEV',
          'TOLIST',
        ].includes(op)
      ) {
        const property = r.property
          ? opts?.schema?.[r.property]
            ? `@_${r.property}`
            : `@${r.property}`
          : '';
        command.push(
          'REDUCE',
          op,
          '1',
          property,
          'AS',
          r.as ?? `${r.operation}_${r.property}`,
        );
      }
    });

    // Add sort
    if (sort.length > 0) {
      command.push(
        'SORTBY',
        `${2 * sort.length}`,
        ...sort.flatMap((s) => [
          opts?.schema?.[s.field] ? `@_${s.field}` : `@${s.field}`,
          s.order.toUpperCase() || 'DESC',
        ]),
      );
    }

    return command;
  }

  /**
   * Build filter command
   */
  private buildFilterCommand(
    filter: { field: string; is: '=' | '[]' | '>=' | '<='; value: string }[],
  ): string {
    if (filter.length === 0) return '*';

    const opts = this.getSearchOptions();
    return filter
      .map((q) => {
        const type: 'TAG' | 'NUMERIC' | 'TEXT' =
          opts?.schema?.[q.field]?.type ?? 'TEXT';
        switch (type) {
          case 'TAG':
            return `@_${q.field}:{${q.value}}`;
          case 'TEXT':
            return `@_${q.field}:${q.value}`;
          case 'NUMERIC':
            return `@_${q.field}:[${q.value}]`;
        }
      })
      .join(' ');
  }

  // Static registration methods

  /**
   * Register a database
   */
  static registerDatabase(id: string, config: any): void {
    MeshOS.databases[id] = config;
  }

  /**
   * Register a namespace
   */
  static registerNamespace(id: string, config: any): void {
    MeshOS.namespaces[id] = config;
  }

  /**
   * Register an entity
   */
  static registerEntity(id: string, config: any): void {
    MeshOS.entities[id] = config;
  }

  /**
   * Register a schema
   */
  static registerSchema(id: string, schema: Types.WorkflowSearchSchema): void {
    MeshOS.schemas[id] = schema;
  }

  /**
   * Register a profile
   */
  static registerProfile(id: string, config: any): void {
    MeshOS.profiles[id] = config;
  }

  /**
   * Register a class
   */
  static registerClass(id: string, entityClass: typeof MeshOS): void {
    MeshOS.classes[id] = entityClass;
  }

  /**
   * Initialize profiles
   */
  static async init(p = MeshOS.profiles): Promise<void> {
    for (const key in p) {
      const profile = p[key];
      if (profile.db.config.REDIS_HOST) {
        console.log(`!!Initializing ${profile.db.name} [${key}]...`);
        profile.instances = {};
        for (const ns in profile.namespaces) {
          const namespace = profile.namespaces[ns];
          console.log(`  - ${ns}: ${namespace.label}`);
          let pinstances = profile.instances[ns];
          if (!pinstances) {
            pinstances = {};
            profile.instances[ns] = pinstances;
          }
          for (const entity of namespace.entities) {
            console.log(`    - ${entity.name}: ${entity.label}`);
            const instance = pinstances[entity.name] = new entity.class(
              ns,
              namespace.type,
              profile.db.config,
            );
            await instance.init(profile.db.search);
          }
        }
      }
    }
  }

  /**
   * Find entity instance
   */
  static findEntity(
    database: string,
    namespace: string,
    entity: string,
  ): Types.EntityInstanceTypes | undefined {
    if (
      !database ||
      !MeshOS.profiles[database] ||
      !MeshOS.profiles[database]?.db?.config?.REDIS_HOST
    ) {
      const activeProfiles = Object.keys(MeshOS.profiles).filter(
        (key) => MeshOS.profiles[key]?.db?.config?.REDIS_HOST,
      );
      throw new Error(
        `The database query parameter [${database}] was not found. Use one of: ${activeProfiles.join(', ')}`,
      );
    }

    if (!namespace || !MeshOS.profiles[database]?.instances?.[namespace]) {
      const activeNamespaces = Object.keys(
        MeshOS.profiles[database]?.instances ?? {},
      );
      throw new Error(
        `The namespace query parameter [${namespace}] was not found. Use one of: ${activeNamespaces.join(', ')}`,
      );
    }

    const entities = MeshOS.profiles[database]?.instances?.[namespace] ?? {};
    if (!entity || entity?.startsWith('-') || entity === '*') {
      entity = Object.keys(entities)[0];
    } else if (entity?.endsWith('*')) {
      entity = entity.slice(0, -1);
    }

    const target = MeshOS.profiles[database]?.instances?.[namespace]?.[
      entity
    ] as Types.EntityInstanceTypes | undefined;
    if (!target) {
      console.error(`Entity not found: ${database}.${namespace}.${entity}`);
      entity = Object.keys(entities)[0];
      return MeshOS.profiles[database]?.instances?.[namespace]?.[entity] as
        | Types.EntityInstanceTypes
        | undefined;
    }
    return target;
  }

  /**
   * Find schemas
   */
  static findSchemas(
    database: string,
    ns: string,
  ): Record<string, Types.WorkflowSearchSchema> {
    if (
      !database ||
      !MeshOS.profiles[database] ||
      !MeshOS.profiles[database]?.db?.config?.REDIS_HOST
    ) {
      const activeProfiles = Object.keys(MeshOS.profiles).filter(
        (key) => MeshOS.profiles[key]?.db?.config?.REDIS_HOST,
      );
      throw new Error(
        `The database query parameter [${database}] was not found. Use one of: ${activeProfiles.join(', ')}`,
      );
    }
    const profile = MeshOS.profiles[database];
    const namespacedInstance = profile.instances[ns];
    const schemas: Record<string, Types.WorkflowSearchSchema> = {};
    for (const entityName in namespacedInstance) {
      const entityInstance = namespacedInstance[entityName];
      const opts = entityInstance.getSearchOptions();
      schemas[opts.index ?? entityName] = opts.schema;
    }
    return schemas;
  }

  /**
   * Serialize profiles to JSON
   */
  static toJSON(p: Types.Profiles = MeshOS.profiles): any {
    const result: any = {};
    for (const key in p) {
      const profile = p[key];
      if (!profile.db.config.REDIS_HOST) {
        continue;
      } else {
        result[key] = {
          db: { ...profile.db, config: undefined },
          namespaces: {},
        };
      }
      for (const ns in profile.namespaces) {
        const namespace = profile.namespaces[ns];
        result[key].namespaces[ns] = {
          name: namespace.name,
          label: namespace.label,
          entities: [],
        };
        for (const entity of namespace.entities) {
          result[key].namespaces[ns].entities.push({
            name: entity.name,
            label: entity.label,
            schema: entity.schema,
          });
        }
      }
    }
    return result;
  }

  // Instance method to initialize
  async init(search = true): Promise<void> {
    await this.connect();
    if (search) {
      await this.index();
    }
  }

  workflow = {};
}

export { MeshOS };
