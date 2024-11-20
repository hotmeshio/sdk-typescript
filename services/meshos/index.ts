import { MeshData } from '../meshdata/index';
import { arrayToHash, guid } from '../../modules/utils';
import * as Types from '../../types';
import { LoggerService } from '../logger';
import { ProvidersConfig } from '../../types/provider';

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
 * - `getTaskQueue()`: Returns the task queue (use for targeted priority and or version-based routing)
 * - `getEntity()`: Returns the name of the entity.
 * - `getSearchOptions()`: Returns indexing and schema options for the entity.
 *
 * Standard CRUD methods are included and use your provided schema to
 * fields to return in the response: create, retrieve, update, delete.
 *
 * Search methods are included and use your provided schema to
 * fields to return in the response: count, find, aggregate.
 *
 * Implement other methods as needed for the entity's
 * functionality; For example, subclass/override methods like `create`
 * to also spawn a transactional workflow
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
 *   //Return the function version/priority
 *   getTaskQueue(): string {
 *     return 'v1';
 *   }
 *
 *   // Return the entity name
 *   getEntity(): string {
 *     return 'widget';
 *   }
 *
 *   // Return the schema definition, target hash prefixes, and index ID
 *   getSearchOptions(): Types.WorkflowSearchOptions {
 *     return {
 *       index: `${this.getNamespace()}-${this.getEntity()}`,
 *       prefix: [this.getEntity()],
 *       schema,
 *     };
 *   }
 *
 *   //Subclass the `connect` method to connect workers and
 *   // hooks (optional) when the container starts
 *   async connect() {
 *     await this.meshData.connect({
 *       entity: this.getEntity(),
 *       //the `target widget workflow` runs as a transaction
 *       target: function() {
 *         return { hello: 'world' };
 *       },
 *       options: {
 *         namespace: this.getNamespace(),
 *         taskQueue: this.getTaskQueue(),
 *       },
 *     });
 *   }
 *
 *   // subclass the `create` method to start a transactional
 *   // workflow; use the options/search field to set default
 *   // record data `{ ...input}` and invoke the `target widget workflow`
 *   async create(input: Types.StringAnyType): Promise<Types.StringStringType> {
 *     return await this.meshData.exec<Types.StringStringType>({
 *       entity: this.getEntity(),
 *       args: [{ ...input }],
 *       options: {
 *         id: input.id,
 *         ttl: '6 months',
 *         taskQueue: this.getTaskQueue(),
 *         namespace: this.getNamespace(),
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
 *     examples: ['H56789'],
 *   },
 *   /**
 *    * entity type
 *    *\/
 *   $entity: {
 *     type: 'TAG',
 *     primitive: 'string',
 *     required: true,
 *     examples: ['widget'],
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
 * In your entity class (`Widget`), you use this schema in the
 * `getSearchOptions` method to define how your entity's data
 * is indexed and searched.
 */
abstract class MeshOS {
  meshData: MeshData;
  connected = false;
  namespace: string;
  namespaceType: string;

  // Static properties
  static databases: Record<string, Types.DB> = {};
  static namespaces: Types.Namespaces = {};
  static entities: Record<string, Types.Entity> = {};
  static schemas: Record<string, Types.WorkflowSearchSchema> = {};
  static profiles: Types.Profiles = {};
  static classes: Record<string, typeof MeshOS> = {};
  static logger: Types.ILogger = new LoggerService('hotmesh', 'meshos');

  constructor(
    providerClass: Types.ProviderClass,
    namespace: string,
    namespaceType: string,
    config: Types.ProviderConfig,
    connections: ProvidersConfig,
  ) {
    this.namespace = namespace; // e.g., 's'
    this.namespaceType = namespaceType; // e.g., 'fuzzy' (friendly name in case namespace is abbreviated)
    this.meshData = this.initializeMeshData(providerClass, config, connections);
  }

  // Abstract methods to be implemented by child classes
  protected abstract getEntity(): string;
  abstract getSearchOptions(): Types.WorkflowSearchOptions;
  protected abstract getTaskQueue(): string;

  /**
   * Initialize MeshData instance (this backs/supports the class
   * --the true provider of functionality)
   */
  private initializeMeshData(
    providerClass: Types.ProviderClass,
    providerConfig: Types.ProviderConfig,
    connections: ProvidersConfig,
  ): MeshData {
    if (connections) {
      return new MeshData(providerClass, providerConfig, this.getSearchOptions(), connections);
    }
    return new MeshData(providerClass, providerConfig, this.getSearchOptions());
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
   * Shutdown all connections
   */
  static async shutdown(): Promise<void> {
    await MeshData.shutdown();
  }

  /**
   * Get index name
   */
  getIndexName(): string {
    return this.getSearchOptions().index;
  }

  /**
   * Create the data record
   * NOTE: subclasses should override this method (or create
   * an alternate method) for invoking a workflow when
   * creating the record.
   */
  async create(body: Record<string, any>): Promise<Types.StringStringType> {
    const id = body.id || guid();
    await this.meshData.set(this.getEntity(), id, {
      search: { data: body },
      namespace: this.getNamespace(),
    });
    return this.retrieve(id);
  }

  /**
   * Retrieve the record data
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
   * Update the record data
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
   * Delete the record/workflow
   */
  async delete(id: string): Promise<boolean> {
    await this.retrieve(id);
    await this.meshData.flush(this.getEntity(), id, this.getNamespace());
    return true;
  }

  /**
   * Find matching records
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

  /**
   * Instance initializer
   */
  async init(search = true): Promise<void> {
    await this.connect();
    if (search) {
      await this.index();
    }
  }

  // Static registration methods

  /**
   * Register a database
   */
  static registerDatabase(id: string, config: Types.DB): void {
    MeshOS.databases[id] = config;
  }

  /**
   * Register a namespace
   */
  static registerNamespace(id: string, config: Types.Namespace): void {
    MeshOS.namespaces[id] = config;
  }

  /**
   * Register an entity
   */
  static registerEntity(id: string, config: Types.Entity): void {
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
  static registerProfile(id: string, config: Types.Profile): void {
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
      if (profile.db?.connection?.options) {
        this.logger.info(`meshos-initializing`, {
          db: profile.db.name,
          key,
        });
        profile.instances = {};
        for (const ns in profile.namespaces) {
          const namespace = profile.namespaces[ns];
          this.logger.info(`meshos-initializing-namespace`, {
            namespace: ns,
            label: namespace.label,
          });

          let pinstances = profile.instances[ns];
          if (!pinstances) {
            pinstances = {};
            profile.instances[ns] = pinstances;
          }
          for (const entity of namespace.entities) {
            this.logger.info(`meshos-initializing-entity`, {
              entity: entity.name,
              label: entity.label,
            });

            const instance = pinstances[entity.name] = new entity.class(
              profile.db.connection.class,
              ns,
              namespace.type,
              profile.db.connection.options,
              profile.db.connections,
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
    if (!database || !MeshOS.profiles[database]) {
      const activeProfiles = Object.keys(MeshOS.profiles).filter(
        (key) => MeshOS.profiles[key]?.db?.connection,
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
      this.logger.error(`meshos-entity-not-found`, {
        database,
        namespace,
        entity,
      });

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
    if (!database || !MeshOS.profiles[database]) {
      const activeProfiles = Object.keys(MeshOS.profiles).filter(
        (key) => MeshOS.profiles[key]?.db?.connection,
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
      if (!profile.db.connection) {
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

  workflow = {};
}

export { MeshOS };
