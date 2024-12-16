import { MeshOS } from '../services/meshos';

import { WorkflowSearchSchema } from './meshflow';
import { ProviderConfig, ProvidersConfig } from './provider';

export type DB = {
  name: string;
  label: string;
  search: boolean;
  connection: ProviderConfig | ProvidersConfig;
};

export type SubClassInstance<T extends typeof MeshOS> = T extends abstract new (
  ...args: any
) => infer R
  ? R
  : never;
export type AllSubclassInstances = SubClassInstance<
  (typeof MeshOS)['classes'][keyof (typeof MeshOS)['classes']]
>;

//export type EntityClassTypes = typeof SubclassType;
export type EntityInstanceTypes = MeshOS;

export type SubclassType<T extends MeshOS = MeshOS> = new (...args: any[]) => T;
export type Entity = {
  name: string; //can be set via static config
  label: string;
  /**
   * A more-specific value for workers when targeting version-specifc
   * or priority-specific task queues.
   * @default default
   */
  taskQueue?: string; //can be set via static config
  schema: WorkflowSearchSchema; //can be st via static config
  class: SubclassType;
};

export type Namespace = {
  name: string;
  /**
   * @deprecated; unused; name is the type; label is human-readable
   */
  type: string;
  label: string;
  module: 'hotmesh' | 'meshcall' | 'meshflow' | 'meshdata' | 'meshos';
  entities: Entity[];
};

export type Namespaces = {
  [key: string]: Namespace;
};

export type Instance = {
  [key /*entity name*/ : string]: EntityInstanceTypes;
};

export type Instances = {
  [key /*namespace abbreviation*/ : string]: Instance;
};

export type Profile = {
  db: DB;
  namespaces: Namespaces;
  instances?: Instances;
};

export type Profiles = {
  [key: string]: Profile;
};
