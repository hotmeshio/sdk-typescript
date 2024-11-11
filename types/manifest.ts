import { MeshOS } from '../services/meshos';

import * as Types from './index';

export type DB = {
  name: string;
  label: string;
  search: boolean;
  connection: Types.ProviderConfig;
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
  name: string;
  label: string;
  schema: Types.WorkflowSearchSchema;
  class: SubclassType;
};

export type Namespace = {
  name: string;
  type: string;
  label: string;
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
