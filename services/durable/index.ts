import { ClientService } from './client';
import { ConnectionService } from './connection';
import { RedisOSService } from './redisos';
import { WorkerService } from './worker';
import { WorkflowService } from './workflow';
import { ContextType } from '../../types/durable';

export const Durable = {
  Client: ClientService,
  Connection: ConnectionService,
  RedisOS: RedisOSService,
  Worker: WorkerService,
  workflow: WorkflowService,
};

export type { ContextType };
