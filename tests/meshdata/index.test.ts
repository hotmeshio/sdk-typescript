import * as Redis from 'redis';

import config from '../$setup/config';
import { HotMesh } from '../../services/hotmesh';
import { MeshData } from '../../services/meshdata';
import { RedisConnection } from '../../services/connector/clients/redis';
import * as HotMeshTypes from '../../types';

import * as activities from './activities';

describe('MeshData`', () => {
  const options = {
    socket: {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      tls: false,
    },
    password: config.REDIS_PASSWORD,
    database: config.REDIS_DATABASE,
  };
  //redis connection options

  //configure pluck instance will full set of options
  //include redis instance and model/schema for use in search
  const pluck = new MeshData(Redis, options, {
    schema: {
      email: { type: 'TEXT', sortable: true },
      newsletter: { type: 'TAG', sortable: true },
    },
    index: 'greeting',
    prefix: ['greeting'],
  } as HotMeshTypes.WorkflowSearchOptions);

  //wrap expensive/idempotent functions with a proxy
  const { sendNewsLetter } = MeshData.proxyActivities<typeof activities>({
    activities,
    retryPolicy: {
      backoffCoefficient: 1,
      maximumInterval: '1 second',
      maximumAttempts: 3,
    },
  });

  const entityName = 'greeting';
  const idemKey = HotMesh.guid();
  let shouldThrowError = false;
  let errorCount = 0;
  let callCount = 0;
  const reason = 'I am tired of newsletters';

  const howdy = async (depth = 0): Promise<HotMeshTypes.WorkflowContext> => {
    //descend (recursion)
    if (depth > 0) {
      //use promise (collation)
      await Promise.all([
        //exec a child (self)
        MeshData.workflow.execChild<HotMeshTypes.WorkflowContext>({
          entity: 'howdy', //standard pluck syntax (just provide entity)
          args: [depth - 1],
        }),
        //exec a child (self)
        MeshData.workflow.execChild<HotMeshTypes.WorkflowContext>({
          entity: 'howdy',
          workflowId: `EC${HotMesh.guid()}`, //optional: unique id
          args: [depth - 1],
        }),
        //exec a child (self)
        MeshData.workflow.execChild<HotMeshTypes.WorkflowContext>({
          workflowName: 'howdy', //must specify workflowName if 'entity' not provided
          taskQueue: 'howdy', //must specify taskQueue if 'entity' not provided
          args: [depth - 1],
        }),
        //start a child (self)
        MeshData.workflow.startChild({
          workflowName: 'howdy',
          taskQueue: 'howdy',
          workflowId: `SC${HotMesh.guid()}`, //optional: unique id
          args: [depth - 1],
        }),
      ]);
    }
    return MeshData.workflow.getContext();
  };

  const greet = async (
    email: string,
    user: { first: string; last: string },
  ): Promise<string> => {
    callCount++;
    //simulate errors in the user's function (handle gracefully)
    if (shouldThrowError) {
      errorCount++;
      if (errorCount == 1) {
        shouldThrowError = false;
      }
      throw new Error('Error!');
    }

    //set some shared state using 'search'
    const search = await MeshData.workflow.search();
    await search?.set('email', email, 'newsletter', 'yes');

    //`sendNewsletter` is a proxy function and will only run once
    //prove by calling three times (but using the cached instance for the last 2)
    for (let i = 1; i < 4; i++) {
      const cachedI = await sendNewsLetter(email, i);
    }

    //spawn the `sendRecurringNewsLetter` hook (a parallel subroutine)
    if (email === 'floe.doe@pluck.com') {
      const msgId = await MeshData.workflow.hook({
        args: [],
        workflowName: 'subscribe',
        taskQueue: 'subscribe',
      });
    }
    return `Hello, ${user.first} ${user.last}. Your email is [${email}].`;
  };

  const localGreet = async (
    email: string,
    user: { first: string; last: string },
  ): Promise<string> => {
    return `Hello, ${user.first} ${user.last}. Your email is [${email}].`;
  };

  const sleeper = async (email: string) => {
    for (let i = 1; i < 4; i++) {
      const cachedI = await sendNewsLetter(email, i);
      await MeshData.workflow.sleepFor('1 second');
    }
  };

  //once connected by pluck, this function will become a 'hook'
  //hook functions are reentrant processes and use the job
  //state its bound to when initialized.
  const sendRecurringNewsLetter = async () => {
    //access shared state using the 'search' object
    const search = await MeshData.workflow.search();
    let email: string;
    let shouldProceed: boolean;
    do {
      email = await search.get('email');

      //the 'sendNewsLetter' function is a `proxy` and will only run once
      await sendNewsLetter(email);

      //set `newsletter` pref to 'no', to stop cycling while testing
      await search.set('newsletter', 'no');
      shouldProceed = await search.get('newsletter') === 'yes';
    } while (shouldProceed);
  };

  //another hook function to unsubscribe from the newsletter
  const unsubscribeFromNewsLetter = async (reason: string) => {
    const search = await MeshData.workflow.search();
    const email = await search.get('email');
    await search.set('newsletter', 'no', 'reason', reason, 'email', email);
  };

  beforeAll(async () => {
    //init Redis and flush db
    const redisConnection = await RedisConnection.connect(
      HotMesh.guid(),
      Redis as unknown as HotMeshTypes.RedisRedisClassType,
      options,
    );
    redisConnection.getClient().flushDb();
  }, 5_000);

  afterAll(async () => {
    //wait for cleanup (various asyn processes should be allowed to complete)
    //todo: verify that memory space is empty
    await new Promise((resolve) => setTimeout(resolve, 25_000));
    //shutdown all connections
    await MeshData.shutdown();
  }, 30_000);

  describe('connect', () => {
    it('should connect a function and auto-deploy HotMesh to Redis', async () => {
      //should never be more than 5 published events matching 'greeting' entity
      const max = 5;
      let counter = 0;
      await pluck.mesh.sub(async (topic, message) => {
        if (message.entity === entityName) {
          counter++;
        }
        expect(counter).toBeLessThanOrEqual(max);
      });

      const worker = await pluck.connect<Promise<string>>({
        entity: entityName,
        target: greet,
      });
      expect(worker).toBeDefined();

      const sleeperWorker = await pluck.connect({
        entity: 'sleeper',
        target: sleeper,
      });
      expect(sleeperWorker).toBeDefined();
    }, 10_000);

    it('should connect a function and isolate the namespace', async () => {
      const worker = await pluck.connect<Promise<HotMeshTypes.WorkflowContext>>(
        {
          entity: 'howdy',
          target: howdy,
          options: { namespace: 'staging' },
        },
      );
      expect(worker).toBeDefined();
    });

    it('should connect a hook function', async () => {
      const worker = await pluck.connect({
        entity: 'subscribe',
        target: sendRecurringNewsLetter,
      });
      expect(worker).toBeDefined();
    });

    it('should connect another hook function', async () => {
      const worker = await pluck.connect({
        entity: 'unsubscribe',
        target: unsubscribeFromNewsLetter,
      });
      expect(worker).toBeDefined();
    });
  });

  describe('HotMesh Instance', () => {
    it('should return a HotMesh Instance', async () => {
      const instance = await pluck.getHotMesh();
      expect(instance.engine).toBeDefined();
      expect(instance.engine?.store).toBeDefined();
      expect(instance.engine?.store?.redisClient).toBeDefined();
    });
  });

  describe('exec', () => {
    it('should exec a recursive workflow, await, export and flush a durable function at a custom namespace', async () => {
      const context = await pluck.exec<HotMeshTypes.WorkflowContext>({
        entity: 'howdy',
        args: [2],
        options: {
          namespace: 'staging',
          id: 'jimbo123',
          ttl: 'infinity',
        },
      });
      //4 child workflows (20 total workflows as 16 will be nested)
      expect(context.counter).toEqual(4);
      expect(context.namespace).toEqual('staging');
      expect(context.workflowId).toBeDefined();
      expect(context.workflowDimension).toEqual(''); //main context, no dimension
      expect(context.workflowTopic).toEqual('howdy-howdy');

      const exported = await pluck.export(
        'howdy',
        'jimbo123',
        { block: ['transitions'], values: false },
        'staging',
      );
      expect(exported.transitions).toBeUndefined();
      expect(exported.timeline).toBeDefined();

      await pluck.flush('howdy', 'jimbo123', 'staging');

      try {
        let job: HotMeshTypes.JobOutput | undefined;
        do {
          await new Promise((resolve) => setTimeout(resolve, 250));
          job = await pluck.info('howdy', 'jimbo123', { namespace: 'staging' });
        } while (true);
      } catch (error) {
        expect(error.message).toBe(`howdy-jimbo123 Not Found`);
      }
    }, 25_000);

    it('should start a function (and not await)', async () => {
      const email = 'jan@pluck.com';
      const name = { first: 'Jan', last: 'Doe' };
      const entity = 'greeting';
      const id = 'jan';

      //broker using MeshData (Redis will govern the exchange)
      const jobId = await pluck.exec<string>({
        entity,
        args: [email, name],
        options: {
          id,
          await: false,
          signalIn: false,
        },
      });

      expect(jobId).toEqual(`${entity}-${id}`);
    });

    it('should exec a function and await the result', async () => {
      const email = 'jdoe@pluck.com';
      const name = { first: 'John', last: 'Doe' };

      //broker using MeshData (Redis will govern the exchange)
      const brokered = await pluck.exec<Promise<string>>({
        entity: 'greeting',
        args: [email, name],
        options: {
          search: {
            data: {
              fred: 'flintstone',
              barney: 'rubble',
            },
          },
          id: 'jdoe',
          ttl: '60 seconds',
          signalIn: false,
        },
      });

      //call directly (NodeJS will govern the exchange)
      const direct = await localGreet(email, name);
      expect(brokered).toEqual(direct);
    }, 10_000);

    it('should return RAW fields (HGETALL)', async () => {
      const email = 'jdoe@pluck.com';
      const name = { first: 'John', last: 'Doe' };
      const direct = await localGreet(email, name);
      const raw = await pluck.raw('greeting', 'jdoe');
      expect(raw._fred).toEqual('flintstone');
      expect(raw.aBa).toEqual(direct);
    });

    it('should only run proxy functions one time', async () => {
      await pluck.exec<void>({
        entity: 'sleeper',
        args: ['sleeper@pluck.com'],
        options: {
          signalIn: false,
        },
      });
    }, 20_000);

    it('should return ALL `state` fields', async () => {
      const all = await pluck.all('greeting', 'jdoe');
      expect(all.fred).toEqual('flintstone');
      expect(all.aBa).toBeUndefined();
    });

    it('should GET named `state` fields', async () => {
      const some = await pluck.get('greeting', 'jdoe', {
        fields: ['fred', 'newsletter'],
      });
      expect(some.fred).toEqual('flintstone');
      expect(some.newsletter).toEqual('yes');
      expect(some.barney).toBeUndefined();
    });

    it('should SET named `state` fields', async () => {
      const numAdded = await pluck.set('greeting', 'jdoe', {
        //set 2 new fields and overwrite 1 existing field
        search: {
          data: {
            wilma: 'flintstone',
            bce: '-1000000',
            email: 'wstone@pluck.com',
          },
        },
      });
      expect(numAdded).toEqual(2);
    });

    it('should INCR named `state` field', async () => {
      const newAmount = await pluck.incr('greeting', 'jdoe', 'bce', 1);
      expect(newAmount).toEqual(-999999);
    });

    it('should DEL named `state` fields', async () => {
      const numDeleted = await pluck.del('greeting', 'jdoe', {
        //delete 2 fields (and ignore 1 non-existent field: emails)
        fields: ['wilma', 'bce', 'emails'],
      });
      expect(numDeleted).toEqual(2);
    });

    it('should exec a long-running function that calls a proxy', async () => {
      const email = 'fdoe@pluck.com';
      const name = { first: 'Fred', last: 'Doe' };

      //call with MeshData (Redis will govern the exchange)
      const brokered = await pluck.exec<Promise<string>>({
        entity: 'greeting',
        args: [email, name],
        options: { ttl: '1 second', id: 'abc123' },
      });

      //call directly (NodeJS will govern the exchange)
      const direct = await localGreet(email, name);
      expect(brokered).toEqual(direct);
    }, 10_000);

    it('should exec a durable function (ttl:infinity) that calls a proxy and hook', async () => {
      const email = 'floe.doe@pluck.com';
      const name = { first: 'Floe', last: 'Doe' };

      //call with MeshData (Redis will govern the exchange)
      const brokered = await pluck.exec<Promise<string>>({
        entity: 'greeting',
        args: [email, name],
        options: { ttl: 'infinity', id: 'abc456' },
      });

      //call directly (NodeJS will govern the exchange)
      const direct = await localGreet(email, name);
      expect(brokered).toEqual(direct);
    }, 10_000);

    it('should flush a durable function (ttl:infinity)', async () => {
      //flush causes the main thread to exit (it waits for the flush signal)
      await pluck.flush('greeting', 'abc456');
      //sleep long enough for running hooks in the test to awaken from sleep
      await new Promise((resolve) => setTimeout(resolve, 1_000));
      let pluckResponse: HotMeshTypes.JobOutput;
      try {
        pluckResponse = await pluck.info('greeting', 'abc456');
      } catch (error) {
        expect(error.message).toBe(`greeting-abc456 Not Found`);
        return;
      }
      expect(pluckResponse.data.done).toEqual(true);
    }, 15_000);

    it('should retry if it fails', async () => {
      const email = 'jim.doe@pluck.com';
      const name = { first: 'Jim', last: 'Doe' };

      //call with MeshData (Redis will govern the exchange)
      //a) pass an id to make sure this test starts fresh
      //b) redis will retry until `showThrowError` switches to `false`
      //c) the 'greet' function will set shouldThrowError to false after 2 runs
      shouldThrowError = true;
      const brokered = await pluck.exec<Promise<string>>({
        entity: 'greeting',
        args: [email, name],
        options: {
          id: idemKey,
          ttl: '60 seconds',
          //configure the retry policy to run quickly during testing
          config: {
            backoffCoefficient: 1, //value of throttle is: `backoffCoefficient^retryCount` (first retry is 1 second)
            maximumInterval: '1 second',
            maximumAttempts: 3,
          },
        },
      });
      expect(errorCount).toEqual(1);
      expect(shouldThrowError).toBeFalsy();

      //call directly (NodeJS will now govern the exchange)
      const direct = await localGreet(email, name);

      expect(brokered).toEqual(direct);
    }, 15_000);
  });

  describe('info', () => {
    it('should return the full function profile', async () => {
      const pluckResponse = await pluck.info('greeting', idemKey);
      expect(pluckResponse.data.done).toEqual(true);
    });
  });

  describe('rollCall', () => {
    it('should rollCall multiple namespaces', async () => {
      const pluckNamespaceResponse = await pluck.rollCall({
        namespace: 'staging',
        delay: 2500,
      });
      expect(pluckNamespaceResponse.length).toBeGreaterThan(0);
    }, 10_000);
  });

  describe('hook', () => {
    it('should call the `unsubscribe` hook function', async () => {
      let pluckData: any;
      do {
        await new Promise((resolve) => setTimeout(resolve, 500));
        pluckData = await pluck.all('greeting', idemKey);
      } while (pluckData.newsletter !== 'yes');
      expect(pluckData.newsletter).toEqual('yes');
      //hooks only return an id (this is the `guarantee` the hook will complete)
      const hookId = await pluck.hook({
        entity: 'greeting',
        id: idemKey,
        hookEntity: 'unsubscribe',
        hookArgs: [reason],
      });
      expect(hookId).toBeDefined();
      //hooks are async; sleep to allow the hook to run
      await new Promise((resolve) => setTimeout(resolve, 1_000));
      await pluck.info('greeting', idemKey);

      //by now the data should have been updated to 'no'
      pluckData = await pluck.all('greeting', idemKey);
      expect(pluckData.newsletter).toEqual('no');
      expect(pluckData.reason).toEqual(reason);
    }, 10_000);
  });

  describe('export', () => {
    it('should export the job timelines, actions, and dependencies', async () => {
      let exported = await pluck.export('greeting', idemKey);
      expect(exported.state?.data.response).toEqual(
        'Hello, Jim Doe. Your email is [jim.doe@pluck.com].',
      );
      exported = await pluck.export('greeting', idemKey, {
        block: ['transitions'],
        values: false,
      });
      expect(exported.transitions).toBeUndefined();
      expect(exported.timeline).toBeDefined();
    });
  });

  describe('findJobs', () => {
    it('should find matching jobs using a wildcard (SCAN)', async () => {
      const [cursor, jobs] = await pluck.findJobs({
        match: 'greeting*',
      });
      expect(cursor).toBe('0');
      //2 jobs with 'greeting' prefix (these are greeting entities)
      //this allows jobs to be cursored/paginated when FT not installed
      // TODO: Temporary solution to flaky test, needs further review
      // other greeting jobs may exist (e.g. greeting-abc123), but we expect at least 2
      // one path is register another function with a unique entity to filter by
      expect(jobs.length).toBeGreaterThanOrEqual(2);
      expect(jobs[0].startsWith('hmsh:durable:j:greeting-')).toBeTruthy();
    });

    it('should find all jobs without a wildcard', async () => {
      const [_cursor, jobs] = await pluck.findJobs();
      expect(jobs.length).toBeGreaterThan(5);
    });
  });

  describe('search', () => {
    it('should create a search index', async () => {
      await pluck.createSearchIndex('greeting', undefined, {
        schema: {
          email: { type: 'TEXT', sortable: true },
          newsletter: { type: 'TAG', sortable: true },
        },
        index: 'greeting',
        prefix: ['greeting'],
      });
    });

    it('should list search indexes', async () => {
      const indexes = await pluck.listSearchIndexes();
      expect(indexes.length).toBeGreaterThan(0);
    });

    it('should conditionally search and limit response fields', async () => {
      const indexedResults = (await pluck.findWhere('greeting', {
        query: [{ field: 'newsletter', is: '=', value: 'no' }],
        return: ['email', 'newsletter', 'reason'],
      })) as { count: number; data: HotMeshTypes.StringStringType[] };
      //most recent result includes a reason
      //console.log('Indexed Search Results >', indexedResults);
      expect(indexedResults.data.length).toBeGreaterThan(0);
      expect(
        indexedResults.data[indexedResults.data.length - 1].newsletter,
      ).toEqual('no');
      expect(
        indexedResults.data[indexedResults.data.length - 1].reason,
      ).toEqual(reason);
    });

    it('should conditionally search and paginate responses', async () => {
      const indexedResults = (await pluck.findWhere('greeting', {
        query: [{ field: 'newsletter', is: '=', value: 'no' }],
        return: ['email', 'newsletter', 'reason'],
        limit: { start: 0, size: 1 }, // 0-based index (get first result)
      })) as { count: number; data: HotMeshTypes.StringStringType[] };
      //most recent result includes a reason
      expect(indexedResults.data.length).toBeGreaterThanOrEqual(1); //`max count` is 1 less than `return count`
      expect(
        indexedResults.data[indexedResults.data.length - 1].newsletter,
      ).toEqual('no');
    });

    it('should conditionally count records', async () => {
      const count = (await pluck.findWhere('greeting', {
        query: [{ field: 'newsletter', is: '=', value: 'no' }],
        count: true,
      })) as number;
      expect(count).toBeGreaterThan(0);
    });
  });
});
