import { HMNS } from '../../../modules/key';
import { IORedisStoreService as IORedisStore } from '../../../services/store/providers/redis/ioredis';
import { IORedisStreamService as IORedisStream } from '../../../services/stream/providers/redis/ioredis';
import { CompilerService } from '../../../services/compiler';
import { LoggerService } from '../../../services/logger';
import { RedisConnection, RedisClientType } from '../../$setup/cache/ioredis';

describe('FUNCTIONAL | Compile', () => {
  const appConfig = { id: 'test-app', version: '1' };
  const CONNECTION_KEY = 'manual-test-connection';

  //use when testing static YAML as input
  const APP_YAML = `app:
  id: test-app
  version: '1'
  graphs:
    - subscribes: abc.test
      activities:
        tx:
          type: trigger
        ax:
          type: hook
      transitions:
        tx:
          - to: ax
`;

  let redisConnection: RedisConnection;
  let redisClient: RedisClientType;
  let redisStore: IORedisStore;
  let redisStream: IORedisStream;

  beforeAll(async () => {
    redisConnection = await RedisConnection.getConnection(CONNECTION_KEY);
    redisClient = await redisConnection.getClient();
    redisClient.flushdb();
    redisStore = new IORedisStore(redisClient);
    await redisStore.init(HMNS, appConfig.id, new LoggerService());
    redisStream = new IORedisStream(redisClient, redisClient);
    await redisStream.init(HMNS, appConfig.id, new LoggerService());
  });

  afterAll(async () => {
    await RedisConnection.disconnectAll();
  });

  describe('plan()', () => {
    it('should plan an app deployment, using a path', async () => {
      const compilerService = new CompilerService(
        redisStore,
        redisStream,
        new LoggerService(),
      );
      await compilerService.plan('/app/tests/$setup/seeds/hotmesh.yaml');
    });
  });

  describe('deploy()', () => {
    it('should deploy an app to Redis, using a path', async () => {
      const compilerService = new CompilerService(
        redisStore,
        redisStream,
        new LoggerService(),
      );
      await compilerService.deploy('/app/tests/$setup/seeds/hotmesh.yaml');
    });
  });

  describe('activate()', () => {
    it('should activate a deployed app version', async () => {
      const compilerService = new CompilerService(
        redisStore,
        redisStream,
        new LoggerService(),
      );
      await compilerService.activate('test-app', '1');
    });
  });

  describe('plan()', () => {
    it('should plan an app deployment, using a model', async () => {
      const compilerService = new CompilerService(
        redisStore,
        redisStream,
        new LoggerService(),
      );
      await compilerService.plan(APP_YAML);
    });
  });

  describe('deploy()', () => {
    it('should deploy an app to Redis, using a model', async () => {
      const compilerService = new CompilerService(
        redisStore,
        redisStream,
        new LoggerService(),
      );
      await compilerService.deploy(APP_YAML);
    });
  });

  describe('activate()', () => {
    it('should activate a deployed app model', async () => {
      const compilerService = new CompilerService(
        redisStore,
        redisStream,
        new LoggerService(),
      );
      await compilerService.activate('test-app', '1');
    });
  });
});
