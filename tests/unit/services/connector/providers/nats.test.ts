// /app/tests/unit/services/connector/providers/nats.test.ts
import { connect } from 'nats';
import config from '../../../../$setup/config';
import { NatsConnection } from '../../../../../services/connector/providers/nats';
import { NatsClientType } from '../../../../../types/nats';

describe('NatsConnection', () => {
  let natsConnection: NatsConnection;
  let natsClient: NatsClientType;

  beforeAll(async () => {
    natsConnection = await NatsConnection.connect(
      'test-connection-1',
      connect,
      {
        servers: config.NATS_SERVERS,
      },
    );
    natsClient = natsConnection.getClient();
  });

  afterAll(async () => {
    await NatsConnection.disconnectAll();
  });

  it('should connect to NATS successfully', async () => {
    const status = await natsClient.status();
    expect(status).toBeDefined();
  });

  it('should throw error when getting client without connection', async () => {
    const newConnection = new NatsConnection();
    expect(() => newConnection.getClient()).toThrow(
      'nats-provider-connection-failed'
    );
  });

  it('should disconnect successfully', async () => {
    const tempConnection = await NatsConnection.connect(
      'test-connection-2',
      connect,
      {
        servers: config.NATS_SERVERS,
      },
    );
    await tempConnection.disconnect();
    expect(() => tempConnection.getClient()).toThrow(
      'nats-provider-connection-failed'
    );
  });
});
