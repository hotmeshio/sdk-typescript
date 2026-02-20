import * as dotenv from 'dotenv';
import development from './development';
import test from './test';
import staging from './staging';
import production from './production';

dotenv.config();

const env = process.env.NODE_ENV || 'development';

const baseConfig = {
  // Redis Configuration
  REDIS_DATABASE: 0,
  REDIS_PORT: 6379,
  REDIS_HOST: 'redis',
  REDIS_PASSWORD: 'key_admin',

  // PostgreSQL Configuration to connect to local docker-compose postgres instance
  //  (override with .env if targeting a remote postgres instance)
  POSTGRES_PORT: process.env.POSTGRES_PORT || 5432,
  POSTGRES_HOST: process.env.POSTGRES_HOST || 'postgres',
  POSTGRES_USER: process.env.POSTGRES_USER || 'postgres',
  POSTGRES_PASSWORD: process.env.POSTGRES_PASSWORD || 'password',
  POSTGRES_DB: process.env.POSTGRES_DB || 'hotmesh',
  POSTGRES_SSL:
    process.env.POSTGRES_IS_REMOTE === 'true'
      ? {
          rejectUnauthorized: false,
        }
      : undefined,

  // NATS Configuration
  NATS_SERVERS: ['nats:4222'],
};

const envConfig = { development, test, staging, production };

export default { ...baseConfig, ...envConfig[env] };
