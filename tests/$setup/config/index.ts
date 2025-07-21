import * as dotenv from 'dotenv';
dotenv.config();

const env = process.env.NODE_ENV || 'development';

const baseConfig = {
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

const envConfig = {
  development: require('./development').default,
  test: require('./test').default,
  staging: require('./staging').default,
  production: require('./production').default,
};

export default { ...baseConfig, ...envConfig[env] };
