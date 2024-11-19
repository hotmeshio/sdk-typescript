const env = process.env.NODE_ENV || 'development';

const baseConfig = {
  // Redis Configuration
  REDIS_DATABASE: 0,
  REDIS_PORT: 6379,
  REDIS_HOST: 'redis',
  REDIS_PASSWORD: 'key_admin',
  
  // PostgreSQL Configuration
  POSTGRES_PORT: 5432,
  POSTGRES_HOST: 'postgres',
  POSTGRES_USER: 'postgres',
  POSTGRES_PASSWORD: 'password',
  POSTGRES_DB: 'hotmesh',
  
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
