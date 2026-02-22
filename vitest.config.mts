import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    exclude: ['node_modules', 'build', 'config'],
    testTimeout: 60_000,
    hookTimeout: 120_000,
    fileParallelism: false,
    pool: 'forks',
    env: {
      NODE_ENV: 'test',
    },
  },
});
