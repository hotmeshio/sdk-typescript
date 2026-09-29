import { describe, it, expect, vi } from 'vitest';

import {
  CollationError,
  DuplicateJobError,
  ErrorCategory,
  HotMeshConnectionError,
  LeaseExpiredError,
  classifyError,
  isConnectionError,
} from '../../../modules/errors';
import { detach, sleepFor } from '../../../modules/utils';

const withCode = (message: string, code: string) =>
  Object.assign(new Error(message), { code });

describe('connection errors', () => {
  describe('isConnectionError', () => {
    it.each([
      ['a HotMeshConnectionError', new HotMeshConnectionError('postgres-client-reconnecting')],
      ['pg "Connection terminated unexpectedly"', new Error('Connection terminated unexpectedly')],
      ['pg "Connection terminated"', new Error('Connection terminated')],
      ['pg "not queryable" after a socket error', new Error('Client has encountered a connection error and is not queryable')],
      ['pg "not queryable" after end()', new Error('Client was closed and is not queryable')],
      ['pg connect timeout', new Error('timeout expired')],
      ['pg connection timeout during connect', new Error('Connection terminated due to connection timeout')],
      ['ECONNRESET', withCode('read ECONNRESET', 'ECONNRESET')],
      ['ECONNREFUSED', withCode('connect ECONNREFUSED 127.0.0.1:5432', 'ECONNREFUSED')],
      ['ETIMEDOUT', withCode('connect ETIMEDOUT', 'ETIMEDOUT')],
      ['EPIPE', withCode('write EPIPE', 'EPIPE')],
      ['EHOSTUNREACH', withCode('connect EHOSTUNREACH', 'EHOSTUNREACH')],
      ['ENOTFOUND', withCode('getaddrinfo ENOTFOUND db', 'ENOTFOUND')],
      ['EAI_AGAIN', withCode('getaddrinfo EAI_AGAIN db', 'EAI_AGAIN')],
      ['SQLSTATE 08006', withCode('connection failure', '08006')],
      ['SQLSTATE 08001', withCode('unable to connect', '08001')],
      ['SQLSTATE 57P01 (admin shutdown / restart)', withCode('terminating connection due to administrator command', '57P01')],
      ['SQLSTATE 57P02 (crash shutdown)', withCode('crash shutdown', '57P02')],
      ['SQLSTATE 57P03 (starting up)', withCode('the database system is starting up', '57P03')],
      ['a wrapped native connection error', Object.assign(new Error('outer'), { cause: withCode('x', 'ECONNRESET') })],
    ])('is true for %s', (_label, error) => {
      expect(isConnectionError(error)).toBe(true);
    });

    it.each([
      ['a unique violation (23505)', withCode('duplicate key value', '23505')],
      ['a serialization failure (40001)', withCode('could not serialize access', '40001')],
      ['an undefined table (42P01)', withCode('relation "x" does not exist', '42P01')],
      ['a plain error', new Error('boom')],
      ['a DuplicateJobError', new DuplicateJobError('job-1')],
      ['null', null],
      ['undefined', undefined],
      ['a string', 'Connection terminated'],
    ])('is false for %s', (_label, error) => {
      expect(isConnectionError(error)).toBe(false);
    });

    it('stops walking a cyclic cause chain', () => {
      const a: any = new Error('a');
      const b: any = new Error('b');
      a.cause = b;
      b.cause = a;
      expect(isConnectionError(a)).toBe(false);
    });
  });

  describe('HotMeshConnectionError', () => {
    it('carries a stable code, the connection id and the native cause', () => {
      const cause = new Error('Connection terminated unexpectedly');
      const error = new HotMeshConnectionError('postgres-session-lost', 'conn-1', cause);
      expect(error).toBeInstanceOf(Error);
      expect(error.name).toBe('HotMeshConnectionError');
      expect(error.code).toBe('HMSH_PG_UNAVAILABLE');
      expect(error.connectionId).toBe('conn-1');
      expect(error.cause).toBe(cause);
    });

    it('never uses the words the shutdown paths match (closed, queryable)', () => {
      for (const message of [
        'postgres-client-reconnecting',
        'postgres-client-idle',
        'postgres-session-lost',
        'postgres-transaction-lost',
      ]) {
        const error = new HotMeshConnectionError(message);
        expect(error.message.includes('closed')).toBe(false);
        expect(error.message.includes('queryable')).toBe(false);
      }
    });
  });

  describe('classifyError', () => {
    it('classifies a connection outage as UNAVAILABLE', () => {
      expect(classifyError(new HotMeshConnectionError('postgres-client-reconnecting'))).toBe(
        ErrorCategory.UNAVAILABLE,
      );
    });

    it('keeps the existing categories', () => {
      expect(classifyError(new LeaseExpiredError(1000, 1))).toBe(ErrorCategory.FATAL);
      expect(classifyError(new DuplicateJobError('j'))).toBe(ErrorCategory.COLLATION);
      expect(
        classifyError(new CollationError(0, 1 as any, 'enter' as any)),
      ).toBe(ErrorCategory.COLLATION);
      expect(classifyError(new Error('anything else'))).toBe(ErrorCategory.RETRYABLE);
    });
  });

  describe('detach', () => {
    it('logs a rejection instead of leaving it unhandled', async () => {
      const unhandled = vi.fn();
      process.on('unhandledRejection', unhandled);
      try {
        const logger = { warn: vi.fn() };
        detach(Promise.reject(new Error('lost')), logger, 'label-x', { id: 1 });
        await sleepFor(20);
        expect(logger.warn).toHaveBeenCalledTimes(1);
        expect(logger.warn.mock.calls[0][0]).toBe('label-x');
        expect(logger.warn.mock.calls[0][1].id).toBe(1);
        expect(logger.warn.mock.calls[0][1].error.message).toBe('lost');
        expect(unhandled).not.toHaveBeenCalled();
      } finally {
        process.off('unhandledRejection', unhandled);
      }
    });

    it('ignores fulfilled promises and non-promise values', async () => {
      const logger = { warn: vi.fn() };
      detach(Promise.resolve(1), logger, 'ok');
      detach(undefined, logger, 'undefined');
      detach(42, logger, 'number');
      await sleepFor(10);
      expect(logger.warn).not.toHaveBeenCalled();
    });

    it('survives a missing or faulty logger', async () => {
      const unhandled = vi.fn();
      process.on('unhandledRejection', unhandled);
      try {
        detach(Promise.reject(new Error('a')), undefined, 'no-logger');
        detach(
          Promise.reject(new Error('b')),
          {
            warn: () => {
              throw new Error('logger down');
            },
          },
          'faulty-logger',
        );
        await sleepFor(20);
        expect(unhandled).not.toHaveBeenCalled();
      } finally {
        process.off('unhandledRejection', unhandled);
      }
    });
  });
});
