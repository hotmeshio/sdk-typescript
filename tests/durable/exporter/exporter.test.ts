import { describe, it, expect } from 'vitest';

import {
  parseTimestamp,
  computeDuration,
  extractOperation,
  extractActivityName,
  isSystemActivity,
  mapStatus,
} from '../../../services/durable/exporter';

// ── parseTimestamp ───────────────────────────────────────────────────────────

describe('parseTimestamp', () => {
  it('should parse HotMesh compact format with dot separator', () => {
    expect(parseTimestamp('20240115103045.123')).toBe('2024-01-15T10:30:45.123Z');
  });

  it('should parse HotMesh compact format without dot separator', () => {
    expect(parseTimestamp('20240115103045123')).toBe('2024-01-15T10:30:45.123Z');
  });

  it('should parse compact format without milliseconds', () => {
    expect(parseTimestamp('20240115103045')).toBe('2024-01-15T10:30:45.000Z');
  });

  it('should pad short milliseconds to 3 digits', () => {
    expect(parseTimestamp('20240115103045.1')).toBe('2024-01-15T10:30:45.100Z');
  });

  it('should truncate long milliseconds to 3 digits', () => {
    expect(parseTimestamp('20240115103045.12345')).toBe('2024-01-15T10:30:45.123Z');
  });

  it('should pass through ISO 8601 strings', () => {
    const iso = '2024-01-15T10:30:45.000Z';
    expect(parseTimestamp(iso)).toBe(iso);
  });

  it('should return null for null/undefined/empty', () => {
    expect(parseTimestamp(null)).toBeNull();
    expect(parseTimestamp(undefined)).toBeNull();
    expect(parseTimestamp('')).toBeNull();
  });

  it('should return null for non-string input', () => {
    expect(parseTimestamp(12345 as any)).toBeNull();
  });

  it('should return null for garbage strings', () => {
    expect(parseTimestamp('not-a-date')).toBeNull();
    expect(parseTimestamp('abc')).toBeNull();
  });
});

// ── computeDuration ─────────────────────────────────────────────────────────

describe('computeDuration', () => {
  it('should compute duration between two HotMesh timestamps', () => {
    const ac = '20240115103045.000';
    const au = '20240115103047.500';
    expect(computeDuration(ac, au)).toBe(2500);
  });

  it('should return 0 for identical timestamps', () => {
    const ts = '20240115103045.000';
    expect(computeDuration(ts, ts)).toBe(0);
  });

  it('should return null if either timestamp is missing', () => {
    expect(computeDuration(undefined, '20240115103045.000')).toBeNull();
    expect(computeDuration('20240115103045.000', undefined)).toBeNull();
    expect(computeDuration(undefined, undefined)).toBeNull();
  });

  it('should return null for unparseable timestamps', () => {
    expect(computeDuration('garbage', '20240115103045.000')).toBeNull();
  });
});

// ── extractOperation ────────────────────────────────────────────────────────

describe('extractOperation', () => {
  it('should extract "proxy" from a proxy timeline key', () => {
    expect(extractOperation('-proxy,0,0-1-')).toBe('proxy');
  });

  it('should extract "proxy" from a key without dimension', () => {
    expect(extractOperation('-proxy-5-')).toBe('proxy');
  });

  it('should extract "child" from a child timeline key', () => {
    expect(extractOperation('-child-0-')).toBe('child');
  });

  it('should extract "start" from a start timeline key', () => {
    expect(extractOperation('-start-0-')).toBe('start');
  });

  it('should extract "wait" from a wait timeline key', () => {
    expect(extractOperation('-wait-0-')).toBe('wait');
  });

  it('should extract "sleep" from a sleep timeline key', () => {
    expect(extractOperation('-sleep-0-3-')).toBe('sleep');
  });

  it('should extract "sleep" from a key with dimension', () => {
    expect(extractOperation('-sleep,0,0-1-')).toBe('sleep');
  });

  it('should return "unknown" for empty key', () => {
    expect(extractOperation('')).toBe('unknown');
  });
});

// ── extractActivityName ─────────────────────────────────────────────────────

describe('extractActivityName', () => {
  it('should extract activity name from job_id without dimension', () => {
    expect(extractActivityName({ job_id: '-wfId-$analyzeContent-5' })).toBe('analyzeContent');
  });

  it('should extract activity name from job_id with dimension', () => {
    expect(extractActivityName({ job_id: '-wfId-$processOrder,0,0-3' })).toBe('processOrder');
  });

  it('should handle complex workflow IDs', () => {
    expect(extractActivityName({ job_id: '-my-complex-wf-$greet-1' })).toBe('greet');
  });

  it('should return "unknown" for null value', () => {
    expect(extractActivityName(null)).toBe('unknown');
  });

  it('should return "unknown" for missing job_id', () => {
    expect(extractActivityName({ data: 'foo' })).toBe('unknown');
  });

  it('should return "unknown" for non-string job_id', () => {
    expect(extractActivityName({ job_id: 123 })).toBe('unknown');
  });

  it('should return the job_id when no $ separator exists', () => {
    expect(extractActivityName({ job_id: 'simple-id' })).toBe('simple-id');
  });
});

// ── isSystemActivity ────────────────────────────────────────────────────────

describe('isSystemActivity', () => {
  it('should return true for names starting with "lt"', () => {
    expect(isSystemActivity('ltGetWorkflowConfig')).toBe(true);
    expect(isSystemActivity('ltResolve')).toBe(true);
  });

  it('should return false for user activity names', () => {
    expect(isSystemActivity('greet')).toBe(false);
    expect(isSystemActivity('processOrder')).toBe(false);
    expect(isSystemActivity('analyzeContent')).toBe(false);
  });

  it('should return false for empty string', () => {
    expect(isSystemActivity('')).toBe(false);
  });
});

// ── mapStatus ───────────────────────────────────────────────────────────────

describe('mapStatus', () => {
  it('should return "completed" when done flag is true, even with positive status', () => {
    expect(mapStatus(5, true)).toBe('completed');
    expect(mapStatus(1, true, false)).toBe('completed');
  });

  it('should return "completed" for status 0', () => {
    expect(mapStatus(0)).toBe('completed');
    expect(mapStatus(0, false)).toBe('completed');
  });

  it('should return "running" for positive status without done flag', () => {
    expect(mapStatus(1)).toBe('running');
    expect(mapStatus(100, false)).toBe('running');
  });

  it('should return "failed" for negative status', () => {
    expect(mapStatus(-1)).toBe('failed');
    expect(mapStatus(-100, true)).toBe('failed');
  });

  it('should return "failed" when hasError is true', () => {
    expect(mapStatus(0, true, true)).toBe('failed');
    expect(mapStatus(1, false, true)).toBe('failed');
  });

  it('should return "running" for undefined status', () => {
    expect(mapStatus(undefined)).toBe('running');
    expect(mapStatus(undefined, false)).toBe('running');
  });

  it('should return "running" for NaN status', () => {
    expect(mapStatus(NaN)).toBe('running');
  });
});
