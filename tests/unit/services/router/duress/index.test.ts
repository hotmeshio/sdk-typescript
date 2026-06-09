import { describe, it, expect, beforeEach } from 'vitest';
import { DuressManager } from '../../../../../services/router/duress';
import { StreamDataType } from '../../../../../types/stream';

describe('DuressManager', () => {
  let manager: DuressManager;

  beforeEach(() => {
    manager = new DuressManager();
  });

  describe('EMA computation', () => {
    it('seeds EMA with first sample', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 100);
      const snapshot = manager.evaluate();
      expect(snapshot.per_type[StreamDataType.TRANSITION]).toBe(100);
    });

    it('converges EMA with alpha=0.3', () => {
      // Seed with 100ms
      manager.recordLatency(StreamDataType.TRANSITION, 100);
      // Next sample: EMA = 0.3*200 + 0.7*100 = 60 + 70 = 130
      manager.recordLatency(StreamDataType.TRANSITION, 200);
      const snapshot = manager.evaluate();
      expect(snapshot.per_type[StreamDataType.TRANSITION]).toBe(130);
    });

    it('tracks multiple message types independently', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 50);
      manager.recordLatency(StreamDataType.TIMEHOOK, 300);
      const snapshot = manager.evaluate();
      expect(snapshot.per_type[StreamDataType.TRANSITION]).toBe(50);
      expect(snapshot.per_type[StreamDataType.TIMEHOOK]).toBe(300);
    });
  });

  describe('level detection', () => {
    it('reports healthy when all EMAs < 200ms', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 50);
      manager.recordLatency(StreamDataType.TIMEHOOK, 100);
      const snapshot = manager.evaluate();
      expect(snapshot.level).toBe('healthy');
      expect(snapshot.throttle_ms).toBe(0);
    });

    it('reports mild when max EMA is 200-1000ms', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 500);
      const snapshot = manager.evaluate();
      expect(snapshot.level).toBe('mild');
      expect(snapshot.throttle_ms).toBeGreaterThanOrEqual(100);
      expect(snapshot.throttle_ms).toBeLessThanOrEqual(500);
    });

    it('reports moderate when max EMA is 1000-5000ms', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 3000);
      const snapshot = manager.evaluate();
      expect(snapshot.level).toBe('moderate');
      expect(snapshot.throttle_ms).toBeGreaterThanOrEqual(500);
      expect(snapshot.throttle_ms).toBeLessThanOrEqual(2000);
    });

    it('reports severe when max EMA > 5000ms', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 8000);
      const snapshot = manager.evaluate();
      expect(snapshot.level).toBe('severe');
      expect(snapshot.throttle_ms).toBeGreaterThanOrEqual(2000);
      expect(snapshot.throttle_ms).toBeLessThanOrEqual(5000);
    });

    it('uses max across types (one bad type = duress)', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 10); // healthy
      manager.recordLatency(StreamDataType.TIMEHOOK, 3000); // moderate
      const snapshot = manager.evaluate();
      expect(snapshot.level).toBe('moderate');
      // Score is the max EMA
      expect(snapshot.score_ms).toBe(3000);
    });
  });

  describe('hysteresis', () => {
    it('does not drop level after single improving evaluation', () => {
      // Drive to severe
      manager.recordLatency(StreamDataType.TRANSITION, 8000);
      manager.evaluate();
      expect(manager.getCurrentLevel()).toBe('severe');

      // Single healthy sample — level should hold at severe
      manager.recordLatency(StreamDataType.TRANSITION, 10);
      // EMA: 0.3*10 + 0.7*8000 = 5603 → still severe
      manager.evaluate();
      expect(manager.getCurrentLevel()).toBe('severe');
    });

    it('drops level after HYSTERESIS_COUNT consecutive improvements', () => {
      // Drive to mild
      manager.recordLatency(StreamDataType.TRANSITION, 500);
      manager.evaluate();
      expect(manager.getCurrentLevel()).toBe('mild');

      // Feed enough healthy samples to decay EMA below 200ms
      // and trigger 3 consecutive evaluations below threshold
      for (let i = 0; i < 20; i++) {
        manager.recordLatency(StreamDataType.TRANSITION, 10);
      }

      // Evaluate 3+ times to clear hysteresis
      manager.evaluate();
      manager.evaluate();
      manager.evaluate();
      expect(manager.getCurrentLevel()).toBe('healthy');
    });

    it('escalates immediately without hysteresis', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 50);
      manager.evaluate();
      expect(manager.getCurrentLevel()).toBe('healthy');

      // Sudden spike to severe
      manager.recordLatency(StreamDataType.TRANSITION, 10000);
      manager.evaluate();
      // EMA: 0.3*10000 + 0.7*50 = 3035 → moderate (not severe yet, since EMA smooths)
      // But the level should be at least moderate — escalation is immediate
      expect(['moderate', 'severe']).toContain(manager.getCurrentLevel());
    });
  });

  describe('remote duress', () => {
    it('adopts remote signal when worse than local', () => {
      // Local is healthy
      manager.recordLatency(StreamDataType.TRANSITION, 50);
      manager.evaluate();
      expect(manager.getCurrentLevel()).toBe('healthy');

      // Remote says severe
      manager.applyRemoteDuress(3000, 'severe');
      expect(manager.getCurrentLevel()).toBe('severe');
      expect(manager.getDuressThrottle()).toBe(3000);
    });

    it('ignores remote signal when local is worse', () => {
      // Drive to severe locally
      manager.recordLatency(StreamDataType.TRANSITION, 8000);
      manager.evaluate();

      const throttleBefore = manager.getDuressThrottle();
      // Remote says mild
      manager.applyRemoteDuress(200, 'mild');
      expect(manager.getCurrentLevel()).toBe('severe');
      expect(manager.getDuressThrottle()).toBe(throttleBefore);
    });
  });

  describe('throttle interpolation', () => {
    it('returns 0 for healthy', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 50);
      const snapshot = manager.evaluate();
      expect(snapshot.throttle_ms).toBe(0);
    });

    it('interpolates within mild band', () => {
      // At the exact mild floor (200ms), throttle should be at mild min (100)
      manager.recordLatency(StreamDataType.TRANSITION, 200);
      const snapshot = manager.evaluate();
      expect(snapshot.throttle_ms).toBe(100);
    });

    it('interpolates within severe band', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 10000);
      const snapshot = manager.evaluate();
      // Should be clamped to severe max
      expect(snapshot.throttle_ms).toBeLessThanOrEqual(5000);
      expect(snapshot.throttle_ms).toBeGreaterThanOrEqual(2000);
    });
  });

  describe('broadcast gating', () => {
    it('allows broadcast when level changes', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 500);
      manager.evaluate();
      expect(manager.shouldBroadcast()).toBe(true);
    });

    it('blocks broadcast within rate limit interval', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 500);
      manager.evaluate();
      manager.markBroadcast();
      // Immediately after marking, should not broadcast again
      expect(manager.shouldBroadcast()).toBe(false);
    });

    it('does not broadcast when healthy and unchanged', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 50);
      manager.evaluate();
      // healthy → healthy, no broadcast needed
      expect(manager.shouldBroadcast()).toBe(false);
    });
  });

  describe('getSnapshot', () => {
    it('returns current state without side effects', () => {
      manager.recordLatency(StreamDataType.TRANSITION, 500);
      manager.evaluate();

      const snap1 = manager.getSnapshot();
      const snap2 = manager.getSnapshot();
      expect(snap1).toEqual(snap2);
      expect(snap1.level).toBe('mild');
      expect(snap1.per_type[StreamDataType.TRANSITION]).toBe(500);
    });
  });
});
